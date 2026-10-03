using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using BeastieBot3.Iucn.Doi;
using BeastieBot3.Web.Flows;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// The count behind the public site workflow's DOI light, over small synthetic databases: which
// latest global assessments have no DOI from another source (counted the way `iucn resolve-dois`
// counts them), how many of those the DOI cache has a result for, and when the slow part of the
// count is made again.
public sealed class SiteDoiCountTests : IDisposable {
    private static readonly DateTime Now = new(2026, 10, 3, 4, 0, 0, DateTimeKind.Utc);
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-site-dois-" + Guid.NewGuid().ToString("N"));

    public SiteDoiCountTests() {
        Directory.CreateDirectory(_dir);
        SiteDoiCountReader.Invalidate();
    }

    public void Dispose() {
        SiteDoiCountReader.Invalidate();
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private string P(string name) => Path.Combine(_dir, name);

    private PublicSitePaths Paths() => new() {
        IucnDatabase = P("IUCN_2026-1.sqlite"),
        ApiCache = P("api.sqlite"),
        GbifDir = P("gbif"),
        DoiCache = P("doi.sqlite"),
        WikidataCache = P("wikidata.sqlite"),
    };

    // ---- when the slow part is counted again ----

    private static SiteDoiCountReader.TargetCount Last(string fast = "f", string slow = "s", DateTime? at = null, string? error = null) =>
        new(fast, slow, at ?? Now, "2026-1", 3, Array.Empty<DoiTarget>(), error);

    [Fact]
    public void Recount_when_nothing_is_counted_yet_or_the_iucn_database_or_gbif_checklist_changed() {
        Assert.True(SiteDoiCountReader.ShouldRecount(null, "f", "s", Now, TimeSpan.FromMinutes(30)));
        Assert.True(SiteDoiCountReader.ShouldRecount(Last(), "f2", "s", Now.AddSeconds(5), TimeSpan.FromMinutes(30)));
        Assert.False(SiteDoiCountReader.ShouldRecount(Last(), "f", "s", Now.AddDays(3), TimeSpan.FromMinutes(30)));
    }

    // The API cache and the Wikidata cache change for hours during a refresh or `wikipedia update`.
    [Fact]
    public void Recount_after_an_api_or_wikidata_cache_change_waits_for_the_interval() {
        Assert.False(SiteDoiCountReader.ShouldRecount(Last(), "f", "s2", Now.AddMinutes(29), TimeSpan.FromMinutes(30)));
        Assert.True(SiteDoiCountReader.ShouldRecount(Last(), "f", "s2", Now.AddMinutes(30), TimeSpan.FromMinutes(30)));
    }

    [Fact]
    public void Recount_after_a_failure_waits_for_the_interval_unless_the_iucn_database_changed() {
        var failed = Last(error: "database is locked");
        Assert.False(SiteDoiCountReader.ShouldRecount(failed, "f", "s", Now.AddMinutes(5), TimeSpan.FromMinutes(30)));
        Assert.True(SiteDoiCountReader.ShouldRecount(failed, "f", "s", Now.AddMinutes(30), TimeSpan.FromMinutes(30)));
        Assert.True(SiteDoiCountReader.ShouldRecount(failed, "f2", "s", Now.AddMinutes(1), TimeSpan.FromMinutes(30)));
    }

    [Fact]
    public void Keys_split_the_inputs_by_how_often_they_change() {
        var state = State(api: Now.AddDays(-1));
        var (fast, slow) = SiteDoiCountReader.Keys(Paths(), state);

        var (fastAfterApi, slowAfterApi) = SiteDoiCountReader.Keys(Paths(), State(api: Now));
        Assert.Equal(fast, fastAfterApi);
        Assert.NotEqual(slow, slowAfterApi);

        var (fastAfterGbif, slowAfterGbif) = SiteDoiCountReader.Keys(Paths(), state with { GbifZipName = "iucn-checklist-2026-11-01.zip" });
        Assert.NotEqual(fast, fastAfterGbif);
        Assert.Equal(slow, slowAfterGbif);
    }

    // ---- the count ----

    [Fact]
    public void Counts_targets_the_way_resolve_dois_does_and_follows_the_doi_cache() {
        BuildSources();
        var targets = SiteDoiCountReader.CountTargets(Paths(), "f", "s", Now);
        Assert.Null(targets.Error);
        Assert.Equal("2026-1", targets.Release);
        Assert.Equal(3, targets.InScope);
        // 201's citation has its DOI; 202's has none; 203 has no cached payload.
        Assert.Equal(new long[] { 202, 203 }, targets.Targets.Select(t => t.AssessmentId).ToArray());

        // No DOI cache yet: nothing is checked.
        var none = SiteDoiCountReader.CountChecks(targets, P("doi.sqlite"), Now)!;
        Assert.Equal(2, none.WithoutSourceDoi);
        Assert.Equal(2, none.NotChecked);

        // A result for 202, and one for an assessment outside the scope, which is not counted.
        BuildDoiCache(
            (202, 22, "10.2305/IUCN.UK.2020-1.RLTS.T22A202.en", 0),
            (999, 99, null, 0));
        var some = SiteDoiCountReader.CountChecks(targets, P("doi.sqlite"), Now)!;
        Assert.Equal(1, some.Found);
        Assert.Equal(0, some.NotFound);
        Assert.Equal(1, some.NotChecked);

        AddCheck(203, 23, null, 3);
        var all = SiteDoiCountReader.CountChecks(targets, P("doi.sqlite"), Now)!;
        Assert.Equal(1, all.Found);
        Assert.Equal(1, all.NotFound);
        Assert.Equal(0, all.NotChecked);
    }

    [Fact]
    public void A_doi_cache_with_no_doi_check_table_has_checked_nothing() {
        BuildSources();
        Exec("doi.sqlite", "CREATE TABLE crossref_listings (id INTEGER PRIMARY KEY)");
        var targets = SiteDoiCountReader.CountTargets(Paths(), "f", "s", Now);
        Assert.Equal(2, SiteDoiCountReader.CountChecks(targets, P("doi.sqlite"), Now)!.NotChecked);
    }

    [Fact]
    public void A_missing_database_is_a_failed_count_not_an_empty_one() {
        var targets = SiteDoiCountReader.CountTargets(Paths(), "f", "s", Now);
        Assert.NotNull(targets.Error);
        Assert.Empty(targets.Targets);
    }

    // The poll never waits: the first read starts the count and says nothing; a later read has it.
    [Fact]
    public void Read_returns_nothing_until_the_background_count_finishes() {
        BuildSources();
        BuildDoiCache((202, 22, "10.2305/IUCN.UK.2020-1.RLTS.T22A202.en", 0));
        var state = State(api: Now);

        SiteDoiCount? count = SiteDoiCountReader.Read(Paths(), state);
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (count is null && DateTime.UtcNow < deadline) {
            Thread.Sleep(50);
            count = SiteDoiCountReader.Read(Paths(), state);
        }
        Assert.NotNull(count);
        Assert.Equal(1, count!.NotChecked);

        // Another IUCN import: the old count is about another database, so it is not shown.
        Assert.Null(SiteDoiCountReader.Read(Paths(), State(api: Now, iucn: Now)));
    }

    // ---- fixtures ----

    private PublicSiteState State(DateTime api, DateTime? iucn = null) => new() {
        GbifZipName = null,
        DoiCachePath = P("doi.sqlite"),
        Inputs = new[] {
            new SiteInputChange(PublicSiteStateReader.IucnInput, iucn ?? Now.AddDays(-50)),
            new SiteInputChange(PublicSiteStateReader.ApiCacheInput, api),
        },
    };

    private void BuildSources() {
        Exec("IUCN_2026-1.sqlite", """
            CREATE TABLE import_metadata (redlist_version TEXT);
            INSERT INTO import_metadata VALUES ('2026-1');
            CREATE TABLE assessments_html (assessmentId INTEGER, taxonId INTEGER, yearPublished TEXT, language TEXT, scopes TEXT);
            CREATE TABLE taxonomy_html (taxonId INTEGER, infraType TEXT, subpopulationName TEXT);
            INSERT INTO assessments_html VALUES
                (201, 21, '2019', 'English', 'Global'),
                (202, 22, '2020', 'English', 'Global'),
                (203, 23, '2021', 'English', 'Global'),
                (204, 24, '2021', 'English', 'Europe');
            INSERT INTO taxonomy_html VALUES (21, NULL, NULL), (22, NULL, NULL), (23, NULL, NULL), (24, NULL, NULL);
            """);
        Exec("api.sqlite", """
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, root_sis_id INTEGER, json TEXT);
            CREATE TABLE assessments (id INTEGER PRIMARY KEY, assessment_id INTEGER, json TEXT);
            """);
        using var conn = Open("api.sqlite");
        foreach (var (aid, tid, year) in new[] { (201L, 21L, "2019"), (202L, 22L, "2020") }) {
            Insert(conn, "INSERT INTO taxa (root_sis_id, json) VALUES (@a, @b)", tid,
                $$"""{"assessments":[{"assessment_id":{{aid}},"sis_taxon_id":{{tid}},"latest":true,"year_published":"{{year}}"}]}""");
        }
        Insert(conn, "INSERT INTO assessments (assessment_id, json) VALUES (@a, @b)", 201,
            Payload(201, 21, "2019", "Xus yus", "10.2305/IUCN.UK.2019-2.RLTS.T21A201.en"));
        Insert(conn, "INSERT INTO assessments (assessment_id, json) VALUES (@a, @b)", 202,
            Payload(202, 22, "2020", "Xus vus", null));
    }

    private static string Payload(long aid, long tid, string year, string name, string? doi) {
        var citation = $"Smith, J. {year}. {name}. The IUCN Red List of Threatened Species {year}: e.T{tid}A{aid}."
            + (doi is null ? "" : $" https://dx.doi.org/{doi}.") + " Accessed on 20 August 2026.";
        return $$"""
            {"assessment_id":{{aid}},"sis_taxon_id":{{tid}},"year_published":"{{year}}","latest":true,
             "citation":{{JsonSerializer.Serialize(citation)}},
             "taxon":{"sis_id":{{tid}},"scientific_name":{{JsonSerializer.Serialize(name)}},"subpopulation_name":null},
             "credits":[{"credit_type_name":"assessor","full":"Smith, J.","value":["v1"]}],"errata":[],
             "scopes":[{"description":{"en":"Global"},"code":"1"}]}
            """;
    }

    private void BuildDoiCache(params (long Aid, long Tid, string? Doi, int Tried)[] rows) {
        Exec("doi.sqlite", "CREATE TABLE doi_check (assessment_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL, doi TEXT, checked_at TEXT NOT NULL, candidates_tried INTEGER NOT NULL)");
        foreach (var row in rows) AddCheck(row.Aid, row.Tid, row.Doi, row.Tried);
    }

    private void AddCheck(long aid, long tid, string? doi, int tried) {
        using var conn = Open("doi.sqlite");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO doi_check VALUES (@a, @t, @d, '2026-10-02T00:00:00.0000000Z', @n)";
        cmd.Parameters.AddWithValue("@a", aid);
        cmd.Parameters.AddWithValue("@t", tid);
        cmd.Parameters.AddWithValue("@d", (object?)doi ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@n", tried);
        cmd.ExecuteNonQuery();
    }

    private SqliteConnection Open(string name) {
        var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = P(name), Pooling = false }.ConnectionString);
        conn.Open();
        return conn;
    }

    private void Exec(string name, string sql) {
        using var conn = Open(name);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void Insert(SqliteConnection conn, string sql, long a, string b) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@a", a);
        cmd.Parameters.AddWithValue("@b", b);
        cmd.ExecuteNonQuery();
    }
}
