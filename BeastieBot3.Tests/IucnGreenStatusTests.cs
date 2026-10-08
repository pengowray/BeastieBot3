using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Flows;
using BeastieBot3.Web.Status;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// `iucn api green-status`: reading /api/v4/green_status/all, storing one download in the API
// cache's green_status table (the site build reads it), and the public site workflow's light.
// The rules that matter: a row keeps when it was first stored, the Red List version of that time
// and its baseline flag; only the first download is the baseline; rows a download does not have
// are deleted; and an answer that cannot be used changes nothing.
public class IucnGreenStatusTests {
    // ---- the answer ----

    private static string Record(long sisId, string date, string category = "Largely Depleted", string? url = "default",
        string name = "Lynx pardinus") {
        var urlJson = url switch {
            null => "null",
            "default" => $"\"https://www.iucnredlist.org/species/{sisId}/{sisId * 10 + 1}\"",
            _ => $"\"{url}\"",
        };
        return $$"""
            {"assessment_year":"{{date[..4]}}","assessment_date":"{{date}}","species_recovery_category":"{{category}}",
             "species_recovery_score_best":"22%","justification":"<p>Text</p>","assessor_names":"Salcedo, J.",
             "url":{{urlJson}},"taxon":{"sis_id":{{sisId}},"scientific_name":"{{name}}"} }
            """;
    }

    private static string Answer(params string[] records) => "{\"assessments\":[" + string.Join(",", records) + "]}";

    [Fact]
    public void Parse_reads_each_record_with_its_key_page_id_and_raw_json() {
        var a = IucnGreenStatusParser.Parse(Answer(
            Record(12520, "2023-10-31"),
            Record(3989, "2021-05-01", "Critically Depleted", url: null, name: "Ateles geoffroyi")));

        Assert.True(a.Usable);
        Assert.Equal(2, a.Records.Count);
        var lynx = a.Records[0];
        Assert.Equal(12520, lynx.SisId);
        Assert.Equal("2023-10-31", lynx.AssessmentDate);
        Assert.Equal(125201, lynx.RedListAssessmentId);
        Assert.Equal("Lynx pardinus", lynx.ScientificName);
        Assert.Equal("Largely Depleted", lynx.SpeciesRecoveryCategory);
        Assert.Contains("\"justification\":\"<p>Text</p>\"", lynx.Json);
        Assert.StartsWith("{\"assessment_year\"", lynx.Json);

        Assert.Null(a.Records[1].RedListAssessmentId);
        Assert.Equal("Critically Depleted", a.Records[1].SpeciesRecoveryCategory);
    }

    [Theory]
    [InlineData("https://www.iucnredlist.org/species/12520/218695618", 218695618L)]
    [InlineData("https://www.iucnredlist.org/species/12520/218695618/", 218695618L)]
    [InlineData("https://www.iucnredlist.org/species/12520/218695618?x=1", 218695618L)]
    [InlineData("https://www.iucnredlist.org/species/12520/", 12520L)]
    [InlineData("https://www.iucnredlist.org/species/lynx", null)]
    [InlineData("", null)]
    public void AssessmentIdFromUrl_takes_the_last_number(string url, long? expected) =>
        Assert.Equal(expected, IucnGreenStatusParser.AssessmentIdFromUrl(url));

    [Theory]
    [InlineData("2023-10-31", "2023-10-31")]
    [InlineData("2023-10-31T00:00:00Z", "2023-10-31")]
    [InlineData("2023-13-31", null)]
    [InlineData("2023", null)]
    [InlineData(null, null)]
    public void DateOf_keeps_a_valid_day(string? text, string? expected) =>
        Assert.Equal(expected, IucnGreenStatusParser.DateOf(text));

    [Fact]
    public void Parse_refuses_an_answer_that_cannot_be_used() {
        Assert.Equal(IucnGreenStatusProblem.NotJson, IucnGreenStatusParser.Parse("<html>Bad gateway</html>").Problem);
        Assert.Equal(IucnGreenStatusProblem.NoList, IucnGreenStatusParser.Parse("{\"data\":[]}").Problem);
        Assert.Equal(IucnGreenStatusProblem.NoList, IucnGreenStatusParser.Parse("[]").Problem);
        Assert.Equal(IucnGreenStatusProblem.Empty, IucnGreenStatusParser.Parse("{\"assessments\":[]}").Problem);

        var noDate = IucnGreenStatusParser.Parse(Answer(Record(1, "2023-10-31"), Record(2, "unknown"), Record(3, "2020-01-01")));
        Assert.Equal((IucnGreenStatusProblem.NoDate, 2, 3), (noDate.Problem, noDate.Index, noDate.Total));
        Assert.Empty(noDate.Records);

        var noTaxon = IucnGreenStatusParser.Parse("{\"assessments\":[{\"assessment_date\":\"2023-10-31\",\"taxon\":{}}]}");
        Assert.Equal((IucnGreenStatusProblem.NoTaxonId, 1, 1), (noTaxon.Problem, noTaxon.Index, noTaxon.Total));
    }

    [Fact]
    public void Parse_keeps_the_last_of_records_with_the_same_key_and_counts_them() {
        var a = IucnGreenStatusParser.Parse(Answer(
            Record(1, "2023-10-31", "Largely Depleted"),
            Record(2, "2022-01-01"),
            Record(1, "2023-10-31", "Moderately Depleted")));

        Assert.True(a.Usable);
        Assert.Equal(1, a.DuplicateKeys);
        Assert.Equal(new long[] { 1, 2 }, a.Records.Select(r => r.SisId));
        Assert.Equal("Moderately Depleted", a.Records[0].SpeciesRecoveryCategory);
    }

    [Theory]
    [InlineData("{\"red_list_version\":\"2026-1\"}", "2026-1")]
    [InlineData("{\"red_list_version\":\" 2026-1 \"}", "2026-1")]
    [InlineData("{\"red_list_version\":\"\"}", null)]
    [InlineData("{\"version\":\"2026-1\"}", null)]
    [InlineData("not json", null)]
    public void RedListVersion_reads_the_release_name(string body, string? expected) =>
        Assert.Equal(expected, IucnRedListVersion.Parse(body));

    // ---- the store ----

    private static IucnGreenStatusRecord Rec(long sisId, string date, string json = "{}", long? pageId = null) =>
        new(sisId, date, pageId, json, "Name " + sisId, "Largely Depleted");

    private sealed record Row(long SisId, string Date, long? PageId, string Json, string FirstSeenAt, string? FirstSeenVersion,
        long Baseline, string LastSeenAt);

    private static Dictionary<(long, string), Row> Rows(SqliteConnection conn) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sis_id, assessment_date, red_list_assessment_id, json, first_seen_at, first_seen_version, baseline, last_seen_at FROM green_status";
        using var r = cmd.ExecuteReader();
        var rows = new Dictionary<(long, string), Row>();
        while (r.Read()) {
            rows[(r.GetInt64(0), r.GetString(1))] = new Row(r.GetInt64(0), r.GetString(1), r.IsDBNull(2) ? null : r.GetInt64(2),
                r.GetString(3), r.GetString(4), r.IsDBNull(5) ? null : r.GetString(5), r.GetInt64(6), r.GetString(7));
        }
        return rows;
    }

    // A deleted row is named from its stored JSON.
    private const string Taxon2 = "{\"b\":1,\"taxon\":{\"scientific_name\":\"Name 2\"}}";

    private static readonly DateTime Run1 =new(2026, 10, 8, 1, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Run2 = new(2026, 10, 9, 1, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Run3 = new(2027, 4, 2, 1, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Save_keeps_first_seen_and_baseline_and_deletes_what_a_download_does_not_have() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);

        // Run 1: the first download is the baseline.
        var first = store.SaveGreenStatus(new[] { Rec(1, "2023-10-31", "{\"a\":1}", 11), Rec(2, "2022-01-01", Taxon2, 21) }, "2026-1", Run1);
        Assert.True(first.FirstDownload);
        Assert.Equal(2, first.Stored);
        Assert.Equal(2, first.Added.Count);
        Assert.Empty(first.Removed);
        var rows = Rows(conn);
        Assert.All(rows.Values, r => {
            Assert.Equal(1, r.Baseline);
            Assert.Equal("2026-1", r.FirstSeenVersion);
            Assert.Equal(Run1.ToString("O"), r.FirstSeenAt);
            Assert.Equal(Run1.ToString("O"), r.LastSeenAt);
        });
        Assert.Equal(11, rows[(1, "2023-10-31")].PageId);

        // Run 2: the same list, one record's JSON and page id changed. Nothing new or removed;
        // first_seen_at, first_seen_version and baseline stay; last_seen_at, json and the page id move.
        var second = store.SaveGreenStatus(new[] { Rec(1, "2023-10-31", "{\"a\":2}", 12), Rec(2, "2022-01-01", Taxon2, 21) }, "2026-2", Run2);
        Assert.False(second.FirstDownload);
        Assert.Empty(second.Added);
        Assert.Empty(second.Removed);
        Assert.Equal(1, second.Changed);
        rows = Rows(conn);
        var one = rows[(1, "2023-10-31")];
        Assert.Equal((1L, "2026-1", Run1.ToString("O"), Run2.ToString("O")), (one.Baseline, one.FirstSeenVersion, one.FirstSeenAt, one.LastSeenAt));
        Assert.Equal("{\"a\":2}", one.Json);
        Assert.Equal(12, one.PageId);

        // Run 3: taxon 1 has a newer Green Status (new date) and taxon 2's is withdrawn.
        var third = store.SaveGreenStatus(new[] { Rec(1, "2023-10-31", "{\"a\":2}", 12), Rec(1, "2027-03-01", "{\"c\":1}") }, "2027-1", Run3);
        Assert.Equal(new[] { (1L, "2027-03-01") }, third.Added.Select(a => (a.SisId, a.AssessmentDate)));
        Assert.Equal(new (long, string, string?)[] { (2L, "2022-01-01", "Name 2") }, third.Removed.Select(r => (r.SisId, r.AssessmentDate, r.ScientificName)));
        Assert.Equal(0, third.Changed);
        rows = Rows(conn);
        Assert.Equal(2, rows.Count);
        var added = rows[(1, "2027-03-01")];
        Assert.Equal((0L, "2027-1", Run3.ToString("O")), (added.Baseline, added.FirstSeenVersion, added.FirstSeenAt));
        Assert.Null(added.PageId);
        var kept = rows[(1, "2023-10-31")];
        Assert.Equal((1L, "2026-1", Run1.ToString("O"), Run3.ToString("O")), (kept.Baseline, kept.FirstSeenVersion, kept.FirstSeenAt, kept.LastSeenAt));

        var summary = store.GetGreenStatusSummary()!;
        Assert.Equal((2L, 1L), (summary.Rows, summary.BaselineRows));
        Assert.Equal(Run1, summary.FirstSeenMin);
        Assert.Equal(Run3, summary.LastSeenMax);
        Assert.Equal(new (string?, bool, long)[] { ("2026-1", true, 1L), ("2027-1", false, 1L) }, summary.ByFirstSeenVersion.Select(v => (v.Version, v.Baseline, v.Rows)));
    }

    [Fact]
    public void Save_refuses_an_empty_download_and_leaves_the_table() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        store.SaveGreenStatus(new[] { Rec(1, "2023-10-31") }, "2026-1", Run1);

        Assert.Throws<ArgumentException>(() => store.SaveGreenStatus(Array.Empty<IucnGreenStatusRecord>(), "2026-1", Run2));
        Assert.Single(Rows(conn));
    }

    [Fact]
    public void Summary_is_null_for_an_empty_or_missing_table() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        Assert.Null(store.GetGreenStatusSummary());

        using (var drop = conn.CreateCommand()) {
            drop.CommandText = "DROP TABLE green_status";
            drop.ExecuteNonQuery();
        }
        Assert.Null(store.GetGreenStatusSummary());
    }

    [Fact]
    public void Ddl_is_the_table_the_site_build_reads() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        IucnApiCacheStore.OpenFromConnection(conn);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name, type, \"notnull\", pk FROM pragma_table_info('green_status') ORDER BY cid";
        using var r = cmd.ExecuteReader();
        var columns = new List<string>();
        while (r.Read()) columns.Add($"{r.GetString(0)} {r.GetString(1)} {r.GetInt64(2)} {r.GetInt64(3)}");
        Assert.Equal(new[] {
            "sis_id INTEGER 1 1",
            "assessment_date TEXT 1 2",
            "red_list_assessment_id INTEGER 0 0",
            "json TEXT 1 0",
            "first_seen_at TEXT 1 0",
            "first_seen_version TEXT 0 0",
            "baseline INTEGER 1 0",
            "last_seen_at TEXT 1 0",
        }, columns);
    }

    // ---- one run, over a fake API ----

    private sealed class FakeApi : HttpMessageHandler {
        private readonly Dictionary<string, (HttpStatusCode Status, string Body)> _answers;
        public FakeApi(Dictionary<string, (HttpStatusCode, string)> answers) => _answers = answers;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            var (status, body) = _answers.TryGetValue(request.RequestUri!.AbsolutePath, out var a) ? a : (HttpStatusCode.NotFound, "");
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private static IucnApiClient Client(string versionBody, string greenBody, HttpStatusCode greenStatus = HttpStatusCode.OK) =>
        new(new IucnApiConfiguration(new Uri("https://example.test"), "token", TimeSpan.FromSeconds(30), 1,
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(2), TimeSpan.FromSeconds(1), 1),
            new FakeApi(new Dictionary<string, (HttpStatusCode, string)> {
                [IucnApiClient.RedListVersionPath] = (HttpStatusCode.OK, versionBody),
                [IucnApiClient.GreenStatusAllPath] = (greenStatus, greenBody),
            }),
            delay: (_, _) => Task.CompletedTask);

    private static long Scalar(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Run_stores_the_download_with_the_red_list_version_and_logs_both_requests() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        using var client = Client("{\"red_list_version\":\"2026-1\"}", Answer(Record(12520, "2023-10-31"), Record(3989, "2021-05-01")));

        var outcome = await IucnGreenStatusRun.RunAsync(store, client, Run1, CancellationToken.None);

        Assert.Equal(GreenStatusRefusal.None, outcome.Refusal);
        Assert.Equal("2026-1", outcome.RedListVersion);
        Assert.Equal(2, outcome.Saved!.Stored);
        Assert.Equal(2, Scalar(conn, "SELECT COUNT(*) FROM green_status WHERE first_seen_version = '2026-1' AND baseline = 1"));
        Assert.Equal(2, Scalar(conn, "SELECT COUNT(*) FROM http_request_log WHERE http_status = 200 AND ended_at IS NOT NULL"));
    }

    [Theory]
    [InlineData("{\"red_list_version\":\"2026-1\"}", "{\"assessments\":[]}", HttpStatusCode.OK, "Empty")]
    [InlineData("{\"red_list_version\":\"2026-1\"}", "<html></html>", HttpStatusCode.OK, "NotJson")]
    [InlineData("{\"red_list_version\":\"2026-1\"}", "{\"assessments\":[{\"taxon\":{\"sis_id\":1}}]}", HttpStatusCode.OK, "NoDate")]
    [InlineData("{\"red_list_version\":\"2026-1\"}", "", HttpStatusCode.Forbidden, "DownloadFailed")]
    [InlineData("{}", "{\"assessments\":[]}", HttpStatusCode.OK, "VersionUnreadable")]
    public async Task Run_refuses_an_answer_it_cannot_use_and_changes_nothing(string version, string green, HttpStatusCode status,
        string expected) {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        store.SaveGreenStatus(new[] { Rec(1, "2023-10-31") }, "2026-1", Run1);
        using var client = Client(version, green, status);

        var outcome = await IucnGreenStatusRun.RunAsync(store, client, Run2, CancellationToken.None);

        Assert.Equal(Enum.Parse<GreenStatusRefusal>(expected), outcome.Refusal);
        Assert.Null(outcome.Saved);
        var row = Assert.Single(Rows(conn).Values);
        Assert.Equal((1L, Run1.ToString("O")), (row.SisId, row.LastSeenAt));
    }

    [Fact]
    public async Task Run_logs_a_failed_download_with_its_status() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        using var client = Client("{\"red_list_version\":\"2026-1\"}", "", HttpStatusCode.Forbidden);

        var outcome = await IucnGreenStatusRun.RunAsync(store, client, Run1, CancellationToken.None);

        Assert.Equal(GreenStatusRefusal.DownloadFailed, outcome.Refusal);
        Assert.Equal("HTTP 403 Forbidden", outcome.Detail);
        Assert.Equal(1, Scalar(conn, $"SELECT COUNT(*) FROM http_request_log WHERE url = '{IucnApiClient.GreenStatusAllPath}' AND http_status = 403 AND error IS NOT NULL"));
        Assert.Equal(0, Scalar(conn, "SELECT COUNT(*) FROM green_status"));
    }

    // ---- the workflow light and the data source count ----

    private static readonly DateTime Now = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private static (string, string?) Result(FlowProbeResult r) => (r.Status, r.Detail);

    [Fact]
    public void Light_is_todo_until_downloaded_and_again_after_30_days() {
        var site = new PublicSiteState { ReadAtUtc = Now, ApiCachePath = "/data/iucn_api_cache.sqlite" };
        Assert.Equal(("todo", "Not downloaded yet."), Result(PublicSiteProbes.GreenStatusStep(site)));
        Assert.Equal(("ok", "182 assessments, downloaded 2026-10-07."),
            Result(PublicSiteProbes.GreenStatusStep(site with { GreenStatus = new GreenStatusState(182, Now.AddDays(-1)) })));
        Assert.Equal(("todo", "182 assessments, downloaded 2026-09-07, 31 days ago."),
            Result(PublicSiteProbes.GreenStatusStep(site with { GreenStatus = new GreenStatusState(182, Now.AddDays(-31)) })));
        Assert.Equal("todo", PublicSiteProbes.GreenStatusStep(new PublicSiteState { ReadAtUtc = Now }).Status);

        Assert.True(PublicSiteProbes.IsProbe(PublicSiteProbes.GreenStatus));
        Assert.Equal("ok", PublicSiteProbes.Evaluate(PublicSiteProbes.GreenStatus,
            site with { GreenStatus = new GreenStatusState(182, Now) })!.Status);
    }

    [Fact]
    public void Data_source_card_counts_the_stored_assessments() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        store.SaveGreenStatus(new[] { Rec(1, "2023-10-31"), Rec(2, "2022-01-01") }, "2026-1", Run1);

        var metric = DataSourceCatalogue.All.Single(d => d.Id == "iucn-api-cache").Metrics.Single(m => m.Label == "Green Status assessments");
        var result = StatusService.RunMetric(conn, metric);
        Assert.Null(result.Error);
        Assert.Equal(2L, result.Value);
    }
}

// The reader behind the light, over a real file: a cache without the table, an empty table, and
// a stored download. The API cache's change time for the site build counts Green Status rows from
// first_seen_at, so a run that stores nothing new does not make the site database out of date.
public class IucnGreenStatusReaderTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-green-" + Guid.NewGuid().ToString("N"));

    public IucnGreenStatusReaderTests() => Directory.CreateDirectory(_dir);

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private string Api => Path.Combine(_dir, "api.sqlite");

    private void Exec(string sql) {
        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = Api, Pooling = false }.ConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private PublicSiteState Read() => PublicSiteStateReader.Read(new PublicSitePaths { ApiCache = Api });

    [Fact]
    public void Reads_the_count_and_last_download_and_tolerates_an_older_cache() {
        Exec("""
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, downloaded_at TEXT NOT NULL);
            CREATE TABLE assessments (id INTEGER PRIMARY KEY, downloaded_at TEXT NOT NULL);
            INSERT INTO taxa (downloaded_at) VALUES ('2026-09-05T08:00:00.0000000Z');
            """);
        var before = Read();
        Assert.Equal(Api, before.ApiCachePath);
        Assert.Null(before.GreenStatus);
        Assert.Equal(new DateTime(2026, 9, 5, 8, 0, 0, DateTimeKind.Utc), before.Inputs.Single().ChangedAtUtc);

        Exec(IucnApiCacheStore.GreenStatusDdl);
        Assert.Null(Read().GreenStatus);

        Exec("""
            INSERT INTO green_status VALUES
                (1, '2023-10-31', 11, '{}', '2026-10-01T00:00:00.0000000Z', '2026-1', 1, '2026-10-08T01:00:00.0000000Z'),
                (2, '2022-01-01', 21, '{}', '2026-10-01T00:00:00.0000000Z', '2026-1', 1, '2026-10-08T01:00:00.0000000Z');
            """);
        var after = Read();
        Assert.Equal(new GreenStatusState(2, new DateTime(2026, 10, 8, 1, 0, 0, DateTimeKind.Utc)), after.GreenStatus);
        // first_seen_at, not last_seen_at.
        Assert.Equal(new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc), after.Inputs.Single().ChangedAtUtc);
    }
}
