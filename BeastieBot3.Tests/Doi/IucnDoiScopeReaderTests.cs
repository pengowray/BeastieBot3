using System.Text.Json;
using BeastieBot3.Iucn.Doi;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests.Doi;

// Pins which assessments `iucn resolve-dois` works on, over small synthetic copies of the CSV
// exports, the API cache and the Wikidata cache: an assessment with a DOI from IUCN's citation or
// Wikidata is left out (a predecessor's DOI only for an errata version), and the targets carry the
// year, language, errata year and the release they were new in.
public sealed class IucnDoiScopeReaderTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "doi-scope-" + Guid.NewGuid().ToString("N"));

    public IucnDoiScopeReaderTests() {
        Directory.CreateDirectory(_dir);
        BuildCsv(Path.Combine(_dir, "IUCN_2026-1.sqlite"), "2026-1", [
            (101, 1, "2019", "English", "Global", null, null),
            (102, 2, "2020", "Spanish; Castilian", "Global", null, null),
            (103, 3, "2016", "English", "Global & Europe", null, null),
            (104, 4, "2019", "English", "Global", null, null),
            (105, 5, "2020", "English", "Global", null, "West Africa"),
            (106, 6, "2015", "French", "Europe", null, null),
            (107, 7, "2021", "Portuguese", "Global", "subspecies", null),
        ]);
        BuildCsv(Path.Combine(_dir, "IUCN_2025-2.sqlite"), "2025-2", [
            (101, 1, "2019", "English", "Global", null, null),
            (102, 2, "2020", "Spanish; Castilian", "Global", null, null),
            (103, 3, "2016", "English", "Global", null, null),
            (105, 5, "2020", "English", "Global", null, "West Africa"),
            (106, 6, "2015", "French", "Europe", null, null),
            (107, 7, "2021", "Portuguese", "Global", "subspecies", null),
            (90, 3, "2016", "Spanish; Castilian", "Global", null, null),
        ]);
        BuildApiCache(Path.Combine(_dir, "api.sqlite"));
        BuildWikidata(Path.Combine(_dir, "wikidata.sqlite"));
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private DoiScopeSources Sources() => new(
        Path.Combine(_dir, "IUCN_2026-1.sqlite"),
        IucnDoiScopeReader.FindPreviousRelease(Path.Combine(_dir, "IUCN_2026-1.sqlite"), "2026-1"),
        Path.Combine(_dir, "api.sqlite"),
        GbifChecklist: null,
        Path.Combine(_dir, "wikidata.sqlite"));

    private static DoiScopeResult Read(DoiScopeSources sources, DoiScope scope) =>
        IucnDoiScopeReader.Read(sources, scope, _ => { }, CancellationToken.None);

    [Fact]
    public void FindPreviousRelease_PicksTheNewestOlderRelease() {
        File.WriteAllText(Path.Combine(_dir, "IUCN_2024-2.sqlite"), "");
        File.WriteAllText(Path.Combine(_dir, "IUCN_2026-2.sqlite"), "");
        Assert.Equal(Path.Combine(_dir, "IUCN_2025-2.sqlite"), IucnDoiScopeReader.FindPreviousRelease(Path.Combine(_dir, "IUCN_2026-1.sqlite"), "2026-1"));
        Assert.Null(IucnDoiScopeReader.FindPreviousRelease(Path.Combine(_dir, "IUCN_2026-1.sqlite"), "2024-2"));
    }

    [Theory]
    [InlineData(null, "LatestGlobal")]
    [InlineData("latest-regional", "LatestRegional")]
    [InlineData("ALL-LATEST", "AllLatest")]
    [InlineData("history", "History")]
    [InlineData("regional", null)]
    public void ParseScope(string? text, string? expected) {
        Assert.Equal(expected, IucnDoiScopeReader.ParseScope(text)?.ToString());
    }

    [Fact]
    public void LatestGlobal_LeavesOutAssessmentsWithADoi_AndDescribesTheRest() {
        var result = Read(Sources(), DoiScope.LatestGlobal);

        Assert.Equal("2026-1", result.Release);
        Assert.Equal("2025-2", result.PreviousRelease);
        Assert.Equal(6, result.Counts.InScope);
        Assert.Equal(1, result.Counts.FromCitation);
        Assert.Equal(2, result.Counts.FromWikidata);
        Assert.Equal(0, result.Counts.FromGbif);
        Assert.Equal(1, result.Counts.NoPayload);
        Assert.Equal(new long[] { 104, 105, 107 }, result.Targets.Select(t => t.AssessmentId));

        var newOne = result.Targets[0];
        Assert.Equal(2019, newOne.YearPublished);
        Assert.Equal("2026-1", newOne.NewInRelease);
        Assert.Equal("en", newOne.Language);
        Assert.True(newOne.HasPayload);
        Assert.Equal("global", newOne.Scope);
        Assert.Equal("species", newOne.Kind);

        var subpopulation = result.Targets[1];
        Assert.False(subpopulation.HasPayload);
        Assert.Equal(2020, subpopulation.YearPublished);
        Assert.Equal("subpopulation", subpopulation.Kind);
        Assert.Null(subpopulation.NewInRelease);

        var subspecies = result.Targets[2];
        Assert.Equal("pt", subspecies.Language);
        Assert.Equal("subspecies", subspecies.Kind);
        Assert.Equal(2021, subspecies.YearPublished);
    }

    [Fact]
    public void WithoutThePreviousRelease_NothingIsPinned() {
        var result = Read(Sources() with { PreviousIucnDatabase = null }, DoiScope.LatestGlobal);
        Assert.Null(result.PreviousRelease);
        Assert.All(result.Targets, t => Assert.Null(t.NewInRelease));
    }

    [Fact]
    public void LatestRegional_TakesAssessmentsWithNoGlobalScope() {
        var result = Read(Sources(), DoiScope.LatestRegional);
        var target = Assert.Single(result.Targets);
        Assert.Equal(106, target.AssessmentId);
        Assert.Equal("fr", target.Language);
        Assert.Equal("regional", target.Scope);
        Assert.Equal(1, result.Counts.InScope);
    }

    [Fact]
    public void History_TakesEarlierAssessments_WithThePreviousExportsLanguage() {
        var result = Read(Sources(), DoiScope.History);
        var target = Assert.Single(result.Targets);
        Assert.Equal(90, target.AssessmentId);
        Assert.Equal(3, target.TaxonId);
        Assert.Equal(2016, target.YearPublished);
        Assert.Equal("es", target.Language);
        Assert.Equal("history", target.Scope);
        Assert.Equal(1, result.Counts.HistoryUnpublished);
    }

    // ------------------------------------------------------------ fixtures

    private static void Execute(SqliteConnection connection, string sql, params (string Name, object? Value)[] parameters) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        command.ExecuteNonQuery();
    }

    private static SqliteConnection Create(string path) {
        var connection = new SqliteConnection($"Data Source={path}");
        connection.Open();
        return connection;
    }

    private static void BuildCsv(string path, string release,
        (long Aid, long Tid, string Year, string Language, string Scopes, string? InfraType, string? Subpopulation)[] rows) {
        using var connection = Create(path);
        Execute(connection, "CREATE TABLE import_metadata (redlist_version TEXT)");
        Execute(connection, "INSERT INTO import_metadata VALUES (@r)", ("@r", release));
        Execute(connection, "CREATE TABLE assessments_html (assessmentId INTEGER, taxonId INTEGER, yearPublished TEXT, language TEXT, scopes TEXT)");
        Execute(connection, "CREATE TABLE taxonomy_html (taxonId INTEGER, infraType TEXT, subpopulationName TEXT)");
        foreach (var row in rows) {
            Execute(connection, "INSERT INTO assessments_html VALUES (@a, @t, @y, @l, @s)",
                ("@a", row.Aid), ("@t", row.Tid), ("@y", row.Year), ("@l", row.Language), ("@s", row.Scopes));
            Execute(connection, "INSERT INTO taxonomy_html SELECT @t, @i, @p WHERE NOT EXISTS (SELECT 1 FROM taxonomy_html WHERE taxonId = @t)",
                ("@t", row.Tid), ("@i", row.InfraType), ("@p", row.Subpopulation));
        }
    }

    private static string Header(long aid, long tid, bool latest, string? year) =>
        $$"""{"assessment_id":{{aid}},"sis_taxon_id":{{tid}},"latest":{{(latest ? "true" : "false")}},"year_published":{{(year is null ? "null" : $"\"{year}\"")}},"scopes":[{"description":{"en":"Global"},"code":"1"}]}""";

    private static string Payload(long aid, long tid, string year, string name, string annotation = "", string? doi = null) {
        var citation = $"Smith, J. {year}. {name}{annotation}. The IUCN Red List of Threatened Species {year}: e.T{tid}A{aid}."
            + (doi is null ? "" : $" https://dx.doi.org/{doi}.") + " Accessed on 20 August 2026.";
        return $$"""
            {"assessment_id":{{aid}},"sis_taxon_id":{{tid}},"year_published":"{{year}}","latest":true,
             "citation":{{JsonSerializer.Serialize(citation)}},
             "taxon":{"sis_id":{{tid}},"scientific_name":{{JsonSerializer.Serialize(name)}},"subpopulation_name":null},
             "credits":[{"credit_type_name":"assessor","full":"Smith, J.","value":["v1"]}],"errata":[],
             "scopes":[{"description":{"en":"Global"},"code":"1"}]}
            """;
    }

    private static void BuildApiCache(string path) {
        using var connection = Create(path);
        Execute(connection, "CREATE TABLE taxa (id INTEGER PRIMARY KEY, root_sis_id INTEGER, json TEXT)");
        Execute(connection, "CREATE TABLE assessments (id INTEGER PRIMARY KEY, assessment_id INTEGER, json TEXT)");
        var taxa = new (long Root, string[] Headers)[] {
            (1, [Header(101, 1, true, "2019")]),
            (2, [Header(102, 2, true, "2020")]),
            (3, [Header(103, 3, true, "2016"), Header(90, 3, false, "2016")]),
            (4, [Header(104, 4, true, "2019"), Header(80, 4, false, null)]),
            (6, [Header(106, 6, true, "2015")]),
            (7, [Header(107, 7, true, "2021")]),
        };
        foreach (var (root, headers) in taxa) {
            Execute(connection, "INSERT INTO taxa (root_sis_id, json) VALUES (@r, @j)", ("@r", root), ("@j", $$"""{"assessments":[{{string.Join(",", headers)}}]}"""));
        }
        var payloads = new (long Aid, string Json)[] {
            (101, Payload(101, 1, "2019", "Xus yus", doi: "10.2305/IUCN.UK.2019-2.RLTS.T1A101.en")),
            (102, Payload(102, 2, "2020", "Xus vus")),
            (103, Payload(103, 3, "2016", "Xus zus", " (errata version published in 2017)")),
            (104, Payload(104, 4, "2019", "Xus wus")),
            (106, Payload(106, 6, "2015", "Xus tus")),
            (107, Payload(107, 7, "2021", "Xus sus")),
            (90, Payload(90, 3, "2016", "Xus zus")),
        };
        foreach (var (aid, json) in payloads) {
            Execute(connection, "INSERT INTO assessments (assessment_id, json) VALUES (@a, @j)", ("@a", aid), ("@j", json));
        }
    }

    private static void BuildWikidata(string path) {
        using var connection = Create(path);
        Execute(connection, "CREATE TABLE wikidata_iucn_assessment_items (qid_numeric INTEGER, assessment_id INTEGER, doi TEXT, all_dois TEXT)");
        Execute(connection, "INSERT INTO wikidata_iucn_assessment_items VALUES (1, 102, '10.2305/IUCN.UK.2020-1.RLTS.T2A102.es', NULL)");
        // The errata version 103's Wikidata DOI names the assessment it replaced, 90.
        Execute(connection, "INSERT INTO wikidata_iucn_assessment_items VALUES (2, 103, NULL, 'https://doi.org/10.2305/IUCN.UK.2016-2.RLTS.T3A90.en')");
        // A DOI naming the wrong assessment does not count.
        Execute(connection, "INSERT INTO wikidata_iucn_assessment_items VALUES (3, 104, '10.2305/IUCN.UK.2019-1.RLTS.T4A999.en', NULL)");
    }
}
