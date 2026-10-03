using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests.SiteBuild;

// Writes the source databases for the `site build-db` tests, and has the SQL helpers the tests use
// to read the database the build writes. Each instance creates its own temporary folder and
// deletes it on Dispose. A test class creates one in a field, and xUnit makes a new class instance
// for each test, so no two tests share files.
//
// The writers create the tables and columns that `site build-db` reads, cut down:
//   - the IUCN CSV export (WriteIucnCsv): import_metadata with the release, taxonomy_html and
//     assessments_html, whose rows the test passes as SQL VALUES tuples in the column order below;
//   - the IUCN API cache (WriteApiCache): taxa, taxa_lookup and assessments, with JSON built by
//     Header (an assessment entry in a taxon record) and Payload (a cached assessment).
// Sources only one test class uses (common names store, SPRAT, DOI cache, ColDP zip) stay in that class.
//
// Tests import the static members with `using static`, so they read as plain calls: Rows(db, sql).
internal sealed class SiteBuildSourceFixture : IDisposable {
    /// downloaded_at of every cached taxon record.
    public const string TaxaDownloadedAt = "2026-08-18T00:00:00Z";

    public SiteBuildSourceFixture() {
        Dir = Path.Combine(Path.GetTempPath(), "beastiebot-sitebuild-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Dir);
    }

    public string Dir { get; }

    public string PathOf(string fileName) => Path.Combine(Dir, fileName);

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(Dir, recursive: true);
        } catch (IOException) {
        }
    }

    public static IAnsiConsole QuietConsole() => AnsiConsole.Create(new AnsiConsoleSettings {
        Out = new AnsiConsoleOutput(TextWriter.Null),
        Interactive = InteractionSupport.No,
    });

    // ------------------------------------------------------------ IUCN CSV export

    /// Writes the CSV export of one release. Row columns, in order:
    ///   taxonomy_html: import_id, taxonId, scientificName, kingdomName, phylumName, className,
    ///     orderName, familyName, genusName, speciesName, infraType, infraName, infraAuthority,
    ///     subpopulationName, authority, taxonomicNotes
    ///   assessments_html: import_id, assessmentId, taxonId, scientificName, redlistCategory,
    ///     redlistCriteria, yearPublished, assessmentDate, criteriaVersion, language, rationale,
    ///     populationTrend, possiblyExtinct, possiblyExtinctInTheWild, scopes
    /// import_id is 1. taxonomicNotes and rationale are narrative text, which the build must not copy.
    public static void WriteIucnCsv(string path, string release, string taxonomyRows, string assessmentRows) {
        using var c = OpenWritable(path);
        Execute(c, $"""
            CREATE TABLE import_metadata (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
            INSERT INTO import_metadata VALUES (1, '{release}/a.zip', '{release}', '2026-08-14', NULL);
            CREATE TABLE taxonomy_html (import_id INTEGER, taxonId INTEGER, scientificName TEXT, kingdomName TEXT, phylumName TEXT,
                className TEXT, orderName TEXT, familyName TEXT, genusName TEXT, speciesName TEXT, infraType TEXT, infraName TEXT,
                infraAuthority TEXT, subpopulationName TEXT, authority TEXT, taxonomicNotes TEXT);
            INSERT INTO taxonomy_html VALUES
            {taxonomyRows};
            CREATE TABLE assessments_html (import_id INTEGER, assessmentId INTEGER, taxonId INTEGER, scientificName TEXT, redlistCategory TEXT,
                redlistCriteria TEXT, yearPublished TEXT, assessmentDate TEXT, criteriaVersion TEXT, language TEXT, rationale TEXT,
                populationTrend TEXT, possiblyExtinct TEXT, possiblyExtinctInTheWild TEXT, scopes TEXT);
            INSERT INTO assessments_html VALUES
            {assessmentRows};
            """);
    }

    // ------------------------------------------------------------ IUCN API cache

    /// One row of the cache's taxa table: a /taxa record, stored under the root taxon's id.
    public sealed record CachedTaxonRecord(long Id, long RootSisId, string Json);

    /// One row of the cache's assessments table: a cached /assessment payload.
    public sealed record CachedAssessment(long AssessmentId, long SisId, string DownloadedAt, string Json);

    /// One row of taxa_lookup: a taxon listed inside another taxon's record (a subpopulation).
    public sealed record CachedTaxonLookup(long SisId, long TaxaId, long RootSisId, string Scope);

    public static void WriteApiCache(string path, IEnumerable<CachedTaxonRecord> taxa, IEnumerable<CachedAssessment> assessments,
        IEnumerable<CachedTaxonLookup>? lookups = null) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, root_sis_id INTEGER NOT NULL UNIQUE, downloaded_at TEXT NOT NULL, json TEXT NOT NULL);
            CREATE TABLE assessments (id INTEGER PRIMARY KEY, assessment_id INTEGER NOT NULL UNIQUE, sis_id INTEGER NOT NULL, downloaded_at TEXT NOT NULL, json TEXT NOT NULL);
            CREATE TABLE taxa_lookup (sis_id INTEGER NOT NULL, taxa_id INTEGER NOT NULL, root_sis_id INTEGER NOT NULL, scope TEXT NOT NULL, PRIMARY KEY (sis_id, taxa_id));
            """);
        foreach (var taxon in taxa) {
            Execute(c, "INSERT INTO taxa (id, root_sis_id, downloaded_at, json) VALUES (@id, @root, @downloaded, @json)",
                ("@id", taxon.Id), ("@root", taxon.RootSisId), ("@downloaded", TaxaDownloadedAt), ("@json", taxon.Json));
        }
        foreach (var lookup in lookups ?? Array.Empty<CachedTaxonLookup>()) {
            Execute(c, "INSERT INTO taxa_lookup VALUES (@sis, @taxa, @root, @scope)",
                ("@sis", lookup.SisId), ("@taxa", lookup.TaxaId), ("@root", lookup.RootSisId), ("@scope", lookup.Scope));
        }
        foreach (var assessment in assessments) {
            Execute(c, "INSERT INTO assessments (assessment_id, sis_id, downloaded_at, json) VALUES (@id, @taxon, @downloaded, @json)",
                ("@id", assessment.AssessmentId), ("@taxon", assessment.SisId), ("@downloaded", assessment.DownloadedAt), ("@json", assessment.Json));
        }
    }

    /// A scopes array with one scope.
    public static string Scope(string code, string description) =>
        $$"""[{"description":{"en":"{{description}}"},"code":"{{code}}"}]""";

    public static string GlobalScope => Scope("1", "Global");

    /// An assessment entry of a taxon record. A null year is an unpublished draft. The date defaults
    /// to 1 January of the year, the scopes to Global.
    public static string Header(long id, long taxonId, bool latest, string? year, string code, string? date = null, string? scopes = null) =>
        $$"""{"assessment_id":{{id}},"sis_taxon_id":{{taxonId}},"latest":{{(latest ? "true" : "false")}},"year_published":{{(year is null ? "null" : $"\"{year}\"")}},"assessment_date":"{{date ?? $"{year}-01-01T00:00:00.000+00:00"}}","red_list_category_code":"{{code}}","criteria":null,"possibly_extinct":false,"possibly_extinct_in_the_wild":false,"scopes":{{scopes ?? GlobalScope}}}""";

    /// A cached assessment payload with one assessor credit, whose value[] lists `people` entries.
    /// The documentation holds narrative text, which the build must not copy: the rationale, and the
    /// taxonomic notes when taxonomicNotes is given. The scopes default to Global.
    public static string Payload(long id, long taxonId, string name, string year, string citation, string assessor,
        int people = 1, string version = "3.1", string? trend = null, string? scopes = null, string? taxonomicNotes = null) =>
        JsonSerializer.Serialize(new Dictionary<string, object?> {
            ["assessment_id"] = id,
            ["sis_taxon_id"] = taxonId,
            ["year_published"] = year,
            ["latest"] = false,
            ["citation"] = citation,
            ["taxon"] = new Dictionary<string, object?> { ["sis_id"] = taxonId, ["scientific_name"] = name, ["subpopulation_name"] = null },
            ["credits"] = new[] { new Dictionary<string, object?> {
                ["credit_type_name"] = "assessor", ["full"] = assessor, ["value"] = Enumerable.Range(1, people).Select(i => $"v{i}").ToArray(),
            } },
            ["errata"] = Array.Empty<object>(),
            ["scopes"] = JsonSerializer.Deserialize<JsonElement>(scopes ?? GlobalScope),
            ["red_list_category"] = new Dictionary<string, object?> { ["version"] = version },
            ["population_trend"] = trend is null ? null : new Dictionary<string, object?> { ["description"] = new Dictionary<string, string> { ["en"] = trend } },
            ["documentation"] = taxonomicNotes is null
                ? new Dictionary<string, string> { ["rationale"] = "NARRATIVE rationale" }
                : new Dictionary<string, string> { ["rationale"] = "NARRATIVE rationale", ["taxonomic_notes"] = taxonomicNotes },
        });

    // ------------------------------------------------------------ SQL helpers

    /// Opens (and creates) a database for writing, without pooling so the file can be deleted afterwards.
    public static SqliteConnection OpenWritable(string path) {
        var connection = new SqliteConnection($"Data Source={path};Pooling=False");
        connection.Open();
        return connection;
    }

    public static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    public static void Execute(SqliteConnection c, string sql, params (string Name, object? Value)[] parameters) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        command.ExecuteNonQuery();
    }

    /// The first column of the first row as invariant text, or null.
    public static string? Scalar(SqliteConnection c, string sql) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar() is { } value and not DBNull ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
    }

    /// Every row, with NULL as null.
    public static List<object?[]> Rows(SqliteConnection c, string sql, params (string Name, object? Value)[] parameters) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<object?[]>();
        while (reader.Read()) {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            rows.Add(row);
        }
        return rows;
    }
}
