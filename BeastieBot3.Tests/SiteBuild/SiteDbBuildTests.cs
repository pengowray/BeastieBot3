using System.IO.Compression;
using System.Text;
using System.Text.Json;
using BeastieBot3.CommonNames;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db` end to end over tiny source databases: an IUCN CSV export, an IUCN API cache, a
// common names store, GBIF's checklist and a Catalogue of Life ColDP zip, with the other sources
// missing. Checks the contract the public site depends on (BeastieBot3.Site reads only what
// SiteDbSchema describes).
public sealed class SiteDbBuildTests : IDisposable {
    private const long PolarBear = 22823;
    private const long PolarBearGlobal = 14871490;
    private const long PolarBearEurope = 217912462;
    private const long PolarBear1996 = 9390941;
    private const long PolarBearDraft = 9390999;
    private const long PolarBearNoScope = 9390998;
    private const long Subspecies = 900001;
    private const long SubspeciesLatest = 900101;
    private const long Subpopulation = 900002;
    private const long SubpopulationLatest = 900102;
    private const long NoScopeTaxon = 900003;
    // Earlier polar bear assessments: an amended version, and an errata version.
    private const long PolarBear2005 = 9390905;
    private const long PolarBear2006Amended = 9390906;
    private const long PolarBear2008 = 9390950;
    private const long PolarBear2008Errata = 9390951;

    // The replacement character, written this way so it stays visible in the source.
    private const char Lost = (char)0xFFFD;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "beastiebot-sitebuild-tests", Guid.NewGuid().ToString("N"));

    public SiteDbBuildTests() {
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    [Fact]
    public void Build_WritesTheSchemaTheSiteReads() {
        var output = Build();

        Assert.False(File.Exists(output + ".building"));
        Assert.False(File.Exists(output + "-wal"));
        using var db = OpenReadOnly(output);
        Assert.Equal("delete", Scalar(db, "PRAGMA journal_mode"));

        // The schema is SiteDbSchema.Ddl exactly.
        var expected = Path.Combine(_dir, "expected.sqlite");
        using (var reference = new SqliteConnection($"Data Source={expected};Pooling=False")) {
            reference.Open();
            Execute(reference, SiteDbSchema.Ddl);
        }
        using (var reference = OpenReadOnly(expected)) {
            Assert.Equal(SchemaOf(reference), SchemaOf(db));
        }

        var meta = Rows(db, "SELECT key, value FROM meta").ToDictionary(r => (string)r[0]!, r => (string)r[1]!);
        Assert.Equal(SiteDbSchema.Version.ToString(), meta[SiteDbSchema.MetaKeys.SchemaVersion]);
        Assert.Equal("2026-1", meta[SiteDbSchema.MetaKeys.IucnRelease]);
        Assert.Equal("2026-08-21", meta[SiteDbSchema.MetaKeys.IucnApiDownloadedFrom]);
        Assert.Equal("2026-08-24", meta[SiteDbSchema.MetaKeys.IucnApiDownloadedTo]);
        Assert.Equal("4", meta[SiteDbSchema.MetaKeys.TaxonCount]);
        Assert.Equal(Scalar(db, "SELECT COUNT(*) FROM assessment"), meta[SiteDbSchema.MetaKeys.AssessmentCount]);
        Assert.True(meta.ContainsKey(SiteDbSchema.MetaKeys.BuiltAtUtc));
    }

    [Fact]
    public void Build_StoresTheGbifAndCatalogueOfLifeCitations() {
        using var db = OpenReadOnly(Build());
        var meta = Rows(db, "SELECT key, value FROM meta").ToDictionary(r => (string)r[0]!, r => (string)r[1]!);

        Assert.Equal("2026-1", meta[SiteDbSchema.MetaKeys.GbifChecklistVersion]);
        Assert.Equal("2026-07-28", meta[SiteDbSchema.MetaKeys.GbifChecklistPublished]);
        Assert.Equal("IUCN (2026). The IUCN Red List of Threatened Species. Version 2026-1. https://www.iucnredlist.org. Downloaded on 2026-07-28. https://doi.org/10.15468/0qnb58",
            meta[SiteDbSchema.MetaKeys.GbifChecklistCitation]);
        Assert.Equal("10.15468/0qnb58", meta[SiteDbSchema.MetaKeys.GbifChecklistDoi]);

        Assert.Equal("COL26.7 XR", meta[SiteDbSchema.MetaKeys.ColRelease]);
        Assert.Equal("10.48580/dgykv", meta[SiteDbSchema.MetaKeys.ColDoi]);
        Assert.Equal("Bánki, O., Roskov, Y., & Hernández Robles, D. R. (2026). Catalogue of Life (2026-07-17 XR). "
            + "Catalogue of Life Foundation, Amsterdam, Netherlands. https://doi.org/10.48580/dgykv",
            meta[SiteDbSchema.MetaKeys.ColCitation]);
    }

    // An errata version replaced the earlier assessment of the same year; an amended version replaced
    // the one of the year its title names. The column is set on the older row.
    [Fact]
    public void Build_LinksEachReplacedAssessmentToTheVersionThatReplacedIt() {
        using var db = OpenReadOnly(Build());
        var links = Rows(db, "SELECT assessment_id, replaced_by_assessment_id FROM assessment WHERE replaced_by_assessment_id IS NOT NULL")
            .ToDictionary(r => (long)r[0]!, r => (long)r[1]!);

        Assert.Equal(new Dictionary<long, long> {
            [PolarBear2005] = PolarBear2006Amended,
            [PolarBear2008] = PolarBear2008Errata,
        }, links);
    }

    // "Kry?tufek, B." lost a letter to an encoding error; the 2006 assessment, read after it, credits
    // "Kryštufek, B.". A name no other credit matches keeps its U+FFFD.
    [Fact]
    public void Build_RepairsAuthorNamesThatLostALetter() {
        using var db = OpenReadOnly(Build());
        var repaired = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {PolarBear2005}"))!;
        var kept = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {PolarBear2008Errata}"))!;

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Person, "Kryštufek, B.", "Kryštufek", "B."), Assert.Single(repaired.Authors));
        Assert.Equal(new CitationAuthor(CitationAuthorKind.Verbatim, $"Mo{Lost}brucker, H."), kept.Authors[1]);
        Assert.Equal(2009, kept.ErrataYear);
    }

    [Fact]
    public void Build_PutsEveryScientificNameInNameAndNameKey_AndTheSearchIndexWorks() {
        using var db = OpenReadOnly(Build());

        Assert.Equal("0", Scalar(db, """
            SELECT COUNT(*) FROM taxon t WHERE NOT EXISTS (
                SELECT 1 FROM name n JOIN name_key k ON k.name_id = n.name_id
                WHERE n.taxon_id = t.taxon_id AND n.name_type = 'scientific' AND n.name = t.scientific_name
                  AND k.key = lower(t.scientific_name))
            """));
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM name n WHERE NOT EXISTS (SELECT 1 FROM name_key k WHERE k.name_id = n.name_id)"));

        var hits = Rows(db, """
            SELECT DISTINCT n.taxon_id FROM name_fts JOIN name n ON n.name_id = name_fts.rowid
            WHERE name_fts MATCH '"thalarc"*'
            """);
        Assert.Equal(PolarBear, (long)Assert.Single(hits)[0]!);

        var byKey = Rows(db, "SELECT taxon_id FROM name_key WHERE key = @key", ("@key", SiteNameKey.Fold("Polar  BEAR")));
        Assert.Contains(byKey, r => (long)r[0]! == PolarBear);
    }

    [Fact]
    public void Build_TakesTheLatestFromTheCsv_AndTheHistoryFromTheApi() {
        using var db = OpenReadOnly(Build());
        var rows = Rows(db, """
            SELECT assessment_id, scope, is_latest, category, criteria_version, year_published, assessment_date,
                   population_trend, citation_json IS NOT NULL
            FROM assessment WHERE taxon_id = @id ORDER BY assessment_id
            """, ("@id", PolarBear)).ToDictionary(r => (long)r[0]!);

        Assert.Equal(new[] { PolarBear2005, PolarBear2006Amended, PolarBear1996, PolarBear2008, PolarBear2008Errata, PolarBearGlobal, PolarBearEurope },
            rows.Keys.Order());

        var global = rows[PolarBearGlobal];
        Assert.Equal(("Global", 1L, "VU", "3.1", 2015L, "2015-08-27", "Unknown"),
            ((string)global[1]!, (long)global[2]!, (string)global[3]!, (string)global[4]!, (long)global[5]!, (string)global[6]!, (string)global[7]!));
        Assert.Equal(("Europe", 1L), ((string)rows[PolarBearEurope][1]!, (long)rows[PolarBearEurope][2]!));

        // The earlier assessment: the header's category exactly, the payload's criteria version and
        // the header's date in UTC.
        var old = rows[PolarBear1996];
        Assert.Equal(("Global", 0L, "LR/cd", "2.3", 1996L, "1996-08-01"),
            ((string)old[1]!, (long)old[2]!, (string)old[3]!, (string)old[4]!, (long)old[5]!, (string)old[6]!));

        Assert.Equal(PolarBearGlobal.ToString(), Scalar(db, $"SELECT latest_global_assessment_id FROM taxon WHERE taxon_id = {PolarBear}"));
        // No two latest rows for one taxon and scope.
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM (SELECT taxon_id, scope FROM assessment WHERE is_latest = 1 GROUP BY 1, 2 HAVING COUNT(*) > 1)"));
    }

    [Fact]
    public void Build_StoresTheCitationParts_WithoutNarrative() {
        using var db = OpenReadOnly(Build());
        var json = Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {PolarBearGlobal}");
        var parts = IucnCitationParts.FromJson(json);
        Assert.NotNull(parts);
        Assert.Equal(new[] { "Wiig", "Amstrup" }, parts!.Authors.Select(a => a.Last));
        Assert.Equal("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", parts.Doi);
        Assert.Equal(DoiSource.Citation, parts.DoiSource);
        Assert.DoesNotContain("NARRATIVE", json!, StringComparison.Ordinal);
        Assert.DoesNotContain("Accessed on", json!, StringComparison.Ordinal);
    }

    [Fact]
    public void Build_LinksSubspeciesAndSubpopulationsToTheirParents() {
        using var db = OpenReadOnly(Build());
        var taxa = Rows(db, "SELECT taxon_id, kind, infra_rank, parent_taxon_id, latest_global_assessment_id, authority FROM taxon")
            .ToDictionary(r => (long)r[0]!);

        Assert.Equal(("subspecies", "ssp.", PolarBear), ((string)taxa[Subspecies][1]!, (string)taxa[Subspecies][2]!, (long)taxa[Subspecies][3]!));
        Assert.Equal("Tester & Other, 1999", taxa[Subspecies][5]);
        Assert.Equal(("subpopulation", PolarBear), ((string)taxa[Subpopulation][1]!, (long)taxa[Subpopulation][3]!));

        // The subpopulation's assessment is in the CSV only: no payload, so no citation.
        Assert.Equal("1", Scalar(db, $"SELECT COUNT(*) FROM assessment WHERE assessment_id = {SubpopulationLatest} AND citation_json IS NULL"));
        // Its names come from the record that lists it.
        Assert.Contains(Rows(db, $"SELECT name, language FROM name WHERE taxon_id = {Subpopulation} AND name_type = 'common'"),
            r => (string)r[0]! == "Test Bear" && (string)r[1]! == "en");

        // A taxon whose only assessment has no scope keeps no assessment.
        Assert.Null(taxa[NoScopeTaxon][4]);
        Assert.Equal("0", Scalar(db, $"SELECT COUNT(*) FROM assessment WHERE taxon_id = {NoScopeTaxon}"));
    }

    [Fact]
    public void Build_NamesCommonNamesFromIucnAndTheStore_WithLanguageCodesAndTheBestEnglishName() {
        using var db = OpenReadOnly(Build());
        var names = Rows(db, $"SELECT name, name_type, language, source, is_preferred FROM name WHERE taxon_id = {PolarBear} ORDER BY name_id")
            .Select(r => ((string)r[0]!, (string)r[1]!, r[2] as string, (string)r[3]!, (long)r[4]!))
            .ToList();

        Assert.Equal(("Ursus maritimus", "scientific", null, "iucn", 1L), names[0]);
        Assert.Equal(("Polar Bear", "common", "en", "iucn", 1L), names[1]);
        Assert.Contains(("Ours blanc", "common", "fr", "iucn", 0L), names);
        Assert.Contains(("Polar bear", "common", "en", "wikipedia", 0L), names);
        Assert.Contains(("Thalarctos maritimus", "synonym", null, "iucn", 0L), names);
        Assert.Contains(("Ursus marinus", "synonym", null, "col", 0L), names);
        // The store's IUCN copy of "Polar Bear" is the same row as the API's.
        Assert.Single(names, n => n.Item1 == "Polar Bear" && n.Item4 == "iucn");
        // A language the API does not give is stored without one.
        Assert.Contains(("Bear of the north", "common", null, "iucn", 0L), names);

        // The best English name is the Wikipedia title (highest source priority).
        Assert.Equal("Polar bear", BestName(db, PolarBear));
        // "Shared name" belongs to several taxa, so the next name is used, capitalised by the caps rules.
        Assert.Equal("Test bear", BestName(db, Subpopulation));
        // The best name is the scientific name again, so the taxon gets none, as in the lists.
        Assert.Null(BestName(db, Subspecies));
        // rules-list.txt overrides the store.
        Assert.Equal("Nemo bear", BestName(db, NoScopeTaxon));
    }

    private static string? BestName(SqliteConnection db, long taxonId) =>
        Scalar(db, $"SELECT common_name_en FROM taxon WHERE taxon_id = {taxonId}");

    [Fact]
    public void Build_RemovesJournalFilesLeftBesideTheOldDatabase() {
        var output = Path.Combine(_dir, "site.sqlite");
        File.WriteAllText(output, "old database");
        File.WriteAllText(output + "-journal", "stale journal");
        File.WriteAllText(output + "-wal", "stale wal");

        new SiteDbBuild(Inputs(output), QuietConsole()).Run(CancellationToken.None);

        Assert.False(File.Exists(output + "-journal"));
        Assert.False(File.Exists(output + "-wal"));
        using var db = OpenReadOnly(output);
        Assert.Equal("4", Scalar(db, "SELECT COUNT(*) FROM taxon"));
    }

    [Fact]
    public void Build_ThatFails_LeavesTheOldDatabaseAndNoTemporaryFile() {
        var output = Path.Combine(_dir, "site.sqlite");
        File.WriteAllText(output, "old database");
        var brokenCache = Path.Combine(_dir, "broken-cache.sqlite");
        using (var connection = new SqliteConnection($"Data Source={brokenCache};Pooling=False")) {
            connection.Open();
            Execute(connection, "CREATE TABLE unrelated (x INTEGER);");
        }
        var inputs = Inputs(output) with { ApiCache = brokenCache };

        Assert.Throws<SqliteException>(() => new SiteDbBuild(inputs, QuietConsole()).Run(CancellationToken.None));

        Assert.Equal("old database", File.ReadAllText(output));
        Assert.False(File.Exists(output + ".building"));
    }

    [Fact]
    public void Build_ThatIsCancelled_LeavesTheOldDatabase() {
        var output = Path.Combine(_dir, "site.sqlite");
        File.WriteAllText(output, "old database");
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() => new SiteDbBuild(Inputs(output), QuietConsole()).Run(cancelled.Token));

        Assert.Equal("old database", File.ReadAllText(output));
        Assert.False(File.Exists(output + ".building"));
    }

    // ------------------------------------------------------------ the build

    private string Build() {
        var output = Path.Combine(_dir, "site.sqlite");
        new SiteDbBuild(Inputs(output), QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    private SiteBuildInputs Inputs(string output) {
        var iucn = Path.Combine(_dir, "iucn.sqlite");
        var cache = Path.Combine(_dir, "cache.sqlite");
        var names = Path.Combine(_dir, "common_names.sqlite");
        var rules = Path.Combine(_dir, "rules-list.txt");
        var gbif = Path.Combine(_dir, "iucn-checklist-2026-07-28.zip");
        var colDir = Path.Combine(_dir, "col");
        if (!File.Exists(iucn)) {
            WriteIucn(iucn);
            WriteCache(cache);
            WriteCommonNames(names);
            File.WriteAllText(rules, "// manual common names\nUrsus nemo = nemo bear\n");
            using (var archive = Gbif.GbifIucnChecklistReaderTests.BuildArchive()) {
                File.WriteAllBytes(gbif, archive.ToArray());
            }
            WriteColZip(colDir);
        }
        return new SiteBuildInputs {
            IucnDatabase = iucn,
            ApiCache = cache,
            CommonNames = names,
            RulesList = rules,
            WikidataCache = Path.Combine(_dir, "missing-wikidata.sqlite"),
            GbifChecklist = gbif,
            // Only the file name is read, for the release.
            ColDatabase = Path.Combine(_dir, "col_coldp_COL26.7_XR.sqlite"),
            ColDir = colDir,
            Output = output,
        };
    }

    // A ColDP zip with an older release beside it; metadata.yaml cut down to the fields the citation uses.
    private static void WriteColZip(string folder) {
        Directory.CreateDirectory(folder);
        foreach (var (file, alias, version, issued) in new[] {
                     ("bf2146be.zip", "COL26.7 XR", "2026-07-17 XR", "2026-07-17"),
                     ("a1b2c3d4.zip", "COL26.5 XR", "2026-05-15 XR", "2026-05-15"),
                 }) {
            using var zip = ZipFile.Open(Path.Combine(folder, file), ZipArchiveMode.Create);
            using var writer = new StreamWriter(zip.CreateEntry("metadata.yaml").Open(), new UTF8Encoding(false));
            writer.Write($$"""
                ---
                key: 315834
                doi: {{(alias == "COL26.7 XR" ? "10.48580/dgykv" : "10.48580/dgyxx")}}
                title: Catalogue of Life
                alias: {{alias}}
                description: "The Catalogue of Life is building a comprehensive catalogue\
                  \ of all known species."
                issued: {{issued}}
                version: {{version}}
                creator:
                 -
                  orcid: 0000-0001-6197-9951
                  given: Olaf
                  family: Bánki
                  organisation: Catalogue of Life Foundation
                 -
                  given: Yury
                  family: Roskov
                 -
                  given: Diana Raquel
                  family: Hernández Robles
                publisher:
                  city: Amsterdam
                  country: NL
                  address: "Amsterdam, Netherlands"
                  organisation: Catalogue of Life Foundation
                license: cc by
                url: https://www.checklistbank.org/dataset/315834
                source:
                 -
                  id: 1
                  type: [this is not valid yaml and must not be read
                """);
        }
    }

    private static IAnsiConsole QuietConsole() => AnsiConsole.Create(new AnsiConsoleSettings {
        Out = new AnsiConsoleOutput(TextWriter.Null),
        Interactive = InteractionSupport.No,
    });

    // ------------------------------------------------------------ source fixtures

    private static void WriteIucn(string path) {
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        Execute(c, """
            CREATE TABLE import_metadata (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
            INSERT INTO import_metadata VALUES (1, '2026-1/a.zip', '2026-1', '2026-08-14', NULL);
            CREATE TABLE taxonomy_html (import_id INTEGER, taxonId INTEGER, scientificName TEXT, kingdomName TEXT, phylumName TEXT,
                className TEXT, orderName TEXT, familyName TEXT, genusName TEXT, speciesName TEXT, infraType TEXT, infraName TEXT,
                infraAuthority TEXT, subpopulationName TEXT, authority TEXT, taxonomicNotes TEXT);
            INSERT INTO taxonomy_html VALUES
                (1, 22823, 'Ursus maritimus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'URSIDAE', 'Ursus', 'maritimus', NULL, NULL, NULL, NULL, 'Phipps, 1774', '<p>NARRATIVE notes</p>'),
                (1, 900001, 'Ursus maritimus ssp. testus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'URSIDAE', 'Ursus', 'maritimus', 'subspecies', 'testus', NULL, NULL, 'Tester &amp; Other, 1999', NULL),
                (1, 900002, 'Ursus maritimus Test subpopulation', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'URSIDAE', 'Ursus', 'maritimus', NULL, NULL, NULL, 'Test subpopulation', 'Phipps, 1774', NULL),
                (1, 900003, 'Ursus nemo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'URSIDAE', 'Ursus', 'nemo', NULL, NULL, NULL, NULL, NULL, NULL);
            CREATE TABLE assessments_html (import_id INTEGER, assessmentId INTEGER, taxonId INTEGER, scientificName TEXT, redlistCategory TEXT,
                redlistCriteria TEXT, yearPublished TEXT, assessmentDate TEXT, criteriaVersion TEXT, language TEXT, rationale TEXT,
                populationTrend TEXT, possiblyExtinct TEXT, possiblyExtinctInTheWild TEXT, scopes TEXT);
            INSERT INTO assessments_html VALUES
                (1, 14871490, 22823, 'Ursus maritimus', 'Vulnerable', 'A3c', '2015', '2015-08-27 00:00:00 UTC', '3.1', 'English', 'NARRATIVE rationale', 'Unknown', 'false', 'false', 'Global'),
                (1, 217912462, 22823, 'Ursus maritimus', 'Vulnerable', 'A3c', '2025', '2022-12-09 00:00:00 UTC', '3.1', 'English', 'NARRATIVE', 'Stable', 'false', 'false', 'Europe'),
                (1, 900101, 900001, 'Ursus maritimus ssp. testus', 'Critically Endangered', 'D', '2020', '2020-01-01 00:00:00 UTC', '3.1', 'English', NULL, NULL, 'true', 'false', 'Global'),
                (1, 900102, 900002, 'Ursus maritimus Test subpopulation', 'Endangered', NULL, '2019', '2019-05-05 00:00:00 UTC', '3.1', 'English', NULL, 'Decreasing', 'false', 'false', 'Global & Europe'),
                (1, 900103, 900003, 'Ursus nemo', 'Least Concern', NULL, '2023', '2023-01-01 00:00:00 UTC', '3.1', 'English', NULL, NULL, 'false', 'false', NULL);
            """);
    }

    private static string Scope(string code, string description) =>
        $$"""[{"description":{"en":"{{description}}"},"code":"{{code}}"}]""";

    private static string Header(long id, long taxonId, bool latest, string? year, string date, string code, string scopes) =>
        $$"""{"assessment_id":{{id}},"sis_taxon_id":{{taxonId}},"latest":{{(latest ? "true" : "false")}},"year_published":{{(year is null ? "null" : $"\"{year}\"")}},"assessment_date":"{{date}}","red_list_category_code":"{{code}}","criteria":null,"possibly_extinct":false,"possibly_extinct_in_the_wild":false,"scopes":{{scopes}}}""";

    private static string Payload(long id, long taxonId, string year, string citation, string assessor, int people, string version, string? trend, string scopes) =>
        JsonSerializer.Serialize(new Dictionary<string, object?> {
            ["assessment_id"] = id,
            ["sis_taxon_id"] = taxonId,
            ["year_published"] = year,
            ["latest"] = false,
            ["citation"] = citation,
            ["taxon"] = new Dictionary<string, object?> { ["sis_id"] = taxonId, ["scientific_name"] = "Ursus maritimus", ["subpopulation_name"] = null },
            ["credits"] = new[] { new Dictionary<string, object?> {
                ["credit_type_name"] = "assessor", ["full"] = assessor, ["value"] = Enumerable.Range(1, people).Select(i => $"v{i}").ToArray(),
            } },
            ["errata"] = Array.Empty<object>(),
            ["scopes"] = JsonSerializer.Deserialize<JsonElement>(scopes),
            ["red_list_category"] = new Dictionary<string, object?> { ["version"] = version },
            ["population_trend"] = trend is null ? null : new Dictionary<string, object?> { ["description"] = new Dictionary<string, string> { ["en"] = trend } },
            ["documentation"] = new Dictionary<string, string> { ["rationale"] = "NARRATIVE rationale" },
        });

    private static void WriteCache(string path) {
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        Execute(c, """
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, root_sis_id INTEGER NOT NULL UNIQUE, downloaded_at TEXT NOT NULL, json TEXT NOT NULL);
            CREATE TABLE assessments (id INTEGER PRIMARY KEY, assessment_id INTEGER NOT NULL UNIQUE, sis_id INTEGER NOT NULL, downloaded_at TEXT NOT NULL, json TEXT NOT NULL);
            CREATE TABLE taxa_lookup (sis_id INTEGER NOT NULL, taxa_id INTEGER NOT NULL, root_sis_id INTEGER NOT NULL, scope TEXT NOT NULL, PRIMARY KEY (sis_id, taxa_id));
            """);
        var global = Scope("1", "Global");
        var europe = Scope("2", "Europe");
        var polarBear = $$"""
            {"sis_id":22823,"taxon":{"sis_id":22823,"scientific_name":"Ursus maritimus","species_taxa":[],
              "common_names":[{"main":false,"name":"Ours blanc","language":"fre"},{"main":true,"name":"Polar Bear","language":"eng"},
                              {"main":false,"name":"Bear of the north","language":"und"}],
              "synonyms":[{"name":"Thalarctos maritimus (Phipps, 1774)","genus_name":"Thalarctos","species_name":"maritimus","species_author":"(Phipps, 1774)","infra_type":null,"infra_name":null,"subpopulation_name":null}],
              "subpopulation_taxa":[{"sis_id":900002,"scientific_name":"Ursus maritimus Test subpopulation","common_names":[{"main":true,"name":"Test Bear","language":"eng"}],"synonyms":[]}]},
             "assessments":[
               {{Header(PolarBearEurope, PolarBear, true, "2025", "2022-12-09T00:00:00.000+00:00", "VU", europe)}},
               {{Header(PolarBearGlobal, PolarBear, true, "2015", "2015-08-27T01:00:00.000+01:00", "VU", global)}},
               {{Header(PolarBear1996, PolarBear, false, "1996", "1996-08-01T01:00:00.000+01:00", "LR/cd", global)}},
               {{Header(PolarBear2005, PolarBear, false, "2005", "2005-01-01T00:00:00.000+00:00", "VU", global)}},
               {{Header(PolarBear2006Amended, PolarBear, false, "2006", "2005-01-01T00:00:00.000+00:00", "VU", global)}},
               {{Header(PolarBear2008, PolarBear, false, "2008", "2008-06-30T00:00:00.000+00:00", "VU", global)}},
               {{Header(PolarBear2008Errata, PolarBear, false, "2008", "2008-06-30T00:00:00.000+00:00", "VU", global)}},
               {{Header(PolarBearDraft, PolarBear, false, null, "2027-01-01T00:00:00.000+00:00", "EN", global)}},
               {{Header(PolarBearNoScope, PolarBear, false, "2001", "2001-01-01T00:00:00.000+00:00", "EN", "[]")}}]}
            """;
        var subspecies = $$"""
            {"sis_id":900001,"taxon":{"sis_id":900001,"scientific_name":"Ursus maritimus ssp. testus","species_taxa":[{"sis_id":22823}],"common_names":[],"synonyms":[]},
             "assessments":[{{Header(SubspeciesLatest, Subspecies, true, "2020", "2020-01-01T00:00:00.000+00:00", "CR", global)}}]}
            """;
        Execute(c, "INSERT INTO taxa (id, root_sis_id, downloaded_at, json) VALUES (1, 22823, '2026-08-18T00:00:00Z', @json)", ("@json", polarBear));
        Execute(c, "INSERT INTO taxa (id, root_sis_id, downloaded_at, json) VALUES (2, 900001, '2026-08-18T00:00:00Z', @json)", ("@json", subspecies));
        Execute(c, "INSERT INTO taxa_lookup VALUES (900002, 1, 22823, 'subpopulation')");

        var payloads = new (long Id, long Taxon, string Downloaded, string Json)[] {
            (PolarBearGlobal, PolarBear, "2026-08-21T03:23:18.6842353Z", Payload(PolarBearGlobal, PolarBear, "2015",
                "Wiig, Ø. & Amstrup, S. 2015. Ursus maritimus. The IUCN Red List of Threatened Species 2015: e.T22823A14871490. https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en. Accessed on 21 August 2026.",
                "Wiig, Ø. & Amstrup, S.", 2, "3.1", "Unknown", global)),
            (PolarBearEurope, PolarBear, "2026-08-22T00:00:00Z", Payload(PolarBearEurope, PolarBear, "2025",
                "Wiig, Ø. 2025. Ursus maritimus (Europe assessment). The IUCN Red List of Threatened Species 2025: e.T22823A217912462. Accessed on 22 August 2026.",
                "Wiig, Ø.", 1, "3.1", "Stable", europe)),
            (PolarBear1996, PolarBear, "2026-08-24T03:15:26.3246382Z", Payload(PolarBear1996, PolarBear, "1996",
                "Polar Bear Specialist Group 1996. Ursus maritimus. The IUCN Red List of Threatened Species 1996: e.T22823A9390941. Accessed on 24 August 2026.",
                "Polar Bear Specialist Group", 1, "2.3", null, global)),
            // Read before the 2006 assessment that has the name right, so it waits for the name pool.
            (PolarBear2005, PolarBear, "2026-08-23T00:00:00Z", Payload(PolarBear2005, PolarBear, "2005",
                "Kry?tufek, B. 2005. Ursus maritimus. The IUCN Red List of Threatened Species 2005: e.T22823A9390905. Accessed on 23 August 2026.",
                "Kry?tufek, B.", 1, "3.1", null, global)),
            (PolarBear2006Amended, PolarBear, "2026-08-23T00:00:00Z", Payload(PolarBear2006Amended, PolarBear, "2006",
                "Kryštufek, B. 2006. Ursus maritimus (amended version of 2005 assessment). The IUCN Red List of Threatened Species 2006: e.T22823A9390906. Accessed on 23 August 2026.",
                "Kryštufek, B.", 1, "3.1", null, global)),
            (PolarBear2008, PolarBear, "2026-08-23T00:00:00Z", Payload(PolarBear2008, PolarBear, "2008",
                "Wiig, Ø. 2008. Ursus maritimus. The IUCN Red List of Threatened Species 2008: e.T22823A9390950. Accessed on 23 August 2026.",
                "Wiig, Ø.", 1, "3.1", null, global)),
            (PolarBear2008Errata, PolarBear, "2026-08-23T00:00:00Z", Payload(PolarBear2008Errata, PolarBear, "2008",
                $"Wiig, Ø. & Mo{Lost}brucker, H. 2008. Ursus maritimus (errata version published in 2009). The IUCN Red List of Threatened Species 2008: e.T22823A9390951. Accessed on 23 August 2026.",
                $"Wiig, Ø. & Mo{Lost}brucker, H.", 2, "3.1", null, global)),
        };
        foreach (var (id, taxon, downloaded, json) in payloads) {
            Execute(c, "INSERT INTO assessments (assessment_id, sis_id, downloaded_at, json) VALUES (@id, @taxon, @downloaded, @json)",
                ("@id", id), ("@taxon", taxon), ("@downloaded", downloaded), ("@json", json));
        }
    }

    private static void WriteCommonNames(string path) {
        using (CommonNameStore.Open(path)) {
            // Creates the schema.
        }
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        Execute(c, """
            INSERT INTO taxa (id, canonical_name, original_name, rank, kingdom, validity_status, primary_source, primary_source_id, created_at, updated_at) VALUES
                (1, 'ursus maritimus', 'Ursus maritimus', 'species', 'ANIMALIA', 'valid', 'iucn', '22823', 'x', 'x'),
                (2, 'ursus nemo', 'Ursus nemo', 'species', 'ANIMALIA', 'valid', 'iucn', '900003', 'x', 'x'),
                (3, 'ursus maritimus testus', 'Ursus maritimus ssp. testus', 'subspecies', 'ANIMALIA', 'valid', 'iucn', '900001', 'x', 'x'),
                (4, 'ursus maritimus', 'Ursus maritimus Test subpopulation', 'species', 'ANIMALIA', 'valid', 'iucn', '900002', 'x', 'x');
            INSERT INTO common_names (taxon_id, raw_name, normalized_name, language, source, source_identifier, is_preferred, created_at) VALUES
                (1, 'Polar Bear', 'polarbear', 'en', 'iucn', '22823', 1, 'x'),
                (1, 'Polar bear', 'polarbear', 'en', 'wikipedia_title', 'Polar bear', 1, 'x'),
                (1, 'Ours polaire', 'ourspolaire', 'fr', 'iucn', '22823', 0, 'x'),
                (2, 'Shared name', 'sharedname', 'en', 'col', 'C1', 0, 'x'),
                (3, 'Shared name', 'sharedname', 'en', 'col', 'C2', 0, 'x'),
                (3, 'Ursus maritimus testus', 'ursusmaritimustestus', 'en', 'wikidata_label', 'Q1', 0, 'x'),
                (4, 'Shared name', 'sharedname', 'en', 'wikipedia_title', 'Shared name', 1, 'x'),
                (4, 'Test Bear', 'testbear', 'en', 'col', 'C3', 0, 'x');
            INSERT INTO scientific_name_synonyms (taxon_id, normalized_name, original_name, source, synonym_type, created_at) VALUES
                (1, 'ursus marinus', 'Ursus marinus', 'col', 'synonym', 'x'),
                (1, 'thalarctos maritimus', 'Thalarctos maritimus', 'col', 'synonym', 'x'),
                (1, 'ursus ambiguus', 'Ursus ambiguus', 'col', 'ambiguous_synonym', 'x'),
                (1, 'ursus maritimus', 'Ursus maritimus', 'constructed', 'rank_variant', 'x');
            """);
    }

    // ------------------------------------------------------------ SQL helpers

    private static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    private static List<string> SchemaOf(SqliteConnection c) =>
        Rows(c, "SELECT type || ' ' || name || ': ' || COALESCE(sql, '') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name")
            .Select(r => (string)r[0]!).ToList();

    private static void Execute(SqliteConnection c, string sql, params (string Name, object? Value)[] parameters) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        command.ExecuteNonQuery();
    }

    private static string? Scalar(SqliteConnection c, string sql) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar() is { } value and not DBNull ? Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) : null;
    }

    private static List<object?[]> Rows(SqliteConnection c, string sql, params (string Name, object? Value)[] parameters) {
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
