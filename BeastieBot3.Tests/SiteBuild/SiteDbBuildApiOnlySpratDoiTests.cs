using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db` over tiny source databases, for three things the site database holds since schema
// version 3:
//   - taxa that are in the IUCN API cache but not in the CSV export (in_release = 0): the Amur
//     leopard (15957, an IUCN subspecies no longer assessed) and an old id of Bettongia penicillata
//     (2785) whose name is now taxon 2790's;
//   - SPRAT profiles and EPBC listings that apply to a population (the koala: profile 197 has no
//     listing, profile 85104 lists "combined populations of Qld, NSW and the ACT" as Endangered);
//   - DOIs found by checking doi.org (`iucn resolve-dois`'s doi_check table).
// Ids and names are IUCN's and SPRAT's where they are known; the assessments are cut down.
public sealed class SiteDbBuildApiOnlySpratDoiTests : IDisposable {
    private const long Leopard = 15954;
    private const long LeopardLatest = 50659089;
    private const long AmurLeopard = 15957;
    private const long AmurLeopard2016Ne = 96947390;
    private const long AmurLeopard2008 = 5333757;
    private const long AmurLeopard1996 = 5333803;
    private const long Koala = 16892;
    private const long KoalaLatest = 166496779;
    private const long Woylie = 2790;
    private const long WoylieLatest = 2790001;
    private const long WoylieOld = 2785;
    private const long WoylieOld2008 = 6143;
    private const string KoalaDoi = "10.2305/IUCN.UK.2016-1.RLTS.T16892A166496779.en";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "beastiebot-sitebuild-tests", Guid.NewGuid().ToString("N"));

    public SiteDbBuildApiOnlySpratDoiTests() {
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    // ------------------------------------------------------------ taxa not in the release

    [Fact]
    public void Build_AddsTaxaThatAreOnlyInTheApiCache() {
        using var db = OpenReadOnly(Build());
        var row = Rows(db, $"""
            SELECT scientific_name, kind, kingdom, family, genus, species_epithet, infra_rank, infra_name, authority,
                   parent_taxon_id, latest_global_assessment_id, in_release, current_taxon_id
            FROM taxon WHERE taxon_id = {AmurLeopard}
            """).Single();

        Assert.Equal("Panthera pardus ssp. orientalis", row[0]);
        Assert.Equal("subspecies", row[1]);
        Assert.Equal(("ANIMALIA", "FELIDAE", "Panthera", "pardus", "ssp.", "orientalis"),
            ((string)row[2]!, (string)row[3]!, (string)row[4]!, (string)row[5]!, (string)row[6]!, (string)row[7]!));
        Assert.Equal("(Schlegel, 1857)", row[8]);
        // The species from the record's species_taxa.
        Assert.Equal(Leopard, row[9]);
        Assert.Null(row[10]);
        Assert.Equal(0L, row[11]);
        // No taxon in the release has its name.
        Assert.Null(row[12]);

        Assert.Equal(1L, Convert.ToInt64(Scalar(db, $"SELECT in_release FROM taxon WHERE taxon_id = {Koala}")));
        Assert.Equal(TaxonCount, Convert.ToInt64(Scalar(db, "SELECT COUNT(*) FROM taxon")));
        Assert.Equal(TaxonCount.ToString(), Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.TaxonCount}'"));
    }

    // Every assessment of a taxon not in the release is an earlier one, including the header the API
    // still flags latest; their citations are parsed from the cached payloads.
    [Fact]
    public void Build_StoresEveryAssessmentOfATaxonNotInTheReleaseAsEarlier() {
        using var db = OpenReadOnly(Build());
        var rows = Rows(db, $"""
            SELECT assessment_id, is_latest, category, year_published, citation_json IS NOT NULL
            FROM assessment WHERE taxon_id = {AmurLeopard} ORDER BY assessment_id
            """);

        Assert.Equal(new[] { AmurLeopard2008, AmurLeopard1996, AmurLeopard2016Ne }.Order(), rows.Select(r => (long)r[0]!));
        Assert.All(rows, r => Assert.Equal(0L, r[1]));
        Assert.Equal("NE", rows.Single(r => (long)r[0]! == AmurLeopard2016Ne)[2]);
        Assert.Equal(1L, rows.Single(r => (long)r[0]! == AmurLeopard2008)[4]);
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM assessment a JOIN taxon t ON t.taxon_id = a.taxon_id WHERE t.in_release = 0 AND a.is_latest = 1"));
    }

    [Fact]
    public void Build_LinksATaxonNotInTheReleaseToTheTaxonWithItsName() {
        using var db = OpenReadOnly(Build());
        var row = Rows(db, $"SELECT in_release, current_taxon_id, kind FROM taxon WHERE taxon_id = {WoylieOld}").Single();

        Assert.Equal((0L, Woylie, "species"), ((long)row[0]!, (long)row[1]!, (string)row[2]!));
        Assert.Equal("Global", Scalar(db, $"SELECT scope FROM assessment WHERE assessment_id = {WoylieOld2008}"));
        // Its names come from its own record and are searchable like any other.
        Assert.Contains(Rows(db, $"SELECT name FROM name WHERE taxon_id = {WoylieOld} AND name_type = 'common'"), r => (string)r[0]! == "Woylie");
        Assert.Contains(Rows(db, $"SELECT n.taxon_id FROM name_key k JOIN name n ON n.name_id = k.name_id WHERE k.key = 'amur leopard'"),
            r => (long)r[0]! == AmurLeopard);
    }

    // ------------------------------------------------------------ SPRAT and the EPBC Act

    [Fact]
    public void Build_StoresTheKoalasSpratProfiles_AndThePopulationListing() {
        using var db = OpenReadOnly(Build());
        var rows = Rows(db, $"""
            SELECT sprat_taxon_id, listed_name, status, applies_to, population
            FROM epbc_listing WHERE taxon_id = {Koala} ORDER BY sprat_taxon_id
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new object?[] { 197L, "Phascolarctos cinereus", null, "taxon", null }, rows[0]);
        Assert.Equal(new object?[] { 85104L, "Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)", "EN", "population",
            "combined populations of Qld, NSW and the ACT" }, rows[1]);
    }

    // A listed name that differs from SPRAT's scientific name is stored; a voucher in brackets is
    // not a population; a sense in brackets is the whole taxon.
    [Fact]
    public void Build_TellsPopulationsFromVouchersAndSenses() {
        using var db = OpenReadOnly(Build());
        var leopard = Rows(db, $"SELECT sprat_taxon_id, listed_name, status, applies_to FROM epbc_listing WHERE taxon_id = {Leopard}");
        var woylie = Rows(db, $"SELECT sprat_taxon_id, applies_to, status FROM epbc_listing WHERE taxon_id = {Woylie}");

        Assert.Equal(new object?[] { 90001L, "Panthera pardus melas", "VU", "taxon" }, Assert.Single(leopard));
        // The sense row goes to the taxon in the release, not the old id with the same name.
        Assert.Equal(new object?[] { 90003L, "taxon", "EN" }, Assert.Single(woylie));
        Assert.Equal("0", Scalar(db, $"SELECT COUNT(*) FROM epbc_listing WHERE taxon_id = {WoylieOld} OR sprat_taxon_id = 90002"));
    }

    [Theory]
    [InlineData("Phascolarctos cinereus", "Phascolarctos cinereus", "Taxon", null)]
    [InlineData("Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)", "Phascolarctos cinereus",
        "Population", "combined populations of Qld, NSW and the ACT")]
    [InlineData("Rhinonicteris aurantia (Pilbara form)", "Rhinonicteris aurantia", "Population", "Pilbara form")]
    [InlineData("Tursiops aduncus (Arafura/Timor Sea populations)", "Tursiops aduncus", "Population", "Arafura/Timor Sea populations")]
    [InlineData("Dasyurus maculatus maculatus (sensu lato)", "Dasyurus maculatus maculatus", "Taxon", null)]
    [InlineData("Acacia sp. Castletower (N.Gibson TOI345)", "Acacia sp. Castletower", "NotPopulation", null)]
    [InlineData("Erythroxylum sp. Cholmondely Creek (J.R.Clarkson 9367) (Northern Territory Population)", "Erythroxylum sp. Cholmondely Creek",
        "NotPopulation", null)]
    [InlineData("Erythroxylum sp. Cholmondely Creek (J.R.Clarkson 9367) (Northern Territory Population)",
        "Erythroxylum sp. Cholmondely Creek (J.R.Clarkson 9367)", "Population", "Northern Territory Population")]
    [InlineData("Dasyurus maculatus maculatus (Tasmanian population)", "Dasyurus maculatus", "None", null)]
    [InlineData("Carcharias taurusx (east coast population)", "Carcharias taurus", "None", null)]
    public void ClassifySpratName_FindsPopulations(string spratName, string taxonName, string kind, string? population) =>
        Assert.Equal(new SpratNameMatch(Enum.Parse<SpratNameKind>(kind), population), SiteBuildRules.ClassifySpratName(spratName, taxonName));

    [Theory]
    [InlineData(false, false, "Panthera pardus", "species")]
    [InlineData(true, false, "Panthera pardus ssp. orientalis", "subspecies")]
    [InlineData(true, false, "Olea europaea subsp. cerasiformis", "subspecies")]
    [InlineData(true, false, "Monodora junodii var. macrantha", "variety")]
    [InlineData(false, true, "Panthera leo West Africa subpopulation", "subpopulation")]
    public void KindFromApiFlags_ReadsTheRecordsFlags(bool infrarank, bool subpopulation, string name, string expected) =>
        Assert.Equal(expected, SiteBuildRules.KindFromApiFlags(infrarank, subpopulation, name));

    // ------------------------------------------------------------ DOIs found by checking doi.org

    [Fact]
    public void Build_UsesADoiFoundAtDoiOrg_WhenNoOtherSourceHasOne() {
        using var db = OpenReadOnly(Build());
        var koala = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {KoalaLatest}"))!;
        // doi_check names a DOI with another taxon id for this assessment: rejected.
        var amur = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {AmurLeopard2008}"))!;
        // IUCN's own citation text has a DOI, which comes first.
        var leopard = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {LeopardLatest}"))!;

        Assert.Equal((KoalaDoi, DoiSource.Resolved), (koala.Doi, koala.DoiSource));
        Assert.Equal((null, DoiSource.None), (amur.Doi, amur.DoiSource));
        Assert.Equal(DoiSource.Citation, leopard.DoiSource);
        Assert.Equal("2026-09-30", Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.IucnDoiCheckedTo}'"));
    }

    [Fact]
    public void Build_WithADoiCacheThatHasNoTable_WarnsAndCarriesOn() {
        var empty = Path.Combine(_dir, "empty-doi-cache.sqlite");
        using (var c = new SqliteConnection($"Data Source={empty};Pooling=False")) {
            c.Open();
            Execute(c, "CREATE TABLE something_else (x INTEGER);");
        }
        var output = Path.Combine(_dir, "site-no-doi.sqlite");
        var stats = new SiteDbBuild(Inputs(output) with { DoiCache = empty }, QuietConsole()).Run(CancellationToken.None);

        Assert.Contains(stats.Warnings, w => w.Contains("no doi_check table", StringComparison.Ordinal));
        using var db = OpenReadOnly(output);
        var koala = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {KoalaLatest}"))!;
        Assert.Null(koala.Doi);
        Assert.Null(Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.IucnDoiCheckedTo}'"));
    }

    [Fact]
    public void Select_TakesTheDoiFoundAtDoiOrgLast() {
        var parts = new IucnCitationParts { TaxonId = Koala, AssessmentId = KoalaLatest, Year = 2016, ScientificName = "Phascolarctos cinereus" };
        Assert.Equal(new DoiChoice(KoalaDoi, DoiSource.Resolved), IucnDoiSelector.Select(parts, null, null, null, null, KoalaDoi));
        Assert.Equal(new DoiChoice(KoalaDoi, DoiSource.Wikidata), IucnDoiSelector.Select(parts, null, null, new[] { KoalaDoi }, null, KoalaDoi));
        Assert.Equal(new DoiChoice(null, DoiSource.None),
            IucnDoiSelector.Select(parts, null, null, null, null, "10.2305/IUCN.UK.2016-1.RLTS.T16893A166496779.en"));
    }

    // ------------------------------------------------------------ the build

    private const long TaxonCount = 5;

    private string Build() {
        var output = Path.Combine(_dir, "site.sqlite");
        if (!File.Exists(output)) {
            new SiteDbBuild(Inputs(output), QuietConsole()).Run(CancellationToken.None);
        }
        return output;
    }

    private SiteBuildInputs Inputs(string output) {
        var iucn = Path.Combine(_dir, "iucn.sqlite");
        var cache = Path.Combine(_dir, "cache.sqlite");
        var sprat = Path.Combine(_dir, "sprat.sqlite");
        var doiCache = Path.Combine(_dir, "iucn_doi_cache.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucn(iucn);
            WriteCache(cache);
            WriteSprat(sprat);
            WriteDoiCache(doiCache);
        }
        return new SiteBuildInputs {
            IucnDatabase = iucn,
            ApiCache = cache,
            SpratDatabase = sprat,
            DoiCache = doiCache,
            Output = output,
        };
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
                infraAuthority TEXT, subpopulationName TEXT, authority TEXT);
            INSERT INTO taxonomy_html VALUES
                (1, 15954, 'Panthera pardus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'pardus', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)'),
                (1, 16892, 'Phascolarctos cinereus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'DIPROTODONTIA', 'PHASCOLARCTIDAE', 'Phascolarctos', 'cinereus', NULL, NULL, NULL, NULL, '(Goldfuss, 1817)'),
                (1, 2790, 'Bettongia penicillata', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'DIPROTODONTIA', 'POTOROIDAE', 'Bettongia', 'penicillata', NULL, NULL, NULL, NULL, 'Gray, 1837');
            CREATE TABLE assessments_html (import_id INTEGER, assessmentId INTEGER, taxonId INTEGER, scientificName TEXT, redlistCategory TEXT,
                redlistCriteria TEXT, yearPublished TEXT, assessmentDate TEXT, criteriaVersion TEXT, populationTrend TEXT,
                possiblyExtinct TEXT, possiblyExtinctInTheWild TEXT, scopes TEXT);
            INSERT INTO assessments_html VALUES
                (1, 50659089, 15954, 'Panthera pardus', 'Vulnerable', 'A2cd', '2024', '2023-01-01 00:00:00 UTC', '3.1', 'Decreasing', 'false', 'false', 'Global'),
                (1, 166496779, 16892, 'Phascolarctos cinereus', 'Vulnerable', 'A2bc', '2016', '2014-07-08 00:00:00 UTC', '3.1', 'Decreasing', 'false', 'false', 'Global'),
                (1, 2790001, 2790, 'Bettongia penicillata', 'Critically Endangered', 'A3e', '2015', '2014-01-01 00:00:00 UTC', '3.1', 'Decreasing', 'false', 'false', 'Global');
            """);
    }

    private static string Scope(string code, string description) =>
        $$"""[{"description":{"en":"{{description}}"},"code":"{{code}}"}]""";

    private static string Header(long id, long taxonId, bool latest, string year, string code) =>
        $$"""{"assessment_id":{{id}},"sis_taxon_id":{{taxonId}},"latest":{{(latest ? "true" : "false")}},"year_published":"{{year}}","assessment_date":"{{year}}-01-01T00:00:00.000+00:00","red_list_category_code":"{{code}}","criteria":null,"possibly_extinct":false,"possibly_extinct_in_the_wild":false,"scopes":{{Scope("1", "Global")}}}""";

    private static string Payload(long id, long taxonId, string name, string year, string citation, string assessor) =>
        JsonSerializer.Serialize(new Dictionary<string, object?> {
            ["assessment_id"] = id,
            ["sis_taxon_id"] = taxonId,
            ["year_published"] = year,
            ["latest"] = false,
            ["citation"] = citation,
            ["taxon"] = new Dictionary<string, object?> { ["sis_id"] = taxonId, ["scientific_name"] = name, ["subpopulation_name"] = null },
            ["credits"] = new[] { new Dictionary<string, object?> {
                ["credit_type_name"] = "assessor", ["full"] = assessor, ["value"] = new[] { "v1" },
            } },
            ["errata"] = Array.Empty<object>(),
            ["scopes"] = JsonSerializer.Deserialize<JsonElement>(Scope("1", "Global")),
            ["red_list_category"] = new Dictionary<string, object?> { ["version"] = "3.1" },
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
        var amurLeopard = $$"""
            {"sis_id":15957,"taxon":{"sis_id":15957,"scientific_name":"Panthera pardus ssp. orientalis",
              "species_taxa":[{"sis_id":15954,"scientific_name":"Panthera pardus"}],"subpopulation_taxa":[],"infrarank_taxa":[],
              "kingdom_name":"ANIMALIA","phylum_name":"CHORDATA","class_name":"MAMMALIA","order_name":"CARNIVORA","family_name":"FELIDAE",
              "genus_name":"Panthera","species_name":"pardus","subpopulation_name":null,"infra_name":"orientalis","authority":"(Schlegel, 1857)",
              "species":false,"subpopulation":false,"infrarank":true,
              "common_names":[{"main":false,"name":"Bars","language":"rus"},{"main":true,"name":"Amur Leopard","language":"eng"}],
              "synonyms":[{"name":"Felis orientalis Schlegel, 1857","genus_name":"Felis","species_name":"orientalis","infra_type":null,"infra_name":null,"subpopulation_name":null}]},
             "assessments":[
               {{Header(AmurLeopard2016Ne, AmurLeopard, false, "2016", "NE")}},
               {{Header(AmurLeopard2008, AmurLeopard, true, "2008", "CR")}},
               {{Header(AmurLeopard1996, AmurLeopard, false, "1996", "CR")}}]}
            """;
        var woylieOld = $$"""
            {"sis_id":2785,"taxon":{"sis_id":2785,"scientific_name":"Bettongia penicillata","species_taxa":[],"subpopulation_taxa":[],
              "kingdom_name":"ANIMALIA","phylum_name":"CHORDATA","class_name":"MAMMALIA","order_name":"DIPROTODONTIA","family_name":"POTOROIDAE",
              "genus_name":"Bettongia","species_name":"penicillata","subpopulation_name":null,"infra_name":null,"authority":"Gray, 1837",
              "species":true,"subpopulation":false,"infrarank":false,
              "common_names":[{"main":true,"name":"Woylie","language":"eng"}],"synonyms":[]},
             "assessments":[{{Header(WoylieOld2008, WoylieOld, false, "2008", "CR")}}]}
            """;
        var koala = $$"""
            {"sis_id":16892,"taxon":{"sis_id":16892,"scientific_name":"Phascolarctos cinereus","species_taxa":[],"subpopulation_taxa":[],
              "species":true,"subpopulation":false,"infrarank":false,
              "common_names":[{"main":true,"name":"Koala","language":"eng"}],"synonyms":[]},
             "assessments":[{{Header(KoalaLatest, Koala, true, "2016", "VU")}}]}
            """;
        var rows = new[] { (1, AmurLeopard, amurLeopard), (2, WoylieOld, woylieOld), (3, Koala, koala) };
        foreach (var (id, root, json) in rows) {
            Execute(c, "INSERT INTO taxa (id, root_sis_id, downloaded_at, json) VALUES (@id, @root, '2026-08-18T00:00:00Z', @json)",
                ("@id", id), ("@root", root), ("@json", json));
        }

        var payloads = new (long Id, long Taxon, string Name, string Year, string Citation, string Assessor)[] {
            (AmurLeopard2016Ne, AmurLeopard, "Panthera pardus ssp. orientalis", "2016", "Stein, A.B. 2016. Panthera pardus ssp. orientalis. The IUCN Red List of Threatened Species 2016: e.T15957A96947390. Accessed on 18 August 2026.", "Stein, A.B."),
            (AmurLeopard2008, AmurLeopard, "Panthera pardus ssp. orientalis", "2008", "Jackson, P. 2008. Panthera pardus ssp. orientalis. The IUCN Red List of Threatened Species 2008: e.T15957A5333757. Accessed on 18 August 2026.", "Jackson, P."),
            (KoalaLatest, Koala, "Phascolarctos cinereus", "2016", "Woinarski, J. 2016. Phascolarctos cinereus. The IUCN Red List of Threatened Species 2016: e.T16892A166496779. Accessed on 18 August 2026.", "Woinarski, J."),
            (LeopardLatest, Leopard, "Panthera pardus", "2024", "Stein, A.B. 2024. Panthera pardus. The IUCN Red List of Threatened Species 2024: e.T15954A50659089. https://dx.doi.org/10.2305/IUCN.UK.2024-1.RLTS.T15954A50659089.en. Accessed on 18 August 2026.", "Stein, A.B."),
        };
        foreach (var (id, taxon, name, year, citation, assessor) in payloads) {
            Execute(c, "INSERT INTO assessments (assessment_id, sis_id, downloaded_at, json) VALUES (@id, @taxon, '2026-08-21T00:00:00Z', @json)",
                ("@id", id), ("@taxon", taxon), ("@json", Payload(id, taxon, name, year, citation, assessor)));
        }
    }

    // Only the columns `site build-db` reads.
    private static void WriteSprat(string path) {
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        Execute(c, """
            CREATE TABLE import_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
            INSERT INTO import_metadata (filename, redlist_version, started_at) VALUES ('25062026-070407-report.csv', 'x', 'x');
            CREATE TABLE sprat_species (import_id INTEGER NOT NULL, sprat_taxon_id TEXT, scientific_name TEXT, epbc_status TEXT,
                EPBC_Threatened_Species_Listed_Name TEXT, IUCN_Red_List_Listed_Names TEXT);
            INSERT INTO sprat_species VALUES
                (1, '197', 'Phascolarctos cinereus', NULL, NULL, 'Phascolarctos cinereus'),
                (1, '85104', 'Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)', 'Endangered',
                    'Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)', NULL),
                (1, '90001', 'Panthera pardus', 'Vulnerable', 'Panthera pardus melas', 'Panthera pardus'),
                (1, '90002', 'Panthera pardus (A.B.Smith 123)', 'Endangered', NULL, NULL),
                (1, '90003', 'Bettongia penicillata (sensu lato)', 'Endangered', NULL, NULL);
            """);
    }

    private static void WriteDoiCache(string path) {
        using var c = new SqliteConnection($"Data Source={path};Pooling=False");
        c.Open();
        Execute(c, """
            CREATE TABLE doi_check (assessment_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL, doi TEXT, checked_at TEXT NOT NULL,
                candidates_tried INTEGER NOT NULL);
            INSERT INTO doi_check VALUES
                (166496779, 16892, '10.2305/IUCN.UK.2016-1.RLTS.T16892A166496779.en', '2026-09-30T23:10:00.0000000Z', 3),
                (5333757, 15957, '10.2305/IUCN.UK.2008.RLTS.T15958A5333757.en', '2026-09-29T10:00:00.0000000Z', 2),
                (2790001, 2790, NULL, '2026-09-28T10:00:00.0000000Z', 4);
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

    private static List<object?[]> Rows(SqliteConnection c, string sql) {
        using var command = c.CreateCommand();
        command.CommandText = sql;
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
