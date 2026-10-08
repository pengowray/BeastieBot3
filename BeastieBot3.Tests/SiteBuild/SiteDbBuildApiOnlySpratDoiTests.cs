using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db` over tiny source databases, for three things the site database holds since schema
// version 3:
//   - taxa that are in the IUCN API cache but not in the CSV export (in_release = 0): the Amur
//     leopard (15957, an IUCN subspecies no longer assessed) and an old id of Bettongia penicillata
//     (2785) whose name is now taxon 2790's;
//   - SPRAT profiles and EPBC listings that apply to a population (the koala: profile 197 has no
//     listing, profile 85104 lists "combined populations of Qld, NSW and the ACT" as Endangered);
//   - DOIs `iucn resolve-dois` found in Crossref's list or at doi.org (its doi_check table).
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

    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

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

    // other_status: the EPBC Act listing and the state and territory statuses of each profile the
    // taxon got, with the population of a population's profile. A listed name that is the taxon's own
    // name, with or without the population, is left out.
    [Fact]
    public void Build_StoresTheEpbcAndStateStatusesOfEachProfile() {
        using var db = OpenReadOnly(Build());
        var koala = Rows(db, $"""
            SELECT system, status, listed_name, population, source, source_id, listed_on
            FROM other_status WHERE taxon_id = {Koala} AND source = 'sprat' ORDER BY source_id, system
            """);
        var leopard = Rows(db, $"SELECT system, status, listed_name, listed_on FROM other_status WHERE taxon_id = {Leopard} AND source = 'sprat' ORDER BY system");

        Assert.Equal(new[] {
            new object?[] { "au-nsw", "Endangered", null, null, "sprat", "197", null },
            new object?[] { "au-qld", "Endangered", null, null, "sprat", "197", null },
            new object?[] { "au-act", "Endangered", null, "combined populations of Qld, NSW and the ACT", "sprat", "85104", null },
            new object?[] { "au-epbc", "Endangered", null, "combined populations of Qld, NSW and the ACT", "sprat", "85104", "2022-02-12" },
        }, koala);
        Assert.Equal(new[] {
            new object?[] { "au-epbc", "Vulnerable", "Panthera pardus melas", "2000-07-16" },
            new object?[] { "au-nsw", "Vulnerable", "Panthera pardus melas", null },
        }, leopard);
    }

    // NatureServe and ECOS rows: a taxon gets the Standard NatureServe record of its name before a
    // Provisional one, an infraspecific taxon is found without IUCN's "ssp.", a record under another
    // name is found by its synonym (and keeps that name), unranked ranks are left out, and an ECOS
    // listing wherever found is a listing of the whole taxon.
    [Fact]
    public void Build_StoresNatureServeAndEcosStatuses() {
        using var db = OpenReadOnly(Build());
        IReadOnlyList<object?[]> Of(long taxonId) => Rows(db, $"""
            SELECT system, status, status_code, listed_name, population, source, source_id, listed_on
            FROM other_status WHERE taxon_id = {taxonId} AND source <> 'sprat' ORDER BY system, source_id
            """);

        Assert.Equal(new[] {
            new object?[] { "br-salve", "Critically Endangered (Possibly Extinct)", "CR(PE)", null, null, "salve", "abc1", "2022-08-19" },
            new object?[] { "ca-cosewic", "Extirpated", null, null, null, "natureserve", "101", null },
            new object?[] { "ca-sara", "Extirpated", null, null, null, "natureserve", "101", null },
            new object?[] { "natureserve-global", "G4G5", "G4", null, null, "natureserve", "101", null },
            new object?[] { "nz-nztcs", "Introduced and Naturalised", null, null, null, "nztcs", "5001", null },
            new object?[] { "us-esa", "Endangered", null, null, null, "ecos", "7001", "1970-06-02" },
            new object?[] { "us-esa", "Threatened", null, null, "Gabon, Congo southward", "ecos", "7002", "1982-01-28" },
        }, Of(Leopard));
        Assert.Equal("Mammals 2024 (Example et al. 2024)", Scalar(db, "SELECT report FROM other_status WHERE source_id = '5001'"));
        Assert.Equal("https://nztcs.org.nz/assessments/5001", Scalar(db, "SELECT url FROM other_status WHERE source_id = '5001'"));
        Assert.Equal("2026-10-08", Scalar(db, "SELECT value FROM meta WHERE key = 'nztcs_fetched'"));
        Assert.Equal(new[] {
            new object?[] { "br-salve", "Not Applicable", "NA", null, null, "salve", "abc2", null },
            new object?[] { "natureserve-global", "G4T1", "T1", null, null, "natureserve", "102", null },
        }, Of(AmurLeopard));
        Assert.Equal("https://doi.org/10.37002/salve.ficha.1.2", Scalar(db, "SELECT url FROM other_status WHERE source_id = 'abc1'"));
        Assert.Equal("https://salve.icmbio.gov.br/salve-api/public/fichaPdf/abc2", Scalar(db, "SELECT url FROM other_status WHERE source_id = 'abc2'"));
        Assert.Equal("2026-10-08", Scalar(db, "SELECT value FROM meta WHERE key = 'salve_fetched'"));
        Assert.Equal(new[] { new object?[] { "natureserve-global", "G2?", "G2", "Bettongia ogilbyi", null, "natureserve", "104", null } },
            Of(Woylie));
        Assert.Empty(Of(Koala));
        Assert.Equal("https://ecos.fws.gov/ecp/species/4086", Scalar(db, $"SELECT url FROM other_status WHERE source_id = '7001'"));
        Assert.Equal("2026-10-08", Scalar(db, "SELECT value FROM meta WHERE key = 'natureserve_fetched'"));
        Assert.Equal("2026-10-07", Scalar(db, "SELECT value FROM meta WHERE key = 'ecos_fetched'"));
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

    // The two listed-name columns are named from the SPRAT report's header text, so a report can lack
    // them. They then read as empty: the build warns, and each listing gives SPRAT's scientific name.
    [Fact]
    public void Build_WithoutTheSpratListedNameColumns_WarnsAndUsesTheScientificName() {
        var sprat = _sources.PathOf("sprat-no-listed-names.sqlite");
        using (var c = OpenWritable(sprat)) {
            Execute(c, """
                CREATE TABLE import_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
                INSERT INTO import_metadata (filename, redlist_version, started_at) VALUES ('25062026-070407-report.csv', 'x', 'x');
                CREATE TABLE sprat_species (import_id INTEGER NOT NULL, sprat_taxon_id TEXT, scientific_name TEXT, epbc_status TEXT);
                INSERT INTO sprat_species VALUES (1, '90001', 'Panthera pardus', 'Vulnerable');
                """);
        }
        var output = _sources.PathOf("site-no-listed-names.sqlite");
        var stats = new SiteDbBuild(Inputs(output) with { SpratDatabase = sprat }, QuietConsole()).Run(CancellationToken.None);

        Assert.Contains(stats.Warnings, w => w.Contains("no IUCN_Red_List_Listed_Names column", StringComparison.Ordinal));
        Assert.Contains(stats.Warnings, w => w.Contains("no EPBC_Threatened_Species_Listed_Name column", StringComparison.Ordinal));
        using var db = OpenReadOnly(output);
        Assert.Equal(new object?[] { 90001L, "Panthera pardus", "VU", "taxon" },
            Assert.Single(Rows(db, $"SELECT sprat_taxon_id, listed_name, status, applies_to FROM epbc_listing WHERE taxon_id = {Leopard}")));
        Assert.Equal("25062026-070407-report.csv", Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.SpratReport}'"));
    }

    // A SPRAT database with no sprat_species table (`sprat import` of an empty report) is skipped with
    // a warning; the rest of the build carries on.
    [Fact]
    public void Build_WithASpratDatabaseThatHasNoTable_WarnsAndCarriesOn() {
        var sprat = _sources.PathOf("sprat-empty.sqlite");
        using (var c = OpenWritable(sprat)) {
            Execute(c, "CREATE TABLE import_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);");
        }
        var output = _sources.PathOf("site-no-sprat.sqlite");
        var stats = new SiteDbBuild(Inputs(output) with { SpratDatabase = sprat }, QuietConsole()).Run(CancellationToken.None);

        Assert.Contains(stats.Warnings, w => w.Contains("has no sprat_species table", StringComparison.Ordinal));
        using var db = OpenReadOnly(output);
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM epbc_listing"));
        Assert.Null(Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.SpratReport}'"));
        Assert.Equal(TaxonCount.ToString(), Scalar(db, "SELECT COUNT(*) FROM taxon"));
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

    // ------------------------------------------------------------ DOIs from `iucn resolve-dois`

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
        var empty = _sources.PathOf("empty-doi-cache.sqlite");
        using (var c = OpenWritable(empty)) {
            Execute(c, "CREATE TABLE something_else (x INTEGER);");
        }
        var output = _sources.PathOf("site-no-doi.sqlite");
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

    // ------------------------------------------------------------ common names left out or repaired

    // The koala's record lists an author citation as a name (junk, left out) and a name with a
    // citation template after it (stored as "Native bear"); the summary counts one of each.
    [Fact]
    public void Build_CountsCommonNamesLeftOutAsJunkAndRepaired() {
        var output = _sources.PathOf("site-names.sqlite");
        var stats = new SiteDbBuild(Inputs(output), QuietConsole()).Run(CancellationToken.None);

        Assert.Equal((1, 1), (stats.CommonNamesJunk, stats.CommonNamesRepaired));
        using var db = OpenReadOnly(output);
        Assert.Equal(new[] { "Koala", "Native bear" },
            Rows(db, $"SELECT name FROM name WHERE taxon_id = {Koala} AND name_type = 'common' ORDER BY name_id").Select(r => (string)r[0]!));
    }

    // ------------------------------------------------------------ the build

    private const long TaxonCount = 5;

    private string Build() {
        var output = _sources.PathOf("site.sqlite");
        if (!File.Exists(output)) {
            new SiteDbBuild(Inputs(output), QuietConsole()).Run(CancellationToken.None);
        }
        return output;
    }

    private SiteBuildInputs Inputs(string output) {
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var sprat = _sources.PathOf("sprat.sqlite");
        var doiCache = _sources.PathOf("iucn_doi_cache.sqlite");
        var statusLists = _sources.PathOf("status_lists.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucn(iucn);
            WriteCache(cache);
            WriteSprat(sprat);
            WriteDoiCache(doiCache);
            WriteStatusLists(statusLists);
        }
        return new SiteBuildInputs {
            IucnDatabase = iucn,
            ApiCache = cache,
            SpratDatabase = sprat,
            DoiCache = doiCache,
            StatusListsDatabase = statusLists,
            Output = output,
        };
    }

    // ------------------------------------------------------------ source fixtures

    private static void WriteIucn(string path) =>
        WriteIucnCsv(path, "2026-1", """
                (1, 15954, 'Panthera pardus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'pardus', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                (1, 16892, 'Phascolarctos cinereus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'DIPROTODONTIA', 'PHASCOLARCTIDAE', 'Phascolarctos', 'cinereus', NULL, NULL, NULL, NULL, '(Goldfuss, 1817)', NULL),
                (1, 2790, 'Bettongia penicillata', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'DIPROTODONTIA', 'POTOROIDAE', 'Bettongia', 'penicillata', NULL, NULL, NULL, NULL, 'Gray, 1837', NULL)
            """, """
                (1, 50659089, 15954, 'Panthera pardus', 'Vulnerable', 'A2cd', '2024', '2023-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 166496779, 16892, 'Phascolarctos cinereus', 'Vulnerable', 'A2bc', '2016', '2014-07-08 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 2790001, 2790, 'Bettongia penicillata', 'Critically Endangered', 'A3e', '2015', '2014-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global')
            """);

    private static void WriteCache(string path) {
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
              "common_names":[{"main":true,"name":"Koala","language":"eng"},{"main":false,"name":"Calvert, 1902","language":"eng"},
                              {"main":false,"name":"Native bear{sfn|Troughton|1941}","language":"eng"}],"synonyms":[]},
             "assessments":[{{Header(KoalaLatest, Koala, true, "2016", "VU")}}]}
            """;

        const string downloaded = "2026-08-21T00:00:00Z";
        WriteApiCache(path,
            new[] { new CachedTaxonRecord(1, AmurLeopard, amurLeopard), new CachedTaxonRecord(2, WoylieOld, woylieOld), new CachedTaxonRecord(3, Koala, koala) },
            new[] {
                new CachedAssessment(AmurLeopard2016Ne, AmurLeopard, downloaded, Payload(AmurLeopard2016Ne, AmurLeopard, "Panthera pardus ssp. orientalis", "2016", "Stein, A.B. 2016. Panthera pardus ssp. orientalis. The IUCN Red List of Threatened Species 2016: e.T15957A96947390. Accessed on 18 August 2026.", "Stein, A.B.")),
                new CachedAssessment(AmurLeopard2008, AmurLeopard, downloaded, Payload(AmurLeopard2008, AmurLeopard, "Panthera pardus ssp. orientalis", "2008", "Jackson, P. 2008. Panthera pardus ssp. orientalis. The IUCN Red List of Threatened Species 2008: e.T15957A5333757. Accessed on 18 August 2026.", "Jackson, P.")),
                new CachedAssessment(KoalaLatest, Koala, downloaded, Payload(KoalaLatest, Koala, "Phascolarctos cinereus", "2016", "Woinarski, J. 2016. Phascolarctos cinereus. The IUCN Red List of Threatened Species 2016: e.T16892A166496779. Accessed on 18 August 2026.", "Woinarski, J.")),
                new CachedAssessment(LeopardLatest, Leopard, downloaded, Payload(LeopardLatest, Leopard, "Panthera pardus", "2024", "Stein, A.B. 2024. Panthera pardus. The IUCN Red List of Threatened Species 2024: e.T15954A50659089. https://dx.doi.org/10.2305/IUCN.UK.2024-1.RLTS.T15954A50659089.en. Accessed on 18 August 2026.", "Stein, A.B.")),
            });
    }

    // Only the columns `site build-db` reads.
    private static void WriteSprat(string path) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE import_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL, redlist_version TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT);
            INSERT INTO import_metadata (filename, redlist_version, started_at) VALUES ('25062026-070407-report.csv', 'x', 'x');
            CREATE TABLE sprat_species (import_id INTEGER NOT NULL, sprat_taxon_id TEXT, scientific_name TEXT, epbc_status TEXT,
                EPBC_Threatened_Species_Listed_Name TEXT, IUCN_Red_List_Listed_Names TEXT, EPBC_Threatened_Species_Date_Effective TEXT,
                act_status TEXT, Listed_Name TEXT, nsw_status TEXT, Listed_Name_2 TEXT, qld_status TEXT, Listed_Name_4 TEXT);
            INSERT INTO sprat_species VALUES
                (1, '197', 'Phascolarctos cinereus', NULL, NULL, 'Phascolarctos cinereus', NULL,
                    NULL, NULL, 'Endangered', 'Phascolarctos cinereus', 'endangered ', NULL),
                (1, '85104', 'Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)', 'Endangered',
                    'Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)', NULL, '12-FEB-2022',
                    'Endangered', 'Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)', NULL, NULL, NULL, NULL),
                (1, '90001', 'Panthera pardus', 'Vulnerable', 'Panthera pardus melas', 'Panthera pardus', '16-JUL-2000',
                    NULL, NULL, 'Vulnerable, vulnerable', NULL, NULL, NULL),
                (1, '90002', 'Panthera pardus (A.B.Smith 123)', 'Endangered', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                (1, '90003', 'Bettongia penicillata (sensu lato)', 'Endangered', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            """);
    }

    // NatureServe: the leopard twice (Standard and Provisional), the Amur leopard as a bare trinomial,
    // an unranked koala, and the woylie only under another name with Bettongia penicillata as a
    // synonym. ECOS: the leopard's listing wherever found and one for a population, under a name with
    // a bracketed earlier genus, and a listing of a species IUCN does not have.
    private static void WriteStatusLists(string path) {
        using (BeastieBot3.StatusLists.StatusListStore.Open(path)) {
        }
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO status_source (source, title, url, licence, fetched_at, row_count) VALUES
                ('natureserve', 'NatureServe Explorer', 'https://explorer.natureserve.org/', 'CC BY 4.0', '2026-10-08T01:04:00.0000000Z', 5),
                ('ecos', 'ECOS', 'https://ecos.fws.gov/', 'Public domain', '2026-10-07T23:30:00.0000000Z', 3),
                ('nztcs', 'NZTCS', 'https://nztcs.org.nz/', 'CC BY 4.0', '2026-10-08T03:00:00.0000000Z', 3),
                ('salve', 'SALVE', 'https://salve.icmbio.gov.br/', 'Public', '2026-10-08T03:20:00.0000000Z', 2);
            INSERT INTO salve_assessment (ficha_id, scientific_name, category, possibly_extinct, assessed_on, doi, published, imported_at) VALUES
                ('abc1', 'Panthera pardus', 'CR', 1, '2022-08-19', '10.37002/salve.ficha.1.2', 1, 'x'),
                ('abc2', 'Panthera pardus orientalis', 'NA', 0, NULL, NULL, 0, 'x');
            INSERT INTO nztcs_assessment (assessment_id, species_id, scientific_name, assessment_name, category, status, report_name, imported_at) VALUES
                (5001, 501, 'Panthera pardus', 'Panthera pardus (Linnaeus, 1758)', 'Introduced and Naturalised', 'Introduced and Naturalised',
                    'Mammals 2024 (Example et al. 2024)', 'x'),
                (5002, 502, NULL, 'Panthera sp. "Kaitorete"', 'Data Deficient', 'Data Deficient', 'Mammals 2024 (Example et al. 2024)', 'x'),
                (5003, 503, 'Phascolarctos cinereus', 'Phascolarctos cinereus', 'Not assessed', 'Not assessed', NULL, 'x');
            INSERT INTO natureserve_species (element_global_id, unique_id, scientific_name, g_rank, rounded_g_rank, classification_status,
                kingdom, infraspecies, cosewic_code, sara_code, nsx_url, fetched_at) VALUES
                (101, 'ELEMENT_GLOBAL.2.101', 'Panthera pardus', 'G4G5', 'G4', 'Standard', 'Animalia', 0, 'XT', 'Extirpated',
                    'https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.101/Panthera_pardus', 'x'),
                (100, 'ELEMENT_GLOBAL.2.100', 'Panthera pardus', 'G2', 'G2', 'Provisional', 'Animalia', 0, NULL, NULL,
                    'https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.100/Panthera_pardus', 'x'),
                (102, 'ELEMENT_GLOBAL.2.102', 'Panthera pardus orientalis', 'G4T1', 'T1', 'Standard', 'Animalia', 1, NULL, NULL,
                    'https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.102/Panthera_pardus_orientalis', 'x'),
                (103, 'ELEMENT_GLOBAL.2.103', 'Phascolarctos cinereus', 'GNR', 'GNR', 'Standard', 'Animalia', 0, NULL, NULL,
                    'https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.103/Phascolarctos_cinereus', 'x'),
                (104, 'ELEMENT_GLOBAL.2.104', 'Bettongia ogilbyi', 'G2?', 'G2', 'Standard', 'Animalia', 0, NULL, NULL,
                    'https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.104/Bettongia_ogilbyi', 'x');
            INSERT INTO natureserve_synonym VALUES (104, 'Bettongia penicillata');
            INSERT INTO ecos_listing (entity_id, species_id, scientific_name_raw, scientific_name, status, entity_description,
                listing_date, kingdom, url, imported_at) VALUES
                (7001, 4086, 'Panthera (=Felis) pardus', 'Panthera pardus', 'Endangered', 'Wherever found', '1970-06-02', 'Animal',
                    'https://ecos.fws.gov/ecp/species/4086', 'x'),
                (7002, 4086, 'Panthera (=Felis) pardus', 'Panthera pardus', 'Threatened', 'Gabon,  Congo southward', '1982-01-28', 'Animal',
                    'https://ecos.fws.gov/ecp/species/4086', 'x'),
                (7003, 9999, 'Nonexistus fictus', 'Nonexistus fictus', 'Endangered', 'Wherever found', '2001-01-01', 'Plant',
                    'https://ecos.fws.gov/ecp/species/9999', 'x');
            INSERT INTO ecos_name VALUES (7001, 'Panthera pardus'), (7001, 'Felis pardus'), (7002, 'Panthera pardus'), (7002, 'Felis pardus'),
                (7003, 'Nonexistus fictus');
            """);
    }

    private static void WriteDoiCache(string path) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE doi_check (assessment_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL, doi TEXT, checked_at TEXT NOT NULL,
                candidates_tried INTEGER NOT NULL);
            INSERT INTO doi_check VALUES
                (166496779, 16892, '10.2305/IUCN.UK.2016-1.RLTS.T16892A166496779.en', '2026-09-30T23:10:00.0000000Z', 3),
                (5333757, 15957, '10.2305/IUCN.UK.2008.RLTS.T15958A5333757.en', '2026-09-29T10:00:00.0000000Z', 2),
                (2790001, 2790, NULL, '2026-09-28T10:00:00.0000000Z', 4);
            """);
    }
}
