using BeastieBot3.SiteBuild;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// Links from taxa not in the release (old IUCN ids) to taxa in the release (taxon_link), and
// assessment.has_taxonomic_notes. Ids and names are IUCN's where they are known:
//   - Bettongia penicillata: old id 2785, same name as 2790;
//   - Acropora minuta (old id 133018), a synonym of Acropora palmerae (133531);
//   - Platanista gangetica: old id 41758 has the name of 41756 and is a synonym of Platanista minor
//     (41757), as after a split;
//   - Ovis orientalis (old id 15739), listed as a synonym by Ovis gmelini and Ovis vignei: no link;
//   - a made-up animal whose name a plant lists as a synonym: no link.
public sealed class SiteTaxonLinksTests : IDisposable {
    private const long Woylie = 2790;
    private const long WoylieLatest = 2790001;
    private const long WoylieOld = 2785;
    private const long WoylieOld2008 = 6143;
    private const long Palmerae = 133531;
    private const long PalmeraeLatest = 133531001;
    private const long Minuta = 133018;
    private const long Minuta2008 = 133018001;
    private const long Gangetica = 41756;
    private const long Minor = 41757;
    private const long GangeticaOld = 41758;
    private const long Gmelini = 54940218;
    private const long Vignei = 54940655;
    private const long Orientalis = 15739;
    private const long AnimalOld = 900001;
    private const long Plant = 900002;

    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    // ------------------------------------------------------------ the link rule

    private static SiteTaxon Taxon(long id, string name, string? kingdom, bool inRelease, params string[] synonyms) => new() {
        TaxonId = id,
        ScientificName = name,
        Kind = SiteTaxonKind.Species,
        Kingdom = kingdom,
        InRelease = inRelease,
        IucnSynonyms = synonyms.Select(s => new SiteSynonym(s)).ToList(),
    };

    [Fact]
    public void Find_LinksBySameNameAndByTheOnlyTaxonListingTheNameAsASynonym() {
        var taxa = new List<SiteTaxon> {
            Taxon(Woylie, "Bettongia penicillata", "ANIMALIA", true),
            Taxon(WoylieOld, "Bettongia penicillata", "ANIMALIA", false),
            Taxon(Palmerae, "Acropora palmerae", "ANIMALIA", true, "Acropora minuta"),
            Taxon(Minuta, "Acropora minuta", "ANIMALIA", false),
            Taxon(Gangetica, "Platanista gangetica", "ANIMALIA", true),
            Taxon(Minor, "Platanista minor", "ANIMALIA", true, "Platanista gangetica"),
            Taxon(GangeticaOld, "Platanista gangetica", "ANIMALIA", false),
        };
        var stats = new SiteBuildStats();

        var links = SiteTaxonLinks.Find(taxa, stats);

        Assert.Equal(new[] {
            new SiteTaxonLink(WoylieOld, Woylie, SiteLinkKind.SameName),
            new SiteTaxonLink(GangeticaOld, Gangetica, SiteLinkKind.SameName),
            new SiteTaxonLink(GangeticaOld, Minor, SiteLinkKind.IucnSynonym),
            new SiteTaxonLink(Minuta, Palmerae, SiteLinkKind.IucnSynonym),
        }, links);
        Assert.Equal(Woylie, taxa.Single(t => t.TaxonId == WoylieOld).CurrentTaxonId);
        Assert.Null(taxa.Single(t => t.TaxonId == Minuta).CurrentTaxonId);
        Assert.Equal((2, 2, 0), (stats.NotInReleaseWithCurrentTaxon, stats.NotInReleaseSynonymLinks, stats.NotInReleaseSynonymOfSeveral));
    }

    // A name that two taxa list as a synonym, a lister in another kingdom, and an unknown kingdom on
    // either side give no link.
    [Fact]
    public void Find_GivesNoSynonymLink_WhenTheTaxonListingTheNameIsNotTheOnlyOneInTheKingdom() {
        var taxa = new List<SiteTaxon> {
            Taxon(Gmelini, "Ovis gmelini", "ANIMALIA", true, "Ovis orientalis"),
            Taxon(Vignei, "Ovis vignei", "ANIMALIA", true, "Ovis orientalis"),
            Taxon(Orientalis, "Ovis orientalis", "ANIMALIA", false),
            Taxon(Plant, "Plantus verus", "PLANTAE", true, "Animalus falsus", "Nullus regnum"),
            Taxon(AnimalOld, "Animalus falsus", "ANIMALIA", false),
            Taxon(AnimalOld + 10, "Nullus regnum", null, false),
            Taxon(Plant + 10, "Plantus alter", null, true, "Plantus vetus"),
            Taxon(Plant + 20, "Plantus vetus", "PLANTAE", false),
        };
        var stats = new SiteBuildStats();

        Assert.Empty(SiteTaxonLinks.Find(taxa, stats));
        Assert.Equal(1, stats.NotInReleaseSynonymOfSeveral);
        Assert.Equal(0, stats.NotInReleaseSynonymLinks);
    }

    // Kingdoms are compared ignoring case, and a synonym listed twice by one taxon counts once.
    [Fact]
    public void Find_IgnoresKingdomCase_AndRepeatedSynonyms() {
        var taxa = new List<SiteTaxon> {
            Taxon(Palmerae, "Acropora palmerae", "ANIMALIA", true, "Acropora minuta", "Acropora minuta"),
            Taxon(Minuta, "Acropora minuta", "Animalia", false),
        };
        Assert.Equal(new SiteTaxonLink(Minuta, Palmerae, SiteLinkKind.IucnSynonym), Assert.Single(SiteTaxonLinks.Find(taxa, new SiteBuildStats())));
    }

    [Theory]
    [InlineData("Acropora minuta is now a synonym of this species (WoRMS accessed January 2022).", true)]
    [InlineData("<p><em>Torpedo microdiscus</em>&#160;is included.</p>", true)]
    [InlineData("", false)]
    [InlineData(null, false)]
    [InlineData("<em><br/></em>", false)]
    [InlineData("<span style=\"font-style: italic;\"></span>", false)]
    [InlineData("<span style=\"font-style: italic;\"><br/></span>&nbsp;", false)]
    [InlineData("<br/><span class=\"sheader2\"></span>", false)]
    public void HasText_IsTrueOnlyForNotesWithLettersOrDigits(string? html, bool expected) =>
        Assert.Equal(expected, SiteBuildRules.HasText(html));

    // ------------------------------------------------------------ the build

    [Fact]
    public void Build_WritesTheLinks_AndKeepsCurrentTaxonIdForTheSameNameLink() {
        var (db, stats) = Build();
        using (db) {
            var links = Rows(db, "SELECT taxon_id, current_taxon_id, link_kind FROM taxon_link ORDER BY taxon_id, link_kind DESC")
                .Select(r => ((long)r[0]!, (long)r[1]!, (string)r[2]!))
                .ToList();
            Assert.Equal(new[] {
                (WoylieOld, Woylie, "same-name"),
                (GangeticaOld, Gangetica, "same-name"),
                (GangeticaOld, Minor, "iucn-synonym"),
                (Minuta, Palmerae, "iucn-synonym"),
            }.OrderBy(l => l.Item1).ThenByDescending(l => l.Item3), links);

            Assert.Equal(Woylie.ToString(), Scalar(db, $"SELECT current_taxon_id FROM taxon WHERE taxon_id = {WoylieOld}"));
            Assert.Equal(Gangetica.ToString(), Scalar(db, $"SELECT current_taxon_id FROM taxon WHERE taxon_id = {GangeticaOld}"));
            // A synonym link does not set current_taxon_id, which stays the same-name link.
            Assert.Null(Scalar(db, $"SELECT current_taxon_id FROM taxon WHERE taxon_id = {Minuta}"));
            Assert.Equal("0", Scalar(db, $"SELECT COUNT(*) FROM taxon_link WHERE taxon_id = {Orientalis}"));
        }
        Assert.Equal((2, 2, 1), (stats.NotInReleaseWithCurrentTaxon, stats.NotInReleaseSynonymLinks, stats.NotInReleaseSynonymOfSeveral));
    }

    // 1 when the payload's taxonomic notes have text, 0 when they are empty markup or missing, NULL
    // when the assessment has no cached payload. The notes' text is not stored anywhere.
    [Fact]
    public void Build_RecordsWhetherTheTaxonomicNotesHaveText_WithoutTheirText() {
        var (db, stats) = Build();
        using (db) {
            string? Notes(long id) => Scalar(db, $"SELECT has_taxonomic_notes FROM assessment WHERE assessment_id = {id}");
            Assert.Equal("1", Notes(PalmeraeLatest));
            Assert.Equal("0", Notes(Minuta2008));
            Assert.Equal("0", Notes(WoylieOld2008));
            Assert.Null(Notes(WoylieLatest));
            foreach (var table in new[] { "assessment", "taxon", "name", "meta" }) {
                Assert.Equal("0", Scalar(db, $"SELECT COUNT(*) FROM {table} WHERE {RowText(db, table)} LIKE '%NOTES-TEXT%'"));
            }
        }
        Assert.Equal(1, stats.PayloadsWithTaxonomicNotes);
    }

    // Every column of a table joined into one text expression, to search a whole row.
    private static string RowText(Microsoft.Data.Sqlite.SqliteConnection db, string table) =>
        string.Join(" || ", Rows(db, $"SELECT name FROM pragma_table_info('{table}')").Select(r => $"IFNULL(CAST({r[0]} AS TEXT), '')"));

    private (Microsoft.Data.Sqlite.SqliteConnection Db, SiteBuildStats Stats) Build() {
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        WriteIucnCsv(iucn, "2026-1", """
                (1, 2790, 'Bettongia penicillata', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'DIPROTODONTIA', 'POTOROIDAE', 'Bettongia', 'penicillata', NULL, NULL, NULL, NULL, 'Gray, 1837', NULL),
                (1, 133531, 'Acropora palmerae', 'ANIMALIA', 'CNIDARIA', 'ANTHOZOA', 'SCLERACTINIA', 'ACROPORIDAE', 'Acropora', 'palmerae', NULL, NULL, NULL, NULL, 'Wells, 1954', 'NOTES-TEXT in the CSV'),
                (1, 41756, 'Platanista gangetica', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CETARTIODACTYLA', 'PLATANISTIDAE', 'Platanista', 'gangetica', NULL, NULL, NULL, NULL, '(Roxburgh, 1801)', NULL),
                (1, 41757, 'Platanista minor', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CETARTIODACTYLA', 'PLATANISTIDAE', 'Platanista', 'minor', NULL, NULL, NULL, NULL, 'Owen, 1853', NULL),
                (1, 54940218, 'Ovis gmelini', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CETARTIODACTYLA', 'BOVIDAE', 'Ovis', 'gmelini', NULL, NULL, NULL, NULL, 'Blyth, 1841', NULL),
                (1, 54940655, 'Ovis vignei', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CETARTIODACTYLA', 'BOVIDAE', 'Ovis', 'vignei', NULL, NULL, NULL, NULL, 'Blyth, 1841', NULL)
            """, """
                (1, 2790001, 2790, 'Bettongia penicillata', 'Extinct', NULL, '2026', '2026-02-05 00:00:00 UTC', '3.1', NULL, NULL, 'Unknown', 'false', 'false', 'Global'),
                (1, 133531001, 133531, 'Acropora palmerae', 'Endangered', 'A4c', '2024', '2022-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 41756001, 41756, 'Platanista gangetica', 'Endangered', 'A2abc', '2022', '2021-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 41757001, 41757, 'Platanista minor', 'Endangered', 'A2abc', '2022', '2021-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 54940218001, 54940218, 'Ovis gmelini', 'Near Threatened', NULL, '2020', '2020-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global'),
                (1, 54940655001, 54940655, 'Ovis vignei', 'Near Threatened', NULL, '2020', '2020-01-01 00:00:00 UTC', '3.1', NULL, NULL, 'Decreasing', 'false', 'false', 'Global')
            """);

        const string downloaded = "2026-08-21T00:00:00Z";
        WriteApiCache(cache,
            new[] {
                new CachedTaxonRecord(1, WoylieOld, OldRecord(WoylieOld, "Bettongia penicillata", "Bettongia", "penicillata", Header(WoylieOld2008, WoylieOld, false, "2008", "CR"))),
                new CachedTaxonRecord(2, Minuta, OldRecord(Minuta, "Acropora minuta", "Acropora", "minuta", Header(Minuta2008, Minuta, false, "2008", "VU"))),
                new CachedTaxonRecord(3, GangeticaOld, OldRecord(GangeticaOld, "Platanista gangetica", "Platanista", "gangetica")),
                new CachedTaxonRecord(4, Orientalis, OldRecord(Orientalis, "Ovis orientalis", "Ovis", "orientalis")),
                new CachedTaxonRecord(5, Palmerae, CurrentRecord(Palmerae, ("Acropora", "minuta"))),
                new CachedTaxonRecord(6, Minor, CurrentRecord(Minor, ("Platanista", "gangetica"))),
                new CachedTaxonRecord(7, Gmelini, CurrentRecord(Gmelini, ("Ovis", "orientalis"))),
                new CachedTaxonRecord(8, Vignei, CurrentRecord(Vignei, ("Ovis", "orientalis"))),
            },
            new[] {
                new CachedAssessment(PalmeraeLatest, Palmerae, downloaded, Payload(PalmeraeLatest, Palmerae, "Acropora palmerae", "2024",
                    "Aeby, G. 2024. Acropora palmerae. The IUCN Red List of Threatened Species 2024: e.T133531A133531001.", "Aeby, G.",
                    taxonomicNotes: "<p><em>Acropora minuta</em> NOTES-TEXT is now a synonym of this species.</p>")),
                new CachedAssessment(Minuta2008, Minuta, downloaded, Payload(Minuta2008, Minuta, "Acropora minuta", "2008",
                    "Aeby, G. 2008. Acropora minuta. The IUCN Red List of Threatened Species 2008: e.T133018A133018001.", "Aeby, G.",
                    taxonomicNotes: "<em><br/></em>")),
                new CachedAssessment(WoylieOld2008, WoylieOld, downloaded, Payload(WoylieOld2008, WoylieOld, "Bettongia penicillata", "2008",
                    "Woinarski, J. 2008. Bettongia penicillata. The IUCN Red List of Threatened Species 2008: e.T2785A6143.", "Woinarski, J.")),
            });

        var output = _sources.PathOf("site.sqlite");
        var stats = new SiteDbBuild(new SiteBuildInputs { IucnDatabase = iucn, ApiCache = cache, Output = output }, QuietConsole())
            .Run(CancellationToken.None);
        return (OpenReadOnly(output), stats);
    }

    // The record of a taxon in the release, with IUCN synonyms built from genus and species names.
    private static string CurrentRecord(long id, params (string Genus, string Species)[] synonyms) {
        var list = string.Join(",", synonyms.Select(s =>
            $$"""{"name":"{{s.Genus}} {{s.Species}} Author, 1900","genus_name":"{{s.Genus}}","species_name":"{{s.Species}}","infra_type":null,"infra_name":null,"subpopulation_name":null}"""));
        return $$"""
            {"sis_id":{{id}},"taxon":{"sis_id":{{id}},"species_taxa":[],"subpopulation_taxa":[],"species":true,"subpopulation":false,"infrarank":false,
              "common_names":[],"synonyms":[{{list}}]},"assessments":[]}
            """;
    }

    // The record of a taxon that is not in the CSV export.
    private static string OldRecord(long id, string name, string genus, string species, params string[] headers) => $$"""
        {"sis_id":{{id}},"taxon":{"sis_id":{{id}},"scientific_name":"{{name}}","species_taxa":[],"subpopulation_taxa":[],
          "kingdom_name":"ANIMALIA","phylum_name":"CHORDATA","class_name":"MAMMALIA","order_name":"X","family_name":"Y",
          "genus_name":"{{genus}}","species_name":"{{species}}","subpopulation_name":null,"infra_name":null,"authority":null,
          "species":true,"subpopulation":false,"infrarank":false,"common_names":[],"synonyms":[]},
         "assessments":[{{string.Join(",", headers)}}]}
        """;
}
