using BeastieBot3.Gbif;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// How site build-db matches a site taxon to a GBIF Backbone Taxonomy taxon (SiteGbifNames): by the
// GBIF id on its Wikidata item first, else by its scientific name; and how GBIF's lumps, synonym
// names, language codes and dataset titles are handled.
public class GbifNameMatchTests {
    private static SiteTaxon Taxon(long id, string name, string kind = SiteTaxonKind.Species, string kingdom = "ANIMALIA") {
        var words = name.Split(' ');
        return new SiteTaxon {
            TaxonId = id, ScientificName = name, Kind = kind, Kingdom = kingdom, Genus = words[0], SpeciesEpithet = words[1],
            InfraName = words.Length > 2 ? words[^1] : null,
        };
    }

    private static GbifUsage Usage(long id, string status = "accepted", long? accepted = null, string rank = "species", string kingdom = "Animalia") =>
        new(id, accepted, status, rank, kingdom);

    private static Dictionary<string, List<GbifUsage>> ByName(params (string Name, GbifUsage Usage)[] rows) =>
        rows.GroupBy(r => BeastieBot3.Shared.SiteData.SiteNameKey.Fold(r.Name)).ToDictionary(g => g.Key, g => g.Select(r => r.Usage).ToList());

    [Fact]
    public void Id_MatchesItsAcceptedTaxon_AndASynonymIdLeadsToItsAcceptedTaxon() {
        var tiger = Taxon(15955, "Panthera tigris");
        var ids = new Dictionary<long, GbifUsage> { [5219416] = Usage(5219416), [4969819] = Usage(4969819, "synonym", 5219416) };
        Assert.Equal((5219416L, GbifMatchKind.ById, GbifIdProblem.None), SiteGbifNames.Match(tiger, [5219416], ids, ByName()));
        Assert.Equal((5219416L, GbifMatchKind.ById, GbifIdProblem.None), SiteGbifNames.Match(tiger, [4969819], ids, ByName()));
        // Two ids that lead to one taxon are one match.
        Assert.Equal((5219416L, GbifMatchKind.ById, GbifIdProblem.None), SiteGbifNames.Match(tiger, [5219416, 4969819], ids, ByName()));
    }

    [Fact]
    public void Id_InAnotherKingdomOrRank_OrLeadingToTwoTaxa_IsNotUsed_AndTheNameIsTried() {
        var plant = Taxon(1, "Targionia barbusano", kingdom: "PLANTAE");
        var names = ByName(("Targionia barbusano", Usage(7000, kingdom: "Plantae")));
        var animal = new Dictionary<long, GbifUsage> { [5162348] = Usage(5162348, "synonym", 2085899) };
        Assert.Equal((7000L, GbifMatchKind.ByName, GbifIdProblem.OtherKingdomOrRank), SiteGbifNames.Match(plant, [5162348], animal, names));

        var species = Taxon(2, "Aus bus");
        var genus = new Dictionary<long, GbifUsage> { [10] = Usage(10, rank: "genus") };
        Assert.Equal((null, GbifMatchKind.NotFound, GbifIdProblem.OtherKingdomOrRank), SiteGbifNames.Match(species, [10], genus, ByName()));

        var two = new Dictionary<long, GbifUsage> { [20] = Usage(20), [21] = Usage(21) };
        Assert.Equal((null, GbifMatchKind.NotFound, GbifIdProblem.Several), SiteGbifNames.Match(species, [20, 21], two, ByName()));
        Assert.Equal((null, GbifMatchKind.NotFound, GbifIdProblem.NotInBackbone), SiteGbifNames.Match(species, [30], two, ByName()));
    }

    [Fact]
    public void Name_TakesTheAcceptedUsageInTheKingdom_BeforeADoubtfulOne() {
        var lion = Taxon(15951, "Panthera leo");
        var names = ByName(("Panthera leo", Usage(5219404)), ("Panthera leo", Usage(9000001, kingdom: "Plantae")),
            ("Panthera leo", Usage(9000002, "doubtful")));
        Assert.Equal((5219404L, GbifMatchKind.ByName, GbifIdProblem.None), SiteGbifNames.Match(lion, null, new Dictionary<long, GbifUsage>(), names));
    }

    [Fact]
    public void Name_ThatTwoAcceptedUsagesHaveInTheKingdom_IsNotMatched() {
        var taxon = Taxon(3, "Aus bus");
        var names = ByName(("Aus bus", Usage(40)), ("Aus bus", Usage(41)));
        Assert.Equal((null, GbifMatchKind.NameAmbiguous, GbifIdProblem.None), SiteGbifNames.Match(taxon, null, new Dictionary<long, GbifUsage>(), names));
    }

    [Fact]
    public void Name_OfASubspeciesOrVariety_IsComparedWithoutItsRankMarker() {
        var variety = Taxon(4, "Cerinthe glabra var. pirinica", SiteTaxonKind.Variety, "PLANTAE");
        var names = ByName(("Cerinthe glabra pirinica", Usage(50, rank: "variety", kingdom: "Plantae")));
        Assert.Equal((50L, GbifMatchKind.ByName, GbifIdProblem.None), SiteGbifNames.Match(variety, null, new Dictionary<long, GbifUsage>(), names));
    }

    [Fact]
    public void Lump_GoesOnlyToTheTaxonWithGbifsName() {
        var taxa = new[] { Taxon(1, "Aus bus"), Taxon(2, "Aus cus"), Taxon(3, "Dus eus"), Taxon(4, "Fus gus") }.ToDictionary(t => t.TaxonId);
        var candidates = new Dictionary<long, (long Key, GbifMatchKind Kind)> {
            [1] = (100, GbifMatchKind.ByName), [2] = (100, GbifMatchKind.ById),
            [3] = (200, GbifMatchKind.ById), [4] = (200, GbifMatchKind.ById),
        };
        var canonical = new Dictionary<long, string> { [100] = "Aus bus", [200] = "Hus ius" };
        var outcome = SiteGbifNames.ResolveLumps(candidates, taxa, canonical).OrderBy(o => o.TaxonId).ToList();
        Assert.Equal([(1L, GbifMatchKind.ByName), (2L, GbifMatchKind.Lumped), (3L, GbifMatchKind.Lumped), (4L, GbifMatchKind.Lumped)], outcome);
    }

    [Theory]
    [InlineData("Cerinthe glabra var. pirinica Stoj. & Acht.", "Stoj. & Acht.", "Cerinthe glabra pirinica", "Cerinthe glabra var. pirinica")]
    [InlineData("Cerinthe alpina subsp. alpina", null, "Cerinthe alpina alpina", "Cerinthe alpina subsp. alpina")]
    [InlineData("Felis tigris Linnaeus, 1758", "Linnaeus, 1758", "Felis tigris", "Felis tigris")]
    // An authorship that is not at the end of the name: the canonical name.
    [InlineData("Felis tigris sensu Smith", "Linnaeus, 1758", "Felis tigris", "Felis tigris")]
    public void SynonymName_IsTheNameWithoutItsAuthorship(string scientific, string? authorship, string canonical, string expected) =>
        Assert.Equal(expected, SiteGbifNames.SynonymName(new GbifTaxonRow(1, 2, "synonym", "species", canonical, scientific, authorship, "Animalia")));

    [Theory]
    [InlineData("Felidae", true)]
    [InlineData("Muridae", true)]
    [InlineData("EUNIS Biodiversity Database", false)]
    [InlineData("Cnidaria", false)]
    [InlineData("Felidae of the world", false)]
    [InlineData("Primates", true)] // named in the dataset descriptions
    public void BookChapters_AreTitledWithAFamilyName_OrNamedInTheDescriptions(string title, bool expected) =>
        Assert.Equal(expected, SiteGbifNames.IsBookChapter(title, new HashSet<string> { "Primates" }));

    [Theory]
    [InlineData("en", "en")]
    [InlineData("EN", "en")]
    [InlineData("fr", "fr")]
    [InlineData("nb", "nb")]
    [InlineData("zh", "zh")]
    [InlineData("", null)]
    [InlineData(null, null)]
    [InlineData("language", null)]
    public void Language_KeepsEnglishAndTheCodesTheSiteCanName(string? code, string? expected) =>
        Assert.Equal(expected, SiteGbifNames.Language(code));

    [Fact]
    public void NameSet_KeepsOneRowPerName_WithEachDatasetOnce() {
        var names = new SiteNameSet();
        Assert.True(names.Add("Tigre", SiteNameType.Common, "es", SiteNameSource.Gbif, detail: "EUNIS Biodiversity Database"));
        Assert.False(names.Add("tigre", SiteNameType.Common, "es", SiteNameSource.Gbif, detail: "Belgian Species List"));
        Assert.False(names.Add("Tigre", SiteNameType.Common, "es", SiteNameSource.Gbif, detail: "EUNIS Biodiversity Database"));
        Assert.False(names.Add("Tigre", SiteNameType.Common, "es", SiteNameSource.Gbif));
        var row = Assert.Single(names.Names);
        Assert.Equal("Tigre", row.Name);
        Assert.Equal("EUNIS Biodiversity Database\nBelgian Species List", row.Detail);
    }

    [Fact]
    public void Archive_FileNamesAreDated() {
        Assert.Equal("backbone-2023-08-28.zip", GbifBackboneArchive.FileNameFor(new DateOnly(2023, 8, 28)));
        Assert.Equal(new DateOnly(2023, 8, 28), GbifBackboneArchive.DateOf("/x/backbone-2023-08-28.zip"));
        Assert.Null(GbifBackboneArchive.DateOf("/x/backbone-download.zip.part"));
    }

    [Fact]
    public void DownloadCheck_AcceptsABackboneArchive_AndRefusesAnotherDataset() {
        var dir = Path.Combine(Path.GetTempPath(), "beastiebot-gbif-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try {
            var path = Path.Combine(dir, "backbone-2023-08-28.zip");
            SiteDbBuildGbifNamesTests.WriteBackbone(path);
            Assert.Null(GbifBackboneDownloadCommand.Check(path));
            Assert.Equal(path, GbifBackboneArchive.FindNewest(dir));
            File.WriteAllText(Path.Combine(dir, "other.zip"), "not a zip");
            Assert.NotNull(GbifBackboneDownloadCommand.Check(Path.Combine(dir, "other.zip")));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }
}
