using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;
using static BeastieBot3.Tests.ListTreeFixtures;

namespace BeastieBot3.Tests;

// Pins the curated groups of taxon-rules.yml (Snakes / Worm lizards / Lizards in Squamata, Cetaceans
// in Artiodactyla) going through the tree builder: a CoL clade beats the family lists, CoL layers
// continue below the matched clade, and a single non-empty group gets no heading.
public class VirtualGroupTests {
    private static VirtualGroup Group(string name, string plural, string mainArticle,
        IEnumerable<string>? clades = null, IEnumerable<string>? families = null, bool isDefault = false) => new() {
        Name = name,
        CommonPlural = plural,
        MainArticle = mainArticle,
        Clades = clades?.ToList() ?? new List<string>(),
        Families = families?.ToList() ?? new List<string>(),
        Default = isDefault,
    };

    // Snakes list only Colubridae as a family, so Natricidae can reach Snakes only through its clade.
    private static TaxonRulesService Rules() => new(new TaxonRulesConfig {
        Taxa = new Dictionary<string, TaxonRule> {
            ["Squamata"] = new() { UseVirtualGroups = true },
            ["Artiodactyla"] = new() { UseVirtualGroups = true },
        },
        VirtualGroups = new Dictionary<string, VirtualGroupConfig> {
            ["Squamata"] = new() {
                Groups = new List<VirtualGroup> {
                    Group("Snakes", "snakes", "Snake", clades: new[] { "Serpentes" }, families: new[] { "Colubridae" }),
                    Group("Worm lizards", "worm lizards", "Amphisbaenia", clades: new[] { "Amphisbaenia" }),
                    Group("Lizards", "lizards", "Lizard", isDefault: true),
                },
            },
            ["Artiodactyla"] = new() {
                Groups = new List<VirtualGroup> {
                    Group("Cetaceans", "cetaceans", "Cetacea", clades: new[] { "Cetacea" }),
                    Group("Even-toed ungulates", "even-toed ungulates", "Even-toed ungulate", isDefault: true),
                },
            },
        },
    });

    [Fact]
    public void Natricidae_LandsInSnakes_ThroughCladeSerpentes() {
        var root = BuildTree(SquamataRecords(), SquamataPlacement(), taxonRules: Rules());

        var squamata = Child(root, Squamata);
        Assert.Equal(new[] { "Snakes", "Worm lizards", "Lizards" }, Values(squamata));
        Assert.All(squamata.Children, c => Assert.Equal(TreeNodeKind.Virtual, c.Kind));
        Assert.All(squamata.Children, c => Assert.Equal(Squamata, c.VirtualOwner));
        var snakes = Child(squamata, "Snakes");
        Assert.Contains("NATRICIDAE", Values(snakes));
        Assert.DoesNotContain(Child(squamata, "Lizards").Children, c => c.Value == "NATRICIDAE");
    }

    [Fact]
    public void Lizards_GetTheirSuborderLayer_AndSnakesSkipSerpentes() {
        var records = SquamataRecords();
        // More lizards so the default group passes the layer gates on its own.
        records.AddRange(Family(Reptilia, Squamata, "ANOLIDAE", 8));
        records.AddRange(Family(Reptilia, Squamata, "CHAMAELEONIDAE", 6));
        var placement = new TaxonPlacementIndex(SquamataPlacement().Paths.Concat(new[] {
            OrderToFamily(Reptilia, Squamata, "ANOLIDAE", Suborder("Iguania")),
            OrderToFamily(Reptilia, Squamata, "CHAMAELEONIDAE", Suborder("Iguania")),
        }));

        var root = BuildTree(records, placement, taxonRules: Rules());

        var squamata = Child(root, Squamata);
        var lizards = Child(squamata, "Lizards");
        Assert.Equal(new[] { "Autarchoglossa", "Gekkota", "Iguania", "DIBAMIDAE" }, Values(lizards));
        // Snakes were matched on Serpentes, so their families sit directly under Snakes.
        Assert.All(Child(squamata, "Snakes").Children, c => Assert.Equal("family", c.Key));
        Assert.True(root.Height <= 4);
    }

    [Fact]
    public void Cetaceans_ThenToothedAndBaleenWhales() {
        var records = ArtiodactylaRecords();
        // Enough whales for a layer inside the Cetaceans group.
        records.AddRange(Family(Mammalia, Artiodactyla, "MONODONTIDAE", 4));
        var placement = new TaxonPlacementIndex(ArtiodactylaPlacement().Paths.Append(
            OrderToFamily(Mammalia, Artiodactyla, "MONODONTIDAE", NameOnly("Cetacea", "order"), Suborder("Odontoceti"))));

        var root = BuildTree(records, placement, taxonRules: Rules());

        var artiodactyla = Child(root, Artiodactyla);
        Assert.Equal(new[] { "Cetaceans", "Even-toed ungulates" }, Values(artiodactyla));
        var cetaceans = Child(artiodactyla, "Cetaceans");
        Assert.Equal(new[] { "Mysticeti", "Odontoceti" }, Values(cetaceans));
        Assert.All(cetaceans.Children, c => Assert.Equal(TreeNodeKind.Intermediate, c.Kind));
        Assert.Equal(
            new[] { "BOVIDAE", "CAMELIDAE", "CERVIDAE", "SUIDAE", "TAYASSUIDAE" },
            Values(Child(artiodactyla, "Even-toed ungulates")));
    }

    [Fact]
    public void OneNonEmptyGroup_GetsNoVirtualHeading() {
        var records = SquamataRecords(withTurtles: true)
            .Where(r => r.FamilyName is "COLUBRIDAE" or "ELAPIDAE" or "VIPERIDAE" or "NATRICIDAE" || r.OrderName != Squamata)
            .ToList();

        var root = BuildTree(records, SquamataPlacement(), taxonRules: Rules());

        var squamata = Child(root, Squamata);
        Assert.DoesNotContain(squamata.Children, c => c.Kind == TreeNodeKind.Virtual);
        Assert.Equal(new[] { "COLUBRIDAE", "ELAPIDAE", "NATRICIDAE", "VIPERIDAE" }, Values(squamata));
    }

    [Fact]
    public void SingleOrderPage_StillGetsTheVirtualGroups() {
        var root = BuildTree(SquamataRecords(withTurtles: false), SquamataPlacement(), taxonRules: Rules());

        Assert.Equal(new[] { "Snakes", "Worm lizards", "Lizards" }, Values(root));
    }

    [Fact]
    public void NoPlacement_FallsBackToTheFamilyLists() {
        var root = BuildTree(SquamataRecords(), placement: null, taxonRules: Rules());

        // Only Colubridae is listed for Snakes in this config, so it is the only snake family found
        // and its family level gets no heading.
        var snakes = Child(Child(root, Squamata), "Snakes");
        Assert.Empty(snakes.Children);
        Assert.Equal(20, snakes.ItemCount);
        Assert.All(snakes.Items, r => Assert.Equal("COLUBRIDAE", r.FamilyName));
    }

    [Fact]
    public void ResolveVirtualGroup_PrefersACladeOverAFamilyList() {
        var rules = new TaxonRulesService(new TaxonRulesConfig {
            VirtualGroups = new Dictionary<string, VirtualGroupConfig> {
                ["Squamata"] = new() {
                    Groups = new List<VirtualGroup> {
                        Group("Lizards", "lizards", "Lizard", families: new[] { "Natricidae" }),
                        Group("Snakes", "snakes", "Snake", clades: new[] { "Serpentes" }),
                    },
                },
            },
        });

        var match = rules.ResolveVirtualGroup("SQUAMATA", "NATRICIDAE", new[] { "Serpentes", "Alethinophidia" });

        Assert.NotNull(match);
        Assert.Equal("Snakes", match!.Group.Name);
        Assert.Equal(0, match.CladeIndex);
    }

    private static readonly Lazy<TaxonRulesService> ShippedRules = new(() =>
        TaxonRulesService.Load(Path.Combine(AppContext.BaseDirectory, "rules", "taxon-rules.yml")));

    [Theory]
    [InlineData("NATRICIDAE")]
    [InlineData("GERRHOPILIDAE")]
    [InlineData("ERYCIDAE")]
    [InlineData("PSEUDOXENODONTIDAE")]
    [InlineData("SIBYNOPHIIDAE")]
    [InlineData("CYCLOCORIDAE")]
    [InlineData("CANDOIIDAE")]
    [InlineData("CHARINAIDAE")]
    [InlineData("GRAYIIDAE")]
    [InlineData("MICRELAPIDAE")]
    [InlineData("PSEUDASPIDIDAE")]
    [InlineData("SANZINIIDAE")]
    [InlineData("UNGALIOPHIIDAE")]
    [InlineData("XENOPELTIDAE")]
    [InlineData("XENOTYPHLOPIDAE")]
    [InlineData("CALABARIIDAE")]
    [InlineData("LOXOCEMIDAE")]
    [InlineData("COLUBRIDAE")]
    public void ShippedRules_PutEverySnakeFamilyInSnakes_WithNoPlacement(string family) {
        var match = ShippedRules.Value.ResolveVirtualGroup("SQUAMATA", family, new string[0]);

        Assert.Equal("Snakes", match?.Group.Name);
    }

    [Theory]
    [InlineData("SQUAMATA", "SCINCIDAE", new[] { "Autarchoglossa" }, "Lizards", -1)]
    [InlineData("SQUAMATA", "IGUANIDAE", new[] { "Iguania" }, "Lizards", -1)]
    [InlineData("SQUAMATA", "AMPHISBAENIDAE", new[] { "Amphisbaenia" }, "Worm lizards", 0)]
    [InlineData("SQUAMATA", "NATRICIDAE", new[] { "Serpentes", "Alethinophidia" }, "Snakes", 0)]
    [InlineData("ARTIODACTYLA", "DELPHINIDAE", new[] { "Cetacea", "Odontoceti" }, "Cetaceans", 0)]
    [InlineData("ARTIODACTYLA", "BOVIDAE", new string[0], "Even-toed ungulates", -1)]
    public void ShippedRules_MatchCoLClades(string order, string family, string[] clades, string group, int cladeIndex) {
        var match = ShippedRules.Value.ResolveVirtualGroup(order, family, clades);

        Assert.Equal(group, match?.Group.Name);
        Assert.Equal(cladeIndex, match?.CladeIndex);
    }
}
