using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;
using static BeastieBot3.Tests.ListTreeFixtures;

namespace BeastieBot3.Tests;

// Pins when Catalogue of Life nodes between two grouping levels become headings: suborders inside an
// order when they cut the number of family headings, not for small or lopsided sets, not for a
// suborder with one family, nested layers (Cetacea, then Odontoceti / Mysticeti), and never past H6.
public class IntermediateLayerTests {
    [Fact]
    public void Suborders_AreInsertedBetweenOrderAndFamilies() {
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(SquamataRecords(), SquamataPlacement(), diagnostics: diagnostics);

        var squamata = Child(root, Squamata);
        Assert.Equal(
            new[] { "Autarchoglossa", "Gekkota", "Iguania", "Serpentes", "AMPHISBAENIDAE", "DIBAMIDAE" },
            Values(squamata));
        var serpentes = Child(squamata, "Serpentes");
        Assert.Equal(TreeNodeKind.Intermediate, serpentes.Kind);
        Assert.Equal("suborder", serpentes.Label);
        Assert.True(serpentes.ShowRank);
        Assert.Equal(new[] { "COLUBRIDAE", "ELAPIDAE", "NATRICIDAE", "VIPERIDAE" }, Values(serpentes));

        var decision = Assert.Single(diagnostics.Layers, d => d.Outcome == "accepted");
        Assert.Equal("suborder", decision.Ranks);
        Assert.Equal(102, decision.ItemCount);
        Assert.Equal(12, decision.Anchors);
        Assert.Equal(6, decision.Headings);
    }

    [Fact]
    public void FamilyWithNoSuborder_KeepsItsHeadingBesideTheSuborders() {
        var root = BuildTree(SquamataRecords(), SquamataPlacement());

        var dibamidae = Child(Child(root, Squamata), "DIBAMIDAE");
        Assert.Equal(TreeNodeKind.Level, dibamidae.Kind);
        Assert.Equal("family", dibamidae.Key);
        Assert.Equal(3, dibamidae.Items.Count);
    }

    [Fact]
    public void SuborderWithOneFamily_IsDemoted() {
        var root = BuildTree(SquamataRecords(), SquamataPlacement());

        var squamata = Child(root, Squamata);
        Assert.DoesNotContain("Amphisbaenia", Values(squamata));
        Assert.Equal(TreeNodeKind.Level, Child(squamata, "AMPHISBAENIDAE").Kind);
    }

    [Fact]
    public void SmallSet_GetsNoLayer() {
        // Two species of each family: 23 species, below the 30-species minimum.
        var records = SquamataRecords(withTurtles: false)
            .GroupBy(r => r.FamilyName)
            .SelectMany(g => g.Take(2))
            .ToList();
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(records, SquamataPlacement(), diagnostics: diagnostics);

        Assert.All(root.Children, c => Assert.Equal(TreeNodeKind.Level, c.Kind));
        Assert.Equal("rejected:few_items", Assert.Single(diagnostics.Layers).Outcome);
    }

    [Fact]
    public void DominantSuborder_GetsNoLayer() {
        var records = new List<IucnSpeciesRecord>();
        records.AddRange(Family(Reptilia, Squamata, "COLUBRIDAE", 40));
        records.AddRange(Family(Reptilia, Squamata, "ELAPIDAE", 20));
        records.AddRange(Family(Reptilia, Squamata, "VIPERIDAE", 10));
        records.AddRange(Family(Reptilia, Squamata, "NATRICIDAE", 10));
        records.AddRange(Family(Reptilia, Squamata, "DIPSADIDAE", 10));
        records.AddRange(Family(Reptilia, Squamata, "GEKKONIDAE", 3));
        records.AddRange(Family(Reptilia, Squamata, "PHYLLODACTYLIDAE", 2));
        var placement = new TaxonPlacementIndex(new[] {
            OrderToFamily(Reptilia, Squamata, "COLUBRIDAE", Suborder("Serpentes")),
            OrderToFamily(Reptilia, Squamata, "ELAPIDAE", Suborder("Serpentes")),
            OrderToFamily(Reptilia, Squamata, "VIPERIDAE", Suborder("Serpentes")),
            OrderToFamily(Reptilia, Squamata, "NATRICIDAE", Suborder("Serpentes")),
            OrderToFamily(Reptilia, Squamata, "DIPSADIDAE", Suborder("Serpentes")),
            OrderToFamily(Reptilia, Squamata, "GEKKONIDAE", Suborder("Gekkota")),
            OrderToFamily(Reptilia, Squamata, "PHYLLODACTYLIDAE", Suborder("Gekkota")),
        });
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(records, placement, diagnostics: diagnostics);

        Assert.All(root.Children, c => Assert.Equal(TreeNodeKind.Level, c.Kind));
        var decision = Assert.Single(diagnostics.Layers);
        Assert.Equal("rejected:dominant_group", decision.Outcome);
        Assert.Equal(0.947, decision.LargestShare);
    }

    // Sharks and rays share subclass Neoselachii; only the chimaeras lie outside it.
    private static (List<IucnSpeciesRecord> Records, TaxonPlacementIndex Placement) SharksAndRays() {
        const string chondrichthyes = "CHONDRICHTHYES";
        var orders = new[] {
            ("CARCHARHINIFORMES", 20, "Selachii"), ("LAMNIFORMES", 8, "Selachii"), ("SQUALIFORMES", 10, "Selachii"),
            ("MYLIOBATIFORMES", 15, "Batoidea"), ("RAJIFORMES", 12, "Batoidea"), ("RHINOPRISTIFORMES", 6, "Batoidea"),
            ("CHIMAERIFORMES", 3, (string?)null),
        };
        var records = orders.SelectMany(o => Family(chondrichthyes, o.Item1, o.Item1[..4] + "IDAE", o.Item2)).ToList();
        var placement = new TaxonPlacementIndex(orders.Where(o => o.Item3 != null).Select(o => ClassToOrder(
            chondrichthyes, o.Item1, new PlacementNode("Neoselachii", "subclass", true), new PlacementNode(o.Item3!, "infraclass", true))));
        return (records, placement);
    }

    [Fact]
    public void DominantNode_BlocksTheLayer_WithoutLookThrough() {
        var (records, placement) = SharksAndRays();
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(records, placement, diagnostics: diagnostics,
            layers: new IntermediateLayerOptions(LookThroughDominant: false));

        Assert.All(root.Children, c => Assert.Equal("order", c.Key));
        Assert.Equal("rejected:dominant_group", Assert.Single(diagnostics.Layers).Outcome);
    }

    [Fact]
    public void LookThroughDominant_ReachesSharksAndRays() {
        var (records, placement) = SharksAndRays();
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(records, placement, diagnostics: diagnostics,
            layers: new IntermediateLayerOptions(LookThroughDominant: true));

        Assert.Equal(new[] { "Batoidea", "Selachii", "CHIMAERIFORMES" }, Values(root));
        Assert.Equal("infraclass", Child(root, "Batoidea").Label);
        Assert.Equal(
            new[] { "looked_through:dominant_group", "accepted" },
            diagnostics.Layers.Select(d => d.Outcome));
    }

    [Fact]
    public void NodeSharedByEveryItem_IsSkippedWithoutAHeading() {
        // Every family sits under infraorder Alethinophidia below suborder Serpentes: Serpentes adds
        // nothing, and the next node down is read instead.
        var families = new[] { ("COLUBRIDAE", 20, "Colubroidea"), ("ELAPIDAE", 10, "Colubroidea"), ("VIPERIDAE", 8, "Colubroidea"),
            ("BOIDAE", 6, "Booidea"), ("ERYCIDAE", 5, "Booidea"), ("PYTHONIDAE", 6, "Pythonoidea"), ("XENOPELTIDAE", 1, "Pythonoidea") };
        var records = families.SelectMany(f => Family(Reptilia, Squamata, f.Item1, f.Item2)).ToList();
        var placement = new TaxonPlacementIndex(families.Select(f => OrderToFamily(Reptilia, Squamata, f.Item1,
            Suborder("Serpentes"), new PlacementNode("Alethinophidia", "infraorder", true), new PlacementNode(f.Item3, "superfamily", true))));

        var root = BuildTree(records, placement);

        Assert.Equal(new[] { "Booidea", "Colubroidea", "Pythonoidea" }, Values(root));
        Assert.All(root.Children, c => Assert.Equal("superfamily", c.Label));
    }

    [Fact]
    public void Cetacea_IsShownByName_WithFamiliesBelowIt_ByDefault() {
        // One layer of CoL headings between order and family: Cetacea, not Cetacea and its suborders.
        var root = BuildTree(ArtiodactylaRecords(), ArtiodactylaPlacement());

        var cetacea = Child(Child(root, Artiodactyla), "Cetacea");
        Assert.False(cetacea.ShowRank);
        Assert.All(cetacea.Children, c => Assert.Equal("family", c.Key));
    }

    [Fact]
    public void Cetacea_IsShownByName_ThenItsSuborders_WithTwoLayers() {
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(ArtiodactylaRecords(), ArtiodactylaPlacement(), diagnostics: diagnostics,
            layers: new IntermediateLayerOptions(MaxLayers: 2));

        var artiodactyla = Child(root, Artiodactyla);
        Assert.Equal(
            new[] { "Cetacea", "BOVIDAE", "CAMELIDAE", "CERVIDAE", "SUIDAE", "TAYASSUIDAE" },
            Values(artiodactyla));
        var cetacea = Child(artiodactyla, "Cetacea");
        Assert.Equal(TreeNodeKind.Intermediate, cetacea.Kind);
        Assert.False(cetacea.ShowRank);
        Assert.Null(cetacea.Label);
        Assert.Equal(new[] { "Mysticeti", "Odontoceti" }, Values(cetacea));
        Assert.Equal(new[] { "DELPHINIDAE", "KOGIIDAE", "PHOCOENIDAE", "ZIPHIIDAE" }, Values(Child(cetacea, "Odontoceti")));
        Assert.Equal(2, diagnostics.Layers.Count(d => d.Outcome == "accepted"));
    }

    [Fact]
    public void NoPlacement_GroupsByIucnRanksOnly() {
        var root = BuildTree(SquamataRecords(), placement: null);

        var squamata = Child(root, Squamata);
        Assert.All(squamata.Children, c => Assert.Equal("family", c.Key));
        Assert.Equal(SquamataFamilies.Length, squamata.Children.Count);
    }

    [Fact]
    public void StartingAtH4_TheLayerStillFitsAboveTheFamilies() {
        // H4 order, H5 suborder, H6 family: three heading levels, all used.
        var root = BuildTree(SquamataRecords(), SquamataPlacement(), startHeading: 4);

        Assert.True(root.Height <= 3);
        Assert.Contains("Serpentes", Values(Child(root, Squamata)));
    }

    [Fact]
    public void StartingAtH4_ANestedLayerThatWouldPassH6_IsNotShown() {
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(ArtiodactylaRecords(), ArtiodactylaPlacement(), startHeading: 4, diagnostics: diagnostics,
            layers: new IntermediateLayerOptions(MaxLayers: 2));

        Assert.True(root.Height <= 3);
        var cetacea = Child(Child(root, Artiodactyla), "Cetacea");
        Assert.All(cetacea.Children, c => Assert.Equal("family", c.Key));
        Assert.Contains(diagnostics.Layers, d => d.Outcome == "rejected:heading_depth");
    }

    [Fact]
    public void StartingAtH5_NoLayerFits() {
        var diagnostics = new AutoSplitDiagnosticCollector();

        var root = BuildTree(SquamataRecords(), SquamataPlacement(), startHeading: 5, diagnostics: diagnostics);

        Assert.True(root.Height <= 2);
        Assert.All(Child(root, Squamata).Children, c => Assert.Equal("family", c.Key));
        Assert.Equal("rejected:heading_depth", Assert.Single(diagnostics.Layers).Outcome);
    }

    [Fact]
    public void ConfiguredColRankLevel_ReadsThatNode_AndFamiliesFollowIt() {
        var grouping = new[] {
            new GroupingLevelDefinition { Level = "order", Label = "Order" },
            new GroupingLevelDefinition { Level = "superfamily", Label = "Superfamily", UnknownLabel = "Other superfamilies" },
            new GroupingLevelDefinition { Level = "family", Label = "Family" },
        };
        var records = Family(Reptilia, "TESTUDINES", "TESTUDINIDAE", 3)
            .Concat(Family(Reptilia, "TESTUDINES", "EMYDIDAE", 2))
            .Concat(Family(Reptilia, "TESTUDINES", "CHELIDAE", 2))
            .ToList();
        var placement = new TaxonPlacementIndex(new[] {
            OrderToFamily(Reptilia, "TESTUDINES", "TESTUDINIDAE", Suborder("Cryptodira"), new PlacementNode("Testudinoidea", "superfamily", true)),
            OrderToFamily(Reptilia, "TESTUDINES", "EMYDIDAE", Suborder("Cryptodira"), new PlacementNode("Testudinoidea", "superfamily", true)),
            OrderToFamily(Reptilia, "TESTUDINES", "CHELIDAE", Suborder("Pleurodira"), new PlacementNode("Chelidoidea", "superfamily", true)),
        });

        var root = BuildTree(records, placement, grouping: grouping);

        Assert.Equal(new[] { "Chelidoidea", "Testudinoidea" }, Values(root));
        Assert.All(root.Children, c => Assert.Equal("superfamily", c.Key));
        Assert.Equal(new[] { "EMYDIDAE", "TESTUDINIDAE" }, Values(Child(root, "Testudinoidea")));
    }

    [Fact]
    public void AutoSplit_ReadsSubfamiliesFromThePlacement() {
        var records = new List<IucnSpeciesRecord>();
        records.AddRange(Enumerable.Range(1, 12).Select(i => Rec(Mammalia, "RODENTIA", "MURIDAE", "Mus", $"m{i}")));
        records.AddRange(Enumerable.Range(1, 12).Select(i => Rec(Mammalia, "RODENTIA", "MURIDAE", "Rattus", $"r{i}")));
        records.AddRange(Enumerable.Range(1, 10).Select(i => Rec(Mammalia, "RODENTIA", "MURIDAE", "Gerbillus", $"g{i}")));
        var placement = new TaxonPlacementIndex(new[] {
            FamilyToGenus("MURIDAE", "Mus", new PlacementNode("Murinae", "subfamily", true)),
            FamilyToGenus("MURIDAE", "Rattus", new PlacementNode("Murinae", "subfamily", true)),
            FamilyToGenus("MURIDAE", "Gerbillus", new PlacementNode("Gerbillinae", "subfamily", true)),
        });
        var grouping = DefaultGrouping();
        var autoSplit = TaxonGroupingHelper.BuildAutoSplitOptions(new AutoSplitConfig(), grouping, placement)!;

        Assert.Equal(new[] { "subfamily", "tribe", "subtribe", "genus" }, autoSplit.CandidateLevels.Select(l => l.Key));
        var root = TaxonomyTreeBuilder.Build(records, TaxonGroupingHelper.BuildLevels(grouping, placement),
            new TaxonomyTreeOptions<IucnSpeciesRecord> { AutoSplit = autoSplit, HeadingLevels = 4 });

        Assert.Equal(new[] { "Gerbillinae", "Murinae" }, Values(root));
        Assert.All(root.Children, c => Assert.Equal(TreeNodeKind.AutoSplit, c.Kind));
    }

    [Fact]
    public void AutoSplit_WithNoPlacement_TriesGenusOnly() {
        var autoSplit = TaxonGroupingHelper.BuildAutoSplitOptions(new AutoSplitConfig(), DefaultGrouping(), placement: null)!;

        Assert.Equal(new[] { "genus" }, autoSplit.CandidateLevels.Select(l => l.Key));
    }
}
