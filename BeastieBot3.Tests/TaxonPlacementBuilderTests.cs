using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// Pins the vote and containment rules that decide which Catalogue of Life nodes are inserted
// between IUCN ranks. Lineages are synthetic: "rank:Name" steps from the root down to the genus,
// with the node name doubling as its CoL ID.
public class TaxonPlacementBuilderTests {
    private const string Animals = "kingdom:Animalia|phylum:Chordata";
    private const string Reptiles = Animals + "|class:Reptilia|order:Squamata";
    private const string Mammals = Animals + "|class:Mammalia|subclass:Theria|infraclass:Eutheria";
    private const string RayFins = Animals + "|parvphylum:Osteichthyes|gigaclass:Actinopterygii|superclass:Actinopteri|class:Teleostei";

    private static IReadOnlyList<ColLineageNode> Lineage(string spec) =>
        spec.Split('|').Select(step => {
            var parts = step.Split(':');
            return new ColLineageNode(parts[1], parts[1], parts[0]);
        }).ToList();

    private static IEnumerable<PlacementSample> Species(int count, string kingdom, string cls, string order, string family, string genus, string? lineage) =>
        Enumerable.Range(0, count).Select(_ =>
            new PlacementSample(kingdom, cls, order, family, genus, lineage is null ? null : Lineage(lineage)));

    private static IEnumerable<PlacementSample> Reptile(int count, string family, string genus, string colPath) =>
        Species(count, "ANIMALIA", "REPTILIA", "SQUAMATA", family, genus, $"{Reptiles}|{colPath}|genus:{genus}");

    private static string[] Names(IReadOnlyList<PlacementNode> nodes) => nodes.Select(n => n.Name).ToArray();

    private static AnchorDiagnostics Anchor(PlacementBuildOutput output, PlacementSpan span, string key) =>
        output.Anchors.Single(a => a.Anchor.Span == span && a.Anchor.Key == key);

    [Fact]
    public void SquamataFamilies_GetTheirSuborders() {
        var samples = Reptile(30, "COLUBRIDAE", "Coluber", "suborder:Serpentes|infraorder:Alethinophidia|family:Colubridae")
            .Concat(Reptile(12, "NATRICIDAE", "Natrix", "suborder:Serpentes|infraorder:Alethinophidia|family:Colubridae"))
            .Concat(Reptile(20, "IGUANIDAE", "Iguana", "suborder:Iguania|family:Iguanidae"))
            .Concat(Reptile(5, "DIBAMIDAE", "Dibamus", "family:Dibamidae"));

        var index = TaxonPlacementBuilder.Build(samples).Index;

        var natricids = index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE");
        Assert.Equal(new[] { "Serpentes", "Alethinophidia" }, Names(natricids));
        Assert.Equal(new[] { "suborder", "infraorder" }, natricids.Select(n => n.ColRank).ToArray());
        Assert.All(natricids, n => Assert.True(n.ShowRank));

        Assert.Equal(new[] { "Iguania" }, Names(index.BetweenOrderAndFamily("animalia", "reptilia", "squamata", "iguanidae")));
        Assert.Empty(index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "DIBAMIDAE"));
    }

    [Fact]
    public void FamilySplitAcrossSuborders_GetsNoSuborder() {
        var samples = Reptile(11, "MIXEDIDAE", "Alpha", "suborder:Serpentes|family:Mixedidae")
            .Concat(Reptile(9, "MIXEDIDAE", "Beta", "suborder:Iguania|family:Mixedidae"));

        var output = TaxonPlacementBuilder.Build(samples);

        Assert.Empty(output.Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "MIXEDIDAE"));
        var diag = Anchor(output, PlacementSpan.OrderToFamily, TaxonPlacementIndex.FamilyKey("ANIMALIA", "REPTILIA", "SQUAMATA", "MIXEDIDAE"));
        Assert.Equal(20, diag.Voting);
        Assert.Equal(new[] { "Serpentes", "Iguania" }, diag.Dropped.Select(d => d.Name).ToArray());
        Assert.All(diag.Dropped, d => Assert.Equal(PlacementDropReason.BelowVote, d.Reason));
        Assert.Equal(0.55, diag.Dropped[0].Share, 3);
    }

    [Fact]
    public void UnmatchedSpecies_DoNotVote() {
        var samples = Reptile(2, "RAREIDAE", "Rarus", "suborder:Serpentes|family:Rareidae")
            .Concat(Species(8, "ANIMALIA", "REPTILIA", "SQUAMATA", "RAREIDAE", "Rarus", lineage: null));

        var output = TaxonPlacementBuilder.Build(samples);

        Assert.Equal(new[] { "Serpentes" }, Names(output.Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "RAREIDAE")));
        var diag = Anchor(output, PlacementSpan.OrderToFamily, TaxonPlacementIndex.FamilyKey("ANIMALIA", "REPTILIA", "SQUAMATA", "RAREIDAE"));
        Assert.Equal(10, diag.Species);
        Assert.Equal(2, diag.Voting);
        Assert.Equal(10, output.SampleCount);
        Assert.Equal(2, output.MatchedCount);
    }

    [Fact]
    public void AnchorWithNoMatchedSpecies_GetsNoPath() {
        var samples = Species(4, "ANIMALIA", "REPTILIA", "SQUAMATA", "GHOSTIDAE", "Ghostus", lineage: null);
        var index = TaxonPlacementBuilder.Build(samples).Index;
        Assert.Empty(index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "GHOSTIDAE"));
        Assert.Equal(0, index.Count);
    }

    [Fact]
    public void Cetacea_IsDisplacedUnderArtiodactyla_WithoutItsRank() {
        var samples = Species(40, "ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "DELPHINIDAE", "Delphinus",
                $"{Mammals}|order:Cetacea|suborder:Odontoceti|family:Delphinidae|genus:Delphinus")
            .Concat(Species(15, "ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "BALAENOPTERIDAE", "Balaenoptera",
                $"{Mammals}|order:Cetacea|suborder:Mysticeti|family:Balaenopteridae|genus:Balaenoptera"))
            .Concat(Species(140, "ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "BOVIDAE", "Bos",
                $"{Mammals}|order:Artiodactyla|family:Bovidae|genus:Bos"));

        var index = TaxonPlacementBuilder.Build(samples).Index;

        var dolphins = index.BetweenOrderAndFamily("ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "DELPHINIDAE");
        Assert.Equal(new[] { "Cetacea", "Odontoceti" }, Names(dolphins));
        Assert.Equal("order", dolphins[0].ColRank);
        Assert.False(dolphins[0].ShowRank);
        Assert.True(dolphins[1].ShowRank);
        Assert.Equal(new[] { "Cetacea", "Mysticeti" }, Names(index.BetweenOrderAndFamily("ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "BALAENOPTERIDAE")));
        Assert.Empty(index.BetweenOrderAndFamily("ANIMALIA", "MAMMALIA", "ARTIODACTYLA", "BOVIDAE"));

        // Between class and order the whales and the land artiodactyls agree.
        var above = index.BetweenClassAndOrder("ANIMALIA", "MAMMALIA", "ARTIODACTYLA");
        Assert.Equal(new[] { "Theria", "Eutheria" }, Names(above));
        Assert.All(above, n => Assert.True(n.ShowRank));
    }

    [Fact]
    public void PerciformesUnderScorpaeniformes_FailsContainment_WhileScorpaenoideiIsKept() {
        var samples = Species(60, "ANIMALIA", "ACTINOPTERYGII", "PERCIFORMES", "PERCIDAE", "Perca",
                $"{RayFins}|order:Perciformes|suborder:Percoidei|family:Percidae|genus:Perca")
            .Concat(Species(20, "ANIMALIA", "ACTINOPTERYGII", "SCORPAENIFORMES", "SCORPAENIDAE", "Scorpaena",
                $"{RayFins}|order:Perciformes|suborder:Scorpaenoidei|family:Scorpaenidae|genus:Scorpaena"));

        var output = TaxonPlacementBuilder.Build(samples);

        var scorpions = output.Index.BetweenOrderAndFamily("ANIMALIA", "ACTINOPTERYGII", "SCORPAENIFORMES", "SCORPAENIDAE");
        Assert.Equal(new[] { "Scorpaenoidei" }, Names(scorpions));
        Assert.True(scorpions[0].ShowRank);

        var diag = Anchor(output, PlacementSpan.OrderToFamily,
            TaxonPlacementIndex.FamilyKey("ANIMALIA", "ACTINOPTERYGII", "SCORPAENIFORMES", "SCORPAENIDAE"));
        var perciformes = Assert.Single(diag.Dropped);
        Assert.Equal("Perciformes", perciformes.Name);
        Assert.Equal(PlacementDropReason.NotContained, perciformes.Reason);
        Assert.Equal(1.0, perciformes.Share);
        Assert.Equal(0.25, perciformes.Containment, 3);
        Assert.Equal(80, perciformes.NodeSpecies);
        Assert.Equal("PERCIFORMES", perciformes.MainParent);
        Assert.Equal(60, perciformes.MainParentSpecies);

        // IUCN Perciformes species in CoL Perciformes see only the suborder.
        Assert.Equal(new[] { "Percoidei" }, Names(output.Index.BetweenOrderAndFamily("ANIMALIA", "ACTINOPTERYGII", "PERCIFORMES", "PERCIDAE")));
    }

    [Fact]
    public void Actinopterygii_IsMatchedAtGigaclass() {
        var samples = Species(25, "ANIMALIA", "ACTINOPTERYGII", "CYPRINIFORMES", "CYPRINIDAE", "Cyprinus",
            $"{RayFins}|order:Cypriniformes|family:Cyprinidae|genus:Cyprinus");

        var above = TaxonPlacementBuilder.Build(samples).Index.BetweenClassAndOrder("ANIMALIA", "ACTINOPTERYGII", "CYPRINIFORMES");

        Assert.Equal(new[] { "Actinopteri", "Teleostei" }, Names(above));
        Assert.Equal(new[] { "superclass", "class" }, above.Select(n => n.ColRank).ToArray());
        Assert.All(above, n => Assert.False(n.ShowRank));
    }

    [Fact]
    public void Chondrichthyes_IsMatchedAtParvphylum_AndRaysGetTheirInfraclass() {
        const string sharks = Animals + "|parvphylum:Chondrichthyes|class:Elasmobranchii";
        var samples = Species(30, "ANIMALIA", "CHONDRICHTHYES", "RAJIFORMES", "RAJIDAE", "Raja",
                $"{sharks}|infraclass:Batoidea|order:Rajiformes|family:Rajidae|genus:Raja")
            .Concat(Species(20, "ANIMALIA", "CHONDRICHTHYES", "CARCHARHINIFORMES", "CARCHARHINIDAE", "Carcharhinus",
                $"{sharks}|infraclass:Selachii|superorder:Galeomorphi|order:Carcharhiniformes|family:Carcharhinidae|genus:Carcharhinus"));

        var index = TaxonPlacementBuilder.Build(samples).Index;

        var rays = index.BetweenClassAndOrder("ANIMALIA", "CHONDRICHTHYES", "RAJIFORMES");
        Assert.Equal(new[] { "Elasmobranchii", "Batoidea" }, Names(rays));
        Assert.False(rays[0].ShowRank);
        Assert.True(rays[1].ShowRank);
        Assert.Equal(new[] { "Elasmobranchii", "Selachii", "Galeomorphi" },
            Names(index.BetweenClassAndOrder("ANIMALIA", "CHONDRICHTHYES", "CARCHARHINIFORMES")));
    }

    [Fact]
    public void FamilyToGenus_KeepsSubfamilyAndTribe() {
        var samples = Species(12, "PLANTAE", "MAGNOLIOPSIDA", "GENTIANALES", "RUBIACEAE", "Spermacoce",
            "kingdom:Plantae|phylum:Tracheophyta|class:Magnoliopsida|order:Gentianales|family:Rubiaceae|subfamily:Rubioideae|tribe:Spermacoceae|genus:Spermacoce");

        var path = TaxonPlacementBuilder.Build(samples).Index.BetweenFamilyAndGenus("PLANTAE", "RUBIACEAE", "Spermacoce");

        Assert.Equal(new[] { "Rubioideae", "Spermacoceae" }, Names(path));
        Assert.Equal(new[] { "subfamily", "tribe" }, path.Select(n => n.ColRank).ToArray());
        Assert.All(path, n => Assert.True(n.ShowRank));
    }

    [Fact]
    public void Subfamily_IsUsedOnlyUnderTheIucnFamilyThatHoldsIt() {
        const string cyprinids = Animals + "|class:Teleostei|order:Cypriniformes|family:Cyprinidae|subfamily:Labeoninae";
        var samples = Species(20, "ANIMALIA", "ACTINOPTERYGII", "CYPRINIFORMES", "CYPRINIDAE", "Labeo", $"{cyprinids}|genus:Labeo")
            .Concat(Species(1, "ANIMALIA", "ACTINOPTERYGII", "CYPRINIFORMES", "DANIONIDAE", "Garra", $"{cyprinids}|genus:Garra"));

        var output = TaxonPlacementBuilder.Build(samples);

        Assert.Equal(new[] { "Labeoninae" }, Names(output.Index.BetweenFamilyAndGenus("ANIMALIA", "CYPRINIDAE", "Labeo")));
        Assert.Empty(output.Index.BetweenFamilyAndGenus("ANIMALIA", "DANIONIDAE", "Garra"));
        var dropped = Assert.Single(Anchor(output, PlacementSpan.FamilyToGenus, TaxonPlacementIndex.GenusKey("ANIMALIA", "DANIONIDAE", "Garra")).Dropped);
        Assert.Equal(PlacementDropReason.NotContained, dropped.Reason);
        Assert.Equal("CYPRINIDAE", dropped.MainParent);
    }

    [Fact]
    public void LineageCutShort_DoesNotVote() {
        // A chain that starts at the suborder (its parent row is missing) has no CoL node at or
        // above the IUCN order, so it cannot say what lies between order and family.
        var samples = Reptile(3, "COLUBRIDAE", "Coluber", "suborder:Serpentes|family:Colubridae")
            .Append(new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", "COLUBRIDAE", "Coluber",
                Lineage("infraorder:Scolecophidia|family:Colubridae|genus:Coluber")));

        var output = TaxonPlacementBuilder.Build(samples);

        var diag = Anchor(output, PlacementSpan.OrderToFamily, TaxonPlacementIndex.FamilyKey("ANIMALIA", "REPTILIA", "SQUAMATA", "COLUBRIDAE"));
        Assert.Equal(4, diag.Species);
        Assert.Equal(3, diag.Voting);
        Assert.Equal(new[] { "Serpentes" }, Names(output.Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "COLUBRIDAE")));
    }

    [Fact]
    public void UnrankedNodes_AreKeptInLineageOrder_WithoutRank() {
        var samples = Reptile(6, "PYTHONIDAE", "Python", "suborder:Serpentes|unranked:Henophidia|family:Pythonidae");

        var path = TaxonPlacementBuilder.Build(samples).Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "PYTHONIDAE");

        Assert.Equal(new[] { "Serpentes", "Henophidia" }, Names(path));
        Assert.Equal("unranked", path[1].ColRank);
        Assert.False(path[1].ShowRank);
    }

    [Fact]
    public void Paths_StayNested_AcrossFamiliesOfOneOrder() {
        // A random CoL tree under one IUCN order: suborders, infraorders and superfamilies, with
        // each IUCN family drawn mostly from one leaf but with strays. Whatever is kept, a node must
        // have the same nodes above it in every path that contains it.
        var random = new Random(20261002);
        var leaves = new List<string>();
        for (var s = 0; s < 3; s++) {
            for (var i = 0; i < 2; i++) {
                for (var f = 0; f < 2; f++) {
                    leaves.Add($"suborder:S{s}|infraorder:S{s}I{i}|superfamily:S{s}I{i}F{f}");
                }
                leaves.Add($"suborder:S{s}|infraorder:S{s}I{i}");
            }
            leaves.Add($"suborder:S{s}");
        }
        var samples = new List<PlacementSample>();
        for (var family = 0; family < 60; family++) {
            var home = leaves[random.Next(leaves.Count)];
            var size = 1 + random.Next(30);
            for (var n = 0; n < size; n++) {
                var leaf = random.NextDouble() < 0.85 ? home : leaves[random.Next(leaves.Count)];
                samples.Add(new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", $"FAM{family}IDAE", $"Genus{family}",
                    random.NextDouble() < 0.1 ? null : Lineage($"{Reptiles}|{leaf}|family:Fam{family}idae|genus:Genus{family}")));
            }
        }

        var index = TaxonPlacementBuilder.Build(samples).Index;

        var above = new Dictionary<string, string>();
        var nonEmpty = 0;
        for (var family = 0; family < 60; family++) {
            var path = Names(index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", $"FAM{family}IDAE"));
            if (path.Length > 0) {
                nonEmpty++;
            }
            for (var i = 0; i < path.Length; i++) {
                var prefix = string.Join(">", path.Take(i));
                if (above.TryGetValue(path[i], out var seen)) {
                    Assert.Equal(seen, prefix);
                } else {
                    above[path[i]] = prefix;
                }
                // Names encode their ancestors (S1 > S1I0 > S1I0F1), so each step must extend the one before.
                if (i > 0) {
                    Assert.StartsWith(path[i - 1], path[i]);
                }
            }
        }
        Assert.True(nonEmpty > 40, $"expected most families to get a path, got {nonEmpty}");
    }

    [Theory]
    [InlineData(0.5)]
    [InlineData(0.3)]
    [InlineData(1.1)]
    public void VoteThreshold_MustBeAboveOneHalf(double threshold) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new PlacementBuilderOptions { VoteThreshold = threshold });

    [Fact]
    public void HigherThresholds_KeepFewerNodes() {
        var samples = Reptile(17, "MOSTLYIDAE", "Alpha", "suborder:Serpentes|family:Mostlyidae")
            .Concat(Reptile(3, "MOSTLYIDAE", "Beta", "suborder:Iguania|family:Mostlyidae"));

        Assert.Single(TaxonPlacementBuilder.Build(samples).Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "MOSTLYIDAE"));
        var strict = new PlacementBuilderOptions { VoteThreshold = 0.9 };
        Assert.Empty(TaxonPlacementBuilder.Build(samples, strict).Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "MOSTLYIDAE"));
    }
}
