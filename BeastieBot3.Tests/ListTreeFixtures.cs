using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Hand-built IUCN records and Catalogue of Life placements for the list tree tests. Placement keys
// come from TaxonPlacementIndex's own key functions, so a test cannot miss by key spelling.
internal static class ListTreeFixtures {
    private static long _nextId = 1;

    public const string Animalia = "ANIMALIA";

    public static IucnSpeciesRecord Rec(string className, string order, string family, string genus, string species) =>
        new(
            TaxonId: Interlocked.Increment(ref _nextId), AssessmentId: 1, RedlistCategory: "Least Concern", StatusCode: "LC",
            ScientificNameAssessments: $"{genus} {species}", ScientificNameTaxonomy: $"{genus} {species}",
            KingdomName: Animalia, PhylumName: "CHORDATA", ClassName: className,
            OrderName: order, FamilyName: family, GenusName: genus, SpeciesName: species,
            InfraType: null, InfraName: null, SubpopulationName: null, Scopes: "Global",
            Authority: null, InfraAuthority: null, PossiblyExtinct: null, PossiblyExtinctInTheWild: null,
            YearPublished: "2024");

    /// <summary><paramref name="count"/> species of one family, all in a genus named after the family.</summary>
    public static IEnumerable<IucnSpeciesRecord> Family(string className, string order, string family, int count) {
        var genus = char.ToUpperInvariant(family[0]) + family[1..].ToLowerInvariant().Replace("idae", "us");
        return Enumerable.Range(1, count).Select(i => Rec(className, order, family, genus, $"species{i}"));
    }

    public static PlacementNode Suborder(string name) => new(name, "suborder", ShowRank: true);

    /// <summary>A CoL node with the same rank as the IUCN taxon above it (order Cetacea inside order Artiodactyla).</summary>
    public static PlacementNode NameOnly(string name, string colRank) => new(name, colRank, ShowRank: false);

    public static PlacementPath OrderToFamily(string className, string order, string family, params PlacementNode[] nodes) =>
        new(PlacementSpan.OrderToFamily, TaxonPlacementIndex.FamilyKey(Animalia, className, order, family), nodes);

    public static PlacementPath FamilyToGenus(string family, string genus, params PlacementNode[] nodes) =>
        new(PlacementSpan.FamilyToGenus, TaxonPlacementIndex.GenusKey(Animalia, family, genus), nodes);

    /// <summary>The default grouping from wikipedia-lists.yml: order, then family with lumping below 5.</summary>
    public static IReadOnlyList<GroupingLevelDefinition> DefaultGrouping() => new[] {
        new GroupingLevelDefinition { Level = "order", Label = "Order", UnknownLabel = "Other orders" },
        new GroupingLevelDefinition {
            Level = "family", Label = "Family", UnknownLabel = "Unassigned families",
            MinItems = 5, MinGroupsForOther = 3,
        },
    };

    public static TaxonomyTreeNode<IucnSpeciesRecord> BuildTree(
        IEnumerable<IucnSpeciesRecord> records,
        ITaxonPlacement? placement,
        int startHeading = 3,
        IAutoSplitDiagnostics? diagnostics = null,
        TaxonRulesService? taxonRules = null,
        IReadOnlyList<GroupingLevelDefinition>? grouping = null) {
        grouping ??= DefaultGrouping();
        var options = new TaxonomyTreeOptions<IucnSpeciesRecord> {
            Intermediate = new IntermediateLayerOptions(),
            VirtualGroups = TaxonGroupingHelper.BuildVirtualGroupOptions(taxonRules),
            HeadingLevels = BeastieBot3.WikipediaLists.SectionBodyRenderer.HeadingLevelsFrom(startHeading),
            Diagnostics = diagnostics,
        };
        return TaxonomyTreeBuilder.Build(records.ToList(), TaxonGroupingHelper.BuildLevels(grouping, placement), options);
    }

    public static TaxonomyTreeNode<IucnSpeciesRecord> Child(TaxonomyTreeNode<IucnSpeciesRecord> node, string value) =>
        Assert.Single(node.Children, c => string.Equals(c.Value, value, System.StringComparison.OrdinalIgnoreCase));

    public static IEnumerable<string?> Values(TaxonomyTreeNode<IucnSpeciesRecord> node) => node.Children.Select(c => c.Value);

    // ---- Squamata: four suborders with several families each, Dibamidae with no suborder, and
    // Amphisbaenia with a single family. A second order keeps the order headings visible. ----

    public const string Reptilia = "REPTILIA";
    public const string Squamata = "SQUAMATA";

    public static readonly (string Family, int Count, string? Suborder)[] SquamataFamilies = {
        ("COLUBRIDAE", 20, "Serpentes"), ("ELAPIDAE", 10, "Serpentes"), ("VIPERIDAE", 8, "Serpentes"), ("NATRICIDAE", 6, "Serpentes"),
        ("GEKKONIDAE", 12, "Gekkota"), ("PHYLLODACTYLIDAE", 5, "Gekkota"),
        ("IGUANIDAE", 6, "Iguania"), ("AGAMIDAE", 7, "Iguania"),
        ("SCINCIDAE", 15, "Autarchoglossa"), ("LACERTIDAE", 6, "Autarchoglossa"),
        ("AMPHISBAENIDAE", 4, "Amphisbaenia"),
        ("DIBAMIDAE", 3, null),
    };

    public static List<IucnSpeciesRecord> SquamataRecords(bool withTurtles = true) {
        var records = SquamataFamilies.SelectMany(f => Family(Reptilia, Squamata, f.Family, f.Count)).ToList();
        if (withTurtles) {
            records.AddRange(Family(Reptilia, "TESTUDINES", "TESTUDINIDAE", 6));
        }
        return records;
    }

    public static TaxonPlacementIndex SquamataPlacement() => new(
        SquamataFamilies
            .Where(f => f.Suborder != null)
            .Select(f => OrderToFamily(Reptilia, Squamata, f.Family, Suborder(f.Suborder!))));

    // ---- Artiodactyla: land families with no CoL node, and whales under order Cetacea (shown by
    // name only) with suborders Odontoceti and Mysticeti. ----

    public const string Mammalia = "MAMMALIA";
    public const string Artiodactyla = "ARTIODACTYLA";

    public static readonly (string Family, int Count, string? Suborder)[] ArtiodactylaFamilies = {
        ("BOVIDAE", 20, null), ("CERVIDAE", 8, null), ("SUIDAE", 5, null), ("TAYASSUIDAE", 3, null), ("CAMELIDAE", 4, null),
        ("DELPHINIDAE", 12, "Odontoceti"), ("PHOCOENIDAE", 5, "Odontoceti"), ("ZIPHIIDAE", 6, "Odontoceti"), ("KOGIIDAE", 2, "Odontoceti"),
        ("BALAENOPTERIDAE", 6, "Mysticeti"), ("BALAENIDAE", 3, "Mysticeti"),
    };

    public static List<IucnSpeciesRecord> ArtiodactylaRecords(bool withCarnivores = true) {
        var records = ArtiodactylaFamilies.SelectMany(f => Family(Mammalia, Artiodactyla, f.Family, f.Count)).ToList();
        if (withCarnivores) {
            records.AddRange(Family(Mammalia, "CARNIVORA", "FELIDAE", 6));
        }
        return records;
    }

    public static TaxonPlacementIndex ArtiodactylaPlacement() => new(
        ArtiodactylaFamilies
            .Where(f => f.Suborder != null)
            .Select(f => OrderToFamily(Mammalia, Artiodactyla, f.Family, NameOnly("Cetacea", "order"), Suborder(f.Suborder!))));
}
