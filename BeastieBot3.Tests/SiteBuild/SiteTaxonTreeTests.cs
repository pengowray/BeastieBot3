using BeastieBot3.Iucn;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.SiteBuild;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests.SiteBuild;

public sealed class SiteTaxonTreeTests {
    private static SiteTaxon Taxon(long id, string name, string kind = "species", string kingdom = "ANIMALIA", string phylum = "CHORDATA",
        string cls = "MAMMALIA", string order = "CARNIVORA", string family = "FELIDAE", string genus = "Panthera",
        long? parent = null, string? status = "LC", bool inRelease = true) {
        var parts = name.Split(' ');
        return new SiteTaxon {
            TaxonId = id, ScientificName = name, Kind = kind, Kingdom = kingdom, Phylum = phylum, ClassName = cls,
            OrderName = order, Family = family, Genus = genus, SpeciesEpithet = parts.Length > 1 ? parts[1] : null,
            InRelease = inRelease, ParentTaxonId = parent, LatestGlobalStatusCode = status,
        };
    }

    [Fact]
    public void Numbers_groups_depth_first_and_gives_each_group_its_taxa_range() {
        var taxa = new[] {
            Taxon(3, "Panthera tigris", status: "EN"),
            Taxon(1, "Panthera leo", status: "VU"),
            Taxon(2, "Acinonyx jubatus", genus: "Acinonyx", status: "VU"),
            Taxon(4, "Canis lupus", family: "CANIDAE", genus: "Canis"),
            Taxon(9, "Old name", inRelease: false),
        };
        var tree = SiteTaxonTree.Build(taxa, SitePlacement.Empty, IucnNotAssignedRules.None);

        Assert.Equal(["Animalia", "Chordata", "Mammalia", "Carnivora", "Canidae", "Canis", "Felidae", "Acinonyx", "Panthera"],
            tree.Nodes.Select(n => n.Name));
        Assert.Equal(Enumerable.Range(1, 9), tree.Nodes.Select(n => n.NodeId));
        var felidae = tree.Nodes.Single(n => n.Name == "Felidae");
        Assert.Equal((2, 4), (felidae.FirstPos, felidae.LastPos));
        Assert.Equal(3, felidae.SpeciesCount);
        Assert.Equal(2, felidae.CategoryCounts["VU"][0]);
        Assert.Equal(1, felidae.CategoryCounts["EN"][0]);
        // In a genus, species in name order: leo before tigris.
        Assert.True(taxa[1].TreePos < taxa[0].TreePos);
        Assert.Equal(tree.Nodes.Single(n => n.Name == "Panthera").NodeId, taxa[0].NodeId);
        Assert.Null(taxa[4].TreePos);
    }

    [Fact]
    public void Puts_a_species_before_its_own_subspecies_and_subpopulations() {
        var taxa = new[] {
            Taxon(10, "Panthera pardus orientalis", kind: "subspecies", parent: 12),
            Taxon(11, "Panthera onca"),
            Taxon(12, "Panthera pardus"),
            Taxon(13, "Panthera pardus", kind: "subpopulation", parent: 12),
        };
        SiteTaxonTree.Build(taxa, SitePlacement.Empty, IucnNotAssignedRules.None);

        Assert.Equal([11L, 12L, 10L, 13L], taxa.OrderBy(t => t.TreePos).Select(t => t.TaxonId));
    }

    [Fact]
    public void Adds_Catalogue_of_Life_groups_between_IUCN_ranks() {
        var placement = new SitePlacement(new Dictionary<(PlacementSpan, string), IReadOnlyList<SitePlacementNode>> {
            [(PlacementSpan.OrderToFamily, TaxonPlacementIndex.FamilyKey("ANIMALIA", "MAMMALIA", "CARNIVORA", "FELIDAE"))] =
                [new SitePlacementNode("Feliformia", "suborder", true, "F1")],
            [(PlacementSpan.FamilyToGenus, TaxonPlacementIndex.GenusKey("ANIMALIA", "FELIDAE", "Panthera"))] =
                [new SitePlacementNode("Pantherinae", "subfamily", true, "P1")],
        });
        var taxa = new[] { Taxon(1, "Panthera leo") };
        var tree = SiteTaxonTree.Build(taxa, placement, IucnNotAssignedRules.None);

        Assert.Equal(["Carnivora", "Feliformia", "Felidae", "Pantherinae", "Panthera"], tree.Nodes.Skip(3).Select(n => n.Name));
        var suborder = tree.Nodes.Single(n => n.Name == "Feliformia");
        Assert.Equal(("suborder", GroupSources.Col, "F1"), (suborder.Rank, suborder.Source, suborder.ColId));
    }

    [Fact]
    public void Takes_an_order_from_the_rules_and_skips_a_rank_with_no_rule() {
        var rules = IucnNotAssignedRules.FromYaml("""
            orders:
              - { class: ACTINOPTERYGII, family: POMACENTRIDAE, order: PERCIFORMES }
            families: []
            """);
        var taxa = new[] {
            Taxon(1, "Amphiprion ocellaris", cls: "ACTINOPTERYGII", order: "NOT ASSIGNED", family: "POMACENTRIDAE", genus: "Amphiprion"),
            Taxon(2, "Xus yus", cls: "ACTINOPTERYGII", order: "NOT ASSIGNED", family: "XIDAE", genus: "Xus"),
        };
        var tree = SiteTaxonTree.Build(taxa, SitePlacement.Empty, rules);

        var order = tree.Nodes.Single(n => n.Rank == "order");
        Assert.Equal(("Perciformes", GroupSources.IucnRule), (order.Name, order.Source));
        var xidae = tree.Nodes.Single(n => n.Name == "Xidae");
        Assert.Equal("class", xidae.Parent!.Rank);
        Assert.Equal(1, tree.TaxaWithUnassignedRank);
    }

    [Fact]
    public void Tells_groups_with_the_same_rank_and_name_apart_by_kingdom_then_parent() {
        var taxa = new[] {
            Taxon(1, "Abronia graminea", genus: "Abronia", family: "ANGUIDAE"),
            Taxon(2, "Abronia alpina", kingdom: "PLANTAE", phylum: "TRACHEOPHYTA", cls: "MAGNOLIOPSIDA", order: "CARYOPHYLLALES",
                family: "NYCTAGINACEAE", genus: "Abronia"),
            Taxon(3, "Xus one", genus: "Xus", family: "AIDAE"),
            Taxon(4, "Xus two", genus: "Xus", family: "BIDAE"),
        };
        var tree = SiteTaxonTree.Build(taxa, SitePlacement.Empty, IucnNotAssignedRules.None);

        Assert.Equal(["kingdom=animalia", "kingdom=plantae"],
            tree.Nodes.Where(n => n.Name == "Abronia").Select(n => n.LinkQuery!).Order());
        Assert.Equal(["parent=Aidae", "parent=Bidae"], tree.Nodes.Where(n => n.Name == "Xus").Select(n => n.LinkQuery!).Order());
        Assert.Null(tree.Nodes.Single(n => n.Name == "Anguidae").LinkQuery);
    }
}
