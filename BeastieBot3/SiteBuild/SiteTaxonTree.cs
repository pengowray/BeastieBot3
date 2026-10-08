using BeastieBot3.Iucn;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Taxonomy;

// The tree of groups the site's taxa are in (higher_taxon). Each taxon in the release goes under its
// IUCN kingdom, phylum, class, order, family and genus, with the Catalogue of Life groups that the
// placement file keeps between class and order, order and family, and family and genus. An order or
// family that IUCN gives as "NOT ASSIGNED" takes the value from rules/iucn-not-assigned.yml, as in
// the Wikipedia lists; with no rule, the taxon goes directly under the group above.
//
// Numbering: groups and taxa are numbered depth-first, children in alphabetical order, so the taxa
// in a group have consecutive tree positions (first_pos to last_pos). In a genus each species comes
// before its own subspecies, varieties and subpopulations.

namespace BeastieBot3.SiteBuild;

/// One CoL group of a placement path, as the placement file stores it.
internal sealed record SitePlacementNode(string Name, string ColRank, bool ShowRank, string? ColId);

/// The placement paths, keyed as TaxonPlacementIndex keys them.
internal sealed class SitePlacement {
    public static readonly SitePlacement Empty = new(new Dictionary<(PlacementSpan, string), IReadOnlyList<SitePlacementNode>>());

    private readonly IReadOnlyDictionary<(PlacementSpan Span, string Key), IReadOnlyList<SitePlacementNode>> _paths;

    public SitePlacement(IReadOnlyDictionary<(PlacementSpan Span, string Key), IReadOnlyList<SitePlacementNode>> paths) {
        _paths = paths;
    }

    public int Count => _paths.Count;

    public IReadOnlyList<SitePlacementNode> Between(PlacementSpan span, string key) =>
        _paths.TryGetValue((span, key), out var nodes) ? nodes : Array.Empty<SitePlacementNode>();
}

internal sealed class SiteTreeNode {
    public required string Rank { get; init; }
    public required string Name { get; init; }
    public required string Source { get; init; }
    public required bool ShowRank { get; init; }
    public required string Kingdom { get; init; }
    public string? ColId { get; set; }
    public SiteTreeNode? Parent { get; init; }
    public int Depth { get; init; }

    public int NodeId { get; set; }
    public int FirstPos { get; set; }
    public int LastPos { get; set; }
    public int SpeciesCount { get; set; }
    public int InfraCount { get; set; }
    public int SubpopulationCount { get; set; }
    /// Counts by {{IUCN status}} code of the latest global assessment: species, infraspecific, subpopulations.
    public SortedDictionary<string, int[]> CategoryCounts { get; } = new(StringComparer.Ordinal);

    /// When another group has the same rank and name: "kingdom=plantae" or "parent=Moraceae".
    public string? LinkQuery { get; set; }
    public string? CommonNameEn { get; set; }
    public string? CommonNameSource { get; set; }
    public string? EnwikiTitle { get; set; }
    public List<string> ColNames { get; } = new();
    /// The title of the group's English Wikipedia article and of the redirects to it (SiteGroupWikipediaNames).
    public List<string> WikipediaNames { get; } = new();

    internal Dictionary<string, SiteTreeNode> ChildrenByKey { get; } = new(StringComparer.Ordinal);
    internal List<SiteTaxon> Taxa { get; } = new();
}

internal sealed class SiteTaxonTree {
    public List<SiteTreeNode> Nodes { get; } = new();
    public int TaxaPlaced { get; private set; }
    public int TaxaWithoutKingdom { get; private set; }
    public int TaxaUnderRuleOrder { get; private set; }
    public int TaxaUnderRuleFamily { get; private set; }
    public int TaxaWithUnassignedRank { get; private set; }

    /// Builds the tree from the taxa in the release, sets each taxon's NodeId and TreePos and
    /// numbers the nodes.
    public static SiteTaxonTree Build(IEnumerable<SiteTaxon> taxa, SitePlacement placement, IucnNotAssignedRules notAssigned) {
        var tree = new SiteTaxonTree();
        var roots = new Dictionary<string, SiteTreeNode>(StringComparer.Ordinal);
        var inRelease = taxa.Where(t => t.InRelease).ToList();
        var byId = inRelease.ToDictionary(t => t.TaxonId);
        foreach (var taxon in inRelease) {
            var node = tree.Place(taxon, roots, placement, notAssigned);
            if (node is null) {
                tree.TaxaWithoutKingdom++;
                continue;
            }
            node.Taxa.Add(taxon);
            tree.TaxaPlaced++;
        }

        var pos = 0;
        var nodeId = 0;
        foreach (var root in Sorted(roots.Values)) {
            tree.Number(root, ref nodeId, ref pos, byId);
        }
        SetLinkQueries(tree.Nodes);
        return tree;
    }

    private SiteTreeNode? Place(SiteTaxon taxon, Dictionary<string, SiteTreeNode> roots, SitePlacement placement,
        IucnNotAssignedRules notAssigned) {
        if (Usable(taxon.Kingdom) is not { } kingdom) {
            return null;
        }
        var kingdomKey = kingdom.ToUpperInvariant();
        var (order, family) = notAssigned.Resolve(taxon.ClassName, taxon.OrderName, taxon.Family, taxon.Genus);
        var orderFromRule = IucnNotAssignedRules.IsNotAssigned(taxon.OrderName) && !IucnNotAssignedRules.IsNotAssigned(order);
        var familyFromRule = IucnNotAssignedRules.IsNotAssigned(taxon.Family) && !IucnNotAssignedRules.IsNotAssigned(family);
        if (orderFromRule) {
            TaxaUnderRuleOrder++;
        }
        if (familyFromRule) {
            TaxaUnderRuleFamily++;
        }
        if (IucnNotAssignedRules.IsNotAssigned(order) || IucnNotAssignedRules.IsNotAssigned(family)
            || IucnNotAssignedRules.IsNotAssigned(taxon.Phylum) || IucnNotAssignedRules.IsNotAssigned(taxon.ClassName)) {
            TaxaWithUnassignedRank++;
        }

        if (!roots.TryGetValue(kingdomKey, out var node)) {
            roots[kingdomKey] = node = new SiteTreeNode {
                Rank = "kingdom", Name = TitleCase(kingdom), Source = GroupSources.Iucn, ShowRank = true,
                Kingdom = kingdomKey, Depth = 0,
            };
        }
        node = Child(node, "phylum", taxon.Phylum, GroupSources.Iucn);
        node = Child(node, "class", taxon.ClassName, GroupSources.Iucn);
        if (Usable(order) is not null) {
            foreach (var group in placement.Between(PlacementSpan.ClassToOrder,
                         TaxonPlacementIndex.OrderKey(kingdom, taxon.ClassName, order))) {
                node = ColChild(node, group);
            }
        }
        node = Child(node, "order", order, orderFromRule ? GroupSources.IucnRule : GroupSources.Iucn);
        if (Usable(family) is not null) {
            foreach (var group in placement.Between(PlacementSpan.OrderToFamily,
                         TaxonPlacementIndex.FamilyKey(kingdom, taxon.ClassName, order, family))) {
                node = ColChild(node, group);
            }
        }
        node = Child(node, "family", family, familyFromRule ? GroupSources.IucnRule : GroupSources.Iucn);
        if (Usable(taxon.Genus) is not null) {
            foreach (var group in placement.Between(PlacementSpan.FamilyToGenus,
                         TaxonPlacementIndex.GenusKey(kingdom, family, taxon.Genus))) {
                node = ColChild(node, group);
            }
        }
        node = Child(node, "genus", taxon.Genus, GroupSources.Iucn, keepCase: true);
        return node;
    }

    private static SiteTreeNode Child(SiteTreeNode parent, string rank, string? value, string source, bool keepCase = false) {
        if (Usable(value) is not { } name) {
            return parent;
        }
        var key = rank + ":" + name.ToUpperInvariant();
        if (!parent.ChildrenByKey.TryGetValue(key, out var child)) {
            parent.ChildrenByKey[key] = child = new SiteTreeNode {
                Rank = rank, Name = keepCase ? name : TitleCase(name), Source = source, ShowRank = true,
                Kingdom = parent.Kingdom, Parent = parent, Depth = parent.Depth + 1,
            };
        }
        return child;
    }

    private static SiteTreeNode ColChild(SiteTreeNode parent, SitePlacementNode group) {
        var rank = string.IsNullOrWhiteSpace(group.ColRank) ? "unranked" : group.ColRank.Trim().ToLowerInvariant();
        var key = "col:" + rank + ":" + group.Name.ToUpperInvariant();
        if (!parent.ChildrenByKey.TryGetValue(key, out var child)) {
            parent.ChildrenByKey[key] = child = new SiteTreeNode {
                Rank = rank, Name = group.Name.Trim(), Source = GroupSources.Col, ShowRank = group.ShowRank,
                Kingdom = parent.Kingdom, Parent = parent, Depth = parent.Depth + 1, ColId = group.ColId,
            };
        }
        return child;
    }

    private void Number(SiteTreeNode node, ref int nodeId, ref int pos, IReadOnlyDictionary<long, SiteTaxon> byId) {
        node.NodeId = ++nodeId;
        Nodes.Add(node);
        node.FirstPos = pos + 1;
        foreach (var child in Sorted(node.ChildrenByKey.Values)) {
            Number(child, ref nodeId, ref pos, byId);
            node.SpeciesCount += child.SpeciesCount;
            node.InfraCount += child.InfraCount;
            node.SubpopulationCount += child.SubpopulationCount;
            foreach (var (code, counts) in child.CategoryCounts) {
                var mine = CountsFor(node, code);
                for (var i = 0; i < counts.Length; i++) {
                    mine[i] += counts[i];
                }
            }
        }
        foreach (var taxon in OrderTaxa(node.Taxa, byId)) {
            taxon.NodeId = node.NodeId;
            taxon.TreePos = ++pos;
            var slot = taxon.Kind switch {
                SiteTaxonKind.Species => 0,
                SiteTaxonKind.Subpopulation => 2,
                _ => 1,
            };
            switch (slot) {
                case 0: node.SpeciesCount++; break;
                case 1: node.InfraCount++; break;
                default: node.SubpopulationCount++; break;
            }
            if (taxon.LatestGlobalStatusCode is { } code) {
                CountsFor(node, code)[slot]++;
            }
        }
        node.LastPos = pos;
        node.ChildrenByKey.Clear();
        node.Taxa.Clear();
    }

    // Groups with the same rank and name get what tells them apart in their address: the kingdom
    // when no other one of them is in it, else the parent's name when no other one has that parent.
    internal static void SetLinkQueries(IEnumerable<SiteTreeNode> nodes) {
        foreach (var same in nodes.GroupBy(n => (n.Rank, Key: n.Name.ToUpperInvariant())).Where(g => g.Count() > 1)) {
            var list = same.ToList();
            foreach (var node in list) {
                if (list.Count(n => n.Kingdom == node.Kingdom) == 1) {
                    node.LinkQuery = "kingdom=" + node.Kingdom.ToLowerInvariant();
                } else if (node.Parent is { } parent
                           && list.Count(n => string.Equals(n.Parent?.Name, parent.Name, StringComparison.OrdinalIgnoreCase)) == 1) {
                    node.LinkQuery = "parent=" + parent.Name;
                }
            }
        }
    }

    private static int[] CountsFor(SiteTreeNode node, string code) {
        if (!node.CategoryCounts.TryGetValue(code, out var counts)) {
            node.CategoryCounts[code] = counts = new int[3];
        }
        return counts;
    }

    // Species in name order, each followed by its own subspecies and varieties (in name order) and
    // then its subpopulations. A taxon whose species is not in the same group sorts by its own name.
    internal static IEnumerable<SiteTaxon> OrderTaxa(List<SiteTaxon> taxa, IReadOnlyDictionary<long, SiteTaxon> byId) {
        var here = taxa.Select(t => t.TaxonId).ToHashSet();
        var children = taxa
            .Where(t => t.Kind != SiteTaxonKind.Species && t.ParentTaxonId is { } p && here.Contains(p) && p != t.TaxonId)
            .ToLookup(t => t.ParentTaxonId!.Value);
        var childIds = children.SelectMany(g => g).Select(t => t.TaxonId).ToHashSet();
        foreach (var top in taxa.Where(t => !childIds.Contains(t.TaxonId)).OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(t => t.TaxonId)) {
            yield return top;
            foreach (var child in children[top.TaxonId]
                         .OrderBy(t => t.Kind == SiteTaxonKind.Subpopulation ? 1 : 0)
                         .ThenBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)
                         .ThenBy(t => t.SubpopulationName, StringComparer.OrdinalIgnoreCase)
                         .ThenBy(t => t.TaxonId)) {
                yield return child;
            }
        }
    }

    private static IEnumerable<SiteTreeNode> Sorted(IEnumerable<SiteTreeNode> nodes) =>
        nodes.OrderBy(n => n.Name, StringComparer.OrdinalIgnoreCase).ThenBy(n => n.Rank, StringComparer.Ordinal);

    private static string? Usable(string? value) {
        var trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) || IucnNotAssignedRules.IsNotAssigned(trimmed) ? null : trimmed;
    }

    /// "CARNIVORA" -> "Carnivora", as the site shows IUCN's upper-case ranks.
    internal static string TitleCase(string name) {
        var trimmed = name.Trim();
        return trimmed.Length == 0 ? trimmed : char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }
}
