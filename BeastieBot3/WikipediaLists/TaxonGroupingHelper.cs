using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.WikipediaLists;

// Turns the list config into the tree builder's inputs: grouping levels with selectors, the
// Catalogue of Life nodes between levels (from an ITaxonPlacement), auto-split candidates, the
// intermediate-layer gates and the curated virtual groups. IUCN ranks are read from the record;
// CoL ranks only from the placement, so with no placement they are simply absent. Pure config and
// selector logic: no rendering, no database. Imported with `using static` by the renderer.
internal static class TaxonGroupingHelper {
    private static readonly HashSet<string> IucnRanks = new(StringComparer.OrdinalIgnoreCase) {
        "kingdom", "phylum", "class", "order", "family", "genus",
    };

    // CoL ranks by the pair of IUCN ranks they sit between. A rank not listed is looked for
    // between order and family.
    private static readonly HashSet<string> ClassToOrderRanks = new(StringComparer.OrdinalIgnoreCase) {
        "subclass", "infraclass", "subterclass", "superorder",
    };

    private static readonly HashSet<string> FamilyToGenusRanks = new(StringComparer.OrdinalIgnoreCase) {
        "subfamily", "supertribe", "tribe", "subtribe", "infratribe",
    };

    // Rank order used to find the ranks below the last configured level.
    private static readonly string[] CanonicalRanks = {
        "kingdom", "phylum", "subphylum", "class", "subclass", "infraclass", "superorder",
        "order", "suborder", "infraorder", "parvorder", "superfamily", "epifamily",
        "family", "subfamily", "supertribe", "tribe", "subtribe", "genus",
    };

    // Auto-split candidates, broadest first. The CoL ranks are read from the family-to-genus path.
    private static readonly string[] AutoSplitRanks = { "order", "family", "subfamily", "tribe", "subtribe", "genus" };

    public static bool IsIucnRank(string level) => IucnRanks.Contains(level);

    /// <summary>Reads an IUCN rank from the record; null for any other rank.</summary>
    public static Func<IucnSpeciesRecord, string?> BuildSelector(string level) => level.ToLowerInvariant() switch {
        "kingdom" => record => record.KingdomName,
        "phylum" => record => record.PhylumName,
        "class" => record => record.ClassName,
        "order" => record => record.OrderName,
        "family" => record => record.FamilyName,
        "genus" => record => record.GenusName,
        _ => _ => null
    };

    /// <summary>
    /// Reads an IUCN rank from the record, or a CoL rank (suborder, subfamily, ...) as the name of
    /// the placement node with that rank. A CoL rank reads null when there is no placement.
    /// </summary>
    public static Func<IucnSpeciesRecord, string?> BuildSelector(string level, ITaxonPlacement? placement) {
        var key = level.ToLowerInvariant();
        if (IsIucnRank(key)) {
            return BuildSelector(key);
        }

        if (placement is null) {
            return _ => null;
        }

        var span = SpanOf(key);
        return record => FindRank(PathFor(placement, span, record), key)?.Name;
    }

    /// <summary>Which pair of IUCN ranks a CoL rank sits between.</summary>
    public static PlacementSpan SpanOf(string colRank) =>
        ClassToOrderRanks.Contains(colRank) ? PlacementSpan.ClassToOrder
        : FamilyToGenusRanks.Contains(colRank) ? PlacementSpan.FamilyToGenus
        : PlacementSpan.OrderToFamily;

    private static IReadOnlyList<PlacementNode> PathFor(ITaxonPlacement placement, PlacementSpan span, IucnSpeciesRecord r) => span switch {
        PlacementSpan.ClassToOrder => placement.BetweenClassAndOrder(r.KingdomName, r.ClassName, r.OrderName),
        PlacementSpan.OrderToFamily => placement.BetweenOrderAndFamily(r.KingdomName, r.ClassName, r.OrderName, r.FamilyName),
        PlacementSpan.FamilyToGenus => placement.BetweenFamilyAndGenus(r.KingdomName, r.FamilyName, r.GenusName),
        _ => Array.Empty<PlacementNode>(),
    };

    private static PlacementNode? FindRank(IReadOnlyList<PlacementNode> path, string colRank) {
        foreach (var node in path) {
            if (string.Equals(node.ColRank, colRank, StringComparison.OrdinalIgnoreCase)) {
                return node;
            }
        }
        return null;
    }

    private static int IndexOfRank(IReadOnlyList<PlacementNode> path, string colRank, int start) {
        for (var i = start; i < path.Count; i++) {
            if (string.Equals(path[i].ColRank, colRank, StringComparison.OrdinalIgnoreCase)) {
                return i;
            }
        }
        return -1;
    }

    /// <summary>
    /// The tree levels for a list's grouping. Each IUCN level gets the CoL nodes between it and the
    /// IUCN rank above as intermediates (order: class to order; family: order to family). A level
    /// that names a CoL rank (superfamily) reads that node, and the CoL nodes are split around it:
    /// the nodes before it are its own intermediates and the nodes after it belong to the next level.
    /// </summary>
    public static IReadOnlyList<TaxonomyTreeLevel<IucnSpeciesRecord>> BuildLevels(
        IReadOnlyList<GroupingLevelDefinition> grouping, ITaxonPlacement? placement) {
        var keys = grouping.Select(g => g.Level.Trim().ToLowerInvariant()).ToList();
        var levels = new List<TaxonomyTreeLevel<IucnSpeciesRecord>>(grouping.Count);
        for (var i = 0; i < grouping.Count; i++) {
            var definition = grouping[i];
            levels.Add(new TaxonomyTreeLevel<IucnSpeciesRecord>(
                definition.Label ?? definition.Level,
                BuildSelector(keys[i], placement),
                definition.AlwaysDisplay,
                definition.UnknownLabel,
                definition.MinItems,
                definition.OtherLabel,
                definition.MinGroupsForOther,
                Intermediates: placement is null ? null : BuildIntermediates(keys, i, placement),
                Key: keys[i]));
        }
        return levels;
    }

    private static Func<IucnSpeciesRecord, IReadOnlyList<PlacementNode>>? BuildIntermediates(
        IReadOnlyList<string> keys, int index, ITaxonPlacement placement) {
        var key = keys[index];
        PlacementSpan span;
        string? beforeRank = null;
        switch (key) {
            case "order":
                span = PlacementSpan.ClassToOrder;
                break;
            case "family":
                span = PlacementSpan.OrderToFamily;
                break;
            case "genus":
                span = PlacementSpan.FamilyToGenus;
                break;
            default:
                if (IsIucnRank(key)) {
                    return null;
                }
                span = SpanOf(key);
                beforeRank = key;
                break;
        }

        // A configured CoL rank earlier in the same span has its own headings; this level's
        // intermediates start after its node.
        var afterRank = keys.Take(index).LastOrDefault(k => !IsIucnRank(k) && SpanOf(k) == span);
        return record => Trim(PathFor(placement, span, record), afterRank, beforeRank);
    }

    private static IReadOnlyList<PlacementNode> Trim(IReadOnlyList<PlacementNode> path, string? afterRank, string? beforeRank) {
        if (path.Count == 0 || (afterRank is null && beforeRank is null)) {
            return path;
        }

        var start = 0;
        if (afterRank is not null) {
            var afterIndex = IndexOfRank(path, afterRank, 0);
            if (afterIndex < 0) {
                return Array.Empty<PlacementNode>();
            }
            start = afterIndex + 1;
        }

        var end = path.Count;
        if (beforeRank is not null) {
            var beforeIndex = IndexOfRank(path, beforeRank, start);
            if (beforeIndex >= 0) {
                end = beforeIndex;
            }
        }

        if (start == 0 && end == path.Count) {
            return path;
        }

        return path.Skip(start).Take(Math.Max(0, end - start)).ToList();
    }

    /// <summary>Resolve auto-split config: list-level overrides defaults.</summary>
    public static AutoSplitConfig? ResolveAutoSplitConfig(
        WikipediaListDefinition definition, WikipediaListDefaults defaults) {
        return definition.AutoSplit ?? defaults.AutoSplit;
    }

    /// <summary>Resolve intermediate-layer config: list-level overrides defaults.</summary>
    public static IntermediateGroupsConfig? ResolveIntermediateGroupsConfig(
        WikipediaListDefinition definition, WikipediaListDefaults defaults) {
        return definition.IntermediateGroups ?? defaults.IntermediateGroups;
    }

    /// <summary>The intermediate-layer gates, or null when the config is missing or disabled.</summary>
    public static IntermediateLayerOptions? BuildIntermediateOptions(IntermediateGroupsConfig? config) {
        if (config is null || !config.Enabled) {
            return null;
        }

        return new IntermediateLayerOptions(
            config.MinItems, config.MinAnchors, config.MaxGroups, config.MaxDominance, config.MinGroupSize,
            config.LookThroughDominant, config.MaxLayers, config.MaxHeadingShare);
    }

    /// <summary>
    /// Auto-split options. Candidates are the ranks below the last configured level, broadest
    /// first: order, family, then subfamily, tribe and subtribe from the placement's family-to-genus
    /// path, then genus. The CoL ranks are left out when there is no placement. Each candidate routes
    /// taxa with no value at its rank to "Other {ranks}" (not "Unknown {rank}"), so the
    /// RejectUnknownGroups gate does not block an otherwise good split.
    /// </summary>
    public static AutoSplitOptions<IucnSpeciesRecord>? BuildAutoSplitOptions(
        AutoSplitConfig? config,
        IReadOnlyList<GroupingLevelDefinition> definedLevels,
        ITaxonPlacement? placement) {
        if (config is null || !config.Enabled) {
            return null;
        }

        var definedRanks = new HashSet<string>(
            definedLevels.Select(l => l.Level.Trim().ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);
        var lastDefined = definedRanks.Select(r => Array.IndexOf(CanonicalRanks, r)).DefaultIfEmpty(-1).Max();

        var candidates = new List<TaxonomyTreeLevel<IucnSpeciesRecord>>();
        foreach (var rank in AutoSplitRanks) {
            if (Array.IndexOf(CanonicalRanks, rank) <= lastDefined || definedRanks.Contains(rank)) {
                continue;
            }

            if (!IsIucnRank(rank) && placement is null) {
                continue;
            }

            var otherLabel = GetOtherLabel(rank);
            candidates.Add(new TaxonomyTreeLevel<IucnSpeciesRecord>(
                rank, BuildSelector(rank, placement),
                UnknownLabel: otherLabel,
                MinItems: config.MinItemsPerGroup,
                OtherLabel: otherLabel,
                MinGroupsForOther: 3,
                Key: rank));
        }

        if (candidates.Count == 0) {
            return null;
        }

        return new AutoSplitOptions<IucnSpeciesRecord>(
            config.Threshold, config.MinGroupSize, candidates,
            MaxOtherFraction: config.MaxOtherFraction,
            MaxGroups: config.MaxGroups,
            MaxDepth: config.MaxDepth,
            MinMeaningfulGroups: config.MinMeaningfulGroups,
            RejectUnknownGroups: config.RejectUnknownGroups,
            MaxDominance: config.MaxDominance);
    }

    /// <summary>
    /// The curated groups from taxon-rules.yml (virtual_groups) as tree options: a taxon has groups
    /// when its rule sets use_virtual_groups and a virtual_groups block exists for it. Each item is
    /// matched on the names of its intermediate path nodes, then its family, then the default group.
    /// </summary>
    public static VirtualGroupOptions<IucnSpeciesRecord>? BuildVirtualGroupOptions(TaxonRulesService? taxonRules) {
        if (taxonRules is null) {
            return null;
        }

        return new VirtualGroupOptions<IucnSpeciesRecord>(
            GroupNames: owner => taxonRules.ShouldUseVirtualGroups(owner)
                ? taxonRules.GetVirtualGroups(owner)?.Groups.Select(g => g.Name).ToList()
                : null,
            Resolve: (owner, record, path) => {
                var match = taxonRules.ResolveVirtualGroup(owner, record.FamilyName, path.Select(n => n.Name).ToList());
                return match is null ? null : new VirtualGroupMatch(match.Group.Name, match.CladeIndex);
            });
    }

    /// <summary>The bucket label for taxa lumped or missing at an auto-split rank, e.g. "Other subfamilies".</summary>
    public static string GetOtherLabel(string rank) => $"Other {RankNames.Plural(rank)}";
}
