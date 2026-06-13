using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.WikipediaLists;

// Maps grouping-level names to record-field selectors and builds the auto-split candidate options the
// taxonomy tree builder consumes. Pure config/selector logic — no rendering, no database. Extracted
// from WikipediaListGenerator (R2 carve-up); imported with `using static` so the generator's tree code
// keeps calling BuildSelector / BuildAutoSplitOptions* unqualified.
internal static class TaxonGroupingHelper {
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
    /// Build a selector for enriched records that includes COL's additional ranks.
    /// </summary>
    public static Func<EnrichedSpeciesRecord, string?> BuildEnrichedSelector(string level) => level.ToLowerInvariant() switch {
        // Standard IUCN ranks
        "kingdom" => record => record.KingdomName,
        "phylum" => record => record.PhylumName,
        "class" => record => record.ClassName,
        "order" => record => record.OrderName,
        "family" => record => record.FamilyName,
        "genus" => record => record.GenusName,
        // COL-enriched intermediate ranks
        "subkingdom" => record => record.Subkingdom,
        "subphylum" => record => record.Subphylum,
        "superclass" => record => record.Superclass,
        "subclass" => record => record.Subclass,
        "infraclass" => record => record.Infraclass,
        "superorder" => record => record.Superorder,
        "suborder" => record => record.Suborder,
        "infraorder" => record => record.Infraorder,
        "parvorder" => record => record.Parvorder,
        "superfamily" => record => record.Superfamily,
        "subfamily" => record => record.Subfamily,
        "tribe" => record => record.Tribe,
        "subtribe" => record => record.Subtribe,
        "subgenus" => record => record.Subgenus,
        _ => _ => null
    };

    private static readonly HashSet<string> ColEnrichedRanks = new(StringComparer.OrdinalIgnoreCase) {
        "subkingdom", "subphylum", "superclass", "subclass", "infraclass",
        "superorder", "suborder", "infraorder", "parvorder",
        "superfamily", "subfamily", "tribe", "subtribe", "subgenus"
    };

    public static bool IsColEnrichedRank(string level) => ColEnrichedRanks.Contains(level);

    // Rank hierarchy from broadest to narrowest, used for auto-split candidate selection.
    // Only ranks that are useful for section splitting (not kingdom/phylum/class which are too broad).
    private static readonly string[] RankHierarchy = {
        "order", "suborder", "infraorder", "parvorder", "superfamily",
        "family", "subfamily", "tribe", "subtribe", "subgenus", "genus"
    };

    /// <summary>
    /// Resolve auto-split config: list-level overrides defaults.
    /// </summary>
    public static AutoSplitConfig? ResolveAutoSplitConfig(
        WikipediaListDefinition definition, WikipediaListDefaults defaults) {
        return definition.AutoSplit ?? defaults.AutoSplit;
    }

    /// <summary>
    /// Build auto-split options for non-enriched (IUCN-only) records.
    /// Candidates are limited to family and genus (the only ranks available without COL).
    /// Each candidate level sets <c>UnknownLabel = OtherLabel</c> so that species missing
    /// a rank value route into "Other {rank}" instead of "Unknown {rank}" — this prevents
    /// the RejectUnknownGroups gate from blocking otherwise good splits.
    /// </summary>
    public static AutoSplitOptions<IucnSpeciesRecord>? BuildAutoSplitOptionsIucn(
        AutoSplitConfig? config,
        IReadOnlyList<GroupingLevelDefinition> definedLevels,
        IAutoSplitDiagnostics? diagnostics = null) {
        if (config == null || !config.Enabled) {
            return null;
        }

        var definedRanks = new HashSet<string>(
            definedLevels.Select(l => l.Level.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);

        var candidates = new List<TaxonomyTreeLevel<IucnSpeciesRecord>>();
        var iucnRanks = new[] { "order", "family", "genus" };

        var lastDefinedIndex = -1;
        for (int i = 0; i < iucnRanks.Length; i++) {
            if (definedRanks.Contains(iucnRanks[i])) {
                lastDefinedIndex = i;
            }
        }

        // Add ranks below the last defined rank that aren't already defined
        for (int i = lastDefinedIndex + 1; i < iucnRanks.Length; i++) {
            var rank = iucnRanks[i];
            if (!definedRanks.Contains(rank)) {
                var otherLabel = GetOtherLabel(rank);
                candidates.Add(new TaxonomyTreeLevel<IucnSpeciesRecord>(
                    rank, BuildSelector(rank),
                    UnknownLabel: otherLabel,
                    MinItems: config.MinItemsPerGroup,
                    OtherLabel: otherLabel,
                    MinGroupsForOther: 3));
            }
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
            Diagnostics: diagnostics);
    }

    /// <summary>
    /// Build auto-split options for COL-enriched records.
    /// Candidates are all COL intermediate ranks below the last defined grouping level
    /// (subfamily, tribe, subtribe, subgenus, genus).
    /// Each candidate level sets <c>UnknownLabel = OtherLabel</c> so that species missing
    /// a rank value route into "Other {rank}" instead of "Unknown {rank}".
    /// </summary>
    public static AutoSplitOptions<EnrichedSpeciesRecord>? BuildAutoSplitOptionsEnriched(
        AutoSplitConfig? config,
        IReadOnlyList<GroupingLevelDefinition> definedLevels,
        IAutoSplitDiagnostics? diagnostics = null) {
        if (config == null || !config.Enabled) {
            return null;
        }

        var definedRanks = new HashSet<string>(
            definedLevels.Select(l => l.Level.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);

        // Find the position of the last defined rank in the hierarchy
        var lastDefinedIndex = -1;
        for (int i = 0; i < RankHierarchy.Length; i++) {
            if (definedRanks.Contains(RankHierarchy[i])) {
                lastDefinedIndex = i;
            }
        }

        // Add ranks below the last defined rank that aren't already defined
        var candidates = new List<TaxonomyTreeLevel<EnrichedSpeciesRecord>>();
        for (int i = lastDefinedIndex + 1; i < RankHierarchy.Length; i++) {
            var rank = RankHierarchy[i];
            if (!definedRanks.Contains(rank)) {
                var otherLabel = GetOtherLabel(rank);
                candidates.Add(new TaxonomyTreeLevel<EnrichedSpeciesRecord>(
                    rank, BuildEnrichedSelector(rank),
                    UnknownLabel: otherLabel,
                    MinItems: config.MinItemsPerGroup,
                    OtherLabel: otherLabel,
                    MinGroupsForOther: 3));
            }
        }

        if (candidates.Count == 0) {
            return null;
        }

        return new AutoSplitOptions<EnrichedSpeciesRecord>(
            config.Threshold, config.MinGroupSize, candidates,
            MaxOtherFraction: config.MaxOtherFraction,
            MaxGroups: config.MaxGroups,
            MaxDepth: config.MaxDepth,
            MinMeaningfulGroups: config.MinMeaningfulGroups,
            RejectUnknownGroups: config.RejectUnknownGroups,
            Diagnostics: diagnostics);
    }

    private static string GetOtherLabel(string rank) => rank.ToLowerInvariant() switch {
        "subfamily" => "Other subfamilies",
        "superfamily" => "Other superfamilies",
        "family" => "Other families",
        "subgenus" => "Other subgenera",
        "genus" => "Other genera",
        "tribe" => "Other tribes",
        "subtribe" => "Other subtribes",
        "suborder" => "Other suborders",
        "infraorder" => "Other infraorders",
        "parvorder" => "Other parvorders",
        "order" => "Other orders",
        _ => $"Other {rank}"
    };
}
