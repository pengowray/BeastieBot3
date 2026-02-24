using System;
using System.Collections.Generic;
using System.Linq;

// Generic tree builder for organizing flat records into nested hierarchy.
// Configured via TaxonomyTreeLevel<T> to specify grouping columns and sort order.
// Used for generating hierarchical reports (Kingdom → Phylum → Class → ...).
// Supports force-split groups and skip conditions. Returns TaxonomyTreeNode<T>
// root with Children for each taxonomic rank level.

namespace BeastieBot3.Taxonomy;

internal static class TaxonomyTreeBuilder {
    public static TaxonomyTreeNode<T> Build<T>(IEnumerable<T> items, IReadOnlyList<TaxonomyTreeLevel<T>> levels) {
        return Build(items, levels, shouldSkipGroup: null);
    }

    /// <summary>
    /// Build a taxonomy tree with optional group skipping (for force-split support).
    /// </summary>
    /// <param name="items">Items to organize into the tree.</param>
    /// <param name="levels">Hierarchical grouping levels.</param>
    /// <param name="shouldSkipGroup">Optional callback to determine if a group value should be skipped (items pushed to next level).</param>
    public static TaxonomyTreeNode<T> Build<T>(
        IEnumerable<T> items,
        IReadOnlyList<TaxonomyTreeLevel<T>> levels,
        Func<string, bool>? shouldSkipGroup) {
        return Build(items, levels, shouldSkipGroup, autoSplit: null);
    }

    /// <summary>
    /// Build a taxonomy tree with optional group skipping and auto-split support.
    /// Auto-split dynamically inserts additional grouping levels when leaf groups
    /// exceed a threshold, using CoL-enriched intermediate ranks.
    /// </summary>
    public static TaxonomyTreeNode<T> Build<T>(
        IEnumerable<T> items,
        IReadOnlyList<TaxonomyTreeLevel<T>> levels,
        Func<string, bool>? shouldSkipGroup,
        AutoSplitOptions<T>? autoSplit) {
        if (items is null) {
            throw new ArgumentNullException(nameof(items));
        }

        if (levels is null) {
            throw new ArgumentNullException(nameof(levels));
        }

        var materialized = items as IReadOnlyList<T> ?? items.ToList();
        var root = new TaxonomyTreeNode<T>(label: null, value: null);
        if (materialized.Count == 0) {
            return root;
        }

        BuildRecursive(root, materialized, levels, 0, shouldSkipGroup, autoSplit);
        return root;
    }

    private static void BuildRecursive<T>(
        TaxonomyTreeNode<T> parent,
        IReadOnlyList<T> items,
        IReadOnlyList<TaxonomyTreeLevel<T>> levels,
        int levelIndex,
        Func<string, bool>? shouldSkipGroup,
        AutoSplitOptions<T>? autoSplit) {
        if (items.Count == 0) {
            return;
        }

        if (levelIndex >= levels.Count) {
            // Try auto-split if items exceed threshold
            if (autoSplit != null && items.Count >= autoSplit.Threshold && autoSplit.CandidateLevels.Count > 0) {
                if (TryAutoSplit(parent, items, autoSplit, parent.Value ?? "(root)")) {
                    return;
                }
            }

            parent.AddItems(items);
            return;
        }

        var level = levels[levelIndex];
        var groups = CreateGroups(items, level);
        if (groups.Count == 0) {
            BuildRecursive(parent, items, levels, levelIndex + 1, shouldSkipGroup, autoSplit);
            return;
        }

        if (!level.AlwaysDisplay && groups.Count == 1) {
            BuildRecursive(parent, groups[0].Items, levels, levelIndex + 1, shouldSkipGroup, autoSplit);
            return;
        }

        // Separate groups into normal and force-split
        var normalGroups = new List<TreeGroup<T>>();
        var skipItems = new List<T>();

        foreach (var group in groups) {
            if (shouldSkipGroup != null && shouldSkipGroup(group.DisplayValue)) {
                // Force-split: don't create a heading for this group, push items to next level
                skipItems.AddRange(group.Items);
            } else {
                normalGroups.Add(group);
            }
        }

        // Process normal groups with headings
        foreach (var group in normalGroups) {
            var child = parent.AddChild(level.Label, group.DisplayValue);
            BuildRecursive(child, group.Items, levels, levelIndex + 1, shouldSkipGroup, autoSplit);
        }

        // Process skipped items at the next level (no heading for the current level)
        if (skipItems.Count > 0) {
            BuildRecursive(parent, skipItems, levels, levelIndex + 1, shouldSkipGroup, autoSplit);
        }
    }

    /// <summary>
    /// Attempts to split a large group of items using candidate levels with quality gates.
    /// Tries each candidate level in order; applies lumping to merge small groups into "Other",
    /// then evaluates: meaningful group count, Other/Unknown fraction, max groups, and nesting depth.
    /// Returns true if a split was applied.
    /// </summary>
    private static bool TryAutoSplit<T>(
        TaxonomyTreeNode<T> parent,
        IReadOnlyList<T> items,
        AutoSplitOptions<T> autoSplit,
        string parentPath) {

        // Rule 5: Respect nesting depth limit
        if (autoSplit.CurrentDepth >= autoSplit.MaxDepth) {
            autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                parentPath, items.Count, "(all)", "rejected:depth_limit"));
            return false;
        }

        for (int i = 0; i < autoSplit.CandidateLevels.Count; i++) {
            var candidateLevel = autoSplit.CandidateLevels[i];
            // Rule 2: CreateGroups now applies lumping via MinItems/OtherLabel on the level
            var groups = CreateGroups(items, candidateLevel);

            // Need more than one group for a meaningful split
            if (groups.Count <= 1) {
                autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                    parentPath, items.Count, candidateLevel.Label, "rejected:single_group",
                    GroupCount: groups.Count));
                continue;
            }

            // Rule 1: Only count non-Other/non-Unknown groups as meaningful
            var meaningfulGroups = groups.Where(g => !IsOtherOrUnknownLabel(g.DisplayValue)).ToList();

            // Gate: reject if any group has "Unknown" label (confusing for editors)
            if (autoSplit.RejectUnknownGroups) {
                bool hasUnknown = groups.Any(g => IsUnknownLabel(g.DisplayValue));
                if (hasUnknown) {
                    autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                        parentPath, items.Count, candidateLevel.Label, "rejected:has_unknown",
                        GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count));
                    continue;
                }
            }

            // Genus/subgenus splits need stricter gates than higher-rank splits
            bool isFineGrainedRank = IsFineGrainedRank(candidateLevel.Label);
            int requiredMeaningful = isFineGrainedRank ? autoSplit.MinMeaningfulGroups : 2;
            int requiredGroupSize = isFineGrainedRank ? autoSplit.MinGroupSize : Math.Max(autoSplit.MinGroupSize / 2, 5);

            // Gate: minimum meaningful (named) groups
            if (meaningfulGroups.Count < requiredMeaningful) {
                autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                    parentPath, items.Count, candidateLevel.Label, "rejected:few_meaningful",
                    GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count));
                continue;
            }

            // Gate: group size check (rank-dependent)
            // Fine-grained (genus/subgenus): ALL meaningful groups must be >= MinGroupSize (one exception at 4+)
            // Higher ranks (subfamily, tribe, etc.): at least one meaningful group >= threshold
            if (isFineGrainedRank) {
                var smallMeaningful = meaningfulGroups.Count(g => g.Items.Count < requiredGroupSize);
                int allowedSmall = meaningfulGroups.Count >= 4 ? 1 : 0;
                if (smallMeaningful > allowedSmall) {
                    autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                        parentPath, items.Count, candidateLevel.Label, "rejected:groups_too_small",
                        GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count));
                    continue;
                }
            } else {
                bool hasMeaningfulGroup = meaningfulGroups.Any(g => g.Items.Count >= requiredGroupSize);
                if (!hasMeaningfulGroup) {
                    autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                        parentPath, items.Count, candidateLevel.Label, "rejected:no_meaningful",
                        GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count));
                    continue;
                }
            }

            // Rule 3: Check Other+Unknown fraction
            var otherUnknownCount = groups
                .Where(g => IsOtherOrUnknownLabel(g.DisplayValue))
                .Sum(g => g.Items.Count);
            double otherFraction = items.Count > 0 ? (double)otherUnknownCount / items.Count : 0;
            if (otherFraction > autoSplit.MaxOtherFraction) {
                autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                    parentPath, items.Count, candidateLevel.Label, "rejected:high_other",
                    GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count,
                    OtherFraction: otherFraction));
                continue;
            }

            // Rule 4: Check max groups limit
            if (groups.Count > autoSplit.MaxGroups) {
                autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                    parentPath, items.Count, candidateLevel.Label, "rejected:too_many_groups",
                    GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count,
                    OtherFraction: otherFraction));
                continue;
            }

            // All gates passed — record acceptance and build sub-groups
            var largestMeaningful = meaningfulGroups.Max(g => g.Items.Count);
            autoSplit.Diagnostics?.RecordDecision(new AutoSplitDecision(
                parentPath, items.Count, candidateLevel.Label, "accepted",
                GroupCount: groups.Count, MeaningfulGroups: meaningfulGroups.Count,
                OtherFraction: otherFraction, LargestGroup: largestMeaningful));

            var remainingCandidates = i + 1 < autoSplit.CandidateLevels.Count
                ? autoSplit with {
                    CandidateLevels = autoSplit.CandidateLevels.Skip(i + 1).ToList(),
                    CurrentDepth = autoSplit.CurrentDepth + 1
                  }
                : null;

            foreach (var group in groups) {
                var child = parent.AddChild(candidateLevel.Label, group.DisplayValue);
                // Rule 6: Don't recursively auto-split Other/Unknown groups
                if (remainingCandidates != null &&
                    group.Items.Count >= autoSplit.Threshold &&
                    remainingCandidates.CandidateLevels.Count > 0 &&
                    !IsOtherOrUnknownLabel(group.DisplayValue)) {
                    var childPath = $"{parentPath} → {group.DisplayValue}";
                    if (!TryAutoSplit(child, group.Items, remainingCandidates, childPath)) {
                        child.AddItems(group.Items);
                    }
                } else {
                    child.AddItems(group.Items);
                }
            }

            return true;
        }

        return false;
    }

    private static void BuildRecursive<T>(TaxonomyTreeNode<T> parent, IReadOnlyList<T> items, IReadOnlyList<TaxonomyTreeLevel<T>> levels, int levelIndex) {
        BuildRecursive(parent, items, levels, levelIndex, shouldSkipGroup: null, autoSplit: null);
    }

    private static List<TreeGroup<T>> CreateGroups<T>(IEnumerable<T> items, TaxonomyTreeLevel<T> level) {
        var comparer = StringComparer.OrdinalIgnoreCase;
        var buckets = new Dictionary<string, TreeGroup<T>>(comparer);
        foreach (var item in items) {
            var raw = level.Selector(item);
            var normalized = string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
            var displayValue = normalized ?? level.UnknownLabel ?? $"Unknown {level.Label}";
            if (string.IsNullOrWhiteSpace(displayValue)) {
                continue;
            }

            if (!buckets.TryGetValue(displayValue, out var bucket)) {
                bucket = new TreeGroup<T>(displayValue);
                buckets[displayValue] = bucket;
            }

            bucket.Items.Add(item);
        }

        // Merge small groups into "Other" if MinItems > 1
        if (level.MinItems > 1) {
            var otherLabel = level.OtherLabel ?? $"Other {level.Label?.ToLowerInvariant() ?? "taxa"}";
            var smallGroups = buckets.Values
                .Where(g => g.Items.Count < level.MinItems)
                .ToList();

            if (smallGroups.Count > 0) {
                if (level.MinGroupsForOther > 0 && smallGroups.Count < level.MinGroupsForOther) {
                    return buckets.Values
                        .OrderBy(group => group.DisplayValue, comparer)
                        .ToList();
                }

                // Only merge if there's more than one small group, or if there's at least one item
                var totalSmallItems = smallGroups.Sum(g => g.Items.Count);
                if (totalSmallItems > 0) {
                    // Remove small groups from buckets
                    foreach (var small in smallGroups) {
                        buckets.Remove(small.DisplayValue);
                    }

                    // Create or merge into "Other" bucket
                    if (!buckets.TryGetValue(otherLabel, out var otherBucket)) {
                        otherBucket = new TreeGroup<T>(otherLabel);
                        buckets[otherLabel] = otherBucket;
                    }

                    foreach (var small in smallGroups) {
                        otherBucket.Items.AddRange(small.Items);
                    }
                }
            }
        }

        return buckets.Values
            .OrderBy(group => IsOtherLabel(group.DisplayValue) ? 1 : 0)
            .ThenBy(group => group.DisplayValue, comparer)
            .ToList();
    }

    private static bool IsOtherLabel(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return false;
        }

        var trimmed = value.Trim();
        return trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Genus and subgenus are "fine-grained" ranks that need stricter auto-split gates
    /// because they tend to produce many tiny headings. Higher ranks (subfamily, tribe, etc.)
    /// use more lenient gates.
    /// </summary>
    private static bool IsFineGrainedRank(string rankLabel) {
        return rankLabel.Equals("genus", StringComparison.OrdinalIgnoreCase)
            || rankLabel.Equals("subgenus", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsUnknownLabel(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return false;
        }

        var trimmed = value.Trim();
        return trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsOtherOrUnknownLabel(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return false;
        }

        var trimmed = value.Trim();
        return trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase);
    }

    private sealed class TreeGroup<T> {
        public TreeGroup(string displayValue) {
            DisplayValue = displayValue;
            Items = new List<T>();
        }

        public string DisplayValue { get; }
        public List<T> Items { get; }
    }
}

internal sealed class TaxonomyTreeNode<T> {
    private readonly List<TaxonomyTreeNode<T>> _children = new();
    private readonly List<T> _items = new();

    public TaxonomyTreeNode(string? label, string? value) {
        Label = label;
        Value = value;
    }

    public string? Label { get; }
    public string? Value { get; }
    public IReadOnlyList<TaxonomyTreeNode<T>> Children => _children;
    public IReadOnlyList<T> Items => _items;
    public bool HasChildren => _children.Count > 0;
    public int ItemCount => _items.Count + _children.Sum(child => child.ItemCount);

    public TaxonomyTreeNode<T> AddChild(string label, string value) {
        var node = new TaxonomyTreeNode<T>(label, value);
        _children.Add(node);
        return node;
    }

    public void AddItems(IEnumerable<T> items) {
        if (items is null) {
            return;
        }

        _items.AddRange(items);
    }
}

internal sealed record TaxonomyTreeLevel<T>(
    string Label,
    Func<T, string?> Selector,
    bool AlwaysDisplay = false,
    string? UnknownLabel = null,
    int MinItems = 1,
    string? OtherLabel = null,
    int MinGroupsForOther = 0);

/// <summary>
/// Options for automatic section splitting when leaf groups exceed a threshold.
/// When enabled, the tree builder tries candidate levels (in order) to subdivide
/// large leaf groups. Quality gates ensure splits are informative: meaningful groups
/// must exist, the Other/Unknown fraction must be limited, and nesting depth is capped.
/// </summary>
internal sealed record AutoSplitOptions<T>(
    /// <summary>Minimum item count to trigger auto-split (e.g. 30).</summary>
    int Threshold,
    /// <summary>All meaningful groups must have at least this many items (one exception when 4+ groups).</summary>
    int MinGroupSize,
    /// <summary>Candidate grouping levels to try, in order from broadest to narrowest.</summary>
    IReadOnlyList<TaxonomyTreeLevel<T>> CandidateLevels,
    /// <summary>Maximum fraction (0.0-1.0) of items in Other+Unknown groups before rejecting.</summary>
    double MaxOtherFraction = 0.6,
    /// <summary>Maximum number of groups (after lumping) before rejecting a split.</summary>
    int MaxGroups = 15,
    /// <summary>Maximum auto-split nesting depth (additional heading levels).</summary>
    int MaxDepth = 1,
    /// <summary>Current recursion depth (incremented internally). Starts at 0.</summary>
    int CurrentDepth = 0,
    /// <summary>Minimum number of meaningful (non-Other/Unknown) groups required. Default 3.</summary>
    int MinMeaningfulGroups = 3,
    /// <summary>When true, reject splits that produce "Unknown" groups. Default true.</summary>
    bool RejectUnknownGroups = true,
    /// <summary>Optional diagnostics collector for recording split decisions.</summary>
    IAutoSplitDiagnostics? Diagnostics = null);
