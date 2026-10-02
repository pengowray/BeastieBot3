using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3.Taxonomy;

/// <summary>
/// Builds a heading tree from flat records using configured grouping levels (order, family, ...).
/// Records flow through the levels top-down:
/// <list type="bullet">
///   <item>A level with one value gets no heading unless <c>AlwaysDisplay</c> is set.</item>
///   <item>Groups below <c>MinItems</c> are lumped into an "Other" bucket, which sorts last.</item>
///   <item>A level with <c>Intermediates</c> may get extra headings from the Catalogue of Life nodes
///         between it and the level above (suborder Serpentes between Squamata and its families),
///         when the gates in <see cref="IntermediateLayerOptions"/> pass. See <c>TryLayer</c>.</item>
///   <item>A node whose value has curated groups (<see cref="VirtualGroupOptions{T}"/>, such as
///         Snakes / Lizards for Squamata) has its items split into those groups first.</item>
///   <item>After the last level, a large group may be split by a finer rank (<see cref="AutoSplitOptions{T}"/>).</item>
/// </list>
/// <see cref="TaxonomyTreeOptions{T}.HeadingLevels"/> caps the depth of the tree, so a renderer with
/// a fixed number of heading levels (MediaWiki H2 to H6) never has to flatten a heading.
/// </summary>
internal static class TaxonomyTreeBuilder {
    /// <summary>Builds a tree with no auto-split, intermediate layers, virtual groups or depth limit.</summary>
    public static TaxonomyTreeNode<T> Build<T>(IEnumerable<T> items, IReadOnlyList<TaxonomyTreeLevel<T>> levels) =>
        Build(items, levels, options: null);

    public static TaxonomyTreeNode<T> Build<T>(
        IEnumerable<T> items,
        IReadOnlyList<TaxonomyTreeLevel<T>> levels,
        TaxonomyTreeOptions<T>? options) {
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

        new TreeRun<T>(levels, options ?? new TaxonomyTreeOptions<T>()).Run(root, materialized);
        return root;
    }

    /// <summary>True for "Other X" or "Unknown X" labels (residual buckets).</summary>
    public static bool IsResidualLabel(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return false;
        }

        var trimmed = value.Trim();
        return trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The label of the bucket that small groups are lumped into, e.g. "Other families".</summary>
    public static string DefaultOtherLabel(string? levelLabel) =>
        string.IsNullOrWhiteSpace(levelLabel) ? "Other taxa" : $"Other {RankNames.Plural(levelLabel.ToLowerInvariant())}";

    private static bool IsUnknownLabel(string value) {
        var trimmed = value.Trim();
        return trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Genus and subgenus need stricter auto-split gates because they tend to produce many tiny
    /// headings. Higher ranks (subfamily, tribe) use more lenient gates.
    /// </summary>
    private static bool IsFineGrainedRank(string rankLabel) =>
        rankLabel.Equals("genus", StringComparison.OrdinalIgnoreCase)
        || rankLabel.Equals("subgenus", StringComparison.OrdinalIgnoreCase);

    private static string Extend(string path, string value) => $"{path} → {value}";

    /// <summary>
    /// Groups items by the level's selector. Items with a blank value go to the level's unknown
    /// bucket. When <c>MinItems</c> &gt; 1, groups below that size are lumped into an "Other" bucket,
    /// unless there are fewer than <c>MinGroupsForOther</c> of them. Named groups are sorted
    /// alphabetically and residual buckets come last.
    /// </summary>
    private static List<TreeGroup<T>> CreateGroups<T>(IEnumerable<T> items, TaxonomyTreeLevel<T> level) {
        var comparer = StringComparer.OrdinalIgnoreCase;
        var buckets = new Dictionary<string, TreeGroup<T>>(comparer);
        var unknownLabel = level.UnknownLabel ?? $"Unknown {level.Label}";
        foreach (var item in items) {
            var raw = level.Selector(item);
            var normalized = string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
            var displayValue = normalized ?? unknownLabel;
            if (string.IsNullOrWhiteSpace(displayValue)) {
                continue;
            }

            if (!buckets.TryGetValue(displayValue, out var bucket)) {
                bucket = new TreeGroup<T>(displayValue, isResidual: normalized == null);
                buckets[displayValue] = bucket;
            }

            bucket.Items.Add(item);
        }

        if (level.MinItems > 1) {
            var smallGroups = buckets.Values.Where(g => g.Items.Count < level.MinItems).ToList();
            var enoughToLump = level.MinGroupsForOther <= 0 || smallGroups.Count >= level.MinGroupsForOther;
            if (smallGroups.Count > 0 && enoughToLump) {
                var otherLabel = level.OtherLabel ?? DefaultOtherLabel(level.Label);
                foreach (var small in smallGroups) {
                    buckets.Remove(small.DisplayValue);
                }

                if (!buckets.TryGetValue(otherLabel, out var otherBucket)) {
                    otherBucket = new TreeGroup<T>(otherLabel, isResidual: true);
                    buckets[otherLabel] = otherBucket;
                }

                foreach (var small in smallGroups) {
                    otherBucket.Items.AddRange(small.Items);
                }
            }
        }

        return buckets.Values
            .OrderBy(group => group.IsResidual ? 1 : 0)
            .ThenBy(group => group.DisplayValue, comparer)
            .ToList();
    }

    private sealed class TreeGroup<T> {
        public TreeGroup(string displayValue, bool isResidual) {
            DisplayValue = displayValue;
            IsResidual = isResidual || IsResidualLabel(displayValue);
            Items = new List<T>();
        }

        public string DisplayValue { get; }
        public bool IsResidual { get; }
        public List<T> Items { get; }
    }

    /// <summary>
    /// One build: the levels, options and heading budget, plus the recursive steps. Depth counts
    /// heading levels: the root is depth 0, and a node at depth d renders d - 1 levels below the
    /// first heading level. A node may be created only at a depth within the budget.
    /// </summary>
    private sealed class TreeRun<T> {
        private readonly IReadOnlyList<TaxonomyTreeLevel<T>> _levels;
        private readonly TaxonomyTreeOptions<T> _options;
        private readonly int _budget;
        private readonly IAutoSplitDiagnostics? _diagnostics;

        public TreeRun(IReadOnlyList<TaxonomyTreeLevel<T>> levels, TaxonomyTreeOptions<T> options) {
            _levels = levels;
            _options = options;
            _budget = options.HeadingLevels is { } levelsAvailable ? Math.Max(0, levelsAvailable) : int.MaxValue;
            _diagnostics = options.Diagnostics;
        }

        /// <summary>An item and how far into its intermediate path the current layer reads.</summary>
        private readonly record struct Entry(T Item, int Depth);

        public void Run(TaxonomyTreeNode<T> root, IReadOnlyList<T> items) =>
            Process(root, 0, ToEntries(items), 0, _options.RootLabel ?? "(root)");

        private static List<Entry> ToEntries(IEnumerable<T> items) => items.Select(item => new Entry(item, 0)).ToList();

        private static List<T> ItemsOf(IEnumerable<Entry> entries) => entries.Select(e => e.Item).ToList();

        /// <summary>The configured levels from <paramref name="levelIndex"/> onward, each of which may need a heading.</summary>
        private int LevelsFrom(int levelIndex) => Math.Max(0, _levels.Count - levelIndex);

        /// <summary>Groups <paramref name="entries"/> under <paramref name="parent"/> by the level at <paramref name="levelIndex"/>.</summary>
        private void Process(TaxonomyTreeNode<T> parent, int parentDepth, IReadOnlyList<Entry> entries, int levelIndex, string path) {
            if (entries.Count == 0) {
                return;
            }

            if (levelIndex >= _levels.Count) {
                Leaf(parent, parentDepth, ItemsOf(entries), path);
                return;
            }

            if (parentDepth >= _budget) {
                // No heading level left: the items stay on this node.
                parent.AddItems(ItemsOf(entries));
                return;
            }

            var level = _levels[levelIndex];
            if (level.Intermediates != null && _options.Intermediate != null
                && TryLayer(parent, parentDepth, entries, levelIndex, path)) {
                return;
            }

            GroupByLevel(parent, parentDepth, ItemsOf(entries), levelIndex, keepSingleGroup: false, path);
        }

        /// <summary>
        /// Creates one child per value of the level. A level with one value gets no heading (unless
        /// <paramref name="keepSingleGroup"/> or <c>AlwaysDisplay</c>). A one-item residual bucket and
        /// force-split groups give their items to the next level under the same parent.
        /// </summary>
        private void GroupByLevel(TaxonomyTreeNode<T> parent, int parentDepth, IReadOnlyList<T> items, int levelIndex, bool keepSingleGroup, string path) {
            var level = _levels[levelIndex];
            var groups = CreateGroups(items, level);
            if (groups.Count == 0) {
                Process(parent, parentDepth, ToEntries(items), levelIndex + 1, path);
                return;
            }

            if (groups.Count == 1 && !level.AlwaysDisplay && !keepSingleGroup) {
                Descend(parent, parentDepth, groups[0].DisplayValue, groups[0].Items, levelIndex, path);
                return;
            }

            var skipItems = new List<T>();
            foreach (var group in groups) {
                if (_options.ShouldSkipGroup != null && _options.ShouldSkipGroup(group.DisplayValue)) {
                    skipItems.AddRange(group.Items);
                    continue;
                }

                if (group.IsResidual && group.Items.Count <= 1) {
                    // A one-item "Other" bucket is not worth a heading; the item sits on the parent.
                    Process(parent, parentDepth, ToEntries(group.Items), levelIndex + 1, path);
                    continue;
                }

                var child = parent.AddChild(level.Label, group.DisplayValue, TreeNodeKind.Level, level.KeyOrLabel);
                Descend(child, parentDepth + 1, group.DisplayValue, group.Items, levelIndex, Extend(path, group.DisplayValue));
            }

            if (skipItems.Count > 0) {
                Process(parent, parentDepth, ToEntries(skipItems), levelIndex + 1, path);
            }
        }

        /// <summary>Continues below a value of the level at <paramref name="levelIndex"/>: virtual groups first, then the next level.</summary>
        private void Descend(TaxonomyTreeNode<T> node, int nodeDepth, string value, IReadOnlyList<T> items, int levelIndex, string path) {
            if (TryVirtualGroups(node, nodeDepth, value, items, levelIndex, path)) {
                return;
            }

            Process(node, nodeDepth, ToEntries(items), levelIndex + 1, path);
        }

        /// <summary>
        /// Splits the items of a taxon with curated groups (Squamata: Snakes, Worm lizards, Lizards).
        /// Each item is matched against the intermediate path of the next level, so an item matched
        /// on a clade node (Serpentes) continues its intermediate layers after that node. Returns
        /// false when the taxon has no groups or the heading budget has no room for them.
        /// </summary>
        private bool TryVirtualGroups(TaxonomyTreeNode<T> node, int nodeDepth, string owner, IReadOnlyList<T> items, int levelIndex, string path) {
            var virtualGroups = _options.VirtualGroups;
            if (virtualGroups is null) {
                return false;
            }

            var names = virtualGroups.GroupNames(owner);
            if (names is null || names.Count == 0) {
                return false;
            }

            var nextIndex = levelIndex + 1;
            var pathOf = nextIndex < _levels.Count ? _levels[nextIndex].Intermediates : null;
            var buckets = new Dictionary<string, List<Entry>>(StringComparer.OrdinalIgnoreCase);
            var unmatched = new List<Entry>();
            foreach (var item in items) {
                var nodePath = pathOf?.Invoke(item) ?? Array.Empty<PlacementNode>();
                var match = virtualGroups.Resolve(owner, item, nodePath);
                if (match is null) {
                    unmatched.Add(new Entry(item, 0));
                    continue;
                }

                if (!buckets.TryGetValue(match.Group, out var bucket)) {
                    bucket = new List<Entry>();
                    buckets[match.Group] = bucket;
                }

                bucket.Add(new Entry(item, Math.Max(0, match.CladeIndex + 1)));
            }

            var nonEmpty = names.Where(name => buckets.ContainsKey(name)).ToList();
            var groupCount = nonEmpty.Count + (unmatched.Count > 0 ? 1 : 0);
            if (groupCount <= 1) {
                // One group: no virtual heading, but layers still continue after the matched clade.
                var all = buckets.Values.SelectMany(b => b).Concat(unmatched).ToList();
                Process(node, nodeDepth, all, nextIndex, path);
                return true;
            }

            if (nodeDepth + 1 + LevelsFrom(nextIndex) > _budget) {
                return false;
            }

            foreach (var name in nonEmpty) {
                var child = node.AddChild(label: null, name, TreeNodeKind.Virtual, key: null, virtualOwner: owner);
                Process(child, nodeDepth + 1, buckets[name], nextIndex, Extend(path, name));
            }

            if (unmatched.Count > 0) {
                var other = node.AddChild(label: null, "Other", TreeNodeKind.Virtual, key: null, virtualOwner: owner);
                Process(other, nodeDepth + 1, unmatched, nextIndex, Extend(path, "Other"));
            }

            return true;
        }

        /// <summary>
        /// Tries to add a heading layer from the Catalogue of Life nodes between the previous level and
        /// the level at <paramref name="levelIndex"/>, reading each item's path at its own depth.
        /// <list type="bullet">
        ///   <item>A node every item shares adds nothing and is skipped without a heading.</item>
        ///   <item>A named group whose items share one value of the level (a suborder with one
        ///         family) is demoted: its items are grouped by the level directly.</item>
        ///   <item>The layer is shown only when every gate passes; the decision is recorded.</item>
        /// </list>
        /// When shown, each named group becomes a child and is tried again one node deeper (Cetacea,
        /// then Odontoceti / Mysticeti); items with no node are grouped by the level beside them.
        /// Returns false when the layer is not shown, so the caller groups by the level as usual.
        /// </summary>
        private bool TryLayer(TaxonomyTreeNode<T> parent, int parentDepth, IReadOnlyList<Entry> entries, int levelIndex, string path) {
            var level = _levels[levelIndex];
            var options = _options.Intermediate!;
            var pathOf = level.Intermediates!;
            var current = entries;

            List<LayerGroup> named;
            List<Entry> loose;
            while (true) {
                (named, loose) = SplitByNode(current, pathOf);
                if (named.Count == 0) {
                    return false;
                }

                if (named.Count == 1 && loose.Count == 0) {
                    current = current.Select(e => e with { Depth = e.Depth + 1 }).ToList();
                    continue;
                }

                break;
            }

            var offered = named.Select(g => g.Node.ColRank).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var valueOf = (Func<T, string>)(item => level.Selector(item)?.Trim().ToUpperInvariant() ?? string.Empty);

            // A named group whose items all share one value of the level only repeats that value's heading.
            foreach (var group in named.ToList()) {
                if (group.Entries.Select(e => valueOf(e.Item)).Distinct().Count() == 1) {
                    named.Remove(group);
                    loose.AddRange(group.Entries);
                }
            }

            var itemCount = current.Count;
            var anchors = current.Select(e => valueOf(e.Item)).Distinct().Count();
            var looseValues = loose.Select(e => valueOf(e.Item)).Distinct().Count();
            var headings = named.Count + looseValues;
            var largest = named.Count > 0 ? named.Max(g => g.Entries.Count) : 0;
            var largestShare = itemCount > 0 ? (double)largest / itemCount : 0;

            string outcome;
            if (named.Count == 0) {
                outcome = "rejected:single_value_groups";
            } else if (itemCount < options.MinItems) {
                outcome = "rejected:few_items";
            } else if (anchors < options.MinAnchors) {
                outcome = "rejected:few_anchors";
            } else if (headings >= anchors) {
                outcome = "rejected:no_fewer_headings";
            } else if (named.Count > options.MaxGroups) {
                outcome = "rejected:too_many_groups";
            } else if (largestShare > options.MaxDominance) {
                outcome = "rejected:dominant_group";
            } else if (largest < options.MinGroupSize) {
                outcome = "rejected:groups_too_small";
            } else if (parentDepth + 1 + LevelsFrom(levelIndex) > _budget) {
                outcome = "rejected:heading_depth";
            } else {
                outcome = "accepted";
            }

            var ranks = named.Count > 0
                ? named.Select(g => g.Node.ColRank).Distinct(StringComparer.OrdinalIgnoreCase).ToList()
                : offered;
            _diagnostics?.RecordLayer(new IntermediateLayerDecision(
                path, itemCount, string.Join("/", ranks), level.KeyOrLabel, outcome,
                NamedGroups: named.Count,
                LooseValues: looseValues,
                Anchors: anchors,
                Headings: headings,
                LargestShare: Math.Round(largestShare, 3),
                Groups: string.Join(", ", named.Select(g => g.Node.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase))));

            if (outcome != "accepted") {
                return false;
            }

            foreach (var group in named.OrderBy(g => g.Node.Name, StringComparer.OrdinalIgnoreCase)) {
                var node = group.Node;
                var child = parent.AddChild(
                    node.ShowRank ? node.ColRank : null, node.Name, TreeNodeKind.Intermediate, node.ColRank, showRank: node.ShowRank);
                var deeper = group.Entries.Select(e => e with { Depth = e.Depth + 1 }).ToList();
                Process(child, parentDepth + 1, deeper, levelIndex, Extend(path, node.Name));
            }

            if (loose.Count > 0) {
                GroupByLevel(parent, parentDepth, ItemsOf(loose), levelIndex, keepSingleGroup: true, path);
            }

            return true;
        }

        private sealed record LayerGroup(PlacementNode Node, List<Entry> Entries);

        /// <summary>Groups entries by the path node at each entry's depth; entries with no node there are loose.</summary>
        private static (List<LayerGroup> Named, List<Entry> Loose) SplitByNode(
            IReadOnlyList<Entry> entries, Func<T, IReadOnlyList<PlacementNode>> pathOf) {
            var named = new Dictionary<string, LayerGroup>(StringComparer.OrdinalIgnoreCase);
            var order = new List<LayerGroup>();
            var loose = new List<Entry>();
            foreach (var entry in entries) {
                var nodePath = pathOf(entry.Item);
                var node = entry.Depth < nodePath.Count ? nodePath[entry.Depth] : null;
                if (node is null || string.IsNullOrWhiteSpace(node.Name)) {
                    loose.Add(entry);
                    continue;
                }

                if (!named.TryGetValue(node.Name, out var group)) {
                    group = new LayerGroup(node, new List<Entry>());
                    named[node.Name] = group;
                    order.Add(group);
                }

                group.Entries.Add(entry);
            }

            return (order, loose);
        }

        /// <summary>After the last level: try auto-split, otherwise the items stay on the node.</summary>
        private void Leaf(TaxonomyTreeNode<T> parent, int parentDepth, IReadOnlyList<T> items, string path) {
            var autoSplit = _options.AutoSplit;
            if (autoSplit != null && items.Count >= autoSplit.Threshold && autoSplit.CandidateLevels.Count > 0
                && TryAutoSplit(parent, parentDepth, items, autoSplit, path)) {
                return;
            }

            parent.AddItems(items);
        }

        /// <summary>
        /// Splits a large group by the first candidate rank (broadest to narrowest, e.g. subfamily,
        /// tribe, genus) that passes every gate:
        /// <list type="number">
        ///   <item>Depth: rejects at <c>MaxDepth</c> nested splits, or when no heading level is left.</item>
        ///   <item>Single group: rejects a rank that gives one bucket.</item>
        ///   <item>Unknown: rejects any "Unknown X" bucket (when <c>RejectUnknownGroups</c>).</item>
        ///   <item>Meaningful groups: <c>MinMeaningfulGroups</c> named groups for genus/subgenus, 2 for higher ranks.</item>
        ///   <item>Group size: genus/subgenus need every named group at <c>MinGroupSize</c> (one
        ///         exception at 4+ groups); higher ranks need one group at half that (at least 5).</item>
        ///   <item>Other fraction: at most <c>MaxOtherFraction</c> of the items in residual buckets.</item>
        ///   <item>Max groups: at most <c>MaxGroups</c> buckets.</item>
        /// </list>
        /// Each call records exactly one closing decision: "accepted" for the rank used, or a
        /// "(all)" rejection, so attempts and acceptances can be counted from the decisions.
        /// </summary>
        private bool TryAutoSplit(TaxonomyTreeNode<T> parent, int parentDepth, IReadOnlyList<T> items, AutoSplitOptions<T> autoSplit, string path) {
            if (autoSplit.CurrentDepth >= autoSplit.MaxDepth) {
                Record(new AutoSplitDecision(path, items.Count, "(all)", "rejected:depth_limit"));
                return false;
            }

            if (parentDepth >= _budget) {
                Record(new AutoSplitDecision(path, items.Count, "(all)", "rejected:heading_depth"));
                return false;
            }

            for (var i = 0; i < autoSplit.CandidateLevels.Count; i++) {
                var candidate = autoSplit.CandidateLevels[i];
                var rank = candidate.KeyOrLabel;
                var groups = CreateGroups(items, candidate);
                if (groups.Count <= 1) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:single_group", GroupCount: groups.Count));
                    continue;
                }

                var meaningful = groups.Where(g => !g.IsResidual).ToList();
                if (autoSplit.RejectUnknownGroups && groups.Any(g => IsUnknownLabel(g.DisplayValue))) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:has_unknown",
                        GroupCount: groups.Count, MeaningfulGroups: meaningful.Count));
                    continue;
                }

                var fineGrained = IsFineGrainedRank(rank);
                var requiredMeaningful = fineGrained ? autoSplit.MinMeaningfulGroups : 2;
                var requiredGroupSize = fineGrained ? autoSplit.MinGroupSize : Math.Max(autoSplit.MinGroupSize / 2, 5);
                if (meaningful.Count < requiredMeaningful) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:few_meaningful",
                        GroupCount: groups.Count, MeaningfulGroups: meaningful.Count));
                    continue;
                }

                if (fineGrained) {
                    var smallMeaningful = meaningful.Count(g => g.Items.Count < requiredGroupSize);
                    var allowedSmall = meaningful.Count >= 4 ? 1 : 0;
                    if (smallMeaningful > allowedSmall) {
                        Record(new AutoSplitDecision(path, items.Count, rank, "rejected:groups_too_small",
                            GroupCount: groups.Count, MeaningfulGroups: meaningful.Count));
                        continue;
                    }
                } else if (!meaningful.Any(g => g.Items.Count >= requiredGroupSize)) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:no_meaningful",
                        GroupCount: groups.Count, MeaningfulGroups: meaningful.Count));
                    continue;
                }

                var residualCount = groups.Where(g => g.IsResidual).Sum(g => g.Items.Count);
                var otherFraction = items.Count > 0 ? (double)residualCount / items.Count : 0;
                if (otherFraction > autoSplit.MaxOtherFraction) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:high_other",
                        GroupCount: groups.Count, MeaningfulGroups: meaningful.Count, OtherFraction: otherFraction));
                    continue;
                }

                if (groups.Count > autoSplit.MaxGroups) {
                    Record(new AutoSplitDecision(path, items.Count, rank, "rejected:too_many_groups",
                        GroupCount: groups.Count, MeaningfulGroups: meaningful.Count, OtherFraction: otherFraction));
                    continue;
                }

                Record(new AutoSplitDecision(path, items.Count, rank, "accepted",
                    GroupCount: groups.Count, MeaningfulGroups: meaningful.Count,
                    OtherFraction: otherFraction, LargestGroup: meaningful.Max(g => g.Items.Count)));

                var remaining = i + 1 < autoSplit.CandidateLevels.Count
                    ? autoSplit with {
                        CandidateLevels = autoSplit.CandidateLevels.Skip(i + 1).ToList(),
                        CurrentDepth = autoSplit.CurrentDepth + 1,
                    }
                    : null;

                foreach (var group in groups) {
                    if (group.IsResidual && group.Items.Count <= 1) {
                        parent.AddItems(group.Items);
                        continue;
                    }

                    var child = parent.AddChild(candidate.Label, group.DisplayValue, TreeNodeKind.AutoSplit, rank);
                    var splitAgain = remaining != null
                        && group.Items.Count >= autoSplit.Threshold
                        && !group.IsResidual
                        && TryAutoSplit(child, parentDepth + 1, group.Items, remaining, Extend(path, group.DisplayValue));
                    if (!splitAgain) {
                        child.AddItems(group.Items);
                    }
                }

                return true;
            }

            Record(new AutoSplitDecision(path, items.Count, "(all)", "rejected:no_candidate"));
            return false;
        }

        private void Record(AutoSplitDecision decision) => _diagnostics?.RecordDecision(decision);
    }
}

/// <summary>What a tree node groups by, which decides how its heading is written.</summary>
internal enum TreeNodeKind {
    /// <summary>A value of a configured level (order, family) or the root.</summary>
    Level,
    /// <summary>A Catalogue of Life node inserted between two configured levels.</summary>
    Intermediate,
    /// <summary>A curated group of a taxon (Snakes in Squamata).</summary>
    Virtual,
    /// <summary>A value of a finer rank added by auto-split.</summary>
    AutoSplit,
}

/// <summary>
/// A node in the taxonomy tree: a rank label (e.g. "Order") and value (e.g. "Carnivora"), child
/// nodes, and the items placed directly on this node. A renderer writes a node's own items before
/// its children's headings. <see cref="ItemCount"/> sums items across all descendants.
/// </summary>
internal sealed class TaxonomyTreeNode<T> {
    private readonly List<TaxonomyTreeNode<T>> _children = new();
    private readonly List<T> _items = new();

    public TaxonomyTreeNode(string? label, string? value) {
        Label = label;
        Value = value;
    }

    public string? Label { get; }
    public string? Value { get; }

    public TreeNodeKind Kind { get; private init; } = TreeNodeKind.Level;

    /// <summary>The rank this node groups by, lower case: a level key ("family") or a CoL rank ("suborder").</summary>
    public string? Key { get; private init; }

    /// <summary>False for a CoL node whose heading shows only its name (Cetacea inside order Artiodactyla).</summary>
    public bool ShowRank { get; private init; } = true;

    /// <summary>For a <see cref="TreeNodeKind.Virtual"/> node: the taxon whose curated group this is (e.g. "SQUAMATA").</summary>
    public string? VirtualOwner { get; private init; }

    public IReadOnlyList<TaxonomyTreeNode<T>> Children => _children;
    public IReadOnlyList<T> Items => _items;
    public bool HasChildren => _children.Count > 0;
    public int ItemCount => _items.Count + _children.Sum(child => child.ItemCount);

    /// <summary>The number of heading levels below this node (0 for a node with no children).</summary>
    public int Height => _children.Count == 0 ? 0 : 1 + _children.Max(child => child.Height);

    public TaxonomyTreeNode<T> AddChild(string label, string value) =>
        AddChild(label, value, TreeNodeKind.Level, label.ToLowerInvariant());

    public TaxonomyTreeNode<T> AddChild(
        string? label,
        string value,
        TreeNodeKind kind,
        string? key,
        bool showRank = true,
        string? virtualOwner = null) {
        var node = new TaxonomyTreeNode<T>(label, value) {
            Kind = kind,
            Key = key,
            ShowRank = showRank,
            VirtualOwner = virtualOwner,
        };
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

/// <summary>
/// One grouping level of the tree.
/// </summary>
/// <param name="Label">Rank name used as heading prefix (e.g., "Family").</param>
/// <param name="Selector">Extracts the grouping value from each item (e.g., item → item.Family).</param>
/// <param name="AlwaysDisplay">If true, show this level's heading even when all items share one value.</param>
/// <param name="UnknownLabel">Display value for items where Selector returns null/blank.
/// Defaults to "Unknown {Label}". Set equal to OtherLabel to route unknowns into the Other bucket.</param>
/// <param name="MinItems">Minimum items for a group to keep its own heading. Smaller groups merge into Other.</param>
/// <param name="OtherLabel">Heading text for the merged bucket. Defaults to "Other {plural of Label}", e.g. "Other families".</param>
/// <param name="MinGroupsForOther">Minimum number of small groups before merging kicks in.
/// Prevents a single small group from being renamed to "Other".</param>
/// <param name="Intermediates">The Catalogue of Life nodes between the previous level and this one for
/// an item, broad to narrow; null when the level gets no intermediate layers.</param>
/// <param name="Key">The rank key, lower case (e.g. "family"). Defaults to the lower-cased label.</param>
internal sealed record TaxonomyTreeLevel<T>(
    string Label,
    Func<T, string?> Selector,
    bool AlwaysDisplay = false,
    string? UnknownLabel = null,
    int MinItems = 1,
    string? OtherLabel = null,
    int MinGroupsForOther = 0,
    Func<T, IReadOnlyList<PlacementNode>>? Intermediates = null,
    string? Key = null) {
    public string KeyOrLabel => Key ?? Label.ToLowerInvariant();
}

/// <summary>Everything optional about one tree build.</summary>
internal sealed record TaxonomyTreeOptions<T> {
    /// <summary>A group value that gets no heading; its items go to the next level under the same parent.</summary>
    public Func<string, bool>? ShouldSkipGroup { get; init; }

    public AutoSplitOptions<T>? AutoSplit { get; init; }

    /// <summary>Gates for intermediate layers. Null turns them off.</summary>
    public IntermediateLayerOptions? Intermediate { get; init; }

    public VirtualGroupOptions<T>? VirtualGroups { get; init; }

    /// <summary>
    /// How many heading levels the renderer has below the parent (MediaWiki: 6 - first heading
    /// level + 1). Null means no limit. A configured level that does not fit gets no headings; an
    /// intermediate layer or a virtual heading is added only if every configured level after it
    /// still fits; auto-split only adds headings if a level is left.
    /// </summary>
    public int? HeadingLevels { get; init; }

    public IAutoSplitDiagnostics? Diagnostics { get; init; }

    /// <summary>The start of every path in the diagnostics (e.g. the status section heading).</summary>
    public string? RootLabel { get; init; }
}

/// <summary>
/// Gates for a layer of Catalogue of Life nodes between two configured levels. A layer is shown
/// only when all pass, with N items, D distinct values of the level below (anchors) and F the
/// headings the layer leaves at that point (named groups plus distinct values of loose items).
/// </summary>
/// <param name="MinItems">N must be at least this.</param>
/// <param name="MinAnchors">D must be at least this.</param>
/// <param name="MaxGroups">At most this many named groups.</param>
/// <param name="MaxDominance">No named group may hold more than this share of the N items.</param>
/// <param name="MinGroupSize">At least one named group must have this many items.</param>
internal sealed record IntermediateLayerOptions(
    int MinItems = 30,
    int MinAnchors = 6,
    int MaxGroups = 12,
    double MaxDominance = 0.85,
    int MinGroupSize = 5);

/// <summary>The curated group an item belongs to.</summary>
/// <param name="Group">The group name.</param>
/// <param name="CladeIndex">Index in the item's intermediate path of the node the group matched, or -1
/// when it matched another way (a family list, or the default group). Layers continue after it.</param>
internal sealed record VirtualGroupMatch(string Group, int CladeIndex);

/// <summary>Curated groups for a taxon's items, e.g. Snakes / Worm lizards / Lizards for Squamata.</summary>
/// <param name="GroupNames">The ordered group names for a taxon value, or null when it has none.</param>
/// <param name="Resolve">The group of an item under a taxon, given the item's intermediate path for
/// the next level; null when the item matches no group.</param>
internal sealed record VirtualGroupOptions<T>(
    Func<string, IReadOnlyList<string>?> GroupNames,
    Func<string, T, IReadOnlyList<PlacementNode>, VirtualGroupMatch?> Resolve);

/// <summary>
/// Options for splitting a large group after the last configured level. The tree builder tries
/// candidate levels in order; gates keep the split informative.
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
    bool RejectUnknownGroups = true);

/// <summary>Plural rank names for headings such as "Other families".</summary>
internal static class RankNames {
    public static string Plural(string rank) {
        var lower = rank.Trim().ToLowerInvariant();
        switch (lower) {
            case "genus":
                return "genera";
            case "subgenus":
                return "subgenera";
            case "phylum":
                return "phyla";
            case "subphylum":
                return "subphyla";
            case "species":
                return "species";
            case "taxon":
                return "taxa";
        }

        if (lower.EndsWith("y", StringComparison.Ordinal)) {
            return lower[..^1] + "ies";
        }

        if (lower.EndsWith("s", StringComparison.Ordinal)) {
            return lower + "es";
        }

        return lower + "s";
    }
}
