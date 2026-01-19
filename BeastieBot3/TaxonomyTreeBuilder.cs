using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3;

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

        BuildRecursive(root, materialized, levels, 0, shouldSkipGroup);
        return root;
    }

    private static void BuildRecursive<T>(
        TaxonomyTreeNode<T> parent, 
        IReadOnlyList<T> items, 
        IReadOnlyList<TaxonomyTreeLevel<T>> levels, 
        int levelIndex,
        Func<string, bool>? shouldSkipGroup) {
        if (items.Count == 0) {
            return;
        }

        if (levelIndex >= levels.Count) {
            parent.AddItems(items);
            return;
        }

        var level = levels[levelIndex];
        var groups = CreateGroups(items, level);
        if (groups.Count == 0) {
            BuildRecursive(parent, items, levels, levelIndex + 1, shouldSkipGroup);
            return;
        }

        if (!level.AlwaysDisplay && groups.Count == 1) {
            BuildRecursive(parent, groups[0].Items, levels, levelIndex + 1, shouldSkipGroup);
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
            BuildRecursive(child, group.Items, levels, levelIndex + 1, shouldSkipGroup);
        }

        // Process skipped items at the next level (no heading for the current level)
        if (skipItems.Count > 0) {
            BuildRecursive(parent, skipItems, levels, levelIndex + 1, shouldSkipGroup);
        }
    }

    private static void BuildRecursive<T>(TaxonomyTreeNode<T> parent, IReadOnlyList<T> items, IReadOnlyList<TaxonomyTreeLevel<T>> levels, int levelIndex) {
        BuildRecursive(parent, items, levels, levelIndex, shouldSkipGroup: null);
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
            .OrderBy(group => group.DisplayValue, comparer)
            .ToList();
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
    string? OtherLabel = null);
