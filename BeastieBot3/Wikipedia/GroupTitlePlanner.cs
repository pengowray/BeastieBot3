using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.SiteBuild;

// Which English Wikipedia titles `wikipedia fetch-group-titles` asks for, and in what order: each
// group's own name (and the article a rule names for it), in three bands so the useful part lands
// first: groups from kingdom to family (with the CoL groups above family), then the CoL groups
// between family and genus (subfamilies, tribes), then genera, which are most of the groups.

namespace BeastieBot3.Wikipedia;

/// A title to ask for. Kingdoms are the IUCN kingdoms of the groups that have it (two for a name
/// used in two kingdoms); Band and Depth order the work.
internal sealed record GroupTitle(string Title, string NormalizedTitle, int Band, int Depth, IReadOnlyList<string> Kingdoms);

internal static class GroupTitlePlanner {
    public const int BandFamilyAndAbove = 0;
    public const int BandBelowFamily = 1;
    public const int BandGenus = 2;

    public static readonly IReadOnlyList<string> BandNames = ["kingdom to family", "subfamilies and tribes", "genera"];

    public static int BandOf(SiteTreeNode node) {
        if (node.Rank == "genus" && node.Source != GroupSources.Col) {
            return BandGenus;
        }
        if (node.Source != GroupSources.Col) {
            return BandFamilyAndAbove;
        }
        for (var at = node.Parent; at is not null; at = at.Parent) {
            if (at.Source != GroupSources.Col) {
                return at.Rank is "family" or "genus" ? BandBelowFamily : BandFamilyAndAbove;
            }
        }
        return BandFamilyAndAbove;
    }

    /// <summary>
    /// The titles for <paramref name="nodes"/>: each group's name, and the title
    /// <paramref name="ruleTitle"/> gives for it (a wikilink from the rule files), once each, in
    /// band, depth and title order.
    /// </summary>
    public static IReadOnlyList<GroupTitle> Plan(IEnumerable<SiteTreeNode> nodes, Func<SiteTreeNode, string?>? ruleTitle = null) {
        var byTitle = new Dictionary<string, (string Title, int Band, int Depth, SortedSet<string> Kingdoms)>(StringComparer.Ordinal);
        void Add(string? title, SiteTreeNode node) {
            var normalized = WikipediaTitleHelper.Normalize(title);
            if (normalized.Length == 0) {
                return;
            }
            var band = BandOf(node);
            if (byTitle.TryGetValue(normalized, out var known)) {
                known.Kingdoms.Add(node.Kingdom);
                if ((band, node.Depth).CompareTo((known.Band, known.Depth)) < 0) {
                    byTitle[normalized] = (known.Title, band, node.Depth, known.Kingdoms);
                }
                return;
            }
            byTitle[normalized] = (title!.Trim(), band, node.Depth, new SortedSet<string>(StringComparer.Ordinal) { node.Kingdom });
        }
        foreach (var node in nodes) {
            Add(node.Name, node);
            if (ruleTitle?.Invoke(node) is { } fromRules) {
                Add(fromRules, node);
            }
        }
        return byTitle
            .Select(p => new GroupTitle(p.Value.Title, p.Key, p.Value.Band, p.Value.Depth, p.Value.Kingdoms.ToList()))
            .OrderBy(t => t.Band).ThenBy(t => t.Depth).ThenBy(t => t.NormalizedTitle, StringComparer.Ordinal)
            .ToList();
    }
}
