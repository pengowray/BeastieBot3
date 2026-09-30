using System;
using System.Collections.Generic;
using System.Linq;
using Spectre.Console;

namespace BeastieBot3.WikipediaLists;

// Plain-text messages about a parent list's sub-group links, built from the loader's ChildLinkNotes.
// generate-lists prints the per-list messages under each list's "Generating" line (PrintForList); the
// Taxa grouping page shows the per-group warnings after "Save sub-groups". Only PrintForList writes to
// the console; the builders are pure.
internal static class ChildLinkReport {
    internal enum Severity { Info, Warning }

    /// <param name="Hint">What to change to fix a warning; null for info lines.</param>
    internal sealed record Message(Severity Severity, string Text, string? Hint = null);

    /// <summary>Print <see cref="ForList"/> for one list, indented under its "Generating" line.</summary>
    public static void PrintForList(string listId, IEnumerable<ChildLinkNote> notes) {
        foreach (var m in ForList(listId, notes)) {
            var colour = m.Severity == Severity.Warning ? "yellow" : "grey";
            var prefix = m.Severity == Severity.Warning ? "Warning: " : "";
            AnsiConsole.MarkupLine($"  [{colour}]{Markup.Escape(prefix + m.Text)}[/]");
            if (m.Hint is not null) {
                AnsiConsole.MarkupLine($"  [grey]{Markup.Escape(m.Hint)}[/]");
            }
        }
    }

    /// <summary>
    /// Messages for one list: the sub-group lists it links, each sub-group it cannot link while it is a
    /// parent page, and a note when it has sub-groups but links none of them (an ordinary list).
    /// </summary>
    public static IReadOnlyList<Message> ForList(string listId, IEnumerable<ChildLinkNote> allNotes) {
        var notes = allNotes.Where(n => string.Equals(n.ParentListId, listId, StringComparison.OrdinalIgnoreCase)).ToList();
        var messages = new List<Message>();
        if (notes.Count == 0) return messages;

        var children = notes.Where(n => n.Kind == GroupingKind.Phylogenetic).ToList();
        var linked = children.Where(n => n.LinkedListId is not null).ToList();
        if (linked.Count > 0) {
            messages.Add(new Message(Severity.Info,
                "Sub-group lists linked: " + string.Join(", ", linked.Select(n => n.LinkedListId))));
            foreach (var n in children.Where(n => n.Outcome == ChildLinkOutcome.NoList)) {
                messages.Add(new Message(Severity.Warning, MissingSubListText(n, listId), CreateListHint(n, new[] { n.Preset })));
            }
        } else if (children.Any(n => n.Outcome != ChildLinkOutcome.UnknownGroup)) {
            var first = children[0];
            var names = string.Join(", ", children.Select(n => n.ChildGroup));
            messages.Add(new Message(Severity.Info,
                $"No sub-group of {first.ParentGroup} ({names}) has a list for preset {first.Preset}, so {listId} has no summary table and no links to sub-group lists."));
        }

        foreach (var n in notes.Where(n => n.Outcome == ChildLinkOutcome.UnknownGroup)) {
            messages.Add(UnknownGroupMessage(n));
        }

        foreach (var n in notes.Where(n => n.Kind == GroupingKind.SeeAlso && n.Outcome == ChildLinkOutcome.NoList)) {
            messages.Add(new Message(Severity.Warning, MissingSeeAlsoText(n, listId), CreateListHint(n, new[] { n.Preset })));
        }

        return messages;
    }

    /// <summary>
    /// Warnings for every list of one taxa group, for the Taxa grouping page after "Save sub-groups".
    /// One warning per sub-group, naming every preset whose list it lacks, where <see cref="ForList"/>
    /// gives one per list (a sub-group with no cr, en, vu or ex list would otherwise get four
    /// near-identical warnings). Plus one warning when the group has sub-groups but none of its lists
    /// links any of them.
    /// </summary>
    public static IReadOnlyList<string> WarningsForGroup(
        string group, IReadOnlyCollection<string> childGroups, WikipediaListConfig config) {
        var warnings = new List<string>();
        if (childGroups.Count == 0) return warnings;

        var groupLists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var groupListIds = new HashSet<string>(groupLists.Select(l => l.Id), StringComparer.OrdinalIgnoreCase);
        if (groupListIds.Count == 0) {
            warnings.Add($"{group} has no lists in wikipedia-lists.yml, so no list links to its sub-groups.");
            return warnings;
        }

        var notes = config.ChildLinkNotes.Where(n => groupListIds.Contains(n.ParentListId)).ToList();
        // As in ForList, an unlinked sub-group is a warning only on a parent page (a list that links at
        // least one sub-group list); an ordinary list lists every sub-group's species anyway.
        var parentPages = new HashSet<string>(
            notes.Where(n => n.Kind == GroupingKind.Phylogenetic && n.LinkedListId is not null).Select(n => n.ParentListId),
            StringComparer.OrdinalIgnoreCase);

        var missing = notes
            .Where(n => n.Outcome == ChildLinkOutcome.NoList
                && (n.Kind == GroupingKind.SeeAlso || parentPages.Contains(n.ParentListId)))
            .GroupBy(n => (n.Kind, n.ChildGroup))
            .OrderBy(g => g.Key.Kind == GroupingKind.Phylogenetic ? 0 : 1);
        foreach (var byChild in missing) {
            var first = byChild.First();
            var presets = byChild.Select(n => n.Preset).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var text = (first.Kind, presets.Count) switch {
                (GroupingKind.SeeAlso, 1) => MissingSeeAlsoText(first, first.ParentListId),
                (GroupingKind.SeeAlso, _) => MissingSeeAlsoListsText(first, presets),
                (_, 1) => MissingSubListText(first, first.ParentListId),
                _ => MissingSubListsText(first, presets),
            };
            warnings.Add($"{text} {CreateListHint(first, presets)}");
        }

        foreach (var n in notes.Where(n => n.Outcome == ChildLinkOutcome.UnknownGroup).DistinctBy(n => (n.Kind, n.ChildGroup))) {
            var m = UnknownGroupMessage(n);
            warnings.Add($"{m.Text} {m.Hint}");
        }

        if (parentPages.Count == 0) {
            var presets = string.Join(", ", groupLists.Select(l => l.Preset).Where(p => p is not null).Distinct());
            warnings.Add($"No {group} list links to a sub-group list, because no sub-group of {group} has a list for any of these presets: {presets}.");
        }

        return warnings;
    }

    // "No list monocots-lc, so species in sub-group monocots (Monocotyledons) are listed on plants-lc itself."
    private static string MissingSubListText(ChildLinkNote n, string listId) =>
        $"No list {n.ChildGroup}-{n.Preset}, so species in sub-group {Describe(n)} are listed on {listId} itself.";

    // The same for several presets of one parent group.
    private static string MissingSubListsText(ChildLinkNote n, IReadOnlyList<string> presets) =>
        $"No {n.ChildGroup} lists for presets {JoinAnd(presets)}, so species in sub-group {Describe(n)} are listed on the {n.ParentGroup} lists for those presets.";

    private static string MissingSeeAlsoText(ChildLinkNote n, string listId) =>
        $"No list {n.ChildGroup}-{n.Preset} or {n.ChildGroup}-{WikipediaListDefinitionLoader.AllStatusPreset}, so {listId} has no link to {Describe(n)} under \"Related lists\".";

    private static string MissingSeeAlsoListsText(ChildLinkNote n, IReadOnlyList<string> presets) =>
        $"No list {n.ChildGroup}-{WikipediaListDefinitionLoader.AllStatusPreset} and no {n.ChildGroup} lists for presets {JoinAnd(presets)}, so the {n.ParentGroup} lists for those presets have no link to {Describe(n)} under \"Related lists\".";

    private static Message UnknownGroupMessage(ChildLinkNote n) {
        var key = n.Kind == GroupingKind.SeeAlso ? "see_also" : "children";
        return new Message(Severity.Warning,
            $"Unknown group {n.ChildGroup} in the {key}: list of the {n.ParentGroup} group in taxa-groups.yml, so {n.ChildGroup} is ignored.",
            $"Change {n.ChildGroup} to the key of an existing group in taxa-groups.yml, or remove {n.ChildGroup} from the {key}: list of the {n.ParentGroup} group.");
    }

    // "cr", "cr and ex", "cr, en, vu and ex".
    private static string JoinAnd(IReadOnlyList<string> items) =>
        items.Count <= 1 ? string.Join("", items) : $"{string.Join(", ", items.Take(items.Count - 1))} and {items[^1]}";

    // "liliopsida (Monocotyledons)", or just the group key when it has no display name.
    private static string Describe(ChildLinkNote n) =>
        string.IsNullOrWhiteSpace(n.ChildDisplayName) || string.Equals(n.ChildDisplayName, n.ChildGroup, StringComparison.OrdinalIgnoreCase)
            ? n.ChildGroup
            : $"{n.ChildGroup} ({n.ChildDisplayName})";

    // How to create the sub-group's missing list (one preset) or lists (several presets). category_split
    // overrides presets in a list entry, so an entry that uses it needs category_split replaced.
    private static string CreateListHint(ChildLinkNote n, IReadOnlyList<string> presets) {
        var child = n.ChildGroup;
        if (presets.Count == 1) {
            var preset = presets[0];
            return !n.ChildHasLists
                ? $"To create {child}-{preset}, add an entry for {child} to wikipedia-lists.yml with presets that include {preset}."
                : n.ChildUsesCategorySplit
                    ? $"To create {child}-{preset}, replace category_split in the {child} entry in wikipedia-lists.yml with presets that include {preset}."
                    : $"To create {child}-{preset}, add {preset} to the {child} entry in wikipedia-lists.yml.";
        }
        var list = JoinAnd(presets);
        return !n.ChildHasLists
            ? $"To create the {child} lists, add an entry for {child} to wikipedia-lists.yml with presets that include {list}."
            : n.ChildUsesCategorySplit
                ? $"To create the {child} lists, replace category_split in the {child} entry in wikipedia-lists.yml with presets that include {list}."
                : $"To create the {child} lists, add {list} to the {child} entry in wikipedia-lists.yml.";
    }
}
