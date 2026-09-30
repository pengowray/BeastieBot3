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
                messages.Add(new Message(Severity.Warning,
                    $"No list {n.ChildGroup}-{n.Preset}, so species in sub-group {Describe(n)} are listed on {listId} itself.",
                    CreateListHint(n)));
            }
        } else if (children.Any(n => n.Outcome != ChildLinkOutcome.UnknownGroup)) {
            var first = children[0];
            var names = string.Join(", ", children.Select(n => n.ChildGroup));
            messages.Add(new Message(Severity.Info,
                $"No sub-group of {first.ParentGroup} ({names}) has a list for preset {first.Preset}, so {listId} has no summary table and no links to sub-group lists."));
        }

        foreach (var n in notes.Where(n => n.Outcome == ChildLinkOutcome.UnknownGroup)) {
            var key = n.Kind == GroupingKind.SeeAlso ? "see_also" : "children";
            messages.Add(new Message(Severity.Warning,
                $"Unknown sub-group {n.ChildGroup} in the {key} of {n.ParentGroup}: taxa-groups.yml has no group {n.ChildGroup}.",
                $"To fix it, correct or remove {n.ChildGroup} in the {key} of {n.ParentGroup} in taxa-groups.yml."));
        }

        foreach (var n in notes.Where(n => n.Kind == GroupingKind.SeeAlso && n.Outcome == ChildLinkOutcome.NoList)) {
            messages.Add(new Message(Severity.Warning,
                $"No list {n.ChildGroup}-{n.Preset} or {n.ChildGroup}-{WikipediaListDefinitionLoader.AllStatusPreset}, so {listId} has no link to {Describe(n)} under \"Related lists\".",
                CreateListHint(n)));
        }

        return messages;
    }

    /// <summary>
    /// Warnings for every list of one taxa group, for the Taxa grouping page after "Save sub-groups":
    /// the per-list warnings (duplicates removed), plus one warning when the group has sub-groups but
    /// none of its lists links any of them.
    /// </summary>
    public static IReadOnlyList<string> WarningsForGroup(
        string group, IReadOnlyCollection<string> childGroups, WikipediaListConfig config) {
        var warnings = new List<string>();
        if (childGroups.Count == 0) return warnings;

        var groupLists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var groupListIds = groupLists.Select(l => l.Id).ToList();
        if (groupListIds.Count == 0) {
            warnings.Add($"{group} has no lists in wikipedia-lists.yml, so no list links to its sub-groups.");
            return warnings;
        }

        foreach (var id in groupListIds) {
            foreach (var m in ForList(id, config.ChildLinkNotes).Where(m => m.Severity == Severity.Warning)) {
                var text = m.Hint is null ? m.Text : $"{m.Text} {m.Hint}";
                if (!warnings.Contains(text)) warnings.Add(text);
            }
        }

        var anyLinked = config.ChildLinkNotes.Any(n =>
            n.Kind == GroupingKind.Phylogenetic
            && n.LinkedListId is not null
            && groupListIds.Contains(n.ParentListId, StringComparer.OrdinalIgnoreCase));
        if (!anyLinked) {
            var presets = string.Join(", ", groupLists.Select(l => l.Preset).Where(p => p is not null).Distinct());
            warnings.Add($"No {group} list links to a sub-group list, because no sub-group of {group} has a list for any of these presets: {presets}.");
        }

        return warnings;
    }

    // "liliopsida (Monocotyledons)", or just the group key when it has no display name.
    private static string Describe(ChildLinkNote n) =>
        string.IsNullOrWhiteSpace(n.ChildDisplayName) || string.Equals(n.ChildDisplayName, n.ChildGroup, StringComparison.OrdinalIgnoreCase)
            ? n.ChildGroup
            : $"{n.ChildGroup} ({n.ChildDisplayName})";

    // category_split overrides presets in a list entry, so an entry that uses it needs category_split replaced.
    private static string CreateListHint(ChildLinkNote n) =>
        !n.ChildHasLists
            ? $"To create {n.ChildGroup}-{n.Preset}, add an entry for {n.ChildGroup} to wikipedia-lists.yml."
        : n.ChildUsesCategorySplit
            ? $"To create {n.ChildGroup}-{n.Preset}, replace category_split in the {n.ChildGroup} entry in wikipedia-lists.yml with presets that include {n.Preset}."
            : $"To create {n.ChildGroup}-{n.Preset}, add {n.Preset} to the {n.ChildGroup} entry in wikipedia-lists.yml.";
}
