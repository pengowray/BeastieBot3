using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

// Expands a taxa group's `sub_groups:` block (taxa-groups.yml) into ordinary taxa groups, so a parent
// split into many orders takes one line per order instead of a full group block, a children: entry and
// a wikipedia-lists.yml entry each:
//
//   magnoliopsida:
//     ...
//     sub_groups:
//       rank: order
//       presets: [threatened, lc]
//       keep_name_case: true
//       groups:
//         Malpighiales: {}
//         Fabales: {}
//
//   ray-finned-fishes:
//     ...
//     sub_groups:
//       rank: order
//       presets: [lc, dd]
//       groups:
//         Anguilliformes: { name: "Eels", adjective: "eel" }
//
// Each entry becomes the group "<value in lower case>" (e.g. malpighiales) with the parent's filters
// plus "<rank>: <value>", the parent's display, categories and size_budget, and a list entry for each
// preset, placed right after the parent's own entry in wikipedia-lists.yml. Its name defaults to the
// value and its adjective to the name. The ids are appended to the parent's children.
//
// The loader and the Taxa grouping page both read groups through Expand, so both see the same groups.

namespace BeastieBot3.WikipediaLists;

/// <summary>A parent group's <c>sub_groups:</c> block.</summary>
internal sealed class SubGroupsDefinition {
    /// <summary>Rank the sub-groups are split at, e.g. "order".</summary>
    public string? Rank { get; init; }

    /// <summary>Presets each sub-group gets a list for. All siblings share them, so every parent page
    /// for one of these presets links every sub-group.</summary>
    public List<string>? Presets { get; init; }

    /// <summary>Keep the capital letters of the sub-group names in titles and file names (scientific
    /// names such as "Malpighiales").</summary>
    public bool KeepNameCase { get; init; }

    /// <summary>Rank value → optional name and adjective. Insertion order is the order of the parent's
    /// sections and of the generated lists.</summary>
    public Dictionary<string, SubGroupEntry?>? Groups { get; init; }
}

internal sealed class SubGroupEntry {
    public string? Name { get; init; }
    public string? Adjective { get; init; }
}

/// <summary>A list entry generated for a sub-group, placed after <paramref name="ParentGroup"/>'s entry.</summary>
internal sealed record GeneratedListEntry(string ParentGroup, string Group, IReadOnlyList<string> Presets);

internal sealed record ExpandedTaxaGroups(
    Dictionary<string, TaxaGroupDefinition> Groups,
    IReadOnlyList<GeneratedListEntry> Lists,
    // Generated group id → the parent whose sub_groups block defines it.
    IReadOnlyDictionary<string, string> GeneratedBy,
    IReadOnlyList<string> Warnings);

internal static class TaxaSubGroups {
    public static string IdFor(string value) =>
        Regex.Replace(value.Trim().ToLowerInvariant(), "[^a-z0-9]+", "-").Trim('-');

    public static ExpandedTaxaGroups Expand(Dictionary<string, TaxaGroupDefinition> groups) {
        var result = new Dictionary<string, TaxaGroupDefinition>(StringComparer.Ordinal);
        var lists = new List<GeneratedListEntry>();
        var generatedBy = new Dictionary<string, string>(StringComparer.Ordinal);
        var warnings = new List<string>();

        foreach (var (key, group) in groups) {
            result[key] = group;
            var block = group.SubGroups;
            if (block?.Groups is not { Count: > 0 }) {
                continue;
            }
            if (string.IsNullOrWhiteSpace(block.Rank) || TaxonFilterSql.ResolveColumn(block.Rank) is null) {
                warnings.Add($"sub_groups of '{key}' has no valid rank (use class, order, family or genus); its sub-groups were skipped.");
                continue;
            }

            var children = group.Children ?? new List<string>();
            foreach (var (value, entry) in block.Groups) {
                var id = IdFor(value);
                if (id.Length == 0) {
                    continue;
                }
                if (groups.ContainsKey(id) || generatedBy.ContainsKey(id)) {
                    warnings.Add($"sub_groups of '{key}': '{value}' would be group '{id}', which already exists; skipped.");
                    continue;
                }
                var name = string.IsNullOrWhiteSpace(entry?.Name) ? value.Trim() : entry!.Name!;
                result[id] = new TaxaGroupDefinition {
                    Name = name,
                    KeepNameCase = block.KeepNameCase,
                    Adjective = string.IsNullOrWhiteSpace(entry?.Adjective) ? name : entry!.Adjective,
                    Filters = (group.Filters ?? new()).Append(new TaxonFilterDefinition { Rank = block.Rank.Trim(), Value = value.Trim() }).ToList(),
                    Display = group.Display,
                    Categories = group.Categories,
                    SizeBudget = group.SizeBudget,
                };
                generatedBy[id] = key;
                if (!children.Contains(id, StringComparer.Ordinal)) {
                    children.Add(id);
                }
                if (block.Presets is { Count: > 0 }) {
                    lists.Add(new GeneratedListEntry(key, id, block.Presets));
                }
            }
            group.Children = children;
        }

        return new ExpandedTaxaGroups(result, lists, generatedBy, warnings);
    }

    /// <summary>
    /// The list entries with each generated entry placed after its parent's entry (after the last
    /// entry already placed for that parent), or at the end when the parent has no entry.
    /// </summary>
    public static List<WikipediaListDefinitionRaw> InsertListEntries(
        IReadOnlyList<WikipediaListDefinitionRaw> lists, IReadOnlyList<GeneratedListEntry> generated) {
        var byParent = generated.GroupBy(g => g.ParentGroup, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
        var result = new List<WikipediaListDefinitionRaw>();
        foreach (var list in lists) {
            result.Add(list);
            if (list.TaxaGroup is { } parent && byParent.Remove(parent, out var entries)) {
                result.AddRange(entries.Select(ToRaw));
            }
        }
        foreach (var entries in byParent.Values) {
            result.AddRange(entries.Select(ToRaw));
        }
        return result;

        static WikipediaListDefinitionRaw ToRaw(GeneratedListEntry e) =>
            new() { TaxaGroup = e.Group, Presets = e.Presets.ToList() };
    }
}
