using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.WikipediaLists;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace BeastieBot3.Web.Endpoints;

// Taxa-grouping config + live per-child IUCN counts, to help the user decide which taxa to promote
// into sub-groups of a parent list. Reads the DRAFT taxa-groups.yml; counts come from the SAME
// IucnChartDataBuilder.BuildChildBreakdown that backs the parent summary table, so the web numbers
// match the generated lists and the {{IUCN ... chart}}.
//
//   GET  /api/grouping/groups                                  -> parsed groups (name/filters/children)
//   GET  /api/grouping/children-counts?group=&childRank=class  -> per-child EX..DD breakdown
//   POST /api/grouping/children {group, children[]}            -> rewrite ONLY the children: block (draft)

public static class TaxaGroupingEndpoints {
    private static readonly JsonSerializerOptions JsonOpts =
        new(JsonSerializerDefaults.Web) { WriteIndented = false };

    public static void MapTaxaGroupingEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/grouping/groups", (PathsService paths) => {
            var groups = LoadDraftGroups(paths, out _);
            var result = groups.Select(kv => new {
                name = kv.Key,
                displayName = kv.Value.Name ?? kv.Key,
                adjective = kv.Value.Adjective,
                filters = (kv.Value.Filters ?? new()).Select(DescribeFilter),
                children = kv.Value.Children ?? new(),
                seeAlso = kv.Value.SeeAlso ?? new(),
                isParent = (kv.Value.Children?.Count ?? 0) > 0,
            });
            return Results.Json(new { groups = result }, JsonOpts);
        });

        app.MapGet("/api/grouping/children-counts", (string group, string? childRank, PathsService paths) => {
            var groups = LoadDraftGroups(paths, out _);
            if (!groups.TryGetValue(group, out var def))
                return Results.NotFound(new { error = $"Unknown group '{group}'" });

            var rank = string.IsNullOrWhiteSpace(childRank) ? "class" : childRank.Trim().ToLowerInvariant();
            var entries = ChartStatusOrder.Entries;

            // Which child rank values already correspond to an existing taxa-group (so the UI can mark
            // them and offer a "child" checkbox).
            var valueToGroup = BuildValueToGroupMap(groups, rank);

            var dbPath = paths.ResolveIucnDatabasePath(null);
            using var chart = new IucnChartDataBuilder(dbPath);
            var breakdown = chart.BuildChildBreakdown(def.Filters, rank);

            var rows = breakdown
                .Select(kv => new {
                    key = kv.Key,
                    existingGroup = valueToGroup.GetValueOrDefault(kv.Key),
                    isChild = def.Children?.Contains(valueToGroup.GetValueOrDefault(kv.Key) ?? "\0") == true,
                    counts = kv.Value.Select(c => c.Count).ToArray(),
                    total = kv.Value.Sum(c => c.Count),
                })
                .OrderByDescending(r => r.total)
                .ToList();

            return Results.Json(new {
                group,
                childRank = rank,
                columns = entries.Select(e => e.Code).ToArray(),
                rows,
                grandTotal = rows.Sum(r => r.total),
            }, JsonOpts);
        });

        app.MapPost("/api/grouping/children", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<ChildrenRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Group))
                return Results.BadRequest(new { error = "group is required" });

            var loc = RulesPaths.Resolve(paths);
            EnsureSeeded(loc);
            var draftFile = Path.Combine(loc.DraftRoot, "taxa-groups.yml");
            if (!File.Exists(draftFile))
                return Results.NotFound(new { error = "draft taxa-groups.yml not found" });

            var children = (req.Children ?? Array.Empty<string>())
                .Select(c => c.Trim()).Where(c => c.Length > 0).ToList();

            var original = File.ReadAllText(draftFile);
            if (!TryRewriteChildrenBlock(original, req.Group!, children, out var updated, out var err))
                return Results.BadRequest(new { error = err, hint = "Edit children: directly in the rules editor textarea instead." });

            // Round-trip assertion: the result must still parse AND yield exactly the requested children
            // for this group, with all other groups intact. Never write a file we can't re-read.
            if (!RoundTripOk(original, updated, req.Group!, children, out var rtErr))
                return Results.BadRequest(new { error = rtErr, hint = "Edit children: directly in the rules editor textarea instead." });

            File.WriteAllText(draftFile, updated);
            return Results.Json(new { group = req.Group, children, file = "taxa-groups.yml" }, JsonOpts);
        });

        // Set the per-group tuning knobs in the DRAFT rules: size_budget.max_entries on the group in
        // taxa-groups.yml, and/or category_split on the group's list entry in wikipedia-lists.yml. Each
        // edit is a conservative line rewrite guarded by a round-trip parse — never writes an unreadable
        // file. The user reviews via the Rules-editor diff and Applies to source.
        app.MapPost("/api/grouping/knobs", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<KnobsRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Group))
                return Results.BadRequest(new { error = "group is required" });

            var loc = RulesPaths.Resolve(paths);
            EnsureSeeded(loc);
            var changed = new List<string>();

            if (req.SizeBudgetMaxEntries is { } maxEntries) {
                if (maxEntries < 0)
                    return Results.BadRequest(new { error = "sizeBudgetMaxEntries must be >= 0" });
                var file = Path.Combine(loc.DraftRoot, "taxa-groups.yml");
                if (!File.Exists(file))
                    return Results.NotFound(new { error = "draft taxa-groups.yml not found" });
                var original = File.ReadAllText(file);
                if (!TryReplaceGroupKeyFlow(original, req.Group!, "size_budget", $"{{ max_entries: {maxEntries} }}", out var updated, out var err))
                    return Results.BadRequest(new { error = err, hint = "Edit size_budget directly in the rules editor textarea instead." });
                if (!GroupBudgetRoundTripOk(updated, req.Group!, maxEntries, out var rtErr))
                    return Results.BadRequest(new { error = rtErr, hint = "Edit size_budget directly in the rules editor textarea instead." });
                File.WriteAllText(file, updated);
                changed.Add("taxa-groups.yml");
            }

            if (!string.IsNullOrWhiteSpace(req.CategorySplit)) {
                var split = req.CategorySplit!.Trim();
                var allowed = new[] { "default", "separate", "combined-threatened", "all-status" };
                if (!allowed.Contains(split))
                    return Results.BadRequest(new { error = $"categorySplit must be one of: {string.Join(", ", allowed)}" });
                var file = Path.Combine(loc.DraftRoot, "wikipedia-lists.yml");
                if (!File.Exists(file))
                    return Results.NotFound(new { error = "draft wikipedia-lists.yml not found" });
                var original = File.ReadAllText(file);
                // "default" means remove the override (fall back to the entry's explicit presets).
                var value = split == "default" ? null : split;
                if (!TrySetListCategorySplit(original, req.Group!, value, out var updated, out var err))
                    return Results.BadRequest(new { error = err, hint = "Edit category_split directly in the rules editor textarea instead." });
                if (!YamlStillParses(updated, out var rtErr))
                    return Results.BadRequest(new { error = rtErr, hint = "Edit category_split directly in the rules editor textarea instead." });
                File.WriteAllText(file, updated);
                changed.Add("wikipedia-lists.yml");
            }

            if (changed.Count == 0)
                return Results.BadRequest(new { error = "Provide sizeBudgetMaxEntries and/or categorySplit." });
            return Results.Json(new { group = req.Group, changed }, JsonOpts);
        });
    }

    // ---- group loading ----

    private static Dictionary<string, TaxaGroupDefinition> LoadDraftGroups(PathsService paths, out string path) {
        var loc = RulesPaths.Resolve(paths);
        EnsureSeeded(loc);
        path = Path.Combine(loc.DraftRoot, "taxa-groups.yml");
        if (!File.Exists(path)) return new();
        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
        using var reader = File.OpenText(path);
        var file = deserializer.Deserialize<TaxaGroupsFile>(reader);
        return file?.Groups ?? new();
    }

    private static void EnsureSeeded(RulesLocations loc) {
        if (Directory.Exists(loc.DraftRoot) && Directory.EnumerateFiles(loc.DraftRoot, "*", SearchOption.AllDirectories).Any())
            return;
        // Seed via the editor endpoints' logic by copying source files (read-only path here).
        if (!Directory.Exists(loc.SourceRulesDir)) return;
        foreach (var src in Directory.EnumerateFiles(loc.SourceRulesDir, "*", SearchOption.AllDirectories)) {
            var rel = Path.GetRelativePath(loc.SourceRulesDir, src);
            var dst = Path.Combine(loc.DraftRoot, rel);
            Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
            if (!File.Exists(dst)) File.Copy(src, dst);
        }
    }

    private static object DescribeFilter(TaxonFilterDefinition f) => new {
        rank = string.IsNullOrWhiteSpace(f.Rank) ? null : f.Rank,
        value = string.IsNullOrWhiteSpace(f.Value) ? null : f.Value,
        values = f.Values,
        exclude = f.Exclude,
        system = f.System,
    };

    // Map a normalized child-rank value (e.g. "INSECTA") -> the taxa-group whose single-value filter
    // at that rank matches it (e.g. "insects").
    private static Dictionary<string, string> BuildValueToGroupMap(Dictionary<string, TaxaGroupDefinition> groups, string rank) {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, def) in groups) {
            foreach (var f in def.Filters ?? new()) {
                if (!string.Equals(f.Rank?.Trim(), rank, StringComparison.OrdinalIgnoreCase)) continue;
                if (string.IsNullOrWhiteSpace(f.Value)) continue;
                var norm = TaxonFilterSql.NormalizeValue(rank, f.Value);
                if (norm != null && !map.ContainsKey(norm)) map[norm] = name;
            }
        }
        return map;
    }

    // ---- children: block rewrite (conservative, flow-style) ----

    // Rewrites just the `children:` line of a group to flow style `children: [a, b, c]`, inserting it
    // if absent. Keys on the group header line + indentation; leaves every other line untouched.
    private static bool TryRewriteChildrenBlock(string yaml, string group, List<string> children, out string updated, out string error) {
        updated = yaml;
        error = "";
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        var lines = yaml.Replace("\r\n", "\n").Split('\n').ToList();

        // Find the group header: a line like "  <group>:" at 2-space indent (under groups:).
        var headerIdx = -1;
        var headerIndent = 0;
        for (var i = 0; i < lines.Count; i++) {
            var m = System.Text.RegularExpressions.Regex.Match(lines[i], $"^(\\s+){System.Text.RegularExpressions.Regex.Escape(group)}:\\s*$");
            if (m.Success) { headerIdx = i; headerIndent = m.Groups[1].Value.Length; break; }
        }
        if (headerIdx < 0) { error = $"Group '{group}' not found in taxa-groups.yml."; return false; }

        var childIndent = new string(' ', headerIndent + 2);
        var flow = $"{childIndent}children: [{string.Join(", ", children)}]";

        // Scan the group's block (lines indented deeper than the header) for an existing children: key.
        var blockEnd = lines.Count;
        for (var i = headerIdx + 1; i < lines.Count; i++) {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line)) continue;
            var indent = line.Length - line.TrimStart().Length;
            if (indent <= headerIndent) { blockEnd = i; break; }
        }

        var childIdx = -1;
        for (var i = headerIdx + 1; i < blockEnd; i++) {
            if (System.Text.RegularExpressions.Regex.IsMatch(lines[i], $"^{childIndent}children:")) { childIdx = i; break; }
        }

        if (childIdx >= 0) {
            // Remove any block-style list items that follow on subsequent deeper-indented "- " lines.
            var removeTo = childIdx;
            for (var i = childIdx + 1; i < blockEnd; i++) {
                if (System.Text.RegularExpressions.Regex.IsMatch(lines[i], $"^{childIndent}\\s+-\\s")) removeTo = i; else break;
            }
            lines.RemoveRange(childIdx, removeTo - childIdx + 1);
            if (children.Count > 0) lines.Insert(childIdx, flow);
        } else if (children.Count > 0) {
            lines.Insert(headerIdx + 1, flow);
        }

        updated = string.Join(nl, lines);
        return true;
    }

    private static bool RoundTripOk(string original, string updated, string group, List<string> expectedChildren, out string error) {
        error = "";
        try {
            var deserializer = new DeserializerBuilder()
                .IgnoreUnmatchedProperties()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var beforeFile = deserializer.Deserialize<TaxaGroupsFile>(original);
            var afterFile = deserializer.Deserialize<TaxaGroupsFile>(updated);
            if (afterFile?.Groups is null) { error = "Rewritten YAML did not parse."; return false; }

            // The target group's children must equal the request.
            if (!afterFile.Groups.TryGetValue(group, out var g)) { error = $"Group '{group}' missing after rewrite."; return false; }
            var got = g.Children ?? new();
            if (!got.SequenceEqual(expectedChildren)) {
                error = $"Children mismatch after rewrite (got [{string.Join(", ", got)}]).";
                return false;
            }
            // Every OTHER group must be byte-stable in its parsed shape (no collateral edits): compare
            // the set of group names and each other group's children/filters count.
            var before = beforeFile?.Groups ?? new();
            foreach (var (name, bdef) in before) {
                if (name == group) continue;
                if (!afterFile.Groups.TryGetValue(name, out var adef)) { error = $"Group '{name}' lost during rewrite."; return false; }
                if ((bdef.Children?.Count ?? 0) != (adef.Children?.Count ?? 0)) { error = $"Group '{name}' children changed unexpectedly."; return false; }
                if ((bdef.Filters?.Count ?? 0) != (adef.Filters?.Count ?? 0)) { error = $"Group '{name}' filters changed unexpectedly."; return false; }
            }
            return true;
        } catch (Exception ex) {
            error = $"Round-trip parse failed: {ex.Message}";
            return false;
        }
    }

    // ---- knob rewrites (size_budget, category_split) ----

    // Replaces (or inserts) a single key on a group in taxa-groups.yml with a flow-style value line,
    // removing any prior block-style nesting. Mirrors the children rewriter; leaves all else untouched.
    internal static bool TryReplaceGroupKeyFlow(string yaml, string group, string key, string flowValue, out string updated, out string error) {
        updated = yaml;
        error = "";
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        var lines = yaml.Replace("\r\n", "\n").Split('\n').ToList();

        var headerIdx = -1;
        var headerIndent = 0;
        for (var i = 0; i < lines.Count; i++) {
            var m = Regex.Match(lines[i], $"^(\\s+){Regex.Escape(group)}:\\s*$");
            if (m.Success) { headerIdx = i; headerIndent = m.Groups[1].Value.Length; break; }
        }
        if (headerIdx < 0) { error = $"Group '{group}' not found in taxa-groups.yml."; return false; }

        var indent = new string(' ', headerIndent + 2);
        var flow = $"{indent}{key}: {flowValue}";

        var blockEnd = lines.Count;
        for (var i = headerIdx + 1; i < lines.Count; i++) {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;
            var ind = lines[i].Length - lines[i].TrimStart().Length;
            if (ind <= headerIndent) { blockEnd = i; break; }
        }

        var keyIdx = -1;
        for (var i = headerIdx + 1; i < blockEnd; i++) {
            if (Regex.IsMatch(lines[i], $"^{indent}{Regex.Escape(key)}:")) { keyIdx = i; break; }
        }

        if (keyIdx >= 0) {
            // Remove the key line plus any deeper-indented nested block (e.g. a block-style max_entries).
            var removeTo = keyIdx;
            for (var i = keyIdx + 1; i < blockEnd; i++) {
                if (string.IsNullOrWhiteSpace(lines[i])) break;
                var ind = lines[i].Length - lines[i].TrimStart().Length;
                if (ind > headerIndent + 2) removeTo = i; else break;
            }
            lines.RemoveRange(keyIdx, removeTo - keyIdx + 1);
            lines.Insert(keyIdx, flow);
        } else {
            lines.Insert(headerIdx + 1, flow);
        }

        updated = string.Join(nl, lines);
        return true;
    }

    // Sets (or, when value is null, removes) category_split on the `- taxa_group: <group>` list entry
    // in wikipedia-lists.yml. Keys on the entry's `- taxa_group:` line + sibling indent.
    internal static bool TrySetListCategorySplit(string yaml, string group, string? value, out string updated, out string error) {
        updated = yaml;
        error = "";
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        var lines = yaml.Replace("\r\n", "\n").Split('\n').ToList();

        var itemIdx = -1;
        var itemIndent = 0;
        for (var i = 0; i < lines.Count; i++) {
            var m = Regex.Match(lines[i], $"^(\\s*)-\\s+taxa_group:\\s*{Regex.Escape(group)}\\s*$");
            if (m.Success) { itemIdx = i; itemIndent = m.Groups[1].Value.Length; break; }
        }
        if (itemIdx < 0) { error = $"List entry for taxa_group '{group}' not found in wikipedia-lists.yml."; return false; }

        var keyIndent = new string(' ', itemIndent + 2); // sibling keys align under `taxa_group`
        var blockEnd = lines.Count;
        for (var i = itemIdx + 1; i < lines.Count; i++) {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;
            var ind = lines[i].Length - lines[i].TrimStart().Length;
            if (ind <= itemIndent) { blockEnd = i; break; }
        }

        var csIdx = -1;
        for (var i = itemIdx + 1; i < blockEnd; i++) {
            if (Regex.IsMatch(lines[i], $"^{keyIndent}category_split:")) { csIdx = i; break; }
        }

        var remove = string.IsNullOrWhiteSpace(value);
        if (csIdx >= 0) {
            if (remove) lines.RemoveAt(csIdx);
            else lines[csIdx] = $"{keyIndent}category_split: {value}";
        } else if (!remove) {
            lines.Insert(itemIdx + 1, $"{keyIndent}category_split: {value}");
        }

        updated = string.Join(nl, lines);
        return true;
    }

    // Round-trip: the rewritten taxa-groups.yml must re-parse and yield exactly the requested budget.
    private static bool GroupBudgetRoundTripOk(string updated, string group, int maxEntries, out string error) {
        error = "";
        try {
            var deserializer = new DeserializerBuilder()
                .IgnoreUnmatchedProperties()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var file = deserializer.Deserialize<TaxaGroupsFile>(updated);
            if (file?.Groups is null || !file.Groups.TryGetValue(group, out var def)) {
                error = $"Group '{group}' missing after rewrite.";
                return false;
            }
            if (def.SizeBudget?.MaxEntries != maxEntries) {
                error = $"size_budget.max_entries mismatch after rewrite (got {def.SizeBudget?.MaxEntries?.ToString() ?? "null"}).";
                return false;
            }
            return true;
        } catch (Exception ex) {
            error = $"Round-trip parse failed: {ex.Message}";
            return false;
        }
    }

    // Round-trip: the rewritten YAML must still parse as a document (catches indentation breakage).
    private static bool YamlStillParses(string updated, out string error) {
        error = "";
        try {
            var deserializer = new DeserializerBuilder().IgnoreUnmatchedProperties().Build();
            deserializer.Deserialize<object>(updated);
            return true;
        } catch (Exception ex) {
            error = $"Rewritten YAML did not parse: {ex.Message}";
            return false;
        }
    }

    private sealed class ChildrenRequest {
        public string? Group { get; set; }
        public string[]? Children { get; set; }
    }

    private sealed class KnobsRequest {
        public string? Group { get; set; }
        public int? SizeBudgetMaxEntries { get; set; }
        public string? CategorySplit { get; set; }
    }
}
