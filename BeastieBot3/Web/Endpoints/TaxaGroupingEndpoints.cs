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
                var allowed = new[] { "default", "separate", "combined-threatened", "merged", "all-status" };
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

        // Create a brand-new taxa-group from a single taxonomic rank+value (e.g. a class like
        // MAGNOLIOPSIDA), optionally inheriting the parent group's kingdom filter, and wire it up with
        // a list entry so it actually generates pages. Two conservative draft writes (taxa-groups.yml
        // group block + wikipedia-lists.yml list entry), each guarded by a round-trip parse — never
        // writes a file it can't re-read. The user reviews via the Rules-editor diff and Applies.
        app.MapPost("/api/grouping/create-group", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<CreateGroupRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null) return Results.BadRequest(new { error = "request body required" });

            var key = (req.Key ?? "").Trim();
            if (!Regex.IsMatch(key, "^[a-z0-9][a-z0-9-]*$"))
                return Results.BadRequest(new { error = "key must be a lowercase slug (a-z, 0-9, hyphen), e.g. 'magnoliopsida'" });
            var name = (req.Name ?? "").Trim();
            if (name.Length == 0)
                return Results.BadRequest(new { error = "name is required" });

            var listingStyle = string.IsNullOrWhiteSpace(req.ListingStyle) ? null : req.ListingStyle!.Trim();
            var allowedStyles = new[] { "CommonNameFocus", "ScientificNameFocus", "CommonNameOnly" };
            if (listingStyle != null && !allowedStyles.Contains(listingStyle))
                return Results.BadRequest(new { error = $"listingStyle must be one of: {string.Join(", ", allowedStyles)}" });

            // Page plan: an explicit presets array wins; otherwise a category_split bundle; default 'merged'.
            var presets = (req.Presets ?? Array.Empty<string>())
                .Select(p => p.Trim()).Where(p => p.Length > 0).ToList();
            var split = string.IsNullOrWhiteSpace(req.CategorySplit) ? null : req.CategorySplit!.Trim().ToLowerInvariant();
            var allowedSplits = new[] { "separate", "combined-threatened", "merged", "all-status" };
            if (split != null && !allowedSplits.Contains(split))
                return Results.BadRequest(new { error = $"categorySplit must be one of: {string.Join(", ", allowedSplits)}" });
            if (presets.Count == 0 && split == null) split = "merged"; // sensible default → actually generates pages

            var loc = RulesPaths.Resolve(paths);
            EnsureSeeded(loc);
            var groupsFile = Path.Combine(loc.DraftRoot, "taxa-groups.yml");
            var listsFile = Path.Combine(loc.DraftRoot, "wikipedia-lists.yml");
            if (!File.Exists(groupsFile))
                return Results.NotFound(new { error = "draft taxa-groups.yml not found" });

            var existing = LoadDraftGroups(paths, out _);
            if (existing.ContainsKey(key))
                return Results.Conflict(new { error = $"Group '{key}' already exists. Pick a different key, or edit it in the rules editor." });

            // Build the user's filter list (multi-filter: rank value/values/exclude, or system/systems).
            if (!TryBuildUserFilters(req, out var userFilters, out var fErr))
                return Results.BadRequest(new { error = fErr });

            // Inherit the parent's taxonomic (non-system) filters the user didn't re-specify — so a sub-
            // group of mammals picks up kingdom Animalia + class Mammalia, and the user adds only the
            // distinguishing filter (e.g. a system tag for aquatic mammals, or an order for bats).
            var filters = new List<FilterSpec>();
            if (req.InheritFilters != false && !string.IsNullOrWhiteSpace(req.ParentGroup)
                && existing.TryGetValue(req.ParentGroup!.Trim(), out var parent) && parent.Filters is { Count: > 0 }) {
                var userRanks = userFilters
                    .Where(f => f.Rank != null)
                    .Select(f => f.Rank!.ToLowerInvariant()).ToHashSet();
                foreach (var pf in parent.Filters) {
                    if (!string.IsNullOrWhiteSpace(pf.System) || pf.Systems is { Count: > 0 }) continue; // skip system filters
                    if (string.IsNullOrWhiteSpace(pf.Rank)) continue;
                    if (userRanks.Contains(pf.Rank.Trim().ToLowerInvariant())) continue; // user overrides this rank
                    filters.Add(new FilterSpec(pf.Rank.Trim(),
                        string.IsNullOrWhiteSpace(pf.Value) ? null : pf.Value.Trim(),
                        pf.Values, pf.Exclude, null));
                }
            }
            filters.AddRange(userFilters);
            if (filters.Count == 0)
                return Results.BadRequest(new { error = "At least one filter is required (a rank+value, or a system tag)." });

            var groupsOriginal = File.ReadAllText(groupsFile);
            var filtersYaml = RenderFiltersYaml(filters, groupsOriginal.Contains("\r\n") ? "\r\n" : "\n");
            var block = BuildGroupBlock(groupsOriginal, key, name, req.Adjective, listingStyle, filtersYaml);
            if (!TryAppendGroupBlock(groupsOriginal, block, out var groupsUpdated, out var gErr))
                return Results.BadRequest(new { error = gErr, hint = "Add the group directly in the rules editor textarea instead." });
            if (!NewGroupRoundTripOk(groupsUpdated, key, name, filters.Count, existing.Count + 1, out var gRt))
                return Results.BadRequest(new { error = gRt, hint = "Add the group directly in the rules editor textarea instead." });
            File.WriteAllText(groupsFile, groupsUpdated);
            var changedFiles = new List<string> { "taxa-groups.yml" };

            // Wire a list entry so the new group actually fans out to pages.
            if (File.Exists(listsFile)) {
                var listsOriginal = File.ReadAllText(listsFile);
                if (!ListEntryExists(listsOriginal, key)
                    && TryAppendTaxaGroupListEntry(listsOriginal, key, split, presets, out var listsUpdated, out _)
                    && ListEntryRoundTripOk(listsUpdated, key, split, presets)) {
                    File.WriteAllText(listsFile, listsUpdated);
                    changedFiles.Add("wikipedia-lists.yml");
                }
            }

            return Results.Json(new {
                group = key,
                changed = changedFiles,
                pagePlan = split ?? string.Join(", ", presets),
                hint = "Review the diff in the Rules editor → Apply to source. To break this out from a parent list, "
                     + "select the parent + this rank above, Show counts, tick it, and Save sub-groups.",
            }, JsonOpts);
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
        systems = f.Systems,
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

    // ---- create-group: new group block + list entry (conservative append + round-trip guard) ----

    // A single parsed filter to emit: a rank filter (value / values OR / exclude NOT-IN) OR a system
    // filter (one or more tags, OR'd). Systems non-empty => system filter; otherwise rank filter.
    internal sealed record FilterSpec(
        string? Rank, string? Value, IReadOnlyList<string>? Values, IReadOnlyList<string>? Exclude, IReadOnlyList<string>? Systems);

    // Render the YAML lines that go under `filters:` for a list of FilterSpecs, matching the
    // hand-authored block style (each filter as a `- rank:`/`- system:` item at 6-space indent).
    internal static string RenderFiltersYaml(IReadOnlyList<FilterSpec> filters, string nl) {
        var sb = new StringBuilder();
        foreach (var f in filters) {
            if (f.Systems is { Count: > 0 }) {
                if (f.Systems.Count == 1)
                    sb.Append("      - system: ").Append(f.Systems[0]).Append(nl);
                else
                    sb.Append("      - systems: [").Append(string.Join(", ", f.Systems)).Append(']').Append(nl);
                continue;
            }
            sb.Append("      - rank: ").Append(f.Rank).Append(nl);
            if (f.Values is { Count: > 0 })
                sb.Append("        values: [").Append(string.Join(", ", f.Values)).Append(']').Append(nl);
            else if (!string.IsNullOrWhiteSpace(f.Value))
                sb.Append("        value: ").Append(f.Value).Append(nl);
            if (f.Exclude is { Count: > 0 })
                sb.Append("        exclude: [").Append(string.Join(", ", f.Exclude)).Append(']').Append(nl);
        }
        return sb.ToString();
    }

    // Build a YAML group block matching the hand-authored style (see conifers/cycads): a 2-space
    // group key, double-quoted name/adjective, optional display.listing_style, then a pre-rendered
    // `filters:` body. Leads with a blank line for separation.
    internal static string BuildGroupBlock(
        string yaml, string key, string name, string? adjective, string? listingStyle, string filtersYaml) {
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        string Q(string s) => "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
        var sb = new StringBuilder();
        sb.Append(nl);
        sb.Append("  ").Append(key).Append(':').Append(nl);
        sb.Append("    name: ").Append(Q(name)).Append(nl);
        if (!string.IsNullOrWhiteSpace(adjective))
            sb.Append("    adjective: ").Append(Q(adjective!.Trim())).Append(nl);
        if (!string.IsNullOrWhiteSpace(listingStyle)) {
            sb.Append("    display:").Append(nl);
            sb.Append("      listing_style: ").Append(listingStyle).Append(nl);
        }
        sb.Append("    filters:").Append(nl);
        sb.Append(filtersYaml);
        return sb.ToString();
    }

    // Parse + validate the request's user-supplied filters (or the legacy rank/value shorthand) into
    // FilterSpecs. Each row is a rank filter (value/values/exclude) XOR a system filter (one+ tags).
    private static bool TryBuildUserFilters(CreateGroupRequest req, out List<FilterSpec> filters, out string error) {
        filters = new();
        error = "";
        var raw = req.Filters;
        if ((raw == null || raw.Length == 0) && !string.IsNullOrWhiteSpace(req.Rank) && !string.IsNullOrWhiteSpace(req.Value))
            raw = new[] { new FilterDto { Rank = req.Rank, Value = req.Value } }; // legacy single-filter shorthand
        if (raw == null) return true; // inheritance may still supply filters; final count is checked by the caller

        foreach (var f in raw) {
            var systems = (f.Systems ?? Array.Empty<string>())
                .Concat(string.IsNullOrWhiteSpace(f.System) ? Array.Empty<string>() : new[] { f.System! })
                .Select(s => s.Trim()).Where(s => s.Length > 0).Distinct().ToList();
            var rank = string.IsNullOrWhiteSpace(f.Rank) ? null : f.Rank!.Trim().ToLowerInvariant();

            if (systems.Count > 0 && rank != null) {
                error = "A filter row is a rank filter OR a system filter, not both.";
                return false;
            }
            if (systems.Count > 0) { filters.Add(new FilterSpec(null, null, null, null, systems)); continue; }
            if (rank == null) { error = "Each filter needs a rank (with value/values/exclude) or a system tag."; return false; }
            if (TaxonFilterSql.ResolveColumn(rank) is null) {
                error = $"Unknown rank '{f.Rank}'. Use kingdom, phylum, class, order, family, or genus.";
                return false;
            }
            var value = string.IsNullOrWhiteSpace(f.Value) ? null : f.Value!.Trim();
            var values = (f.Values ?? Array.Empty<string>()).Select(v => v.Trim()).Where(v => v.Length > 0).ToList();
            var exclude = (f.Exclude ?? Array.Empty<string>()).Select(v => v.Trim()).Where(v => v.Length > 0).ToList();
            if (value == null && values.Count == 0 && exclude.Count == 0) {
                error = $"Filter on rank '{rank}' needs a value, values, or exclude.";
                return false;
            }
            filters.Add(new FilterSpec(rank, value, values.Count > 0 ? values : null, exclude.Count > 0 ? exclude : null, null));
        }
        return true;
    }

    // Append a pre-built group block to the end of the groups map. Insertion only — never touches
    // existing lines, so comments + custom_groups elsewhere are preserved byte-for-byte.
    internal static bool TryAppendGroupBlock(string yaml, string block, out string updated, out string error) {
        error = "";
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        updated = (yaml.Length == 0 || yaml.EndsWith("\n") ? yaml : yaml + nl) + block;
        if (!updated.EndsWith("\n")) updated += nl;
        return true;
    }

    // Round-trip: the rewritten taxa-groups.yml must re-parse, contain exactly one MORE group (the new
    // one) with the expected name + filter count, and lose none of the prior groups.
    internal static bool NewGroupRoundTripOk(
        string updated, string key, string expectedName, int expectedFilterCount, int expectedGroupCount, out string error) {
        error = "";
        try {
            var de = new DeserializerBuilder()
                .IgnoreUnmatchedProperties()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var file = de.Deserialize<TaxaGroupsFile>(updated);
            if (file?.Groups is null) { error = "Rewritten YAML did not parse."; return false; }
            if (file.Groups.Count != expectedGroupCount) {
                error = $"Group count changed unexpectedly (got {file.Groups.Count}, expected {expectedGroupCount}).";
                return false;
            }
            if (!file.Groups.TryGetValue(key, out var g)) { error = $"New group '{key}' missing after write."; return false; }
            if (!string.Equals(g.Name, expectedName, StringComparison.Ordinal)) {
                error = $"New group name mismatch (got '{g.Name}').";
                return false;
            }
            if ((g.Filters?.Count ?? 0) != expectedFilterCount) {
                error = $"New group filter count mismatch (got {g.Filters?.Count ?? 0}, expected {expectedFilterCount}).";
                return false;
            }
            return true;
        } catch (Exception ex) {
            error = $"Round-trip parse failed: {ex.Message}";
            return false;
        }
    }

    internal static bool ListEntryExists(string yaml, string key) =>
        Regex.IsMatch(yaml, $@"^\s*-\s+taxa_group:\s*{Regex.Escape(key)}\s*$", RegexOptions.Multiline);

    // Append a `- taxa_group: <key>` entry (with a category_split bundle OR an explicit presets array)
    // to the end of the lists: sequence. Insertion only.
    internal static bool TryAppendTaxaGroupListEntry(
        string yaml, string key, string? categorySplit, List<string> presets, out string updated, out string error) {
        error = "";
        var nl = yaml.Contains("\r\n") ? "\r\n" : "\n";
        var sb = new StringBuilder(yaml.Length == 0 || yaml.EndsWith("\n") ? yaml : yaml + nl);
        sb.Append(nl);
        sb.Append("  - taxa_group: ").Append(key).Append(nl);
        if (!string.IsNullOrWhiteSpace(categorySplit))
            sb.Append("    category_split: ").Append(categorySplit).Append(nl);
        else
            sb.Append("    presets: [").Append(string.Join(", ", presets)).Append(']').Append(nl);
        updated = sb.ToString();
        return true;
    }

    internal static bool ListEntryRoundTripOk(string updated, string key, string? categorySplit, List<string> presets) {
        try {
            var de = new DeserializerBuilder()
                .IgnoreUnmatchedProperties()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var raw = de.Deserialize<WikipediaListConfigRaw>(updated);
            var entry = raw?.Lists?.FirstOrDefault(l => string.Equals(l.TaxaGroup, key, StringComparison.OrdinalIgnoreCase));
            if (entry is null) return false;
            if (!string.IsNullOrWhiteSpace(categorySplit))
                return string.Equals(entry.CategorySplit?.Trim(), categorySplit, StringComparison.OrdinalIgnoreCase);
            return entry.Presets != null && entry.Presets.SequenceEqual(presets);
        } catch {
            return false;
        }
    }

    private sealed class FilterDto {
        public string? Rank { get; set; }
        public string? Value { get; set; }
        public string[]? Values { get; set; }
        public string[]? Exclude { get; set; }
        public string? System { get; set; }
        public string[]? Systems { get; set; }
    }

    private sealed class CreateGroupRequest {
        public string? Key { get; set; }
        public string? Name { get; set; }
        public string? Adjective { get; set; }
        public string? ListingStyle { get; set; }
        public string? ParentGroup { get; set; }
        public bool? InheritFilters { get; set; }      // default true: inherit parent's non-system filters
        public FilterDto[]? Filters { get; set; }      // multi-filter (preferred)
        public string? CategorySplit { get; set; }
        public string[]? Presets { get; set; }
        // Legacy single-filter shorthand — still accepted when Filters is empty.
        public string? Rank { get; set; }
        public string? Value { get; set; }
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
