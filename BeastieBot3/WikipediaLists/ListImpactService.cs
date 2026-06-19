using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace BeastieBot3.WikipediaLists;

// Shared counts-only impact computation for a taxa group: page-option sizes (combined threatened vs
// separate CR/EN/VU, NT, DD, LC) and optional per-sub-page sizing by rank — each as renderable-row
// ("bullet") weight AND canonical species. Used by the `wikipedia preview-impact` CLI and the
// GET /api/lists/impact endpoint so both report identical numbers. Never generates wikitext.

// Real structure metrics for the page option's generated list, from the last generation's
// structure-metrics.json (null when that list hasn't been generated yet).
internal sealed record ImpactStructure(int Headings, int MaxDepth, int SingleItemHeadings, int MaxLeafSize, long FileBytes, IReadOnlyList<string> Problems);

internal sealed record ImpactPageOption(string Key, string Label, int Bullets, int Species, bool? OverBudget,
    string? ListId = null, ImpactStructure? Structure = null);

internal sealed record ImpactSubPage(string Child, int Threatened, int Total, bool? OverBudget);

internal sealed record ListImpactRecord(
    string TaxaGroup,
    IReadOnlyList<string> CurrentPages,
    int? Budget,
    IReadOnlyDictionary<string, int> RenderableByCode,
    IReadOnlyDictionary<string, int> SpeciesByCode,
    IReadOnlyList<ImpactPageOption> Options,
    string? SplitRank,
    IReadOnlyList<ImpactSubPage>? SubPages,
    // The group's current category_split setting ("default" when none is set), so the tuning UI can
    // show what's in effect. Supplied by the caller (read from the draft rules).
    string? CurrentSplit = null);

internal static class ListImpactService {
    private static readonly string[] Threatened = { "CR(PE)", "CR(PEW)", "CR", "EN", "VU" };
    private static readonly string[] AllCr = { "CR(PE)", "CR(PEW)", "CR" };

    // (key, label, codes) for the standard page options shown by both surfaces.
    private static readonly (string Key, string Label, string[] Codes)[] OptionDefs = {
        ("combined-threatened", "Combined threatened (CR+EN+VU)", Threatened),
        ("cr", "Critically endangered", AllCr),
        ("en", "Endangered", new[] { "EN" }),
        ("vu", "Vulnerable", new[] { "VU" }),
        ("nt", "Near threatened", new[] { "NT" }),
        ("dd", "Data deficient", new[] { "DD" }),
        ("lc", "Least concern", new[] { "LC" }),
    };

    /// <summary>
    /// Computes the impact record for a taxa group, or null if the group isn't in the config.
    /// <paramref name="budgetOverride"/> takes precedence over the group's declared size_budget.
    /// </summary>
    public static ListImpactRecord? Compute(
        string databasePath, string configPath, string group, string? splitRank, int? budgetOverride,
        string? metricsDir = null, string? currentSplit = null) {
        var config = new WikipediaListDefinitionLoader().Load(configPath);
        var groupLists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (groupLists.Count == 0) {
            return null;
        }

        var filters = groupLists[0].Filters;
        var budget = budgetOverride ?? groupLists[0].SizeBudgetMaxEntries;
        var metrics = LoadMetrics(metricsDir);

        using var chart = new IucnChartDataBuilder(databasePath);
        var species = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom"));
        var render = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom",
            wherePredicate: TaxonFilterSql.RenderablePredicate()));

        var options = OptionDefs.Select(o => {
            var bullets = Sum(render, o.Codes);
            // Each option maps to a generated list id ({group}-{preset}); attach its real structure
            // metrics from the last generation when present.
            var preset = o.Key == "combined-threatened" ? "threatened" : o.Key;
            var listId = $"{group}-{preset}";
            var structure = metrics.TryGetValue(listId, out var m)
                ? new ImpactStructure(m.HeadingCount, m.MaxHeadingDepth, m.SingleItemHeadings, m.MaxLeafSize, m.FileBytes, m.Problems)
                : null;
            return new ImpactPageOption(o.Key, o.Label, bullets, Sum(species, o.Codes),
                budget.HasValue ? bullets > budget.Value : (bool?)null, listId, structure);
        }).ToList();

        List<ImpactSubPage>? subPages = null;
        if (!string.IsNullOrWhiteSpace(splitRank) && TaxonFilterSql.ResolveColumn(splitRank) is not null) {
            subPages = chart.BuildChildBreakdown(filters, splitRank!, wherePredicate: TaxonFilterSql.RenderablePredicate())
                .Select(kv => (Child: kv.Key, Codes: CodeMap(kv.Value)))
                .Select(r => new ImpactSubPage(
                    string.IsNullOrWhiteSpace(r.Child) ? "(unassigned)" : r.Child,
                    Sum(r.Codes, Threatened),
                    r.Codes.Values.Sum(),
                    budget.HasValue ? r.Codes.Values.Sum() > budget.Value : (bool?)null))
                .Where(s => s.Total > 0)
                .OrderByDescending(s => s.Total)
                .ToList();
        }

        return new ListImpactRecord(
            group,
            groupLists.Select(l => l.Preset ?? l.Id).ToList(),
            budget, render, species, options, splitRank, subPages, currentSplit);
    }

    // Index the last generation's structure-metrics.json by list id (empty if absent/unreadable).
    private static Dictionary<string, ListStructureMetrics> LoadMetrics(string? metricsDir) {
        var map = new Dictionary<string, ListStructureMetrics>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(metricsDir)) {
            return map;
        }
        var path = Path.Combine(metricsDir, "structure-metrics.json");
        if (!File.Exists(path)) {
            return map;
        }
        try {
            var report = JsonSerializer.Deserialize<GenerationMetricsReport>(File.ReadAllText(path));
            if (report != null) {
                foreach (var m in report.Lists) {
                    map[m.ListId] = m;
                }
            }
        } catch {
            // Best-effort: a malformed/old metrics file just means no structure overlay.
        }
        return map;
    }

    private static Dictionary<string, int> TotalsByCode(Dictionary<string, IReadOnlyList<StatusCount>> breakdown) {
        var totals = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var counts in breakdown.Values) {
            foreach (var c in counts) {
                totals[c.Code] = totals.GetValueOrDefault(c.Code) + c.Count;
            }
        }
        return totals;
    }

    private static Dictionary<string, int> CodeMap(IReadOnlyList<StatusCount> counts) {
        var m = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var c in counts) m[c.Code] = c.Count;
        return m;
    }

    private static int Sum(Dictionary<string, int> totals, string[] codes) =>
        codes.Sum(code => totals.GetValueOrDefault(code));
}
