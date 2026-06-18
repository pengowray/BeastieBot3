using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.WikipediaLists;

// Shared counts-only impact computation for a taxa group: page-option sizes (combined threatened vs
// separate CR/EN/VU, NT, DD, LC) and optional per-sub-page sizing by rank — each as renderable-row
// ("bullet") weight AND canonical species. Used by the `wikipedia preview-impact` CLI and the
// GET /api/lists/impact endpoint so both report identical numbers. Never generates wikitext.

internal sealed record ImpactPageOption(string Key, string Label, int Bullets, int Species, bool? OverBudget);

internal sealed record ImpactSubPage(string Child, int Threatened, int Total, bool? OverBudget);

internal sealed record ListImpactRecord(
    string TaxaGroup,
    IReadOnlyList<string> CurrentPages,
    int? Budget,
    IReadOnlyDictionary<string, int> RenderableByCode,
    IReadOnlyDictionary<string, int> SpeciesByCode,
    IReadOnlyList<ImpactPageOption> Options,
    string? SplitRank,
    IReadOnlyList<ImpactSubPage>? SubPages);

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
        string databasePath, string configPath, string group, string? splitRank, int? budgetOverride) {
        var config = new WikipediaListDefinitionLoader().Load(configPath);
        var groupLists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (groupLists.Count == 0) {
            return null;
        }

        var filters = groupLists[0].Filters;
        var budget = budgetOverride ?? groupLists[0].SizeBudgetMaxEntries;

        using var chart = new IucnChartDataBuilder(databasePath);
        var species = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom"));
        var render = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom",
            wherePredicate: TaxonFilterSql.RenderablePredicate()));

        var options = OptionDefs.Select(o => {
            var bullets = Sum(render, o.Codes);
            return new ImpactPageOption(o.Key, o.Label, bullets, Sum(species, o.Codes),
                budget.HasValue ? bullets > budget.Value : (bool?)null);
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
            budget, render, species, options, splitRank, subPages);
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
