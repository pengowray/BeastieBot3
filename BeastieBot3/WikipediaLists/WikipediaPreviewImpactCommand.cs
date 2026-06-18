using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.WikipediaLists;

// Counts-only "impact preview" for list tuning. Answers, WITHOUT generating any wikitext, the two
// headline split questions for a taxa group:
//   - how big is one combined "threatened" page (CR+EN+VU) vs separate CR / EN / VU pages?
//   - if I split this group at class/order, how big is each sub-page (and which bust a budget)?
//
// It reports TWO counts per option: canonical species (the prose headline number) and renderable
// rows (the actual bullet weight, which also counts subspecies/varieties) — because for taxa with
// many subspecies the bullet count, not the species count, drives the "too big" decision.
//
// All numbers come from two GROUP BY scans (IucnChartDataBuilder.BuildChildBreakdown) under the
// canonical / renderable predicates, so they match generated output by construction.

[CommandInfo("wikipedia preview-impact", CommandKind.ReadOnly,
    "Preview list sizes for category-split / taxonomic-split choices without generating (counts only).",
    Examples = new[] {
        "wikipedia preview-impact --taxa-group plants",
        "wikipedia preview-impact --taxa-group plants --split-rank class --budget-entries 5000",
    })]
internal sealed class WikipediaPreviewImpactCommand : Command<WikipediaPreviewImpactCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--database <PATH>")]
        [Description("Override IUCN database path.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--dataset <SOURCE>")]
        [Description("Dataset to read: csv (default) or api.")]
        public string? Dataset { get; init; }

        [CommandOption("--config <FILE>")]
        [Description("Override wikipedia-lists.yml path (defaults to rules/).")]
        public string? ConfigPath { get; init; }

        [CommandOption("-g|--taxa-group <NAME>")]
        [Description("Taxa group to preview (e.g. plants, mammals, fish).")]
        public string? TaxaGroup { get; init; }

        [CommandOption("--split-rank <RANK>")]
        [Description("Also break the group down by this rank (class|order|family) to size sub-pages.")]
        public string? SplitRank { get; init; }

        [CommandOption("--budget-entries <N>")]
        [Description("Flag any page/sub-page whose renderable-row count exceeds N.")]
        public int? BudgetEntries { get; init; }

        [CommandOption("--json")]
        [Description("Emit the impact record as JSON instead of a table.")]
        public bool Json { get; init; }
    }

    // Chart codes that make up a "threatened" page.
    private static readonly string[] ThreatenedCodes = { "CR(PE)", "CR(PEW)", "CR", "EN", "VU" };
    private static readonly string[] AllCrCodes = { "CR(PE)", "CR(PEW)", "CR" };

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(settings.TaxaGroup)) {
            AnsiConsole.MarkupLine("[red]--taxa-group is required.[/] Try [white]wikipedia show-lists[/] for the group names.");
            return 1;
        }

        var paths = settings.CreatePaths();
        var configPath = settings.ConfigPath
            ?? System.IO.Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
        var databasePath = IucnDatasetResolver.Resolve(paths, settings.Dataset, settings.DatabasePath);

        var loader = new WikipediaListDefinitionLoader();
        var config = loader.Load(configPath);
        var groupLists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, settings.TaxaGroup, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (groupLists.Count == 0) {
            AnsiConsole.MarkupLine($"[yellow]No taxa group '{settings.TaxaGroup}'.[/] Try [white]wikipedia show-lists[/].");
            return 1;
        }

        var filters = groupLists[0].Filters;
        var taxaName = groupLists[0].TaxaNameLower ?? settings.TaxaGroup;
        // --budget-entries overrides; otherwise use the group's declared size_budget.max_entries.
        var budget = settings.BudgetEntries ?? groupLists[0].SizeBudgetMaxEntries;

        using var chart = new IucnChartDataBuilder(databasePath);
        // Group total per status code, under both predicates.
        var speciesTotal = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom"));
        var renderTotal = TotalsByCode(chart.BuildChildBreakdown(filters, "kingdom",
            wherePredicate: TaxonFilterSql.RenderablePredicate()));

        if (settings.Json) {
            EmitJson(settings, taxaName, groupLists, speciesTotal, renderTotal, chart, filters);
            return 0;
        }

        AnsiConsole.MarkupLine($"[bold]Impact preview — {settings.TaxaGroup}[/] [grey](renderable bullets / canonical species)[/]");
        AnsiConsole.MarkupLine($"[grey]Currently generates {groupLists.Count} page(s): {string.Join(", ", groupLists.Select(l => l.Preset).Where(p => p != null))}[/]");
        AnsiConsole.WriteLine();

        // Page-option sizing.
        var options = new Table().Border(TableBorder.Rounded);
        options.AddColumn("Page option");
        options.AddColumn(new TableColumn("Bullets").RightAligned());
        options.AddColumn(new TableColumn("Species").RightAligned());
        options.AddColumn("Verdict");
        AddOption(options, "Combined threatened (CR+EN+VU)", ThreatenedCodes, renderTotal, speciesTotal, budget);
        AddOption(options, "  — separately: Critically endangered", AllCrCodes, renderTotal, speciesTotal, budget);
        AddOption(options, "  — separately: Endangered", new[] { "EN" }, renderTotal, speciesTotal, budget);
        AddOption(options, "  — separately: Vulnerable", new[] { "VU" }, renderTotal, speciesTotal, budget);
        AddOption(options, "Near threatened", new[] { "NT" }, renderTotal, speciesTotal, budget);
        AddOption(options, "Data deficient", new[] { "DD" }, renderTotal, speciesTotal, budget);
        AddOption(options, "Least concern", new[] { "LC" }, renderTotal, speciesTotal, budget);
        AnsiConsole.Write(options);

        // Optional sub-page sizing by rank.
        if (!string.IsNullOrWhiteSpace(settings.SplitRank)) {
            if (TaxonFilterSql.ResolveColumn(settings.SplitRank) is null) {
                AnsiConsole.MarkupLine($"[yellow]Unknown split rank '{settings.SplitRank}'.[/] Use class, order, or family.");
            } else {
                RenderSplit(chart, filters, settings.SplitRank!, budget);
            }
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]Bullets = species + subspecies/varieties rendered (global, non-subpopulation). Species = the prose headline count. No wikitext was generated.[/]");
        return 0;
    }

    private void RenderSplit(IucnChartDataBuilder chart, List<TaxonFilterDefinition>? filters, string rank, int? budget) {
        var byChild = chart.BuildChildBreakdown(filters, rank, wherePredicate: TaxonFilterSql.RenderablePredicate());
        var rows = byChild
            .Select(kv => (Child: kv.Key, Codes: CodeMap(kv.Value)))
            .Select(r => (r.Child, Threat: Sum(r.Codes, ThreatenedCodes), Total: r.Codes.Values.Sum()))
            .Where(r => r.Total > 0)
            .OrderByDescending(r => r.Total)
            .ToList();

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[bold]If split at {rank}[/] — {rows.Count} sub-page(s), renderable bullets:");
        var t = new Table().Border(TableBorder.Rounded);
        t.AddColumn(CapFirst(rank));
        t.AddColumn(new TableColumn("Threatened").RightAligned());
        t.AddColumn(new TableColumn("All statuses").RightAligned());
        if (budget.HasValue) t.AddColumn("Verdict");
        foreach (var r in rows) {
            var cells = new List<string> { Title(r.Child), r.Threat.ToString("N0"), r.Total.ToString("N0") };
            if (budget.HasValue) cells.Add(r.Total > budget.Value ? $"[red]exceeds {budget:N0}[/]" : "[green]ok[/]");
            t.AddRow(cells.ToArray());
        }
        AnsiConsole.Write(t);
    }

    private static void AddOption(Table table, string label, string[] codes,
        Dictionary<string, int> render, Dictionary<string, int> species, int? budget) {
        var bullets = Sum(render, codes);
        var sp = Sum(species, codes);
        var verdict = budget.HasValue
            ? (bullets > budget.Value ? $"[red]exceeds {budget:N0}[/]" : "[green]fits[/]")
            : "";
        table.AddRow(Markup.Escape(label), bullets.ToString("N0"), sp.ToString("N0"), verdict);
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

    private static string Title(string raw) =>
        string.IsNullOrWhiteSpace(raw) ? "(unassigned)" : char.ToUpperInvariant(raw[0]) + raw[1..].ToLowerInvariant();

    private static string CapFirst(string s) => string.IsNullOrEmpty(s) ? s : char.ToUpperInvariant(s[0]) + s[1..];

    private void EmitJson(Settings settings, string taxaName, List<WikipediaListDefinition> groupLists,
        Dictionary<string, int> species, Dictionary<string, int> render,
        IucnChartDataBuilder chart, List<TaxonFilterDefinition>? filters) {
        object? split = null;
        if (!string.IsNullOrWhiteSpace(settings.SplitRank) && TaxonFilterSql.ResolveColumn(settings.SplitRank) is not null) {
            split = chart.BuildChildBreakdown(filters, settings.SplitRank!, wherePredicate: TaxonFilterSql.RenderablePredicate())
                .Select(kv => { var m = CodeMap(kv.Value); return new { child = kv.Key, threatened = Sum(m, ThreatenedCodes), total = m.Values.Sum() }; })
                .Where(r => r.total > 0)
                .OrderByDescending(r => r.total);
        }
        var record = new {
            taxaGroup = settings.TaxaGroup,
            pages = groupLists.Select(l => l.Preset).ToList(),
            renderable = render,
            species,
            combinedThreatened = new { bullets = Sum(render, ThreatenedCodes), species = Sum(species, ThreatenedCodes) },
            splitRank = settings.SplitRank,
            subPages = split,
        };
        AnsiConsole.WriteLine(JsonSerializer.Serialize(record, new JsonSerializerOptions { WriteIndented = true }));
    }
}
