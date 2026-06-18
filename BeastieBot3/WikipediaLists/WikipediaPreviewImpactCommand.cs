using System.ComponentModel;
using System.Text.Json;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.WikipediaLists;

// Counts-only "impact preview" for list tuning. Answers, WITHOUT generating any wikitext, the two
// headline split questions for a taxa group:
//   - how big is one combined "threatened" page (CR+EN+VU) vs separate CR / EN / VU pages?
//   - if I split this group at class/order, how big is each sub-page (and which bust a budget)?
//
// Each option reports TWO counts: renderable rows (the actual bullet weight, which also counts
// subspecies/varieties) and canonical species (the prose headline) — because for taxa with many
// subspecies the bullet count, not the species count, drives the "too big" decision. The
// computation lives in ListImpactService and is shared with GET /api/lists/impact.

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
        [Description("Flag any page/sub-page whose renderable-row count exceeds N (else the group's size_budget).")]
        public int? BudgetEntries { get; init; }

        [CommandOption("--json")]
        [Description("Emit the impact record as JSON instead of a table.")]
        public bool Json { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(settings.TaxaGroup)) {
            AnsiConsole.MarkupLine("[red]--taxa-group is required.[/] Try [white]wikipedia show-lists[/] for the group names.");
            return 1;
        }

        var paths = settings.CreatePaths();
        var configPath = settings.ConfigPath
            ?? System.IO.Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
        var databasePath = IucnDatasetResolver.Resolve(paths, settings.Dataset, settings.DatabasePath);

        var record = ListImpactService.Compute(databasePath, configPath, settings.TaxaGroup!, settings.SplitRank,
            settings.BudgetEntries, paths.GetWikipediaOutputDirectory());
        if (record is null) {
            AnsiConsole.MarkupLine($"[yellow]No taxa group '{settings.TaxaGroup}'.[/] Try [white]wikipedia show-lists[/].");
            return 1;
        }

        if (settings.Json) {
            AnsiConsole.WriteLine(JsonSerializer.Serialize(record, new JsonSerializerOptions { WriteIndented = true }));
            return 0;
        }

        AnsiConsole.MarkupLine($"[bold]Impact preview — {record.TaxaGroup}[/] [grey](renderable bullets / canonical species)[/]");
        AnsiConsole.MarkupLine($"[grey]Currently generates {record.CurrentPages.Count} page(s): {string.Join(", ", record.CurrentPages)}[/]"
            + (record.Budget.HasValue ? $"  [grey]· budget {record.Budget:N0} bullets[/]" : ""));
        AnsiConsole.WriteLine();

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Page option");
        table.AddColumn(new TableColumn("Bullets").RightAligned());
        table.AddColumn(new TableColumn("Species").RightAligned());
        table.AddColumn("Verdict");
        table.AddColumn("Structure (last gen)");
        foreach (var o in record.Options) {
            var indent = o.Key is "cr" or "en" or "vu" ? "  — separately: " : "";
            table.AddRow(Markup.Escape(indent + o.Label), o.Bullets.ToString("N0"), o.Species.ToString("N0"),
                Verdict(o.OverBudget, record.Budget), StructureCell(o.Structure));
        }
        AnsiConsole.Write(table);

        if (record.SubPages is { Count: > 0 }) {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[bold]If split at {record.SplitRank}[/] — {record.SubPages.Count} sub-page(s), renderable bullets:");
            var t = new Table().Border(TableBorder.Rounded);
            t.AddColumn(Cap(record.SplitRank!));
            t.AddColumn(new TableColumn("Threatened").RightAligned());
            t.AddColumn(new TableColumn("All statuses").RightAligned());
            t.AddColumn("Verdict");
            foreach (var s in record.SubPages) {
                t.AddRow(Title(s.Child), s.Threatened.ToString("N0"), s.Total.ToString("N0"), Verdict(s.OverBudget, record.Budget));
            }
            AnsiConsole.Write(t);
        } else if (!string.IsNullOrWhiteSpace(settings.SplitRank)) {
            AnsiConsole.MarkupLine($"[yellow]Unknown split rank '{settings.SplitRank}'.[/] Use class, order, or family.");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]Bullets = species + subspecies/varieties rendered (global, non-subpopulation). Species = the prose headline count. No wikitext was generated.[/]");
        return 0;
    }

    private static string Verdict(bool? over, int? budget) =>
        over is null ? "" : over.Value ? $"[red]exceeds {budget:N0}[/]" : "[green]fits[/]";

    private static string StructureCell(ImpactStructure? s) {
        if (s is null) {
            return "[grey]—[/]";
        }
        var core = $"{s.Headings:N0} hd · depth {s.MaxDepth}";
        if (s.SingleItemHeadings > 0) {
            core += $" · {s.SingleItemHeadings}× single";
        }
        if (s.Problems.Count > 0) {
            core = $"[yellow]{core}[/]";
        }
        if (s.FileBytes > 0) {
            var size = s.FileBytes >= 1_000_000 ? $"{s.FileBytes / 1_000_000.0:F1} MB" : $"{s.FileBytes / 1000.0:F0} KB";
            core += s.FileBytes > 2_000_000 ? $" · [red]{size}[/]" : $" · [grey]{size}[/]";
        }
        return core;
    }

    private static string Title(string raw) =>
        string.IsNullOrWhiteSpace(raw) ? "(unassigned)" : char.ToUpperInvariant(raw[0]) + raw[1..].ToLowerInvariant();

    private static string Cap(string s) => string.IsNullOrEmpty(s) ? s : char.ToUpperInvariant(s[0]) + s[1..];
}
