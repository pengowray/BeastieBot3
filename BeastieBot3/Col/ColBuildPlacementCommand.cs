using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using BeastieBot3.Infrastructure;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;
using Spectre.Console;
using Spectre.Console.Cli;

// `col build-placement`: builds (or reports on) the Catalogue of Life placement for an IUCN
// database, the CoL groups such as suborder Serpentes or subfamily Rubioideae that list headings
// can insert between IUCN's ranks. See TaxonPlacement.cs, TaxonPlacementBuilder.cs and
// TaxonPlacementStore.cs. List generation builds the placement itself when it is missing or out of
// date; this command exists to build it ahead of time, check it, and write the report.

namespace BeastieBot3.Col;

[CommandInfo("col build-placement", CommandKind.Mutates,
    "Find the Catalogue of Life groups, such as suborders and subfamilies, that come between IUCN's class, order, family and genus, and save them for Wikipedia list headings.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "The first run matches every IUCN species to Catalogue of Life, which takes a few minutes. Later runs reuse the saved matches until the Catalogue of Life database changes, and take seconds.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "col build-placement",
        "col build-placement --status",
        "col build-placement --report",
        "col build-placement --force",
        "col build-placement --dataset api",
    })]
internal sealed class ColBuildPlacementCommand : Command<ColBuildPlacementCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--status")]
        [Description("Only show whether the saved placement is up to date, then exit without building anything.")]
        public bool StatusOnly { get; init; }

        [CommandOption("--force")]
        [Description("Build the placement again even when the saved one is up to date.")]
        public bool Force { get; init; }

        [CommandOption("--report")]
        [Description("Also write a Markdown report: the CoL groups placed under each IUCN class and order with species counts, and the groups left out and why. The report needs a fresh build, which uses the saved matches and takes seconds.")]
        public bool Report { get; init; }

        [CommandOption("--output <FILE>")]
        [Description("File for the report. Default: col-placement-<date and time>.md in reports_dir from paths.ini.")]
        public string? OutputPath { get; init; }

        [CommandOption("--dataset <SOURCE>")]
        [Description("Which IUCN dataset to read: 'csv' (default, the imported CSV release) or 'api' (the CSV-shaped projection of the API cache built by 'iucn api project-view').")]
        public string? Dataset { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Path to the IUCN SQLite database. Default: IUCN_sqlite_from_cvs in paths.ini, or IUCN_api_projected_sqlite with --dataset api.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--col-database <PATH>")]
        [Description("Path to the Catalogue of Life SQLite database. Default: COL_sqlite in paths.ini.")]
        public string? ColDatabasePath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        string iucnPath;
        try {
            iucnPath = IucnDatasetResolver.Resolve(paths, settings.Dataset, settings.DatabasePath, "--database");
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }
        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLine($"[red]IUCN database not found:[/] {Markup.Escape(iucnPath)}");
            return -1;
        }
        var colPath = settings.ColDatabasePath ?? paths.GetColSqlitePath();
        if (string.IsNullOrWhiteSpace(colPath)) {
            AnsiConsole.MarkupLine("[red]No Catalogue of Life database configured.[/] Set COL_sqlite in paths.ini or use --col-database.");
            return -1;
        }
        colPath = Path.GetFullPath(colPath);
        if (!File.Exists(colPath)) {
            AnsiConsole.MarkupLine($"[red]Catalogue of Life database not found:[/] {Markup.Escape(colPath)}");
            return -1;
        }

        var status = TaxonPlacementBuild.Status(iucnPath, colPath);
        AnsiConsole.MarkupLine($"[grey]IUCN database:[/] {Markup.Escape(iucnPath)}");
        AnsiConsole.MarkupLine($"[grey]Catalogue of Life database:[/] {Markup.Escape(colPath)}");
        AnsiConsole.MarkupLine($"[grey]Placement file:[/] {Markup.Escape(status.SidecarPath)}");
        AnsiConsole.MarkupLine($"[grey]Saved placement:[/] {Markup.Escape(StatusText(status))}");
        AnsiConsole.WriteLine();

        if (settings.StatusOnly) {
            return 0;
        }
        if (status.IsCurrent && !settings.Force && !settings.Report) {
            var loaded = TaxonPlacementBuild.Run(iucnPath, colPath, new PlacementRunOptions(), null, cancellationToken);
            AnsiConsole.MarkupLine("The saved placement is up to date. Run with [yellow]--force[/] to build it again.");
            AnsiConsole.WriteLine();
            AnsiConsole.Write(GroupsTable(loaded.Index, totals: null));
            return 0;
        }

        PlacementRunResult result = null!;
        var options = new PlacementRunOptions { Force = true, WantDiagnostics = settings.Report };
        ProgressConsole.Run("Reading IUCN species", 0, handle => {
            result = TaxonPlacementBuild.Run(iucnPath, colPath, options, new ProgressAdapter(handle), cancellationToken);
        });

        AnsiConsole.MarkupLine($"[green]Built the placement in {Elapsed(result.Elapsed)}.[/]");
        AnsiConsole.WriteLine();
        if (result.Matching is { } matching) {
            AnsiConsole.Write(MatchingTable(matching));
            AnsiConsole.MarkupLine(matching.ColQueries == 0
                ? "[grey]Every match came from the saved matches; the Catalogue of Life database was not read.[/]"
                : $"[grey]{matching.FromCache:N0} matches came from the saved matches and {matching.Species - matching.FromCache:N0} were looked up in the Catalogue of Life database.[/]");
            AnsiConsole.WriteLine();
        }
        AnsiConsole.Write(GroupsTable(result.Index, result.Output));

        if (settings.Report) {
            var reportPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null,
                $"col-placement-{DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture)}.md");
            File.WriteAllText(reportPath, TaxonPlacementReport.Build(result, iucnPath, colPath));
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[green]Report written to[/] {Markup.Escape(reportPath)}");
        }
        return 0;
    }

    internal static string StatusText(TaxonPlacementStatus status) {
        var built = status.Source is { } s ? s.BuiltAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : null;
        return status.State switch {
            PlacementState.Current =>
                $"up to date, built {built} ({status.Source!.Species:N0} IUCN species, {status.Source.Matched:N0} matched to Catalogue of Life)",
            PlacementState.NoFile or PlacementState.NotBuilt => "not built yet for this IUCN database",
            PlacementState.IucnChanged => $"out of date: the IUCN database has changed since the build on {built}",
            PlacementState.ColChanged => $"out of date: the Catalogue of Life database has changed since the build on {built}",
            PlacementState.RulesChanged => $"out of date: built on {built} by an older version of this command",
            PlacementState.ThresholdsChanged => $"out of date: built on {built} with different vote or containment thresholds",
            PlacementState.Unreadable => $"the placement file could not be read: {status.Error}",
            _ => status.State.ToString(),
        };
    }

    private static Table MatchingTable(PlacementMatchStats matching) {
        var table = new Table().Border(TableBorder.Rounded)
            .Title("IUCN species matched to Catalogue of Life")
            .AddColumn("")
            .AddColumn(new TableColumn("Species").RightAligned());
        foreach (var (label, value, indent) in TaxonPlacementReport.MatchingRows(matching)) {
            table.AddRow(Markup.Escape((indent ? "  " : "") + label), Markup.Escape(value));
        }
        return table;
    }

    // Rows per span: IUCN taxa with at least one CoL group, out of all IUCN taxa of that rank when known.
    private static Table GroupsTable(TaxonPlacementIndex index, PlacementBuildOutput? totals) {
        var table = new Table().Border(TableBorder.Rounded)
            .Title("CoL groups placed")
            .AddColumn("CoL groups between")
            .AddColumn(new TableColumn("IUCN taxa with CoL groups").RightAligned());
        if (totals is not null) {
            table.AddColumn(new TableColumn("IUCN taxa in total").RightAligned());
        }
        foreach (var span in new[] { PlacementSpan.ClassToOrder, PlacementSpan.OrderToFamily, PlacementSpan.FamilyToGenus }) {
            var noun = TaxonPlacementReport.AnchorNoun(span, plural: true);
            var placed = index.Paths.Count(p => p.Span == span);
            var label = TaxonPlacementReport.SpanPhrase(span)["between ".Length..];
            if (totals is null) {
                table.AddRow(label, $"{placed:N0} {noun}");
            } else {
                var all = totals.Anchors.Count(a => a.Anchor.Span == span);
                table.AddRow(label, $"{placed:N0} {noun}", $"{all:N0} {noun}");
            }
        }
        return table;
    }

    private static string Elapsed(TimeSpan elapsed) =>
        elapsed.TotalMinutes >= 1
            ? $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds}s"
            : $"{elapsed.TotalSeconds:0.0}s";

    // Shows the matching phase on the progress bar. The phases after it take a few seconds and
    // leave the bar at 100%, so a non-interactive log ends with the matching count.
    private sealed class ProgressAdapter : IPlacementBuildProgress {
        private readonly IProgressHandle _handle;
        private bool _counting;

        public ProgressAdapter(IProgressHandle handle) => _handle = handle;

        public void Phase(string description, int total) {
            if (total > 0 && !_counting) {
                _counting = true;
                _handle.Description = Markup.Escape(description);
                _handle.Total = total;
            } else if (!_counting) {
                _handle.Description = Markup.Escape(description);
            }
        }

        public void Advance(int count) {
            if (_counting && count > 0) {
                _handle.Increment(count);
            }
        }
    }
}
