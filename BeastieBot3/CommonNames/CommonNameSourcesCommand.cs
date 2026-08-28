using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Diagnostic command showing aggregation status of common name sources.
// Queries CommonNameStore for record counts from each source (iucn, wikidata,
// wikipedia, col) and displays availability of upstream databases. Useful for
// verifying that aggregate steps completed before running reports.

namespace BeastieBot3.CommonNames;

/// <summary>
/// Shows the status of common name data sources - which are available and which have been aggregated.
/// </summary>
[CommandInfo("common-names sources", CommandKind.ReadOnly,
    "Show status of common name data sources - which are available and which have been aggregated.",
    Examples = new[] { "common-names sources" })]
internal sealed class CommonNameSourcesCommand : AsyncCommand<CommonNameSourcesCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database.")]
        public string? DatabasePath { get; init; }
    }

    /// <summary>
    /// Defines a data source for common names.
    /// </summary>
    /// <param name="AggregateSource">
    /// The `aggregate --source` name that imports this row. Several display rows can share one
    /// (Wikidata labels arrive with the rest of Wikidata), and a replacement is recorded against
    /// that name.
    /// </param>
    private sealed record SourceDefinition(
        string Id,
        string Name,
        string ImportType,
        string AggregateSource,
        Func<PathsService, string?> GetPath
    );

    private static readonly SourceDefinition[] Sources = {
        new("iucn", "IUCN Red List", "common_names_iucn", "iucn",
            paths => paths.GetIucnApiCachePath()),
        new("wikidata", "Wikidata", "common_names_wikidata", "wikidata",
            paths => paths.GetWikidataCachePath()),
        new("wikidata_label", "Wikidata item labels", "common_names_wikidata_labels", "wikidata",
            paths => paths.GetWikidataCachePath()),
        new("wikipedia", "Wikipedia", "common_names_wikipedia", "wikipedia",
            paths => paths.GetWikipediaCachePath()),
        new("col", "Catalogue of Life", "common_names_col", "col",
            paths => paths.GetColSqlitePath()),
    };

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath);

        AnsiConsole.MarkupLine($"[blue]Common name store:[/] {commonNameDbPath}");
        AnsiConsole.WriteLine();

        if (!File.Exists(commonNameDbPath)) {
            AnsiConsole.MarkupLine("[yellow]Database does not exist. Run 'common-names init' first.[/]");
            return Task.FromResult(1);
        }

        using var store = CommonNameStore.Open(commonNameDbPath);
        var importRuns = store.GetImportRunSummaries();
        var importRunsByType = new Dictionary<string, ImportRunSummary>(StringComparer.OrdinalIgnoreCase);
        foreach (var run in importRuns) {
            importRunsByType[run.ImportType] = run;
        }

        // Actual row counts per source. Some sources (e.g. wikidata_label) are aggregated inside
        // another source's import run, so they have no own run row — drive their status off the
        // real common_names counts rather than import_runs alone.
        var countsBySource = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var (src, count) in store.GetCommonNameCountsBySource()) {
            countsBySource[src] = count;
        }

        var replacements = store.GetSourceReplacements();

        // Build the table
        var table = new Table();
        table.AddColumn("Source");
        table.AddColumn("Available");
        table.AddColumn("Aggregated");
        table.AddColumn(new TableColumn("Records").RightAligned());
        table.AddColumn(new TableColumn("Last run").NoWrap());
        table.AddColumn(new TableColumn("Replaced").NoWrap());

        // Sources that hold names their upstream data may have dropped since.
        var neverReplaced = new List<string>();

        foreach (var source in Sources) {
            var sourcePath = source.GetPath(paths);
            var isAvailable = !string.IsNullOrWhiteSpace(sourcePath) && File.Exists(sourcePath);
            var availableText = isAvailable ? "[green]Yes[/]" : "[dim]No[/]";

            var hasRun = importRunsByType.TryGetValue(source.ImportType, out var runSummary) && runSummary.HasCompleted;
            var rowCount = countsBySource.TryGetValue(source.Id, out var c) ? c : 0;
            var aggregated = hasRun || rowCount > 0;
            var aggregatedText = aggregated ? "[green]Yes[/]" : "[dim]No[/]";
            var recordsText = rowCount > 0 ? rowCount.ToString("N0")
                : (hasRun && runSummary != null ? runSummary.TotalAdded.ToString("N0") : "-");
            var lastRunText = hasRun && runSummary?.LastRun != null
                ? runSummary.LastRun.Value.ToString("yyyy-MM-dd HH:mm")
                : "-";

            string replacedText;
            if (!aggregated) {
                replacedText = "-";
            } else if (replacements.TryGetValue(source.AggregateSource, out var replacement)) {
                replacedText = replacement.ReplacedAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm");
            } else {
                replacedText = "[yellow]never[/]";
                if (!neverReplaced.Contains(source.AggregateSource)) {
                    neverReplaced.Add(source.AggregateSource);
                }
            }

            table.AddRow(
                source.Name,
                availableText,
                aggregatedText,
                recordsText,
                lastRunText,
                replacedText
            );
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();

        if (neverReplaced.Count > 0) {
            AnsiConsole.MarkupLine(
                "A source that has never been replaced still holds every name it has ever contributed, " +
                "including names its upstream data has dropped since. To re-import one from scratch:");
            // IUCN is the least likely one to need replacing, so lead with a source whose upstream
            // data actually changes between imports.
            var example = neverReplaced.Find(s => s is "col" or "wikidata" or "wikipedia") ?? neverReplaced[0];
            AnsiConsole.MarkupLine($"  [grey]common-names aggregate --source {example} --replace[/]");
            AnsiConsole.WriteLine();
        }

        // Show counts by source from common_names table
        AnsiConsole.MarkupLine("[yellow]Current common name counts by source:[/]");
        var countTable = new Table();
        countTable.AddColumn("Source");
        countTable.AddColumn(new TableColumn("Count").RightAligned());

        var sourceCounts = GetSourceCounts(store);
        foreach (var (source, count) in sourceCounts) {
            countTable.AddRow(source, count.ToString("N0"));
        }
        AnsiConsole.Write(countTable);

        return Task.FromResult(0);
    }

    private static IReadOnlyList<(string Source, int Count)> GetSourceCounts(CommonNameStore store) =>
        store.GetCommonNameCountsBySource();
}
