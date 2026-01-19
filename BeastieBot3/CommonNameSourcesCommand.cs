using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

/// <summary>
/// Shows the status of common name data sources - which are available and which have been aggregated.
/// </summary>
internal sealed class CommonNameSourcesCommand : AsyncCommand<CommonNameSourcesCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database.")]
        public string? DatabasePath { get; init; }
    }

    /// <summary>
    /// Defines a data source for common names.
    /// </summary>
    private sealed record SourceDefinition(
        string Id,
        string Name,
        string ImportType,
        Func<PathsService, string?> GetPath,
        string? Description = null
    );

    private static readonly SourceDefinition[] Sources = {
        new("iucn", "IUCN Red List", "common_names_iucn",
            paths => paths.GetIucnApiCachePath(),
            "Common names from IUCN API assessments"),
        new("wikidata", "Wikidata", "common_names_wikidata",
            paths => paths.GetWikidataCachePath(),
            "P1843 taxon common names from Wikidata entities"),
        new("wikidata_label", "Wikidata Labels", "common_names_wikidata_labels",
            paths => paths.GetWikidataCachePath(),
            "Item labels from Wikidata (filtered for common names)"),
        new("wikipedia", "Wikipedia", "common_names_wikipedia",
            paths => paths.GetWikipediaCachePath(),
            "Article titles and taxobox names matched to taxa"),
        new("col", "Catalogue of Life", "common_names_col",
            paths => paths.GetColSqlitePath(),
            "English vernacular names from COL database"),
    };

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile);
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

        // Build the table
        var table = new Table();
        table.AddColumn("Source");
        table.AddColumn("Available");
        table.AddColumn("Aggregated");
        table.AddColumn("Records");
        table.AddColumn("Last Run");
        table.AddColumn("Description");

        foreach (var source in Sources) {
            var sourcePath = source.GetPath(paths);
            var isAvailable = !string.IsNullOrWhiteSpace(sourcePath) && File.Exists(sourcePath);
            var availableText = isAvailable ? "[green]Yes[/]" : "[dim]No[/]";

            var hasRun = importRunsByType.TryGetValue(source.ImportType, out var runSummary) && runSummary.HasCompleted;
            var aggregatedText = hasRun ? "[green]Yes[/]" : "[dim]No[/]";
            var recordsText = hasRun && runSummary != null ? runSummary.TotalAdded.ToString("N0") : "-";
            var lastRunText = hasRun && runSummary?.LastRun != null
                ? runSummary.LastRun.Value.ToString("yyyy-MM-dd HH:mm")
                : "-";

            table.AddRow(
                source.Name,
                availableText,
                aggregatedText,
                recordsText,
                lastRunText,
                source.Description ?? ""
            );
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();

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

    private static IReadOnlyList<(string Source, int Count)> GetSourceCounts(CommonNameStore store) {
        // This queries the common_names table directly for counts by source
        // We need to add this method to CommonNameStore or use reflection
        // For now, use the statistics we can get
        var results = new List<(string, int)>();

        // Get the connection via reflection (not ideal, but works for now)
        var connectionField = typeof(CommonNameStore).GetField("_connection",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        if (connectionField?.GetValue(store) is Microsoft.Data.Sqlite.SqliteConnection connection) {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT source, COUNT(*) FROM common_names GROUP BY source ORDER BY COUNT(*) DESC";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                results.Add((reader.GetString(0), reader.GetInt32(1)));
            }
        }

        return results;
    }
}
