using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

/// <summary>
/// Detects ambiguous common names (same normalized name used for different valid taxa in the same kingdom).
/// </summary>
internal sealed class CommonNameDetectConflictsCommand : AsyncCommand<CommonNameDetectConflictsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--include-fossil")]
        [Description("Include fossil species in conflict detection (normally excluded).")]
        public bool IncludeFossil { get; init; }

        [CommandOption("--clear-existing")]
        [Description("Clear existing conflicts before detection.")]
        public bool ClearExisting { get; init; }

        [CommandOption("--language <LANG>")]
        [Description("Language to check for conflicts. Default: en")]
        public string Language { get; init; } = "en";
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile);
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath);

        AnsiConsole.MarkupLine($"[blue]Common name store:[/] {commonNameDbPath}");

        using var store = CommonNameStore.Open(commonNameDbPath);

        if (settings.ClearExisting) {
            AnsiConsole.MarkupLine("[yellow]Clearing existing conflicts...[/]");
            store.ClearConflicts();
        }

        await DetectAmbiguousNamesAsync(store, settings.Language, settings.IncludeFossil, cancellationToken);

        // Show statistics
        var stats = store.GetStatistics();
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]Conflict detection complete:[/]");
        AnsiConsole.MarkupLine($"  Conflicts detected: [yellow]{stats.ConflictCount:N0}[/]");

        return 0;
    }

    private static Task DetectAmbiguousNamesAsync(CommonNameStore store, string language, bool includeFossil, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Detecting ambiguous common names...[/]");

            var conflictsFound = 0;
            var namesChecked = 0;

            // Get all distinct normalized common names
            var normalizedNames = store.GetDistinctNormalizedCommonNames(language);
            AnsiConsole.MarkupLine($"[blue]Checking {normalizedNames.Count:N0} distinct common names...[/]");

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Checking for conflicts[/]", maxValue: normalizedNames.Count);

                    foreach (var normalizedName in normalizedNames) {
                        cancellationToken.ThrowIfCancellationRequested();
                        namesChecked++;
                        task.Increment(1);

                        // Get all common name records for this normalized name
                        var records = store.GetCommonNamesByNormalized(normalizedName, language);

                        // Filter out invalid taxa and optionally fossil species
                        var validRecords = records
                            .Where(r => r.TaxonValidityStatus == "valid")
                            .Where(r => includeFossil || !r.TaxonIsFossil)
                            .ToList();

                        if (validRecords.Count < 2) {
                            continue;
                        }

                        // Group by kingdom to find same-kingdom conflicts
                        var byKingdom = validRecords
                            .GroupBy(r => r.TaxonKingdom ?? "unknown")
                            .Where(g => g.Select(r => r.TaxonId).Distinct().Count() > 1)
                            .ToList();

                        foreach (var kingdomGroup in byKingdom) {
                            // Get distinct taxa in this kingdom with this common name
                            var distinctTaxa = kingdomGroup
                                .GroupBy(r => r.TaxonId)
                                .Select(g => g.First())
                                .ToList();

                            if (distinctTaxa.Count < 2) {
                                continue;
                            }

                            // Record conflicts between all pairs
                            for (var i = 0; i < distinctTaxa.Count; i++) {
                                for (var j = i + 1; j < distinctTaxa.Count; j++) {
                                    var a = distinctTaxa[i];
                                    var b = distinctTaxa[j];

                                    // Check if these are synonyms of each other
                                    if (AreSynonyms(store, a.TaxonId, b.TaxonId)) {
                                        continue;
                                    }

                                    store.InsertConflict(
                                        normalizedName,
                                        "ambiguous",
                                        a.TaxonId,
                                        a.Id,
                                        b.TaxonId,
                                        b.Id
                                    );
                                    conflictsFound++;
                                }
                            }
                        }
                    }
                });

            AnsiConsole.MarkupLine($"[green]Found {conflictsFound:N0} conflicts among {namesChecked:N0} common names[/]");
        }, cancellationToken);
    }

    private static bool AreSynonyms(CommonNameStore store, long taxonIdA, long taxonIdB) {
        // For now, we only consider taxa from the same source as potentially the same
        // A more sophisticated check would look at cross-references
        // This is a placeholder for future enhancement
        return false;
    }
}
