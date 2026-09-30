using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Scans CommonNameStore for vernacular names that two distinct taxa in the same kingdom share,
// skipping pairs that AreSynonyms says share a scientific name, and stores each pair in
// common_name_conflicts.
//
// Nothing reads those rows. Only their count appears (GetStatistics, in the summaries of init,
// aggregate, report and this command). Wikipedia list generation works out ambiguous names from
// common_names itself on every run (CommonNameStore.GetAmbiguousNamesSet), with a broader rule:
// any two valid taxa, in any kingdom, synonyms included. `common-names report --report ambiguous`
// uses that same rule. On the September 2026 store the two rules gave 8,810 and 10,165 names.
// So this command is kept only for querying the stored pairs by hand; the workflow pages no
// longer have a step for it.

namespace BeastieBot3.CommonNames;

/// <summary>
/// Stores a conflict for each pair of valid taxa in the same kingdom that share a normalized
/// common name. List generation does not read the stored conflicts.
/// </summary>
[CommandInfo("common-names detect-conflicts", CommandKind.Mutates,
    "Store a conflict for each pair of taxa in the same kingdom that share an English common name. The stored conflicts appear only as a count in the summaries of this command and of `common-names init`, `aggregate` and `report`. `wikipedia generate-lists` does not read them: it works out ambiguous common names each time it runs, and `common-names report --report ambiguous` lists those names.",
    Reason = "Writes conflict rows into the store; --clear-existing also wipes prior conflicts.",
    Examples = new[] {
        "common-names detect-conflicts",
        "common-names detect-conflicts --clear-existing"
    })]
internal sealed class CommonNameDetectConflictsCommand : AsyncCommand<CommonNameDetectConflictsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--include-fossil")]
        [Description("Include fossil species in conflict detection (normally excluded).")]
        public bool IncludeFossil { get; init; }

        [CommandOption("--clear-existing")]
        [Description("Delete all stored conflicts before detection, including conflicts for names that are no longer ambiguous. Without this option, stored conflicts are kept and only new conflicts are added.")]
        public bool ClearExisting { get; init; }

        [CommandOption("--language <LANG>")]
        [Description("Language to check for conflicts. Default: en")]
        public string Language { get; init; } = "en";
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath, "--database");

        AnsiConsole.MarkupLine($"[blue]Common names store:[/] {Markup.Escape(commonNameDbPath)}");

        using var store = CommonNameStore.Open(commonNameDbPath);

        if (settings.ClearExisting) {
            AnsiConsole.MarkupLine("[yellow]Clearing existing conflicts...[/]");
            store.ClearConflicts();
        }

        // Recorded as an import run so the workflow page can tell whether this list is older
        // than the names it was built from; an interrupted run is never marked completed, so a
        // half-built list is not mistaken for a current one.
        var runId = store.BeginImportRun("detect_conflicts");
        var scan = await DetectAmbiguousNamesAsync(store, settings.Language, settings.IncludeFossil, cancellationToken);
        // records_added is summed across runs (GetImportRunSummaries), so it counts only the rows
        // this run added; conflicts already stored by an earlier run are not added again.
        store.CompleteImportRun(runId, scan.NamesChecked, scan.NewConflicts, 0, 0,
            $"language={settings.Language}; conflicts found={scan.ConflictsFound}"
            + (settings.ClearExisting ? "; cleared first" : "")
            + (settings.IncludeFossil ? "; fossils included" : ""));

        // Show statistics
        var stats = store.GetStatistics();
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]Conflict detection complete:[/]");
        AnsiConsole.MarkupLine($"  Conflicts in the common names store: [yellow]{stats.ConflictCount:N0}[/]");
        var notFoundThisRun = stats.ConflictCount - scan.ConflictsFound;
        if (notFoundThisRun > 0) {
            // common_name_conflicts has no language column, so a conflict stored by a run with
            // another --language is "not found" by this one, and --clear-existing would delete it.
            // Call the rows out of date only when the run history shows they cannot be that.
            var earlierRuns = store.GetImportRuns("detect_conflicts").Where(r => r.Id != runId).ToList();
            var sameSettings = StoredConflictsAreFromRunsLikeThis(
                earlierRuns, store.GetOldestConflictDetectedAt(), settings.Language, settings.IncludeFossil);
            AnsiConsole.MarkupLine(sameSettings
                ? $"  Stored conflicts not found by this run: [yellow]{notFoundThisRun:N0}[/]. These are out of date. To delete them, run again with --clear-existing."
                : $"  Stored conflicts not found by this run: [yellow]{notFoundThisRun:N0}[/]. Some may be out of date, and some may have been found by an earlier run with a different --language or --include-fossil setting. --clear-existing deletes all stored conflicts before detection.");
        }

        return 0;
    }

    /// <summary>
    /// True when every stored conflict was written by a completed detect-conflicts run with the
    /// same --language and --include-fossil setting as this run, so a stored conflict this run
    /// did not find is out of date. False when that cannot be shown.
    /// </summary>
    /// <param name="earlierRunsNewestFirst">Earlier detect_conflicts runs, not including this one.</param>
    /// <param name="oldestStoredConflict">When the oldest stored conflict was recorded.</param>
    internal static bool StoredConflictsAreFromRunsLikeThis(
        IReadOnlyList<ImportRunRecord> earlierRunsNewestFirst,
        DateTime? oldestStoredConflict,
        string language,
        bool includeFossil) {
        if (oldestStoredConflict is not { } oldest) {
            return true;
        }

        foreach (var run in earlierRunsNewestFirst) {
            // Each conflict is stamped while its run is running, so a run that ended before the
            // oldest stored conflict has no conflicts left in the store, and nor has any older run.
            if (run.EndedAt is { } ended && ended < oldest) {
                return true;
            }
            // An interrupted run records no options, so its conflicts could be for any language.
            if (run.Status != "completed" || run.Notes is null) {
                return false;
            }
            var options = run.Notes.Split("; ");
            var runLanguage = options.FirstOrDefault(o => o.StartsWith("language=", StringComparison.Ordinal))?["language=".Length..];
            if (runLanguage != language || options.Contains("fossils included") != includeFossil) {
                return false;
            }
            if (options.Contains("cleared first")) {
                return true;
            }
        }

        // detect-conflicts has not always recorded its runs, so conflicts older than the oldest
        // recorded run were written by a run whose options are unknown.
        return earlierRunsNewestFirst.Count > 0
            && earlierRunsNewestFirst[^1].StartedAt is { } firstRunStarted
            && oldest >= firstRunStarted;
    }

    /// <summary>Counts from one pass of <see cref="ScanForConflicts"/>.</summary>
    /// <param name="NamesChecked">Distinct normalized common names checked.</param>
    /// <param name="ConflictsFound">Conflicting taxon pairs found, whether or not they were already stored.</param>
    /// <param name="NewConflicts">Conflicting taxon pairs this pass added to the store.</param>
    internal readonly record struct ConflictScanResult(int NamesChecked, int ConflictsFound, int NewConflicts);

    private static Task<ConflictScanResult> DetectAmbiguousNamesAsync(CommonNameStore store, string language, bool includeFossil, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Detecting ambiguous common names...[/]");

            // Get all distinct normalized common names
            var normalizedNames = store.GetDistinctNormalizedCommonNames(language);
            AnsiConsole.MarkupLine($"[blue]Checking {normalizedNames.Count:N0} distinct common names...[/]");

            var result = default(ConflictScanResult);
            ProgressConsole.Run("[green]Checking for conflicts[/]", normalizedNames.Count, progress => {
                result = ScanForConflicts(store, normalizedNames, language, includeFossil, () => progress.Increment(1), cancellationToken);
            });

            var alreadyStored = result.ConflictsFound - result.NewConflicts;
            AnsiConsole.MarkupLine(
                $"[green]Found {result.ConflictsFound:N0} conflicts among {result.NamesChecked:N0} common names: {result.NewConflicts:N0} new, {alreadyStored:N0} already recorded.[/]");
            return result;
        }, cancellationToken);
    }

    /// <summary>
    /// Records a conflict for every pair of distinct valid taxa in the same kingdom that share one
    /// of <paramref name="normalizedNames"/>. Pairs already stored are left as they are, so running
    /// the scan again adds only conflicts it has not recorded before.
    /// </summary>
    internal static ConflictScanResult ScanForConflicts(
        CommonNameStore store,
        IReadOnlyList<string> normalizedNames,
        string language,
        bool includeFossil,
        Action? onNameChecked = null,
        CancellationToken cancellationToken = default) {
        var namesChecked = 0;
        var conflictsFound = 0;
        var newConflicts = 0;

        foreach (var normalizedName in normalizedNames) {
            cancellationToken.ThrowIfCancellationRequested();
            namesChecked++;
            onNameChecked?.Invoke();

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

                        // Skip pairs that are really name-level synonyms of
                        // each other (same taxon under two names) — those aren't
                        // genuine ambiguous-name conflicts.
                        if (store.AreSynonyms(a.TaxonId, b.TaxonId)) {
                            continue;
                        }

                        conflictsFound++;
                        if (store.InsertConflict(normalizedName, "ambiguous", a.TaxonId, a.Id, b.TaxonId, b.Id)) {
                            newConflicts++;
                        }
                    }
                }
            }
        }

        return new ConflictScanResult(namesChecked, conflictsFound, newConflicts);
    }
}
