using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Step 2 of Wikidata caching: downloads full entity JSON for the Q-IDs in the
// wikidata_entities table that have none yet (json_downloaded = 0), plus cached copies
// downloaded before a cutoff (--max-age-hours, or --force for all of them). Uses
// WikidataApiClient. Entity JSON includes labels, sitelinks, and claims (P627 IUCN ID,
// P1843 common names, P225 taxon name). Resume-safe.
// Run via: wikidata cache-entities

namespace BeastieBot3.Wikidata;

public sealed class WikidataCacheItemsSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Maximum number of entities to download in this run (default = all pending).")]
    public int? Limit { get; init; }

    [CommandOption("--batch-size <N>")]
    [Description("Number of entities to pull from the queue per batch (default 250).")]
    public int? BatchSize { get; init; }

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Re-download cached entities that were downloaded more than HOURS hours ago.")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--force")]
    [Description("Download entities not yet cached, then re-download every cached entity, least recently downloaded first. --max-age-hours is ignored. With --refresh-only, re-download only the cached entities.")]
    public bool Force { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry entities that previously failed to download.")]
    public bool FailedOnly { get; init; }

    [CommandOption("--refresh-only")]
    [Description("Re-download cached entities only, ignoring everything never downloaded. Needs --max-age-hours or --force.")]
    public bool RefreshOnly { get; init; }
}

[CommandInfo("wikidata cache-entities", CommandKind.Mutates,
    "Downloads queued Wikidata items into the Wikidata cache and updates the name index, which is used to find a taxon's Wikidata item by scientific name. Items are queued by wikidata seed-taxa and wikidata backfill-iucn; wikidata cache-all runs wikidata seed-taxa and then this command.",
    Reason = "Downloads queued Wikidata entity JSON into the cache (idempotent additive; --force re-downloads already-cached entities).",
    Examples = new[] {
        "wikidata cache-entities",
        "wikidata cache-entities --failed-only"
    })]
public sealed class WikidataCacheItemsCommand : AsyncCommand<WikidataCacheItemsSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataCacheItemsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataCacheItemsSettings settings, CancellationToken cancellationToken) {
        // Taken before anything is read. It is the --force cutoff, and every mode leaves an item
        // already tried in this run (downloaded or failed) for the next run.
        var runStart = DateTime.UtcNow;
        var configuration = WikidataConfiguration.FromEnvironment();
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        using var store = WikidataCacheStore.Open(cachePath);
        using var client = new WikidataApiClient(configuration);

        var (refreshThreshold, totalTarget) = PlanQueue(store, settings, runStart);

        if (settings.RefreshOnly && refreshThreshold is null) {
            AnsiConsole.MarkupLine("[red]--refresh-only needs --max-age-hours <HOURS> (re-download cached entities older than HOURS hours) or --force (re-download every cached entity).[/]");
            return -1;
        }

        var batchSize = Math.Clamp(settings.BatchSize ?? 250, 25, 2_000);

        if (totalTarget == 0) {
            var message = settings.FailedOnly
                ? "[yellow]No previously failed Wikidata entities match the provided filters.[/]"
                : "[yellow]No Wikidata entities are pending download for the provided filters.[/]";
            AnsiConsole.MarkupLine(message);
            return 0;
        }

        var (downloaded, skipped, failures, completed) = await DownloadEntitiesAsync(
            totalTarget,
            batchSize,
            settings,
            refreshThreshold,
            runStart,
            store,
            item => WikidataEntityDownloader.DownloadSingleAsync(client, store, item, cancellationToken),
            AnsiConsole.Console,
            cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}/{totalTarget}");
        AnsiConsole.MarkupLine($"[yellow]Skipped:[/] {skipped}");
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        if (completed < totalTarget) {
            AnsiConsole.MarkupLine($"[yellow]Stopped after {completed} entities because no further items matched the current filters.[/]");
        }
        return failures == 0 ? 0 : -1;
    }

    // Cached copies downloaded before the returned time are queued for download again. --force
    // returns the start of the run, so every cached copy is queued (with --refresh-only too), and
    // it overrides --max-age-hours. A copy re-downloaded in this run gets a later downloaded_at
    // and leaves the queue.
    internal static DateTime? RefreshCutoff(bool force, double? maxAgeHours, DateTime runStart) {
        if (force) {
            return runStart;
        }

        return maxAgeHours is { } hours && hours > 0
            ? runStart - TimeSpan.FromHours(hours)
            : null;
    }

    // The run's plan from its settings: the re-download cutoff (RefreshCutoff) and how many entities
    // the run will try, which is the number queued or --limit, whichever is lower. --refresh-only
    // with no cutoff queues nothing and is not counted; RunAsync reports it as an error. The tests
    // call this too, so they go through the same settings-to-query wiring as the command.
    internal static (DateTime? Cutoff, int Total) PlanQueue(WikidataCacheStore store, WikidataCacheItemsSettings settings, DateTime runStart) {
        var cutoff = RefreshCutoff(settings.Force, settings.MaxAgeHours, runStart);
        if (settings.RefreshOnly && cutoff is null) {
            return (null, 0);
        }

        var queued = settings.FailedOnly
            ? store.CountFailedEntities(runStart)
            : store.CountPendingEntities(cutoff, settings.RefreshOnly, runStart);
        var limit = settings.Limit is { } l && l > 0 ? l : int.MaxValue;
        return (cutoff, Math.Min(limit, queued));
    }

    // download is WikidataEntityDownloader.DownloadSingleAsync in the command; tests pass a fake
    // that records success or failure in the store without a network call.
    internal static async Task<(int downloaded, int skipped, int failed, int completed)> DownloadEntitiesAsync(
        int totalTarget,
        int batchSize,
        WikidataCacheItemsSettings settings,
        DateTime? refreshThreshold,
        DateTime runStart,
        WikidataCacheStore store,
        Func<WikidataEntityWorkItem, Task<bool>> download,
        IAnsiConsole console,
        CancellationToken cancellationToken) {
        var downloaded = 0;
        var skipped = 0;
        var failures = 0;
        var completed = 0;

        await ProgressConsole.RunAsync(console, "Caching Wikidata entities", totalTarget, async progress => {
            UpdateTaskDescription(progress, downloaded, skipped, failures);

            while (completed < totalTarget) {
                var remainingBudget = Math.Min(batchSize, totalTarget - completed);
                if (remainingBudget <= 0) {
                    break;
                }

                var queue = settings.FailedOnly
                    ? store.GetFailedEntities(remainingBudget, runStart)
                    : store.GetPendingEntities(remainingBudget, refreshThreshold, settings.RefreshOnly, runStart);

                if (queue.Count == 0) {
                    break;
                }

                foreach (var item in queue) {
                    if (completed >= totalTarget) {
                        break;
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    if (!settings.Force && !ShouldDownload(item, refreshThreshold)) {
                        skipped++;
                        completed++;
                        progress.Increment(1);
                        UpdateTaskDescription(progress, downloaded, skipped, failures);
                        continue;
                    }

                    if (await download(item).ConfigureAwait(false)) {
                        downloaded++;
                    }
                    else {
                        failures++;
                    }

                    completed++;
                    progress.Increment(1);
                    UpdateTaskDescription(progress, downloaded, skipped, failures);
                }

                if (queue.Count < remainingBudget) {
                    // Queue exhausted sooner than the current budget.
                    break;
                }
            }
        }, cancellationToken).ConfigureAwait(false);

        return (downloaded, skipped, failures, completed);
    }

    // The N/total count is shown by ProgressConsole; here we add the live download/skip/fail breakdown.
    private static void UpdateTaskDescription(IProgressHandle progress, int downloaded, int skipped, int failed) {
        progress.Description = $"Caching Wikidata entities  D:{downloaded} S:{skipped} F:{failed}";
    }

    private static bool ShouldDownload(WikidataEntityWorkItem item, DateTime? refreshThreshold) {
        if (!item.DownloadedAt.HasValue) {
            return true;
        }

        return refreshThreshold.HasValue && item.DownloadedAt.Value < refreshThreshold.Value;
    }

}
