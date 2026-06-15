using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Step 2 of Wikidata caching: downloads full entity JSON for Q-IDs in
// wikidata_items table that have status='pending'. Uses WikidataApiClient to
// fetch from Special:EntityData. Entity JSON includes labels, sitelinks, and
// claims (P627 IUCN ID, P1843 common names, P225 taxon name). Resume-safe.
// Run via: wikidata cache items

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
    [Description("Redownload entities older than the supplied age (forces refresh).")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--force")]
    [Description("Force re-download even if the entity JSON is already cached.")]
    public bool Force { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry entities that previously failed to download.")]
    public bool FailedOnly { get; init; }
}

[CommandInfo("wikidata cache-entities", CommandKind.Mutates,
    "Download Wikidata entity JSON for taxa queued by seed-taxa (or cache-all), populating the cache and its normalised taxon-name lookup indexes.",
    Reason = "Downloads queued Wikidata entity JSON into the cache (idempotent additive; --download-force re-downloads already-cached entities).",
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
        var configuration = WikidataConfiguration.FromEnvironment();
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        using var store = WikidataCacheStore.Open(cachePath);
        using var client = new WikidataApiClient(configuration);

        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;

        var limit = settings.Limit is { } l && l > 0 ? l : int.MaxValue;
        var batchSize = Math.Clamp(settings.BatchSize ?? 250, 25, 2_000);

        var totalCandidates = settings.FailedOnly
            ? store.CountFailedEntities()
            : store.CountPendingEntities(refreshThreshold);

        if (totalCandidates == 0) {
            var message = settings.FailedOnly
                ? "[yellow]No previously failed Wikidata entities match the provided filters.[/]"
                : "[yellow]No Wikidata entities are pending download for the provided filters.[/]";
            AnsiConsole.MarkupLine(message);
            return 0;
        }

        var totalTarget = Math.Min(limit, totalCandidates);

        var (downloaded, skipped, failures, completed) = await DownloadEntitiesAsync(
            totalTarget,
            batchSize,
            settings,
            refreshThreshold,
            client,
            store,
            cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}/{totalTarget}");
        AnsiConsole.MarkupLine($"[yellow]Skipped:[/] {skipped}");
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        if (completed < totalTarget) {
            AnsiConsole.MarkupLine($"[yellow]Stopped after {completed} entities because no further items matched the current filters.[/]");
        }
        return failures == 0 ? 0 : -1;
    }

    private static async Task<(int downloaded, int skipped, int failed, int completed)> DownloadEntitiesAsync(
        int totalTarget,
        int batchSize,
        WikidataCacheItemsSettings settings,
        DateTime? refreshThreshold,
        WikidataApiClient client,
        WikidataCacheStore store,
        CancellationToken cancellationToken) {
        var downloaded = 0;
        var skipped = 0;
        var failures = 0;
        var completed = 0;

        await ProgressConsole.RunAsync("Caching Wikidata entities", totalTarget, async progress => {
            UpdateTaskDescription(progress, downloaded, skipped, failures);

            while (completed < totalTarget) {
                var remainingBudget = Math.Min(batchSize, totalTarget - completed);
                if (remainingBudget <= 0) {
                    break;
                }

                var queue = settings.FailedOnly
                    ? store.GetFailedEntities(remainingBudget)
                    : store.GetPendingEntities(remainingBudget, refreshThreshold);

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

                    if (await WikidataEntityDownloader.DownloadSingleAsync(client, store, item, cancellationToken).ConfigureAwait(false)) {
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
