using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

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

public sealed class WikidataCacheItemsCommand : AsyncCommand<WikidataCacheItemsSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataCacheItemsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataCacheItemsSettings settings, CancellationToken cancellationToken) {
        var configuration = WikidataConfiguration.FromEnvironment();
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        using var store = WikidataCacheStore.Open(cachePath);
        using var client = new WikidataApiClient(configuration);

        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;

        var limit = settings.Limit is { } l && l > 0 ? l : int.MaxValue;
        var batchSize = Math.Clamp(settings.BatchSize ?? 250, 25, 2_000);

        var downloaded = 0;
        var skipped = 0;
        var failures = 0;
        var processed = 0;

        while (processed < limit) {
            var remainingBudget = Math.Min(batchSize, limit - processed);
            if (remainingBudget <= 0) {
                break;
            }

            var queue = settings.FailedOnly
                ? store.GetFailedEntities(remainingBudget)
                : store.GetPendingEntities(remainingBudget, refreshThreshold);

            if (queue.Count == 0) {
                break;
            }

            var (batchDownloaded, batchSkipped, batchFailures) = await ProcessBatchAsync(queue, settings, refreshThreshold, client, store, cancellationToken).ConfigureAwait(false);
            downloaded += batchDownloaded;
            skipped += batchSkipped;
            failures += batchFailures;
            processed += queue.Count;

            if (queue.Count < remainingBudget) {
                // No more pending rows beyond this batch.
                break;
            }
        }

        if (downloaded == 0 && skipped == 0 && failures == 0) {
            AnsiConsole.MarkupLine("[yellow]No Wikidata entities are pending download for the provided filters.[/]");
            return 0;
        }

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}");
        AnsiConsole.MarkupLine($"[yellow]Skipped:[/] {skipped}");
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");
        return failures == 0 ? 0 : -1;
    }

    private static async Task<(int downloaded, int skipped, int failed)> ProcessBatchAsync(
        IReadOnlyList<WikidataEntityWorkItem> queue,
        WikidataCacheItemsSettings settings,
        DateTime? refreshThreshold,
        WikidataApiClient client,
        WikidataCacheStore store,
        CancellationToken cancellationToken) {
        var downloaded = 0;
        var skipped = 0;
        var failures = 0;

        await AnsiConsole.Progress()
            .Columns(new ProgressColumn[] {
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new SpinnerColumn()
            })
            .StartAsync(async ctx => {
                var task = ctx.AddTask("Caching Wikidata entities", maxValue: queue.Count);
                foreach (var item in queue) {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!settings.Force && !ShouldDownload(item, refreshThreshold)) {
                        skipped++;
                        task.Increment(1);
                        continue;
                    }

                    if (await DownloadSingleAsync(client, store, item, cancellationToken).ConfigureAwait(false)) {
                        downloaded++;
                    }
                    else {
                        failures++;
                    }

                    task.Increment(1);
                }
            });

        return (downloaded, skipped, failures);
    }

    private static bool ShouldDownload(WikidataEntityWorkItem item, DateTime? refreshThreshold) {
        if (!item.DownloadedAt.HasValue) {
            return true;
        }

        return refreshThreshold.HasValue && item.DownloadedAt.Value < refreshThreshold.Value;
    }

    private static async Task<bool> DownloadSingleAsync(WikidataApiClient client, WikidataCacheStore store, WikidataEntityWorkItem item, CancellationToken cancellationToken) {
        var url = $"wbgetentities?ids={item.EntityId}";
        var importId = store.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();

        try {
            var response = await client.GetEntityAsync(item.EntityId, cancellationToken).ConfigureAwait(false);
            var record = WikidataEntityParser.Parse(response.Body);
            store.RecordSuccess(record, importId, response.Body, DateTime.UtcNow);
            store.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return true;
        }
        catch (WikidataApiException ex) {
            store.RecordFailure(item.NumericId, ex.Message);
            store.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download {item.EntityId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
        catch (Exception ex) {
            store.RecordFailure(item.NumericId, ex.Message);
            store.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error downloading {item.EntityId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
    }
}
