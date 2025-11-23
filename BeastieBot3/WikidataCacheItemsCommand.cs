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

        await AnsiConsole.Progress()
            .Columns(new ProgressColumn[] {
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new SpinnerColumn()
            })
            .StartAsync(async ctx => {
                var task = ctx.AddTask("Caching Wikidata entities", maxValue: totalTarget);
                UpdateTaskDescription(task, completed, totalTarget, downloaded, skipped, failures);

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
                            task.Increment(1);
                            UpdateTaskDescription(task, completed, totalTarget, downloaded, skipped, failures);
                            continue;
                        }

                        if (await DownloadSingleAsync(client, store, item, cancellationToken).ConfigureAwait(false)) {
                            downloaded++;
                        }
                        else {
                            failures++;
                        }

                        completed++;
                        task.Increment(1);
                        UpdateTaskDescription(task, completed, totalTarget, downloaded, skipped, failures);
                    }

                    if (queue.Count < remainingBudget) {
                        // Queue exhausted sooner than the current budget.
                        break;
                    }
                }
            });

        return (downloaded, skipped, failures, completed);
    }

    private static void UpdateTaskDescription(ProgressTask task, int completed, int total, int downloaded, int skipped, int failed) {
        var totalText = total > 0 ? total.ToString() : "?";
        task.Description = $"Caching Wikidata entities ({completed}/{totalText}) D:{downloaded} S:{skipped} F:{failed}";
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
