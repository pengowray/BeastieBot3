using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class IucnApiCacheTaxaSettings : CommonSettings {
    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? SourceDatabase { get; init; }

    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of SIS IDs processed (mostly for testing).")]
    public long? Limit { get; init; }

    [CommandOption("--force")]
    [Description("Force download even if we already have cached JSON.")]
    public bool Force { get; init; }

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Refresh cache entries older than the supplied age (forces download for stale entries).")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry items that previously failed (skip the main SIS id list).")]
    public bool FailedOnly { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;
}

public sealed class IucnApiCacheTaxaCommand : AsyncCommand<IucnApiCacheTaxaSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, IucnApiCacheTaxaSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(IucnApiCacheTaxaSettings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var sourcePath = paths.ResolveIucnDatabasePath(settings.SourceDatabase);
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]Source CSV database:[/] {Markup.Escape(sourcePath)}");
        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        var provider = new IucnSisIdProvider(sourcePath);
        using var cacheStore = IucnApiCacheStore.Open(cachePath);

        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);

        var ids = BuildSisQueue(cacheStore, provider, settings, cancellationToken);
        if (ids.Count == 0) {
            AnsiConsole.MarkupLine("[green]Nothing to do. Cache is already populated or only failed ids exist but were not requested.[/]");
            return 0;
        }

        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);
        var totalCount = ids.Count;
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
                var task = ctx.AddTask("Downloading taxa JSON", maxValue: totalCount);
                foreach (var sisId in ids) {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!settings.Force && !ShouldDownload(cacheStore, sisId, refreshThreshold)) {
                        skipped++;
                        task.Increment(1);
                        continue;
                    }

                    if (await DownloadSingleAsync(apiClient, cacheStore, sisId, cancellationToken).ConfigureAwait(false)) {
                        downloaded++;
                    }
                    else {
                        failures++;
                    }

                    if (sleep > 0) {
                        await Task.Delay(sleep, cancellationToken).ConfigureAwait(false);
                    }

                    task.Increment(1);
                }
            });

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}");
        AnsiConsole.MarkupLine($"[yellow]Skipped:[/] {skipped}");
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        return failures == 0 ? 0 : -1;
    }

    private static List<long> BuildSisQueue(IucnApiCacheStore cacheStore, IucnSisIdProvider provider, IucnApiCacheTaxaSettings settings, CancellationToken cancellationToken) {
        var queue = new List<long>();
        var seen = new HashSet<long>();

        var totalLimit = settings.Limit;

        var failed = cacheStore.GetFailedEntityIds("taxa_sis");
        foreach (var sisId in failed) {
            if (seen.Add(sisId)) {
                queue.Add(sisId);
                if (totalLimit.HasValue && queue.Count >= totalLimit.Value) {
                    return TrimToLimit(queue, totalLimit.Value);
                }
            }
        }

        if (settings.FailedOnly) {
            return totalLimit.HasValue ? TrimToLimit(queue, totalLimit.Value) : queue;
        }

        foreach (var sisId in provider.ReadSpeciesSisIds(settings.Limit, cancellationToken)) {
            if (seen.Add(sisId)) {
                queue.Add(sisId);
                if (totalLimit.HasValue && queue.Count >= totalLimit.Value) {
                    break;
                }
            }
        }

        return totalLimit.HasValue ? TrimToLimit(queue, totalLimit.Value) : queue;
    }

    private static List<long> TrimToLimit(List<long> queue, long limit) {
        if (limit <= 0 || queue.Count <= limit) {
            return queue;
        }

        var count = (int)Math.Min(limit, queue.Count);
        return queue.GetRange(0, count);
    }

    private static bool ShouldDownload(IucnApiCacheStore cacheStore, long sisId, DateTime? refreshThreshold) {
        var downloadedAt = cacheStore.GetTaxaDownloadedAt(sisId);
        if (downloadedAt is null) {
            return true;
        }

        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value;
    }

    private static async Task<bool> DownloadSingleAsync(IucnApiClient apiClient, IucnApiCacheStore cacheStore, long sisId, CancellationToken cancellationToken) {
        var url = $"/api/v4/taxa/sis/{sisId}";
        var importId = cacheStore.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();

        try {
            var response = await apiClient.GetTaxaSisAsync(sisId, cancellationToken).ConfigureAwait(false);
            var parsed = IucnTaxaJsonParser.Parse(response.Body);
            var taxaId = cacheStore.UpsertTaxa(parsed.RootSisId, importId, response.Body, DateTime.UtcNow);
            cacheStore.ReplaceTaxaLookups(taxaId, parsed.Mappings);
            cacheStore.ReplaceAssessmentBacklog(taxaId, parsed.RootSisId, parsed.Assessments);
            cacheStore.ClearFailedRequest("taxa_sis", sisId);
            cacheStore.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return true;
        }
        catch (IucnApiException ex) {
            cacheStore.RecordFailedRequest("taxa_sis", sisId, ex.Message, (int?)ex.StatusCode);
            cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download SIS {sisId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
        catch (Exception ex) {
            cacheStore.RecordFailedRequest("taxa_sis", sisId, ex.Message, null);
            cacheStore.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error for SIS {sisId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
    }
}
