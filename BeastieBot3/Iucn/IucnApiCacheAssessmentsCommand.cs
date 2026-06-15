using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Step 2 of API caching: downloads /api/v4/assessment/{id} for each assessment_id
// found in previously cached taxa JSON. Stores responses in assessment_cache table.
// Taxa must be cached first (IucnApiCacheTaxaCommand). Assessment JSON includes
// conservation measures, threats, habitat, and population trend details not in CSV.
// Run via: iucn api-cache assessments

namespace BeastieBot3.Iucn;

public sealed class IucnApiCacheAssessmentsSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of assessments processed (mostly for testing).")]
    public long? Limit { get; init; }

    [CommandOption("--force")]
    [Description("Force download even if we already have cached JSON.")]
    public bool Force { get; init; }

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Refresh cache entries older than the supplied age (forces download for stale entries).")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry items that previously failed (skip the backlog queue).")]
    public bool FailedOnly { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;
}

[CommandInfo("iucn api cache-assessments", CommandKind.Mutates,
    "Download /api/v4/assessment/{assessment_id} payloads for assessments queued in the API cache's taxa-assessment backlog (the backlog is populated by cache-taxa or discover-by-family).",
    Reason = "Downloads IUCN /api/v4/assessment payloads into the local cache (idempotent additive; --force re-downloads already-cached assessments).",
    RerunNote = "Skips assessments already downloaded; --force re-downloads everything in the backlog.",
    Examples = new[] {
        "iucn api cache-assessments",
        "iucn api cache-assessments --limit 100",
        "iucn api cache-assessments --failed-only"
    })]
public sealed class IucnApiCacheAssessmentsCommand : AsyncCommand<IucnApiCacheAssessmentsSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, IucnApiCacheAssessmentsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(IucnApiCacheAssessmentsSettings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        using var cacheStore = IucnApiCacheStore.Open(cachePath);
        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);

        var queue = BuildAssessmentQueue(cacheStore, settings);
        if (queue.Count == 0) {
            AnsiConsole.MarkupLine("[green]Nothing to do. Assessment backlog is empty or all entries are up to date.[/]");
            return 0;
        }

        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;
        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);
        var downloaded = 0;
        var skipped = 0;
        var notFound = 0;
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading assessments", queue.Count, async progress => {
            foreach (var item in queue) {
                cancellationToken.ThrowIfCancellationRequested();

                // Skip already-cached/fresh, and ids previously tombstoned as 404 (no record).
                if (!settings.Force &&
                    (!ShouldDownload(item.DownloadedAt, refreshThreshold)
                     || cacheStore.HasPermanentFailure("assessment", item.AssessmentId))) {
                    skipped++;
                    progress.Increment(1);
                    continue;
                }

                switch (await DownloadSingleAsync(apiClient, cacheStore, item.AssessmentId, cancellationToken).ConfigureAwait(false)) {
                    case DownloadOutcome.Success: downloaded++; break;
                    case DownloadOutcome.NotFound: notFound++; break;
                    default: failures++; break;
                }

                if (sleep > 0) {
                    await Task.Delay(sleep, cancellationToken).ConfigureAwait(false);
                }

                progress.Increment(1);
            }
        }, cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}");
        AnsiConsole.MarkupLine($"[yellow]Skipped:[/] {skipped}");
        if (notFound > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]No record (404):[/] {notFound:N0} (removed; tombstoned)");
        }
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        return failures == 0 ? 0 : -1;
    }

    private static List<AssessmentQueueRow> BuildAssessmentQueue(IucnApiCacheStore cacheStore, IucnApiCacheAssessmentsSettings settings) {
        var queue = new List<AssessmentQueueRow>();
        var seen = new HashSet<long>();
        var snapshot = cacheStore.GetAssessmentBacklogOrdered();
        var lookup = new Dictionary<long, AssessmentQueueRow>(snapshot.Count);
        foreach (var row in snapshot) {
            lookup[row.AssessmentId] = row;
        }

        var failed = cacheStore.GetFailedEntityIds("assessment");
        foreach (var assessmentId in failed) {
            if (!seen.Add(assessmentId)) {
                continue;
            }

            if (lookup.TryGetValue(assessmentId, out var row)) {
                queue.Add(row);
            }
            else {
                var downloadedAt = cacheStore.GetAssessmentDownloadedAt(assessmentId);
                queue.Add(new AssessmentQueueRow(assessmentId, 0, 0, false, null, downloadedAt));
            }
        }

        if (settings.FailedOnly) {
            return ApplyLimit(queue, settings.Limit);
        }

        foreach (var row in snapshot) {
            if (!seen.Add(row.AssessmentId)) {
                continue;
            }

            queue.Add(row);
            if (settings.Limit.HasValue && queue.Count >= settings.Limit.Value) {
                break;
            }
        }

        return ApplyLimit(queue, settings.Limit);
    }

    private static List<AssessmentQueueRow> ApplyLimit(List<AssessmentQueueRow> queue, long? limit) {
        if (!limit.HasValue || limit.Value <= 0 || queue.Count <= limit.Value) {
            return queue;
        }

        var count = (int)Math.Min(limit.Value, queue.Count);
        return queue.GetRange(0, count);
    }

    private static bool ShouldDownload(DateTime? downloadedAt, DateTime? refreshThreshold) {
        if (downloadedAt is null) {
            return true;
        }

        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value;
    }

    private static async Task<DownloadOutcome> DownloadSingleAsync(IucnApiClient apiClient, IucnApiCacheStore cacheStore, long assessmentId, CancellationToken cancellationToken) {
        var url = $"/api/v4/assessment/{assessmentId}";
        var importId = cacheStore.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();

        try {
            var response = await apiClient.GetAssessmentAsync(assessmentId, cancellationToken).ConfigureAwait(false);
            var sisId = ExtractSisId(response.Body);
            cacheStore.UpsertAssessment(assessmentId, sisId, importId, response.Body, DateTime.UtcNow);
            cacheStore.ClearFailedRequest("assessment", assessmentId);
            cacheStore.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return DownloadOutcome.Success;
        }
        catch (IucnApiException ex) when (ex.StatusCode == HttpStatusCode.NotFound) {
            // Expected: the assessment was removed. Tombstone so it isn't re-requested every run.
            cacheStore.RecordFailedRequest("assessment", assessmentId, ex.Message, (int?)ex.StatusCode, IucnApiCacheStore.PermanentRetryDelay);
            cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            return DownloadOutcome.NotFound;
        }
        catch (IucnApiException ex) {
            cacheStore.RecordFailedRequest("assessment", assessmentId, ex.Message, (int?)ex.StatusCode);
            cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download assessment {assessmentId}: {Markup.Escape(ex.Message)}[/]");
            return DownloadOutcome.Failed;
        }
        catch (Exception ex) {
            cacheStore.RecordFailedRequest("assessment", assessmentId, ex.Message, null);
            cacheStore.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error for assessment {assessmentId}: {Markup.Escape(ex.Message)}[/]");
            return DownloadOutcome.Failed;
        }
    }

    private static long ExtractSisId(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.TryGetProperty("sis_taxon_id", out var sisElement) && sisElement.ValueKind == JsonValueKind.Number) {
            return sisElement.GetInt64();
        }

        if (root.TryGetProperty("taxon", out var taxonElement) && taxonElement.TryGetProperty("sis_id", out sisElement) && sisElement.ValueKind == JsonValueKind.Number) {
            return sisElement.GetInt64();
        }

        throw new InvalidOperationException("Unable to determine sis_taxon_id from assessment response.");
    }
}
