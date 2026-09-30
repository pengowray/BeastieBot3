using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Step 1 of API caching: iterates SIS IDs from CSV-imported IUCN database,
// fetches /api/v4/taxa/sis/{sisId} via IucnApiClient, stores JSON in the taxa table.
// Taxa JSON includes synonyms, common names, and assessment_id references.
// Uses IucnSisIdProvider for ID enumeration. Run via: iucn api-cache taxa

namespace BeastieBot3.Iucn;

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
    [Description("Refresh cache entries older than the supplied age. A rolling window measured from now, so it moves between runs — prefer --refresh-before, or a refresh started with `iucn api refresh-start`, for anything that takes more than one sitting.")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--refresh-before <DATE>")]
    [Description("Re-download anything fetched before this fixed date (UTC, e.g. 2026-06-16). Unlike --max-age-hours the date does not move, so stopping and re-running carries on rather than repeating work. Taken from the refresh in progress when omitted.")]
    public string? RefreshBefore { get; init; }

    [CommandOption("--retry-tombstones")]
    [Description("Also re-check the taxa the API previously said were gone (404). Skipped by default; worth doing once per release, because a taxon absent from the last one can exist in the new one. With a cutoff date (a refresh in progress, --refresh-before or --max-age-hours), only 404s recorded before that date are re-checked.")]
    public bool RetryTombstones { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry items that previously failed (skip the main SIS id list).")]
    public bool FailedOnly { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;
}

[CommandInfo("iucn api cache-taxa", CommandKind.Mutates,
    "Download /api/v4/taxa/sis/{sis_id} payloads into the local API cache.",
    Reason = "Downloads IUCN /api/v4/taxa payloads into the local cache (idempotent additive).",
    Examples = new[] {
        "iucn api cache-taxa",
        "iucn api cache-taxa --limit 100",
        "iucn api cache-taxa --failed-only"
    })]
public sealed class IucnApiCacheTaxaCommand : AsyncCommand<IucnApiCacheTaxaSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, IucnApiCacheTaxaSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(IucnApiCacheTaxaSettings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var sourcePath = paths.ResolveIucnDatabasePath(settings.SourceDatabase, "--source-db");
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]Source CSV database:[/] {Markup.Escape(sourcePath)}");
        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        var provider = new IucnSisIdProvider(sourcePath);
        using var cacheStore = IucnApiCacheStore.Open(cachePath);

        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);

        // The cutoff comes from this run's flags, or from the refresh in progress. Resolved before
        // the queue is built so the tombstone pass and the skip test agree about what is stale.
        var plan = IucnRefreshRun.Begin(cacheStore, settings.RefreshBefore, settings.MaxAgeHours);
        if (plan is null) return -1;
        var refreshThreshold = plan.Threshold;

        var ids = BuildSisQueue(cacheStore, provider.ReadSpeciesSisIds(settings.Limit, cancellationToken), refreshThreshold, settings);
        if (ids.Count == 0) {
            AnsiConsole.MarkupLine("[green]Nothing to do. Cache is already populated or only failed ids exist but were not requested.[/]");
            return 0;
        }

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);
        var totalCount = ids.Count;
        var downloaded = 0;
        var skipped = 0;
        var notFound = 0;
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading taxa JSON", totalCount, async progress => {
            foreach (var sisId in ids) {
                cancellationToken.ThrowIfCancellationRequested();

                if (!settings.Force && !ShouldDownload(cacheStore, sisId, refreshThreshold, settings.RetryTombstones)) {
                    skipped++;
                    progress.Increment(1);
                    continue;
                }

                switch (await DownloadSingleAsync(apiClient, cacheStore, sisId, cancellationToken).ConfigureAwait(false)) {
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
            AnsiConsole.MarkupLineInterpolated($"[grey]No record (404):[/] {notFound:N0} (removed/unassessed; tombstoned)");
        }
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        return failures == 0 ? 0 : -1;
    }

    // The ids this run looks at, in order: failed ids due a retry, tombstoned ids (only for the
    // re-check pass), the CSV's species, then, during a refresh, every taxa row older than the
    // cutoff. csvSpeciesIds is read lazily, so --failed-only never opens the CSV database.
    internal static List<long> BuildSisQueue(IucnApiCacheStore cacheStore, IEnumerable<long> csvSpeciesIds, DateTime? refreshThreshold, IucnApiCacheTaxaSettings settings) {
        var queue = new List<long>();
        var seen = new HashSet<long>();

        var totalLimit = settings.Limit;

        // True when the queue has reached --limit.
        bool Add(long sisId) {
            if (seen.Add(sisId)) queue.Add(sisId);
            return totalLimit.HasValue && queue.Count >= totalLimit.Value;
        }

        foreach (var sisId in cacheStore.GetFailedEntityIds("taxa_sis")) {
            if (Add(sisId)) return TrimToLimit(queue, totalLimit!.Value);
        }

        // Tombstoned ids are excluded from the failed list on purpose, so the pass that re-checks
        // them has to put them back. Ones only ever seen as an infrarank aren't in the CSV list
        // below, so without this they would never be looked at again.
        if (settings.RetryTombstones) {
            foreach (var sisId in cacheStore.GetTombstonedEntityIds("taxa_sis")) {
                if (Add(sisId)) return TrimToLimit(queue, totalLimit!.Value);
            }
        }

        if (settings.FailedOnly) {
            return totalLimit.HasValue ? TrimToLimit(queue, totalLimit.Value) : queue;
        }

        foreach (var sisId in csvSpeciesIds) {
            if (Add(sisId)) return TrimToLimit(queue, totalLimit!.Value);
        }

        // With a cutoff (a refresh session, --refresh-before or --max-age-hours) every taxa row
        // older than it is due. A refresh's remaining count includes all of them, but the CSV
        // list above and cache-infraranks (subspecies and varieties a cached species lists, or the
        // CSV lists) do not reach all of them: a species dropped from the new CSV keeps its old
        // row. Queue those rows too, after the CSV species so a species comes before its
        // subspecies. Each one then downloads again or gets a 404 tombstone, which the count
        // leaves out, so the refresh can close. Almost all of them are CSV species already queued.
        if (refreshThreshold is { } threshold) {
            foreach (var sisId in cacheStore.GetTaxaRootSisIdsDownloadedBefore(threshold)) {
                if (Add(sisId)) return TrimToLimit(queue, totalLimit!.Value);
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

    internal static bool ShouldDownload(IucnApiCacheStore cacheStore, long sisId, DateTime? refreshThreshold, bool retryTombstones = false) {
        // A prior 404 means this id had no standalone record — don't re-probe it every run. That
        // verdict is only true of the release it was recorded against, so --retry-tombstones (and
        // --force) look again. The re-check decides by when the 404 was recorded, not by a
        // download date: a tombstoned subspecies has no row of its own, and the taxa_lookup path
        // below would report its species' row, which the refresh has just downloaded again, so the
        // re-check skipped almost every tombstone. With a cutoff, an id already asked about since
        // the cutoff is not asked again, so an interrupted re-check carries on where it stopped.
        if (cacheStore.TryGetPermanentFailure("taxa_sis", sisId, out var lastAttemptAt)) {
            if (!retryTombstones) return false;
            return refreshThreshold is null || lastAttemptAt is null || lastAttemptAt.Value < refreshThreshold.Value;
        }

        // The id's own row when it has one. taxa_lookup also maps a subspecies or variety id to
        // its species' row, so a species downloaded again would hide the subspecies' own old row.
        // Without its own row (the API answered with a different root id), fall back to the lookup.
        var downloadedAt = cacheStore.GetTaxaDownloadedAtByRoot(sisId) ?? cacheStore.GetTaxaDownloadedAt(sisId);
        if (downloadedAt is null) {
            return true;
        }

        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value;
    }

    internal static async Task<DownloadOutcome> DownloadSingleAsync(IucnApiClient apiClient, IucnApiCacheStore cacheStore, long sisId, CancellationToken cancellationToken) {
        var url = $"/api/v4/taxa/sis/{sisId}";
        var importId = cacheStore.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();

        try {
            var response = await apiClient.GetTaxaSisAsync(sisId, cancellationToken).ConfigureAwait(false);
            var parsed = IucnTaxaJsonParser.Parse(response.Body);
            // Single transaction: a crash can't leave the taxa row written but its lookups/backlog stale.
            cacheStore.WriteTaxonAtomic(parsed.RootSisId, importId, response.Body, DateTime.UtcNow, parsed.Mappings, parsed.Assessments);
            cacheStore.ClearFailedRequest("taxa_sis", sisId);
            cacheStore.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return DownloadOutcome.Success;
        }
        catch (IucnApiException ex) when (ex.StatusCode == HttpStatusCode.NotFound) {
            // Expected: no standalone taxon record (an unassessed infraspecific taxon listed only in
            // its parent's taxonomy, or a removed species). Tombstone with a far-future retry so it
            // isn't re-probed every run; the caller summarises these rather than logging each.
            cacheStore.RecordFailedRequest("taxa_sis", sisId, ex.Message, (int?)ex.StatusCode, IucnApiCacheStore.PermanentRetryDelay);
            cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            return DownloadOutcome.NotFound;
        }
        catch (IucnApiException ex) {
            cacheStore.RecordFailedRequest("taxa_sis", sisId, ex.Message, (int?)ex.StatusCode);
            cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download SIS {sisId}: {Markup.Escape(ex.Message)}[/]");
            return DownloadOutcome.Failed;
        }
        catch (Exception ex) {
            cacheStore.RecordFailedRequest("taxa_sis", sisId, ex.Message, null);
            cacheStore.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error for SIS {sisId}: {Markup.Escape(ex.Message)}[/]");
            return DownloadOutcome.Failed;
        }
    }
}

// Outcome of a single taxon/assessment download. NotFound (a 404) is treated as expected — the
// SIS/assessment id has no standalone record (an unassessed infraspecific taxon, or a removed
// taxon) — so it's tombstoned and reported separately from a real Failed.
internal enum DownloadOutcome {
    Success,
    NotFound,
    Failed,
}
