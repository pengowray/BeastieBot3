using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Step 2 of API caching: downloads /api/v4/assessment/{id} for each assessment_id
// found in previously cached taxa JSON. Stores responses in the assessments table.
// Taxa must be cached first (IucnApiCacheTaxaCommand). Assessment JSON includes
// conservation measures, threats, habitat, and population trend details not in CSV.
//
// Three queues, one per run:
//   default        the backlog (assessment ids listed in cached taxon records); during a refresh,
//                  also payloads no taxon record lists that are older than the cutoff
//   --csv-missing  assessment ids in the CSV export with no cached payload (subpopulations,
//                  which no taxon record lists, and taxa whose own record answers 404)
//   --stale-latest payloads whose latest flag disagrees with their taxon record's header and
//                  that were downloaded before that record
// Run via: iucn api cache-assessments

namespace BeastieBot3.Iucn;

public sealed class IucnApiCacheAssessmentsSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs). Used only with --csv-missing.")]
    public string? SourceDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of assessments processed (mostly for testing).")]
    public long? Limit { get; init; }

    [CommandOption("--force")]
    [Description("Force download even if we already have cached JSON.")]
    public bool Force { get; init; }

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Download cached assessments again if they were downloaded more than this many hours before the run started. The cutoff moves with each run, so a refresh spread over several runs downloads some assessments twice; for a fixed cutoff date, use --refresh-before or `iucn api refresh-start`.")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--refresh-before <DATE>")]
    [Description("Download cached assessments again if they were downloaded before this date: a UTC date such as 2026-06-16, a full timestamp, or the word now. If you stop the command and run it again with the same date, it continues with the assessments it has not re-downloaded yet. If this option and --max-age-hours are both left empty, the command uses the cutoff date of the refresh in progress (started with `iucn api refresh-start`); with no refresh in progress, it downloads only assessments that are not in the cache yet.")]
    public string? RefreshBefore { get; init; }

    [CommandOption("--retry-tombstones")]
    [Description("Also re-check assessments the API previously said were gone (404), and the ones it keeps erroring on. Skipped by default.")]
    public bool RetryTombstones { get; init; }

    [CommandOption("--failed-only")]
    [Description("Only retry items that previously failed (skip the backlog queue).")]
    public bool FailedOnly { get; init; }

    [CommandOption("--csv-missing")]
    [Description("Instead of the queue, download every assessment in the IUCN CSV export that is not in the API cache. Most of these are subpopulation assessments: no cached taxon record lists them, so they are never queued. Assessments the API answered 404 for are left out unless --retry-tombstones or --force is given.")]
    public bool CsvMissing { get; init; }

    [CommandOption("--stale-latest")]
    [Description("Instead of the queue, download again each cached assessment whose latest flag (whether it is the current assessment) differs from the flag for the same assessment in its taxon record, when the cached assessment was downloaded before the taxon record. Reads every cached assessment, which can take a minute. Assessments the API answered 404 for are left out unless --retry-tombstones or --force is given.")]
    public bool StaleLatest { get; init; }

    [CommandOption("--latest-only")]
    [Description("Download only current assessments, which are about half of all assessments. Current assessments are enough for lists and charts made with --dataset api, because the IUCN API projection uses only current assessments. Historical assessments stay in the queue until a later run without --latest-only downloads them.")]
    public bool LatestOnly { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;
}

[CommandInfo("iucn api cache-assessments", CommandKind.Mutates,
    "Download queued assessments into the IUCN API cache. An assessment is queued when cache-taxa, discover-by-family or cache-infraranks downloads its taxon's record, so run one of those commands first, or run cache-all, which runs cache-taxa and then cache-assessments. --csv-missing downloads the assessments in the IUCN CSV export that no taxon record lists, such as subpopulation assessments.",
    Reason = "Downloads IUCN /api/v4/assessment payloads into the local cache (idempotent additive; --force re-downloads already-cached assessments).",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Downloads queued assessments that are not in the IUCN API cache yet, current assessments first and then historical assessments, newest first. Failed downloads are retried last, after a delay that doubles from 5 minutes up to 3 days, and assessments reported as not found (404) are skipped unless --retry-tombstones or --force is given. --latest-only downloads only current assessments and leaves historical ones for a later run; --force downloads every queued assessment, including those already cached or reported as not found. "
        + RerunNotes.DuringIucnRefreshPrefix + "every cached assessment downloaded before the refresh's cutoff date, including those no taxon record lists. "
        + "With --csv-missing, a run downloads only the assessments in the IUCN CSV export that are not cached yet. With --stale-latest, a run downloads again only the cached assessments whose latest flag disagrees with their taxon record and that were downloaded before the taxon record.",
    Examples = new[] {
        "iucn api cache-assessments",
        "iucn api cache-assessments --latest-only",
        "iucn api cache-assessments --limit 100",
        "iucn api cache-assessments --failed-only",
        "iucn api cache-assessments --csv-missing",
        "iucn api cache-assessments --stale-latest"
    })]
public sealed class IucnApiCacheAssessmentsCommand : AsyncCommand<IucnApiCacheAssessmentsSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, IucnApiCacheAssessmentsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(IucnApiCacheAssessmentsSettings settings, CancellationToken cancellationToken) {
        var conflict = QueueOptionConflict(settings);
        if (conflict is not null) {
            AnsiConsole.MarkupLineInterpolated($"[red]{conflict}[/]");
            return -1;
        }

        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        string? csvPath = null;
        if (settings.CsvMissing) {
            csvPath = TryResolveCsvDatabase(paths, settings.SourceDatabase, out var problem);
            if (csvPath is null) {
                AnsiConsole.MarkupLineInterpolated($"[red]{problem}[/]");
                return -1;
            }
            AnsiConsole.MarkupLine($"[grey]Source CSV database:[/] {Markup.Escape(csvPath)}");
        }

        using var cacheStore = IucnApiCacheStore.Open(cachePath);
        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);

        List<AssessmentQueueRow> queue;
        DateTime? refreshThreshold = null;
        if (settings.CsvMissing) {
            var provider = new IucnSisIdProvider(csvPath!);
            var csvQueue = BuildCsvMissingQueue(cacheStore, provider.ReadAssessmentIds(cancellationToken), settings);
            PrintCsvMissingSummary(csvQueue);
            queue = csvQueue.Rows;
        }
        else if (settings.StaleLatest) {
            AnsiConsole.MarkupLine("[grey]Comparing the latest flag of every cached assessment with its taxon record...[/]");
            var staleQueue = BuildStaleLatestQueue(cacheStore, settings);
            PrintStaleLatestSummary(staleQueue);
            queue = staleQueue.Rows;
        }
        else {
            // The cutoff comes from this run's flags, or from the refresh in progress.
            var plan = IucnRefreshRun.Begin(cacheStore, settings.RefreshBefore, settings.MaxAgeHours);
            if (plan is null) return -1;
            refreshThreshold = plan.Threshold;
            queue = BuildAssessmentQueue(cacheStore, refreshThreshold, settings);
        }

        if (queue.Count == 0) {
            AnsiConsole.MarkupLine(settings.CsvMissing || settings.StaleLatest
                ? "[green]Nothing to download.[/]"
                : "[green]Nothing to do. Assessment backlog is empty or all entries are up to date.[/]");
            return 0;
        }

        // The --csv-missing and --stale-latest queues hold only assessments to download. A stale
        // payload is cached and usually newer than any cutoff, so the usual skip test would drop it.
        var downloadEvery = settings.Force || settings.StaleLatest;

        // Skip already-cached/fresh. (Backed-off / tombstoned failures were already excluded from
        // the queue, so there's no per-item failed_requests lookup here.) Decided before the
        // progress bar starts, so its total and time estimate count downloads only (see
        // IucnDownloadQueueSummary); the queue holds the whole backlog, mostly cached.
        var due = downloadEvery ? queue : queue.Where(item => ShouldDownload(item.DownloadedAt, refreshThreshold)).ToList();
        var skipped = queue.Count - due.Count;
        if (skipped > 0) {
            var summary = IucnDownloadQueueSummary.Describe("Assessments", queue.Count, due.Count, upToDate: skipped, notFoundEarlier: 0, refreshThreshold);
            if (due.Count == 0) {
                AnsiConsole.MarkupLineInterpolated($"[green]Nothing to download.[/] {summary}");
                return 0;
            }
            AnsiConsole.MarkupLineInterpolated($"[grey]{summary}[/]");
        }

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);
        var downloaded = 0;
        var notFound = 0;
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading assessments", due.Count, async progress => {
            foreach (var item in due) {
                cancellationToken.ThrowIfCancellationRequested();

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

    // One queue per run. --refresh-before and --max-age-hours choose which cached copies of queued
    // assessments are out of date; the two other queues have their own rule, so the cutoff
    // options would do nothing there.
    internal static string? QueueOptionConflict(IucnApiCacheAssessmentsSettings settings) {
        var queues = new List<string>(3);
        if (settings.FailedOnly) queues.Add("--failed-only");
        if (settings.CsvMissing) queues.Add("--csv-missing");
        if (settings.StaleLatest) queues.Add("--stale-latest");
        if (queues.Count > 1) {
            return $"Use only one of these options in a run: {string.Join(", ", queues)}.";
        }

        if (settings.CsvMissing || settings.StaleLatest) {
            var cutoff = !string.IsNullOrWhiteSpace(settings.RefreshBefore) ? "--refresh-before"
                : settings.MaxAgeHours is not null ? "--max-age-hours"
                : null;
            if (cutoff is not null) {
                return $"{cutoff} cannot be used with {queues[0]}.";
            }
        }
        return null;
    }

    // The configured CSV database, or null with the reason it can't be used.
    internal static string? TryResolveCsvDatabase(PathsService paths, string? overridePath, out string? problem) {
        string path;
        try {
            path = paths.ResolveIucnDatabasePath(overridePath, "--source-db");
        } catch (InvalidOperationException ex) {
            problem = ex.Message;
            return null;
        }
        if (!File.Exists(path)) {
            problem = $"IUCN CSV database not found: {path}";
            return null;
        }
        problem = null;
        return path;
    }

    // ---- the default queue ----------------------------------------------------------------

    internal static List<AssessmentQueueRow> BuildAssessmentQueue(IucnApiCacheStore cacheStore, DateTime? refreshThreshold, IucnApiCacheAssessmentsSettings settings) {
        var snapshot = cacheStore.GetAssessmentBacklogOrdered();   // latest DESC, year DESC; one row per assessment
        var due = new HashSet<long>(cacheStore.GetFailedEntityIds("assessment"));         // failures due for retry now
        var suppressed = new HashSet<long>(cacheStore.GetSuppressedEntityIds("assessment")); // backed-off / 404 — skip

        bool PassesLatest(AssessmentQueueRow r) => !settings.LatestOnly || r.Latest;

        if (settings.FailedOnly) {
            // Only the failures that are due for retry (in backlog order; stubs for any not in the backlog).
            var lookup = new Dictionary<long, AssessmentQueueRow>(snapshot.Count);
            foreach (var row in snapshot) lookup[row.AssessmentId] = row;

            var failedQueue = new List<AssessmentQueueRow>();
            foreach (var id in due) {
                if (lookup.TryGetValue(id, out var row)) {
                    if (PassesLatest(row)) failedQueue.Add(row);
                }
                else if (!settings.LatestOnly) {
                    failedQueue.Add(new AssessmentQueueRow(id, 0, 0, false, null, cacheStore.GetAssessmentDownloadedAt(id)));
                }
            }
            return ApplyLimit(failedQueue, settings.Limit);
        }

        // Default: walk the backlog (latest-first); skip entities that are backed off (not yet due to
        // retry), and defer the currently-due failures to the END so a persistently-broken record
        // doesn't slow the start. --latest-only drops historical assessments entirely.
        var main = new List<AssessmentQueueRow>();
        var deferred = new List<AssessmentQueueRow>();
        var queued = new HashSet<long>();
        void Consider(AssessmentQueueRow row) {
            if (!PassesLatest(row)) return;
            if (!settings.Force && !settings.RetryTombstones && suppressed.Contains(row.AssessmentId)) return;
            if (!queued.Add(row.AssessmentId)) return;
            if (due.Contains(row.AssessmentId)) deferred.Add(row);
            else main.Add(row);
        }

        foreach (var row in snapshot) Consider(row);

        // Payloads no taxon record lists (downloaded with --csv-missing). A refresh counts every
        // payload older than its cutoff, and the backlog above never reaches these, so without
        // them here the refresh would never close. --latest-only goes by the payload's own flag,
        // because no taxon record says otherwise.
        if (refreshThreshold is { } threshold) {
            foreach (var row in cacheStore.GetAssessmentsNoTaxonRecordLists()) {
                if (row.DownloadedAt is { } at && at < threshold) Consider(row);
            }
        }

        // The tombstone re-check also asks about ids no taxon record lists, which the backlog walk
        // above cannot reach: a CSV assessment that got a 404 from --csv-missing, for example. A
        // 404 is only true of the release it was recorded against.
        if (settings.RetryTombstones && !settings.LatestOnly) {
            var listed = new HashSet<long>(snapshot.Select(r => r.AssessmentId));
            foreach (var id in cacheStore.GetTombstonedEntityIds("assessment")) {
                if (listed.Contains(id) || queued.Contains(id)) continue;
                Consider(new AssessmentQueueRow(id, 0, 0, false, null, cacheStore.GetAssessmentDownloadedAt(id)));
            }
        }

        main.AddRange(deferred);
        return ApplyLimit(main, settings.Limit);
    }

    // ---- --csv-missing ----------------------------------------------------------------------

    internal sealed record CsvMissingQueue(List<AssessmentQueueRow> Rows, long CsvAssessments, long NotCached, QueueLeftOut LeftOut);

    // Every CSV assessment id with no cached payload, in ascending id order. All CSV assessments are
    // current ones, so they are queued as latest. The cutoff options don't apply: a cached copy
    // older than a refresh's cutoff is re-downloaded by the default queue.
    internal static CsvMissingQueue BuildCsvMissingQueue(IucnApiCacheStore cacheStore, IEnumerable<long> csvAssessmentIds, IucnApiCacheAssessmentsSettings settings) {
        var cached = cacheStore.GetCachedAssessmentIds();
        var candidates = new List<AssessmentQueueRow>();
        long csvCount = 0;
        foreach (var id in csvAssessmentIds) {
            csvCount++;
            if (!cached.Contains(id)) {
                candidates.Add(new AssessmentQueueRow(id, 0, 0, true, null, null));
            }
        }
        candidates.Sort((a, b) => a.AssessmentId.CompareTo(b.AssessmentId));

        var (rows, leftOut) = FilterCandidates(cacheStore, candidates, settings);
        return new CsvMissingQueue(rows, csvCount, candidates.Count, leftOut);
    }

    private static void PrintCsvMissingSummary(CsvMissingQueue q) {
        AnsiConsole.MarkupLineInterpolated(
            $"IUCN CSV export: {q.CsvAssessments:N0} assessments, of which {q.NotCached:N0} are not in the API cache.");
        PrintLeftOut(q.LeftOut);
    }

    // ---- --stale-latest ---------------------------------------------------------------------

    internal sealed record StaleLatestQueue(
        List<AssessmentQueueRow> Rows,
        long Disagreeing,
        long OlderThanTaxonRecord,
        IReadOnlyList<StaleLatestRow> NewerThanTaxonRecord,
        QueueLeftOut LeftOut);

    // A payload whose latest flag disagrees with its taxon record is out of date only if it was
    // downloaded before that record: IUCN publishes a new assessment, the taxon record downloaded
    // later says the old one is no longer current, and the old payload still says it is. When the
    // payload is the newer copy, the taxon record is the stale one, and downloading the payload
    // again would change nothing, so a rerun would queue it forever. Those are reported instead.
    internal static StaleLatestQueue BuildStaleLatestQueue(IucnApiCacheStore cacheStore, IucnApiCacheAssessmentsSettings settings) {
        var disagreeing = cacheStore.GetAssessmentsWithDisagreeingLatestFlag();
        var candidates = new List<AssessmentQueueRow>();
        var newer = new List<StaleLatestRow>();
        foreach (var stale in disagreeing) {
            if (IsOlderThanTaxonRecord(stale)) candidates.Add(stale.Row);
            else newer.Add(stale);
        }
        candidates.Sort((a, b) => a.AssessmentId.CompareTo(b.AssessmentId));

        var (rows, leftOut) = FilterCandidates(cacheStore, candidates, settings);
        return new StaleLatestQueue(rows, disagreeing.Count, candidates.Count, newer, leftOut);
    }

    // A payload with no download time is treated as the older copy, so it is downloaded again.
    internal static bool IsOlderThanTaxonRecord(StaleLatestRow stale) =>
        stale.Row.DownloadedAt is not { } payloadAt
        || stale.TaxonDownloadedAt is not { } taxonAt
        || payloadAt < taxonAt;

    private static void PrintStaleLatestSummary(StaleLatestQueue q) {
        AnsiConsole.MarkupLineInterpolated(
            $"Cached assessments whose latest flag disagrees with their taxon record: {q.Disagreeing:N0}");
        AnsiConsole.MarkupLineInterpolated(
            $"  Downloaded before their taxon record (to download again): {q.OlderThanTaxonRecord:N0}");
        PrintLeftOut(q.LeftOut);

        if (q.NewerThanTaxonRecord.Count == 0) return;
        var taxa = q.NewerThanTaxonRecord.Select(r => r.TaxonRootSisId).Distinct().OrderBy(id => id).ToList();
        var shown = string.Join(", ", taxa.Take(20));
        var more = taxa.Count > 20 ? $" and {taxa.Count - 20:N0} more" : "";
        var newestRecord = q.NewerThanTaxonRecord.Max(r => r.TaxonDownloadedAt ?? DateTime.MinValue);
        AnsiConsole.MarkupLineInterpolated(
            $"[yellow]Not queued: {q.NewerThanTaxonRecord.Count:N0} assessments that were downloaded after their taxon record.[/] For these, the taxon record is the out-of-date copy. Taxon records to download again: SIS {shown}{more}.");
        if (newestRecord > DateTime.MinValue) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]`iucn api cache-taxa --refresh-before {CutoffJustAfter(newestRecord)}` downloads these taxon records again, along with every other taxon record downloaded before that time.[/]");
        }
    }

    // A --refresh-before value that includes the given time: the next whole second, in UTC.
    internal static string CutoffJustAfter(DateTime utc) {
        var nextSecond = new DateTime(utc.Ticks - utc.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc).AddSeconds(1);
        return nextSecond.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }

    // ---- shared by --csv-missing and --stale-latest ---------------------------------------------

    /// <summary>Candidates not queued: 404 tombstones, failures still backing off, and (with
    /// --latest-only) historical assessments.</summary>
    internal readonly record struct QueueLeftOut(int NotFound, int WaitingToRetry, int Historical);

    // The same rules as the default queue: --latest-only drops historical assessments; 404s and
    // failures still backing off are left out unless --retry-tombstones or --force asks again; a
    // failure that is due goes to the end, so a record the API keeps failing on comes last.
    private static (List<AssessmentQueueRow> Rows, QueueLeftOut LeftOut) FilterCandidates(
        IucnApiCacheStore cacheStore, IReadOnlyList<AssessmentQueueRow> candidates, IucnApiCacheAssessmentsSettings settings) {
        if (candidates.Count == 0) return (new List<AssessmentQueueRow>(), default);

        var due = new HashSet<long>(cacheStore.GetFailedEntityIds("assessment"));
        var suppressed = new HashSet<long>(cacheStore.GetSuppressedEntityIds("assessment"));
        var tombstoned = new HashSet<long>(cacheStore.GetTombstonedEntityIds("assessment"));
        var askAgain = settings.Force || settings.RetryTombstones;

        var main = new List<AssessmentQueueRow>();
        var deferred = new List<AssessmentQueueRow>();
        int notFound = 0, waiting = 0, historical = 0;
        foreach (var row in candidates) {
            if (settings.LatestOnly && !row.Latest) { historical++; continue; }
            if (!askAgain && suppressed.Contains(row.AssessmentId)) {
                if (tombstoned.Contains(row.AssessmentId)) notFound++;
                else waiting++;
                continue;
            }
            if (due.Contains(row.AssessmentId)) deferred.Add(row);
            else main.Add(row);
        }
        main.AddRange(deferred);
        return (ApplyLimit(main, settings.Limit), new QueueLeftOut(notFound, waiting, historical));
    }

    private static void PrintLeftOut(QueueLeftOut leftOut) {
        if (leftOut.NotFound > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]Not queued: {leftOut.NotFound:N0} assessments the API answered 404 for. Add --retry-tombstones to ask for them again.[/]");
        }
        if (leftOut.WaitingToRetry > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]Not queued: {leftOut.WaitingToRetry:N0} assessments that failed recently and are not due for another try yet. Add --retry-tombstones to try them now.[/]");
        }
        if (leftOut.Historical > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]Not queued: {leftOut.Historical:N0} historical assessments (--latest-only).[/]");
        }
    }

    private static List<AssessmentQueueRow> ApplyLimit(List<AssessmentQueueRow> queue, long? limit) {
        if (!limit.HasValue || limit.Value <= 0 || queue.Count <= limit.Value) {
            return queue;
        }

        var count = (int)Math.Min(limit.Value, queue.Count);
        return queue.GetRange(0, count);
    }

    internal static bool ShouldDownload(DateTime? downloadedAt, DateTime? refreshThreshold) {
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
