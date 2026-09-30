using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Fetches the infraspecific taxa (subspecies/varieties) of the species already in the local
// cache. The discovery source is each cached species' own /taxa/sis payload: its taxon.infrarank_taxa
// array lists its subspecies/varieties (their sis_ids), which cache-taxa/discover-by-family recorded
// in taxa_lookup with scope='infrarank'. An infraspecific taxon's *assessments* are NOT in the
// parent payload, so each needs its own /api/v4/taxa/sis/{id} fetch — this command does that, upserts
// the infra taxon, and queues its assessments to the backlog. Then `iucn api cache-assessments`
// downloads them and `iucn api project-view` includes the subspecies/varieties in --dataset api.
//
// Discovery is API-native (no CSV) but only as complete as the cached species: it CANNOT find an
// assessed subspecies whose parent species is itself unassessed (~0.2% of CSV taxa), because such a
// parent appears in neither the CSV species list nor the family-page listings, so its infrarank_taxa
// is never seen. Those are only reachable by their sis_id (which only the CSV enumerates).

namespace BeastieBot3.Iucn;

public sealed class IucnApiCacheInfraranksSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of infraspecific taxa to download (mostly for testing).")]
    public long? Limit { get; init; }

    [CommandOption("--force")]
    [Description("Request every subspecies and variety from the API again, including those already cached and those reported as not found (HTTP 404) on an earlier run.")]
    public bool Force { get; init; }

    [CommandOption("--from-csv")]
    [Description("Also download the subspecies and varieties listed in the IUCN Red List database (needs `iucn import`). Without this option, the command finds only subspecies and varieties of species already in the IUCN API cache, so it misses those whose parent species IUCN has not assessed.")]
    public bool FromCsv { get; init; }

    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database used by --from-csv (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? SourceDatabase { get; init; }

    [CommandOption("--dry-run")]
    [Description("Download nothing, and print how many subspecies and varieties a real run would download, with their SIS IDs when there are 50 or fewer. The count takes the other options into account, such as --limit, --force and --from-csv.")]
    public bool DryRun { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Refresh cache entries older than the supplied age (forces download for stale entries).")]
    public double? MaxAgeHours { get; init; }

    [CommandOption("--refresh-before <DATE>")]
    [Description("Re-download anything fetched before this fixed date (UTC, e.g. 2026-06-16). Taken from the refresh in progress when omitted.")]
    public string? RefreshBefore { get; init; }
}

[CommandInfo("iucn api cache-infraranks", CommandKind.Mutates,
    "Downloads taxon records for the subspecies and varieties of species already in the IUCN API cache. Run `iucn api cache-taxa` or `iucn api discover-by-family` first. Afterwards, run `iucn api cache-assessments` and then `iucn api project-view` so that lists made with --dataset api include subspecies and varieties.",
    Reason = "Downloads infraspecific taxa + their assessment backlog into the API cache (idempotent additive).",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "A re-run skips any subspecies and varieties that the API reported as not found (HTTP 404) on an earlier run. With --force ticked, the command requests every subspecies and variety again, including the ones reported as not found. Afterwards, run iucn api cache-assessments, then iucn api project-view.",
    Examples = new[] {
        "iucn api cache-infraranks --dry-run",
        "iucn api cache-infraranks",
        "iucn api cache-infraranks --limit 100 --force"
    })]
public sealed class IucnApiCacheInfraranksCommand : AsyncCommand<IucnApiCacheInfraranksSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, IucnApiCacheInfraranksSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    // Callable entry point so the cache-all wrapper can chain this phase.
    internal static async Task<int> RunAsync(IucnApiCacheInfraranksSettings settings, CancellationToken cancellationToken) {

        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        using var cacheStore = IucnApiCacheStore.Open(cachePath);

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);

        // The cutoff comes from this run's flags, or from the refresh in progress.
        var plan = IucnRefreshRun.Begin(cacheStore, settings.RefreshBefore, settings.MaxAgeHours);
        if (plan is null) return -1;
        var refreshThreshold = plan.Threshold;

        // Discovery source 1 (API-native): infraspecific SIS ids surfaced by cached species'
        // taxon.infrarank_taxa. Source 2 (--from-csv): infraspecific taxonIds from the CSV, which
        // also reaches assessed subspecies whose parent species is unassessed (not API-discoverable).
        var candidateIds = new List<long>();
        var candidateSet = new HashSet<long>();
        foreach (var (sisId, _) in cacheStore.GetInfrarankSisIds()) {
            if (candidateSet.Add(sisId)) candidateIds.Add(sisId);
        }
        var apiDiscovered = candidateSet.Count;

        var fromCsvCount = 0;
        if (settings.FromCsv) {
            var sourcePath = paths.ResolveIucnDatabasePath(settings.SourceDatabase);
            if (!File.Exists(sourcePath)) {
                AnsiConsole.MarkupLineInterpolated($"[red]CSV database not found for --from-csv:[/] {sourcePath}");
                return 1;
            }
            AnsiConsole.MarkupLineInterpolated($"[grey]Seeding infraspecific taxa from CSV:[/] {sourcePath}");
            var provider = new IucnSisIdProvider(sourcePath);
            foreach (var sisId in provider.ReadInfraspecificSisIds(null, cancellationToken)) {
                if (candidateSet.Add(sisId)) { candidateIds.Add(sisId); fromCsvCount++; }
            }
        }

        if (candidateSet.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No infraspecific taxa discovered.[/] Cache species first with [yellow]iucn api cache-taxa[/] or [yellow]iucn api discover-by-family[/] (their taxon.infrarank_taxa is the discovery source), or pass [yellow]--from-csv[/] to seed from the CSV import.");
            return 0;
        }
        if (settings.FromCsv) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Candidates:[/] {apiDiscovered:N0} from cached species + {fromCsvCount:N0} new from CSV = {candidateSet.Count:N0}");
        }

        var sorted = BuildQueue(candidateIds, sisId => Classify(cacheStore, sisId, refreshThreshold), settings.Force);
        var queue = sorted.Queue;
        var queuedBeforeLimit = queue.Count;

        if (settings.Limit.HasValue && queue.Count > settings.Limit.Value) {
            var take = (int)Math.Max(0, settings.Limit.Value);
            AnsiConsole.MarkupLineInterpolated($"[grey]Limiting to {take:N0} of {queue.Count:N0} infraspecific taxa.[/]");
            queue = queue.GetRange(0, take);
        }

        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Infraspecific taxa discovered:[/] {candidateSet.Count:N0}   [grey]already cached:[/] {sorted.AlreadyCached:N0}   [grey]not found (HTTP 404) on an earlier run:[/] {sorted.NotFoundEarlier:N0}   [grey]to download:[/] {queue.Count:N0}");
        if (settings.Force && queue.Count > 0 && sorted.AlreadyCached + sorted.NotFoundEarlier > 0) {
            AnsiConsole.MarkupLine("[grey]--force is set, so the command also requests the taxa already cached and the taxa not found (HTTP 404) on an earlier run.[/]");
        }

        if (queue.Count == 0) {
            // A --limit of 0 or less empties a non-empty queue; the "Limiting to" line has said so.
            if (queuedBeforeLimit > 0) {
                return 0;
            }
            if (sorted.NotFoundEarlier == 0) {
                AnsiConsole.MarkupLine("[green]Nothing to download. All discovered infraspecific taxa are already cached.[/]");
            } else {
                AnsiConsole.MarkupLineInterpolated(
                    $"[green]Nothing to download.[/] {sorted.AlreadyCached:N0} infraspecific taxa are already cached. The API reported {sorted.NotFoundEarlier:N0} as not found (HTTP 404) on an earlier run; use --force to request them again.");
            }
            return 0;
        }

        if (settings.DryRun) {
            AnsiConsole.MarkupLine("[yellow]Dry run — no downloads performed.[/]");
            if (queue.Count <= 50) {
                AnsiConsole.MarkupLine("[grey]Infraspecific SIS IDs:[/]");
                foreach (var sisId in queue) {
                    AnsiConsole.MarkupLine($"  {sisId}");
                }
            }
            return 0;
        }

        // Created only now, so a dry run works without IUCN_API_TOKEN.
        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);

        var downloaded = 0;
        var notFound = 0;
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading infraspecific taxa", queue.Count, async progress => {
            foreach (var sisId in queue) {
                cancellationToken.ThrowIfCancellationRequested();

                switch (await IucnApiCacheTaxaCommand.DownloadSingleAsync(apiClient, cacheStore, sisId, cancellationToken).ConfigureAwait(false)) {
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

        AnsiConsole.MarkupLineInterpolated($"[green]Downloaded:[/] {downloaded:N0}");
        if (notFound > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]Not found (HTTP 404):[/] {notFound:N0}. The API has no record for these infraspecific taxa, usually because IUCN has not assessed them separately. Later runs skip them unless --force is given.");
        }
        AnsiConsole.MarkupLineInterpolated($"[red]Failed:[/] {failures:N0}");
        AnsiConsole.MarkupLine("[grey]Next:[/] [yellow]iucn api cache-assessments[/] to download their assessments, then [yellow]iucn api project-view[/].");

        // 404s are expected (unassessed infraranks) — only genuine failures make the run non-zero.
        return failures == 0 ? 0 : -1;
    }

    internal enum InfrarankCandidate { Download, AlreadyCached, NotFoundEarlier }

    internal sealed record InfrarankQueue(List<long> Queue, int AlreadyCached, int NotFoundEarlier);

    // Counts each candidate by its Classify category, and queues the Download ones, or every
    // candidate under --force. The counts ignore --force, so a --force run still reports how many
    // candidates are already cached and how many the API reported as not found on an earlier run.
    internal static InfrarankQueue BuildQueue(IEnumerable<long> candidateIds, Func<long, InfrarankCandidate> classify, bool force) {
        var queue = new List<long>();
        var alreadyCached = 0;
        var notFoundEarlier = 0;
        foreach (var sisId in candidateIds) {
            var kind = classify(sisId);
            if (force || kind == InfrarankCandidate.Download) {
                queue.Add(sisId);
            }
            if (kind == InfrarankCandidate.AlreadyCached) {
                alreadyCached++;
            } else if (kind == InfrarankCandidate.NotFoundEarlier) {
                notFoundEarlier++;
            }
        }
        return new InfrarankQueue(queue, alreadyCached, notFoundEarlier);
    }

    // An infrarank sis_id maps through taxa_lookup to its PARENT species' taxa record, so the
    // shared (lookup-based) ShouldDownload would always see it as cached. Check the taxon's own
    // record instead: do we have a taxa row whose root_sis_id is this infrarank sis_id? Ids
    // tombstoned as 404 (no standalone record) are skipped so they aren't re-probed each run, even
    // under a refresh cutoff; only --force (in BuildQueue) requests them again. The three outcomes
    // are counted separately so the "nothing to download" line doesn't call a 404'd candidate cached.
    internal static InfrarankCandidate Classify(IucnApiCacheStore cacheStore, long sisId, DateTime? refreshThreshold) {
        if (cacheStore.HasPermanentFailure("taxa_sis", sisId)) {
            return InfrarankCandidate.NotFoundEarlier;
        }
        var downloadedAt = cacheStore.GetTaxaDownloadedAtByRoot(sisId);
        if (downloadedAt is null) {
            return InfrarankCandidate.Download;
        }
        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value
            ? InfrarankCandidate.Download
            : InfrarankCandidate.AlreadyCached;
    }
}
