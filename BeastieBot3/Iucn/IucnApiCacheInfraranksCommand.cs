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
    [Description("Download all discovered infraspecific taxa, even those already cached.")]
    public bool Force { get; init; }

    [CommandOption("--from-csv")]
    [Description("Also seed infraspecific taxa from the CSV database, catching assessed subspecies/varieties whose parent species is unassessed (so they never surface via the API's infrarank_taxa). Needs the CSV import; this is the only way to reach those ~0.2% of taxa.")]
    public bool FromCsv { get; init; }

    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database used by --from-csv (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? SourceDatabase { get; init; }

    [CommandOption("--dry-run")]
    [Description("Report the infraspecific SIS IDs that would be downloaded without downloading anything.")]
    public bool DryRun { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Refresh cache entries older than the supplied age (forces download for stale entries).")]
    public double? MaxAgeHours { get; init; }
}

[CommandInfo("iucn api cache-infraranks", CommandKind.Mutates,
    "Fetch the subspecies/varieties of cached species so the API projection (--dataset api) includes infraspecific taxa. The list comes from each cached species' taxon.infrarank_taxa (its subspecies/varieties), so run cache-taxa/discover-by-family first; then run cache-assessments + iucn api project-view. Note: an assessed subspecies whose parent species is unassessed can't be discovered this way (only via its CSV-known sis_id).",
    Reason = "Downloads infraspecific taxa + their assessment backlog into the API cache (idempotent additive).",
    Rerun = RerunEffect.Discovers,
    RerunNote = "Fetches infraspecific taxa not yet cached, read from cached species' infrarank_taxa (use --dry-run to preview, --force to re-download). Follow with cache-assessments + iucn api project-view. Skips ids previously 404'd (no standalone record).",
    Examples = new[] {
        "iucn api cache-infraranks --dry-run",
        "iucn api cache-infraranks",
        "iucn api cache-infraranks --limit 100 --force"
    })]
public sealed class IucnApiCacheInfraranksCommand : AsyncCommand<IucnApiCacheInfraranksSettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, IucnApiCacheInfraranksSettings settings, CancellationToken cancellationToken) {
        _ = context;

        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);
        using var cacheStore = IucnApiCacheStore.Open(cachePath);

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);
        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;

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

        var queue = new List<long>();
        var seen = candidateSet;
        foreach (var sisId in candidateIds) {
            if (settings.Force || ShouldDownloadInfrarank(cacheStore, sisId, refreshThreshold)) {
                queue.Add(sisId);
            }
        }

        if (settings.Limit.HasValue && queue.Count > settings.Limit.Value) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Limiting to {settings.Limit.Value:N0} of {queue.Count:N0} infraspecific taxa.[/]");
            queue = queue.GetRange(0, (int)settings.Limit.Value);
        }

        AnsiConsole.MarkupLineInterpolated($"[grey]Infraspecific taxa discovered:[/] {seen.Count:N0}   [grey]to download:[/] {queue.Count:N0}");

        if (queue.Count == 0) {
            AnsiConsole.MarkupLine("[green]Nothing to download. All discovered infraspecific taxa are already cached.[/]");
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
                $"[grey]No standalone record (404):[/] {notFound:N0} — these infraspecific taxa aren't independently assessed; tombstoned so they aren't re-probed.");
        }
        AnsiConsole.MarkupLineInterpolated($"[red]Failed:[/] {failures:N0}");
        AnsiConsole.MarkupLine("[grey]Next:[/] [yellow]iucn api cache-assessments[/] to download their assessments, then [yellow]iucn api project-view[/].");

        // 404s are expected (unassessed infraranks) — only genuine failures make the run non-zero.
        return failures == 0 ? 0 : -1;
    }

    // An infrarank sis_id maps through taxa_lookup to its PARENT species' taxa record, so the
    // shared (lookup-based) ShouldDownload would always see it as cached. Check the taxon's own
    // record instead: do we have a taxa row whose root_sis_id is this infrarank sis_id? Also skip
    // ids previously tombstoned as 404 (no standalone record) so they aren't re-probed each run.
    private static bool ShouldDownloadInfrarank(IucnApiCacheStore cacheStore, long sisId, DateTime? refreshThreshold) {
        if (cacheStore.HasPermanentFailure("taxa_sis", sisId)) {
            return false;
        }
        var downloadedAt = cacheStore.GetTaxaDownloadedAtByRoot(sisId);
        if (downloadedAt is null) {
            return true;
        }
        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value;
    }
}
