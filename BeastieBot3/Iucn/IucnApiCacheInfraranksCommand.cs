using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Fetches the infraspecific taxa (subspecies/varieties) discovered in cached species'
// taxon.infrarank_taxa (recorded in taxa_lookup with scope='infrarank'). Their assessments
// are NOT included in the parent species payload — each needs its own /api/v4/taxa/sis/{id}
// fetch, which upserts the infra taxon and queues its assessments to the backlog. After this,
// `iucn api cache-assessments` downloads those assessments and `iucn api project-view` includes
// the subspecies/varieties in the --dataset api projection. Discovery is API-native: it works on
// whatever species are cached (from cache-taxa or discover-by-family), no CSV reference needed.

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
    "Fetch the subspecies/varieties (taxon.infrarank_taxa) discovered in cached species and queue their assessments, so the API projection (--dataset api) includes infraspecific taxa. Run after cache-taxa/discover-by-family, then cache-assessments + iucn api project-view.",
    Reason = "Downloads infraspecific taxa + their assessment backlog into the API cache (idempotent additive).",
    Rerun = RerunEffect.Discovers,
    RerunNote = "Fetches infraspecific taxa not yet cached (use --dry-run to preview, --force to re-download). Follow with cache-assessments + iucn api project-view.",
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

        // Infraspecific SIS ids surfaced by cached species (taxon.infrarank_taxa).
        var candidates = cacheStore.GetInfrarankSisIds();
        if (candidates.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No infraspecific taxa discovered.[/] Cache species first with [yellow]iucn api cache-taxa[/] or [yellow]iucn api discover-by-family[/] — their taxon.infrarank_taxa is the discovery source.");
            return 0;
        }

        var queue = new List<long>();
        var seen = new HashSet<long>();
        foreach (var (sisId, _) in candidates) {
            if (!seen.Add(sisId)) continue;
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
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading infraspecific taxa", queue.Count, async progress => {
            foreach (var sisId in queue) {
                cancellationToken.ThrowIfCancellationRequested();

                if (await IucnApiCacheTaxaCommand.DownloadSingleAsync(apiClient, cacheStore, sisId, cancellationToken).ConfigureAwait(false)) {
                    downloaded++;
                }
                else {
                    failures++;
                }

                if (sleep > 0) {
                    await Task.Delay(sleep, cancellationToken).ConfigureAwait(false);
                }

                progress.Increment(1);
            }
        }, cancellationToken).ConfigureAwait(false);

        AnsiConsole.MarkupLineInterpolated($"[green]Downloaded:[/] {downloaded:N0}");
        AnsiConsole.MarkupLineInterpolated($"[red]Failed:[/] {failures:N0}");
        AnsiConsole.MarkupLine("[grey]Next:[/] [yellow]iucn api cache-assessments[/] to download their assessments, then [yellow]iucn api project-view[/].");

        return failures == 0 ? 0 : -1;
    }

    // An infrarank sis_id maps through taxa_lookup to its PARENT species' taxa record, so the
    // shared (lookup-based) ShouldDownload would always see it as cached. Check the taxon's own
    // record instead: do we have a taxa row whose root_sis_id is this infrarank sis_id?
    private static bool ShouldDownloadInfrarank(IucnApiCacheStore cacheStore, long sisId, DateTime? refreshThreshold) {
        var downloadedAt = cacheStore.GetTaxaDownloadedAtByRoot(sisId);
        if (downloadedAt is null) {
            return true;
        }
        return refreshThreshold.HasValue && downloadedAt.Value < refreshThreshold.Value;
    }
}
