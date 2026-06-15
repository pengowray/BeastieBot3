using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Discovers taxa missing from the API cache by iterating IUCN families via
// /api/v4/taxa/family/ and /api/v4/taxa/family/{name}. Useful for finding
// species removed from the Red List or reclassified so they no longer appear
// in the CSV export. Discovered SIS IDs are downloaded via /api/v4/taxa/sis/{id}
// using the same path as cache-taxa. Run via: iucn api discover-by-family

namespace BeastieBot3.Iucn;

public sealed class IucnApiCacheDiscoverByFamilySettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--family <NAME>")]
    [Description("Process only these families (comma-separated). Omit to scan all families from the API.")]
    public string? FamilyFilter { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the total number of SIS IDs to download (mostly for testing).")]
    public long? Limit { get; init; }

    [CommandOption("--force")]
    [Description("Download all discovered taxa, even those already cached.")]
    public bool Force { get; init; }

    [CommandOption("--dry-run")]
    [Description("Report missing SIS IDs without downloading anything.")]
    public bool DryRun { get; init; }

    [CommandOption("--sleep-ms <MS>")]
    [Description("Extra delay between API calls. Defaults to 250ms to avoid throttling.")]
    public int SleepBetweenRequests { get; init; } = 250;

    [CommandOption("--max-age-hours <HOURS>")]
    [Description("Refresh cache entries older than the supplied age (forces download for stale entries).")]
    public double? MaxAgeHours { get; init; }
}

[CommandInfo("iucn api discover-by-family", CommandKind.Mutates,
    "Scan IUCN families via the API to discover taxa missing from the local cache (removed or reclassified species). More complete than cache-taxa, which only fetches SIS ids already present in the CSV export.",
    Reason = "Adds newly-discovered taxa to the cache; --dry-run only inspects.",
    Rerun = RerunEffect.Discovers,
    RerunNote = "Pages every family on the live API to find taxa not yet cached; adds only new ones (use --dry-run to preview).",
    Examples = new[] {
        "iucn api discover-by-family --dry-run",
        "iucn api discover-by-family",
        "iucn api discover-by-family --family Felidae,Canidae",
        "iucn api discover-by-family --force --limit 100"
    })]
public sealed class IucnApiCacheDiscoverByFamilyCommand : AsyncCommand<IucnApiCacheDiscoverByFamilySettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, IucnApiCacheDiscoverByFamilySettings settings, CancellationToken cancellationToken) {
        _ = context;

        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        var configuration = IucnApiConfiguration.FromEnvironment();
        using var apiClient = new IucnApiClient(configuration);
        using var cacheStore = IucnApiCacheStore.Open(cachePath);

        var sleep = Math.Clamp(settings.SleepBetweenRequests, 0, 5_000);

        // Step 1: Fetch the family list
        var families = await FetchFamilyListAsync(apiClient, cacheStore, settings, cancellationToken).ConfigureAwait(false);
        if (families.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No families found from the API.[/]");
            return 0;
        }

        AnsiConsole.MarkupLine($"[grey]Families to scan:[/] {families.Count}");

        // Step 2: Paginate through each family to discover SIS IDs
        var allDiscoveredSisIds = new HashSet<long>();
        var familySummaries = new List<(string Family, int Count, int Missing)>();

        var refreshThreshold = settings.MaxAgeHours is { } hours && hours > 0
            ? DateTime.UtcNow - TimeSpan.FromHours(hours)
            : (DateTime?)null;

        await ProgressConsole.RunAsync("Scanning families", families.Count, async progress => {
            foreach (var family in families) {
                cancellationToken.ThrowIfCancellationRequested();
                progress.Description = $"Scanning {family}";

                var familySisIds = await FetchFamilySisIdsAsync(apiClient, cacheStore, family, sleep, cancellationToken).ConfigureAwait(false);

                var missingCount = 0;
                foreach (var sisId in familySisIds) {
                    if (allDiscoveredSisIds.Add(sisId)) {
                        if (settings.Force || IucnApiCacheTaxaCommand.ShouldDownload(cacheStore, sisId, refreshThreshold)) {
                            missingCount++;
                        }
                    }
                }

                familySummaries.Add((family, familySisIds.Count, missingCount));
                progress.Increment(1);

                if (settings.Limit.HasValue && allDiscoveredSisIds.Count >= settings.Limit.Value) {
                    break;
                }
            }
        }, cancellationToken).ConfigureAwait(false);

        // Build the download queue — only SIS IDs that need downloading
        var downloadQueue = new List<long>();
        foreach (var sisId in allDiscoveredSisIds) {
            if (settings.Force || IucnApiCacheTaxaCommand.ShouldDownload(cacheStore, sisId, refreshThreshold)) {
                downloadQueue.Add(sisId);
            }
        }

        if (settings.Limit.HasValue && downloadQueue.Count > settings.Limit.Value) {
            downloadQueue = downloadQueue.GetRange(0, (int)settings.Limit.Value);
        }

        // Summary
        AnsiConsole.WriteLine();
        var summaryTable = new Table().Border(TableBorder.Rounded);
        summaryTable.AddColumn("Family");
        summaryTable.AddColumn(new TableColumn("Taxa Found").RightAligned());
        summaryTable.AddColumn(new TableColumn("New/Stale").RightAligned());

        // Show top families by missing count, and a summary row for the rest
        var topFamilies = familySummaries.Where(f => f.Missing > 0).OrderByDescending(f => f.Missing).Take(30).ToList();
        var otherFamilies = familySummaries.Where(f => f.Missing > 0).OrderByDescending(f => f.Missing).Skip(30).ToList();

        foreach (var (family, count, missing) in topFamilies) {
            summaryTable.AddRow(Markup.Escape(family), count.ToString(), $"[green]{missing}[/]");
        }

        if (otherFamilies.Count > 0) {
            var otherCount = otherFamilies.Sum(f => f.Count);
            var otherMissing = otherFamilies.Sum(f => f.Missing);
            summaryTable.AddRow($"[grey]({otherFamilies.Count} other families)[/]", otherCount.ToString(), $"[green]{otherMissing}[/]");
        }

        var noMissingCount = familySummaries.Count(f => f.Missing == 0);
        if (noMissingCount > 0) {
            var noMissingTotal = familySummaries.Where(f => f.Missing == 0).Sum(f => f.Count);
            summaryTable.AddRow($"[grey]({noMissingCount} families fully cached)[/]", noMissingTotal.ToString(), "0");
        }

        AnsiConsole.Write(summaryTable);
        AnsiConsole.MarkupLine($"[grey]Total SIS IDs discovered:[/] {allDiscoveredSisIds.Count}");
        AnsiConsole.MarkupLine($"[grey]SIS IDs to download:[/] {downloadQueue.Count}");

        if (downloadQueue.Count == 0) {
            AnsiConsole.MarkupLine("[green]Nothing to download. All discovered taxa are already cached.[/]");
            return 0;
        }

        // Step 3: In dry-run mode, just report
        if (settings.DryRun) {
            AnsiConsole.MarkupLine("[yellow]Dry run — no downloads performed.[/]");
            if (downloadQueue.Count <= 50) {
                AnsiConsole.MarkupLine("[grey]Missing SIS IDs:[/]");
                foreach (var sisId in downloadQueue) {
                    AnsiConsole.MarkupLine($"  {sisId}");
                }
            }

            return 0;
        }

        // Step 4: Download missing taxa
        var downloaded = 0;
        var notFound = 0;
        var failures = 0;

        await ProgressConsole.RunAsync("Downloading missing taxa", downloadQueue.Count, async progress => {
            foreach (var sisId in downloadQueue) {
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

        AnsiConsole.MarkupLine($"[green]Downloaded:[/] {downloaded}");
        if (notFound > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]No record (404):[/] {notFound:N0} (removed/unassessed; tombstoned)");
        }
        AnsiConsole.MarkupLine($"[red]Failed:[/] {failures}");

        // 404s are expected here (discovery surfaces removed/historical taxa) — don't fail the run on them.
        return failures == 0 ? 0 : -1;
    }

    private static async Task<IReadOnlyList<string>> FetchFamilyListAsync(
        IucnApiClient apiClient,
        IucnApiCacheStore cacheStore,
        IucnApiCacheDiscoverByFamilySettings settings,
        CancellationToken cancellationToken) {

        // If the user supplied --family, use that instead of fetching from the API
        if (!string.IsNullOrWhiteSpace(settings.FamilyFilter)) {
            var names = settings.FamilyFilter
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            AnsiConsole.MarkupLine($"[grey]Using family filter:[/] {string.Join(", ", names)}");
            return names;
        }

        AnsiConsole.MarkupLine("[grey]Fetching family list from API...[/]");
        var importId = cacheStore.BeginImport("/api/v4/taxa/family/");
        try {
            var response = await apiClient.GetTaxaFamilyListAsync(cancellationToken).ConfigureAwait(false);
            var families = IucnFamilyResponseParser.ParseFamilyList(response.Body);
            cacheStore.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, TimeSpan.Zero);
            AnsiConsole.MarkupLine($"[grey]API returned {families.Count} families.[/]");
            return families;
        }
        catch (Exception ex) {
            cacheStore.CompleteImportFailure(importId, ex.Message, null, TimeSpan.Zero);
            AnsiConsole.MarkupLine($"[red]Failed to fetch family list: {Markup.Escape(ex.Message)}[/]");
            throw;
        }
    }

    private static async Task<IReadOnlyList<long>> FetchFamilySisIdsAsync(
        IucnApiClient apiClient,
        IucnApiCacheStore cacheStore,
        string familyName,
        int sleepMs,
        CancellationToken cancellationToken) {

        var allSisIds = new HashSet<long>();
        var page = 1;
        var maxPages = 500; // safety limit

        while (page <= maxPages) {
            cancellationToken.ThrowIfCancellationRequested();

            var url = $"/api/v4/taxa/family/{Uri.EscapeDataString(familyName)}?page={page}";
            var importId = cacheStore.BeginImport(url);

            try {
                var response = await apiClient.GetTaxaByFamilyAsync(familyName, page, cancellationToken).ConfigureAwait(false);
                var parsed = IucnFamilyResponseParser.ParseFamilyTaxaPage(response.Body);
                cacheStore.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, TimeSpan.Zero);

                foreach (var sisId in parsed.SisIds) {
                    allSisIds.Add(sisId);
                }

                if (!parsed.HasMorePages || parsed.SisIds.Count == 0) {
                    break;
                }

                page++;

                if (sleepMs > 0) {
                    await Task.Delay(sleepMs, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (IucnApiException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound) {
                // Family not found — skip silently
                cacheStore.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, TimeSpan.Zero);
                AnsiConsole.MarkupLineInterpolated($"[yellow]Family not found: {Markup.Escape(familyName)}[/]");
                break;
            }
            catch (Exception ex) {
                cacheStore.CompleteImportFailure(importId, ex.Message, null, TimeSpan.Zero);
                AnsiConsole.MarkupLineInterpolated($"[red]Error fetching {Markup.Escape(familyName)} page {page}: {Markup.Escape(ex.Message)}[/]");
                break;
            }
        }

        return new List<long>(allSisIds);
    }
}
