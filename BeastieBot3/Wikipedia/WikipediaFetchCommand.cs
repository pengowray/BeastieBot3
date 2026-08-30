using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Processes the Wikipedia page_queue: fetches HTML and wikitext via API,
// stores in pages table. Uses WikipediaPageFetcher for download orchestration.
// Parses taxobox templates from wikitext, storing results in taxobox_data.
// Default batch size 25, respects rate limits. Run via: wikipedia fetch

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia fetch-pages", CommandKind.Mutates,
    "Download queued Wikipedia pages (HTML plus wikitext) into the local cache. --awaited-only narrows the queue to pages a taxon has no article without; --newest-first takes a new release's taxa before the older backlog.",
    Reason = "Downloads queued Wikipedia pages into the local cache.",
    Rerun = RerunEffect.IdempotentAdd,
    Examples = new[] {
        "wikipedia fetch-pages",
        "wikipedia fetch-pages --awaited-only --newest-first --limit 2000",
        "wikipedia fetch-pages --limit 25",
        "wikipedia fetch-pages --refresh-only --refresh-days 365",
        "wikipedia fetch-pages --title \"Ursus maritimus\""
    })]
public sealed class WikipediaFetchCommand : AsyncCommand<WikipediaFetchCommand.Settings> {
    private const int DefaultBatchSize = 25;
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Maximum number of pages to fetch (0 = all pending).")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Re-download cached pages older than the specified number of days.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--title <TITLE>")]
        [Description("Explicit Wikipedia titles to fetch immediately (can be specified multiple times).")]
        public string[] Titles { get; init; } = Array.Empty<string>();

        [CommandOption("--awaited-only")]
        [Description("Only fetch pages a taxon is waiting on: queued titles that an IUCN taxon with no article yet is pointing at. Skips higher-taxon, synonym and redirect titles nothing is blocked on.")]
        public bool AwaitedOnly { get; init; }

        [CommandOption("--newest-first")]
        [Description("Work through the newest queued titles first (a new release's taxa) instead of the oldest.")]
        public bool NewestFirst { get; init; }

        [CommandOption("--refresh-only")]
        [Description("Re-download cached pages only, ignoring everything never fetched. Needs --refresh-days.")]
        public bool RefreshOnly { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);
        var cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);

        using var cacheStore = WikipediaCacheStore.Open(cachePath);
        var workItems = new System.Collections.Generic.List<WikiPageWorkItem>();
        var now = DateTime.UtcNow;
        foreach (var rawTitle in settings.Titles) {
            var normalized = WikipediaTitleHelper.Normalize(rawTitle);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var candidate = new WikiPageCandidate(rawTitle.Trim(), normalized, PageId: null, now, now);
            var upsert = cacheStore.UpsertPageCandidate(candidate);
            workItems.Add(new WikiPageWorkItem(upsert.PageRowId, candidate.Title, candidate.NormalizedTitle, WikiPageDownloadStatus.Pending, null, 0));
        }

        DateTime? refreshThreshold = null;
        if (settings.RefreshDays.HasValue && settings.RefreshDays.Value > 0) {
            refreshThreshold = DateTime.UtcNow.AddDays(-settings.RefreshDays.Value);
        }

        if (settings.RefreshOnly && refreshThreshold is null) {
            AnsiConsole.MarkupLine("[red]--refresh-only needs --refresh-days to say how old a cached page has to be.[/]");
            return -1;
        }

        var scope = new WikipediaCacheStore.WikiFetchScope {
            RefreshThreshold = refreshThreshold,
            AwaitedOnly = settings.AwaitedOnly,
            RefreshOnly = settings.RefreshOnly,
            NewestFirst = settings.NewestFirst,
        };

        // The queue holds six figures of titles, so say up front how many this run's scope
        // covers -- otherwise "--limit 500" gives no sense of what fraction is left.
        if (settings.Titles.Length == 0) {
            var queued = cacheStore.CountPendingPages(scope);
            var everything = cacheStore.CountPendingPages(new WikipediaCacheStore.WikiFetchScope { RefreshThreshold = refreshThreshold });
            if (queued == everything) {
                AnsiConsole.MarkupLineInterpolated($"[grey]Queue:[/] {queued:n0} pages to fetch.");
            } else {
                AnsiConsole.MarkupLineInterpolated($"[grey]Queue:[/] {queued:n0} pages in scope, of {everything:n0} queued in total.");
            }
        }

        var totalLimit = settings.Limit > 0 ? settings.Limit : int.MaxValue;
        var processed = 0;

        var configuration = WikipediaConfiguration.FromEnvironment();
        using var client = new WikipediaApiClient(configuration);
        var fetcher = new WikipediaPageFetcher(cacheStore, client);

        var success = 0;
        var missing = 0;
        var failed = 0;
        var skipped = 0;

        while (processed < totalLimit) {
            cancellationToken.ThrowIfCancellationRequested();

            if (workItems.Count == 0) {
                var needed = Math.Min(DefaultBatchSize, totalLimit - processed);
                if (needed <= 0) {
                    break;
                }

                var pending = cacheStore.GetPendingPages(needed, scope);
                if (pending.Count == 0) {
                    break;
                }

                workItems.AddRange(pending);
            }

            var item = workItems[0];
            workItems.RemoveAt(0);

            // Escape the dynamic title/message: a scientific name or synonym can contain
            // '[' (e.g. "[junior synonym]"), which MarkupLine would otherwise parse as a
            // style tag and throw "Could not find color or style ...", aborting the fetch.
            AnsiConsole.MarkupLine($"[grey]Fetching[/] {Markup.Escape(item.PageTitle)}...");
            var outcome = await fetcher.FetchAsync(item, cancellationToken).ConfigureAwait(false);
            processed++;

            if (outcome.Success) {
                success++;
                AnsiConsole.MarkupLine($"[green]✓[/] {Markup.Escape(outcome.FinalTitle ?? outcome.RequestedTitle)}");
            }
            else if (outcome.Missing) {
                missing++;
                AnsiConsole.MarkupLine($"[yellow]![/] Missing {Markup.Escape(outcome.RequestedTitle)} ({Markup.Escape(outcome.Message ?? "not found")})");
            }
            else if (outcome.Skipped) {
                skipped++;
                AnsiConsole.MarkupLine($"[grey]-[/] Skipped {Markup.Escape(outcome.RequestedTitle)} ({Markup.Escape(outcome.Message ?? "duplicate")})");
            }
            else {
                failed++;
                AnsiConsole.MarkupLine($"[red]x[/] Failed {Markup.Escape(outcome.RequestedTitle)}: {Markup.Escape(outcome.Message ?? "(error)")}");
            }
        }

        if (processed == 0) {
            AnsiConsole.MarkupLine("[yellow]No wikipedia pages are pending for download.[/]");
            return 0;
        }

        AnsiConsole.MarkupLine($"Completed fetches. Success: [green]{success}[/], Missing: [yellow]{missing}[/], Skipped: [grey]{skipped}[/], Failed: [red]{failed}[/].");
        return failed > 0 ? 1 : 0;
    }
}
