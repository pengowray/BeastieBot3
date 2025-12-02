using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

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

                var pending = cacheStore.GetPendingPages(needed, refreshThreshold);
                if (pending.Count == 0) {
                    break;
                }

                workItems.AddRange(pending);
            }

            var item = workItems[0];
            workItems.RemoveAt(0);

            AnsiConsole.MarkupLine($"[grey]Fetching[/] {item.PageTitle}...");
            var outcome = await fetcher.FetchAsync(item, cancellationToken).ConfigureAwait(false);
            processed++;

            if (outcome.Success) {
                success++;
                AnsiConsole.MarkupLine($"[green]✓[/] {outcome.FinalTitle ?? outcome.RequestedTitle}");
            }
            else if (outcome.Missing) {
                missing++;
                AnsiConsole.MarkupLine($"[yellow]![/] Missing {outcome.RequestedTitle} ({outcome.Message ?? "not found"})");
            }
            else if (outcome.Skipped) {
                skipped++;
                AnsiConsole.MarkupLine($"[grey]-[/] Skipped {outcome.RequestedTitle} ({outcome.Message ?? "duplicate"})");
            }
            else {
                failed++;
                AnsiConsole.MarkupLine($"[red]x[/] Failed {outcome.RequestedTitle}: {outcome.Message}");
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
