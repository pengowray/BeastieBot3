using System;
using System.ComponentModel;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikipediaEnqueueCommand : Command<WikipediaEnqueueCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--wikidata-cache <FILE>")]
        [Description("Path to the Wikidata cache SQLite database. Defaults to Datastore:wikidata_cache_sqlite.")]
        public string? WikidataCachePath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Maximum number of Wikidata sitelinks to enqueue (0 = all pending).")]
        public int Limit { get; init; }

        [CommandOption("--resume-after <ID>")]
        [Description("Resume after the specified numeric Wikidata Q-id (e.g. 12345).")]
        public long? ResumeAfter { get; init; }

        [CommandOption("--force-refresh")]
        [Description("Re-enqueue existing titles even if they were seen recently.")]
        public bool ForceRefresh { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Refresh titles last seen before the specified number of days.")]
        public int? RefreshDays { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);
        var cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        var wikidataPath = paths.ResolveWikidataCachePath(settings.WikidataCachePath);

        using var wikiStore = WikipediaCacheStore.Open(cachePath);
        using var wikidataStore = WikidataCacheStore.Open(wikidataPath);

        var limit = settings.Limit <= 0 ? int.MaxValue : Math.Clamp(settings.Limit, 1, int.MaxValue);
        var seeds = wikidataStore.GetEnwikiSitelinks(settings.ResumeAfter, limit);
        if (seeds.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No Wikidata sitelinks found within the given range.[/]");
            return 0;
        }

        var now = DateTime.UtcNow;
        var inserted = 0;
        var refreshed = 0;
        var skipped = 0;
        DateTime? refreshThreshold = null;
        if (settings.RefreshDays.HasValue && settings.RefreshDays.Value > 0) {
            refreshThreshold = now.AddDays(-settings.RefreshDays.Value);
        }

        foreach (var seed in seeds) {
            cancellationToken.ThrowIfCancellationRequested();
            var normalized = WikipediaTitleHelper.Normalize(seed.Title);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var existing = wikiStore.GetPageByNormalizedTitle(normalized);
            var candidate = new WikiPageCandidate(seed.Title.Trim(), normalized, PageId: null, now, now);
            if (existing is null) {
                wikiStore.UpsertPageCandidate(candidate);
                inserted++;
                continue;
            }

            var needsRefresh = settings.ForceRefresh;
            if (!needsRefresh && refreshThreshold.HasValue) {
                needsRefresh = !existing.LastSeenAt.HasValue || existing.LastSeenAt.Value < refreshThreshold.Value;
            }

            if (!needsRefresh) {
                skipped++;
                continue;
            }

            wikiStore.DeletePage(existing.PageRowId);
            wikiStore.UpsertPageCandidate(candidate);
            refreshed++;
        }

        var lastNumericId = seeds[^1].EntityNumericId;
        AnsiConsole.MarkupLine($"Processed [green]{seeds.Count}[/] sitelinks (inserted [green]{inserted}[/], refreshed [grey]{refreshed}[/], skipped [grey]{skipped}[/]).");
        AnsiConsole.MarkupLine($"Next resume hint: Q{lastNumericId}.");
        return 0;
    }
}
