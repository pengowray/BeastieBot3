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
        [Description("Maximum number of Wikidata sitelinks to enqueue. Defaults to 1000.")]
        [DefaultValue(1000)]
        public int Limit { get; init; } = 1000;

        [CommandOption("--resume-after <ID>")]
        [Description("Resume after the specified numeric Wikidata Q-id (e.g. 12345).")]
        public long? ResumeAfter { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);
        var cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        var wikidataPath = paths.ResolveWikidataCachePath(settings.WikidataCachePath);

        using var wikiStore = WikipediaCacheStore.Open(cachePath);
        using var wikidataStore = WikidataCacheStore.Open(wikidataPath);

        var limit = Math.Clamp(settings.Limit, 1, 100_000);
        var seeds = wikidataStore.GetEnwikiSitelinks(settings.ResumeAfter, limit);
        if (seeds.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No Wikidata sitelinks found within the given range.[/]");
            return 0;
        }

        var now = DateTime.UtcNow;
        var inserted = 0;
        var refreshed = 0;
        foreach (var seed in seeds) {
            cancellationToken.ThrowIfCancellationRequested();
            var normalized = WikipediaTitleHelper.Normalize(seed.Title);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var candidate = new WikiPageCandidate(seed.Title.Trim(), normalized, PageId: null, now, now);
            var result = wikiStore.UpsertPageCandidate(candidate);
            if (result.IsNew) {
                inserted++;
            }
            else {
                refreshed++;
            }
        }

        var lastNumericId = seeds[^1].EntityNumericId;
        AnsiConsole.MarkupLine($"Processed [green]{seeds.Count}[/] sitelinks (inserted [green]{inserted}[/], refreshed [grey]{refreshed}[/]).");
        AnsiConsole.MarkupLine($"Next resume hint: Q{lastNumericId}.");
        return 0;
    }
}
