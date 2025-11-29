using System;
using System.ComponentModel;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikipediaCacheStatusCommand : Command<WikipediaCacheStatusCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);
        var cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);

        using var store = WikipediaCacheStore.Open(cachePath);
        var stats = store.GetCacheStats();

        AnsiConsole.MarkupLine($"[grey]Wikipedia cache:[/] {cachePath}");
        var table = new Table().AddColumns("Metric", "Value");
        table.AddRow("Total pages", stats.TotalPages.ToString());
        table.AddRow("Cached pages", stats.CachedPages.ToString());
        table.AddRow("Pending pages", stats.PendingPages.ToString());
        table.AddRow("Failed pages", stats.FailedPages.ToString());
        table.AddRow("Missing pages", stats.MissingPages.ToString());
        table.AddRow("Missing titles", stats.MissingTitles.ToString());
        table.AddRow("Matched taxa", stats.MatchedTaxa.ToString());
        AnsiConsole.Write(table);
        return 0;
    }
}
