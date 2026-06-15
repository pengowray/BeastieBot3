using System;
using System.ComponentModel;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Diagnostic command displaying WikipediaCacheStore statistics:
// - Total pages in cache, pending in queue, failed fetches
// - Matched taxa count (from taxon_matches table)
// - Last fetch timestamp
// Useful for monitoring cache progress. Run via: wikipedia status

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia cache-status", CommandKind.ReadOnly,
    "Show high-level statistics about the local Wikipedia cache database.",
    Examples = new[] {
        "wikipedia cache-status",
        "wikipedia cache-status --cache data/enwiki-cache.sqlite"
    })]
public sealed class WikipediaCacheStatusCommand : Command<WikipediaCacheStatusCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
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
