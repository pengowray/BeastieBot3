using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataResetCacheSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--force")]
    [Description("Skip the interactive confirmation prompt.")]
    public bool Force { get; init; }
}

public sealed class WikidataResetCacheCommand : AsyncCommand<WikidataResetCacheSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataResetCacheSettings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;
        return Task.FromResult(Run(settings));
    }

    private static int Run(WikidataResetCacheSettings settings) {
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        if (!settings.Force) {
            var confirmed = AnsiConsole.Confirm("This will delete all downloaded Wikidata JSON payloads but keep the seed queue. Continue?");
            if (!confirmed) {
                AnsiConsole.MarkupLine("[yellow]Operation cancelled.[/]");
                return 1;
            }
        }

        using var store = WikidataCacheStore.Open(cachePath);
        var affected = store.ResetCachedPayloads();
        AnsiConsole.MarkupLine($"[green]Cleared cached payloads for {affected} entities. They can now be re-downloaded.[/]");
        return 0;
    }
}
