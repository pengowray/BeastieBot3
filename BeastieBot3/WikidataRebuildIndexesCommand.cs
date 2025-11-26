using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataRebuildIndexesSettings : CommonSettings {
    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }

    [CommandOption("--force")]
    [Description("Drop and fully rebuild the selected indexes instead of filling in missing rows only.")]
    public bool Force { get; init; }
}

public sealed class WikidataRebuildIndexesCommand : AsyncCommand<WikidataRebuildIndexesSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataRebuildIndexesSettings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);

        string cachePath;
        try {
            cachePath = paths.ResolveWikidataCachePath(settings.WikidataCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return Task.FromResult(-1);
        }

        AnsiConsole.MarkupLineInterpolated($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");
        using var store = WikidataCacheStore.Open(cachePath);

        try {
            var inserted = store.RebuildTaxonNameIndex(settings.Force, cancellationToken);
            if (inserted == 0 && !settings.Force) {
                AnsiConsole.MarkupLine("[green]Normalized taxon-name index is already up to date.[/]");
            }
            else {
                var action = settings.Force ? "recreated" : "filled";
                AnsiConsole.MarkupLineInterpolated($"[green]Successfully {action} normalized taxon-name index entries ({inserted:n0} new rows).[/]");
            }

            return Task.FromResult(0);
        }
        catch (OperationCanceledException) {
            AnsiConsole.MarkupLine("[yellow]Index rebuild canceled.[/]");
            return Task.FromResult(-2);
        }
    }
}
