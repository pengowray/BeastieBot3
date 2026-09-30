// WikidataResetCacheCommand.cs
// CLI command that clears cached Wikidata JSON payloads while preserving seed
// rows. Allows re-downloading entity data without losing the entity ID list.

using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

namespace BeastieBot3.Wikidata;

public sealed class WikidataResetCacheSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--force")]
    [Description("Skip the interactive confirmation prompt.")]
    public bool Force { get; init; }
}

[CommandInfo("wikidata reset-cache", CommandKind.Destructive,
    "Delete all downloaded item data in the Wikidata cache and everything extracted from the item data, including P627 IDs, P141 statuses, taxon names and the name index. The download queue and the taxon links made by wikidata backfill-iucn are kept. Downloading every item again with wikidata cache-entities takes days.",
    Reason = "Deletes all downloaded Wikidata item data and the P627 IDs, P141 statuses and taxon names extracted from it. Re-downloading every item takes days.",
    PromptOption = "--force",
    Rerun = RerunEffect.ClearsCache,
    RerunNote = "The next run of wikidata cache-entities, wikidata cache-all or wikipedia update downloads every item again.",
    Examples = new[] {
        "wikidata reset-cache",
        "wikidata reset-cache --force"
    })]
public sealed class WikidataResetCacheCommand : AsyncCommand<WikidataResetCacheSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataResetCacheSettings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;
        return Task.FromResult(Run(settings));
    }

    private static int Run(WikidataResetCacheSettings settings) {
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        if (!settings.Force) {
            // A web UI job (or any run without a terminal) cannot answer a prompt, and Spectre throws
            // if asked to. The web UI asks in its own dialog and then adds --force (PromptOption).
            if (!AnsiConsole.Profile.Capabilities.Interactive) {
                AnsiConsole.MarkupLine("[red]Cannot ask for confirmation: this run is not in an interactive terminal. Nothing was deleted.[/] To delete without being asked, run [bold]wikidata reset-cache --force[/].");
                return 1;
            }
            var confirmed = AnsiConsole.Confirm("Delete all downloaded item data in the Wikidata cache? Downloading every item again takes days.");
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
