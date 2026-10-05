using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

namespace BeastieBot3.Wikidata;

public sealed class WikidataQueueSynonymsSettings : CommonSettings {
    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }
}

[CommandInfo("wikidata queue-synonyms", CommandKind.Mutates,
    "Queue the Wikidata items that downloaded items name as taxon synonym (P1420), for wikidata cache-entities to download. site build-db lists the taxon names of downloaded synonym items as Wikidata synonyms on the species site. Reads every downloaded item that mentions P1420 (about a minute).",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Items already in the queue or already downloaded are left as they are.",
    Examples = new[] { "wikidata queue-synonyms" })]
public sealed class WikidataQueueSynonymsCommand : AsyncCommand<WikidataQueueSynonymsSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataQueueSynonymsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        string cachePath;
        try {
            cachePath = paths.ResolveWikidataCachePath(settings.WikidataCache);
        } catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return Task.FromResult(-1);
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");
        using var store = WikidataCacheStore.Open(cachePath);
        try {
            var result = store.QueueTaxonSynonymItems(cancellationToken);
            AnsiConsole.MarkupLineInterpolated(
                $"Items with taxon synonyms: {result.ItemsWithSynonyms:N0}. Synonym items: {result.SynonymItems:N0}. Already in the cache or the queue: {result.AlreadyQueued:N0}. Queued: [green]{result.Queued:N0}[/].");
            if (result.Queued > 0) {
                AnsiConsole.MarkupLine("To download them, run [blue]wikidata cache-entities[/].");
            }
            return Task.FromResult(0);
        } catch (OperationCanceledException) {
            AnsiConsole.MarkupLine("[yellow]Cancelled. Nothing was queued.[/]");
            return Task.FromResult(-2);
        }
    }
}
