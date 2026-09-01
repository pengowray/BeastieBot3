using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Step 1 of Wikidata caching: runs SPARQL query to discover taxon Q-IDs.
// Query: SELECT ?item WHERE { ?item wdt:P627 ?iucnId } finds all Wikidata
// items with IUCN Red List IDs (P627). Inserts Q-IDs into wikidata_items
// table with status='pending'. Run via: wikidata seed

namespace BeastieBot3.Wikidata;

public sealed class WikidataSeedSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Maximum number of seed rows to fetch during this run. Defaults to the SPARQL batch size (~500).")]
    public int? Limit { get; init; }

    [CommandOption("--batch-size <N>")]
    [Description("Override the SPARQL page size. Defaults to WIKIDATA_SPARQL_BATCH_SIZE or 500.")]
    public int? BatchSize { get; init; }

    [CommandOption("--cursor <QID>")]
    [Description("Start cursor (numeric or Q-id, exclusive). Overrides stored cursor for this run only.")]
    public string? Cursor { get; init; }

    [CommandOption("--reset-cursor")]
    [Description("Reset the persisted cursor to zero before fetching.")]
    public bool ResetCursor { get; init; }
}

[CommandInfo("wikidata seed-taxa", CommandKind.Mutates,
    "Fetch Wikidata Q-ids for taxa carrying IUCN identifiers and enqueue them for caching.",
    Reason = "Enqueues Wikidata Q-ids for IUCN-linked taxa.",
    Examples = new[] {
        "wikidata seed-taxa",
        "wikidata seed-taxa --limit 1000"
    })]
public sealed class WikidataSeedCommand : AsyncCommand<WikidataSeedSettings> {
    private const string CursorKey = "wikidata_taxa_cursor";
    private const int MinBatchSize = 50;
    // Waits between retries once the batch is as small as it goes. Roughly seven minutes in all,
    // which rides out the query service's usual bad patches without stalling a workflow run.
    private static readonly TimeSpan[] StallWaits = {
        TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(4),
    };

    public override Task<int> ExecuteAsync(CommandContext context, WikidataSeedSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataSeedSettings settings, CancellationToken cancellationToken) {
        var configuration = WikidataConfiguration.FromEnvironment();
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        using var store = WikidataCacheStore.Open(cachePath);
        using var client = new WikidataApiClient(configuration);

        var startCursor = DetermineCursor(settings, store);
        var batchSize = Math.Clamp(settings.BatchSize ?? configuration.SparqlBatchSize, 5, 2_000);
        var dynamicBatchSize = batchSize;
        var totalGoal = settings.Limit.HasValue && settings.Limit.Value > 0 ? settings.Limit.Value : int.MaxValue;

        if (settings.ResetCursor && settings.Cursor is null) {
            store.SetSyncCursor(CursorKey, startCursor);
        }

        var cursor = startCursor;
        var totalNew = 0;
        var totalTouched = 0;
        var lastBatch = 0;
        var stalledRounds = 0;
        WikidataApiException? outage = null;

        while (!cancellationToken.IsCancellationRequested) {
            var remaining = totalGoal - totalTouched;
            if (remaining <= 0) {
                break;
            }

            var requestSize = Math.Min(dynamicBatchSize, remaining);
            IReadOnlyList<WikidataSeedRow> seeds;
            try {
                seeds = await client.QueryTaxonSeedsAsync(cursor, requestSize, cancellationToken).ConfigureAwait(false);
            }
            catch (WikidataApiException ex) when (IsServerSideFailure(ex)) {
                // The query service goes down, or slows past its own timeout, for minutes at a
                // time. Every completed batch has already stored its cursor, so the only thing at
                // stake is this run: shrink the batch, then wait longer and longer, then stop and
                // say so. Throwing here used to abandon a `wikipedia update` at its first step.
                if (dynamicBatchSize > MinBatchSize) {
                    dynamicBatchSize = Math.Max(MinBatchSize, dynamicBatchSize / 2);
                    AnsiConsole.MarkupLineInterpolated($"[yellow]Wikidata Query Service returned {Describe(ex)}.[/] Retrying from Q{cursor} with a smaller batch of {dynamicBatchSize} items.");
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                stalledRounds++;
                if (stalledRounds > StallWaits.Length) {
                    outage = ex;
                    break;
                }

                var wait = StallWaits[stalledRounds - 1];
                AnsiConsole.MarkupLineInterpolated($"[yellow]Wikidata Query Service returned {Describe(ex)}.[/] Waiting {Format(wait)}, then retrying from Q{cursor} (retry {stalledRounds} of {StallWaits.Length}).");
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                continue;
            }
            stalledRounds = 0;
            if (seeds.Count == 0) {
                break;
            }

            var result = store.UpsertSeeds(seeds);
            cursor = seeds[^1].NumericId;
            store.SetSyncCursor(CursorKey, cursor);

            totalNew += result.NewCount;
            totalTouched += result.NewCount + result.UpdatedCount;
            lastBatch = seeds.Count;
            AnsiConsole.MarkupLineInterpolated($"[grey]Cursor[/] Q{cursor}: +{result.NewCount} new, {result.UpdatedCount} updated (batch {seeds.Count}).");

            if (seeds.Count < requestSize) {
                break; // Likely exhausted results even if goal not met
            }

            // Increase batch size again after a successful request so we eventually ramp back up.
            if (dynamicBatchSize < batchSize) {
                dynamicBatchSize = Math.Min(batchSize, dynamicBatchSize + 50);
            }
        }

        if (outage is not null) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Wikidata Query Service is not responding ({Describe(outage)}).[/] Stopping this step; everything fetched so far is saved.");
            AnsiConsole.MarkupLine(
                "Run `wikipedia update` again later to continue from the cursor below. The remaining steps do not use this service and will run now.");
        }
        else {
            AnsiConsole.MarkupLine(lastBatch == 0
                ? "[yellow]No additional Wikidata taxa found for the given cursor.[/]"
                : "[green]Finished fetching Wikidata taxon ids.[/]");
        }
        AnsiConsole.MarkupLine($"[yellow]Cursor persisted at:[/] Q{cursor}");
        AnsiConsole.MarkupLine($"[green]New rows:[/] {totalNew}");
        AnsiConsole.MarkupLine($"[grey]Touched rows (new + existing):[/] {totalTouched}");
        return 0;
    }

    private static long DetermineCursor(WikidataSeedSettings settings, WikidataCacheStore store) {
        if (!string.IsNullOrWhiteSpace(settings.Cursor)) {
            if (TryParseCursor(settings.Cursor, out var explicitCursor)) {
                return explicitCursor;
            }

            throw new InvalidOperationException($"Unable to parse cursor '{settings.Cursor}'. Use a numeric id or formats like Q12345.");
        }

        if (settings.ResetCursor) {
            return 0;
        }

        return store.GetSyncCursor(CursorKey);
    }

    private static bool TryParseCursor(string text, out long cursor) {
        cursor = 0;
        if (string.IsNullOrWhiteSpace(text)) {
            return false;
        }

        var span = text.AsSpan().Trim();
        if (span.Length > 0 && (span[0] == 'Q' || span[0] == 'q')) {
            span = span[1..];
        }

        return long.TryParse(span, out cursor);
    }
    // Anything that is the query service's problem rather than ours: a gateway or timeout status,
    // or no status at all (the connection died before a response). A 400 stays fatal, because a
    // malformed query does not fix itself by waiting.
    private static bool IsServerSideFailure(WikidataApiException ex) {
        if (!ex.StatusCode.HasValue) {
            return true;
        }

        return ex.StatusCode.Value is System.Net.HttpStatusCode.GatewayTimeout
            or System.Net.HttpStatusCode.RequestTimeout
            or System.Net.HttpStatusCode.ServiceUnavailable
            or System.Net.HttpStatusCode.BadGateway
            or System.Net.HttpStatusCode.InternalServerError
            or System.Net.HttpStatusCode.TooManyRequests;
    }

    private static string Describe(WikidataApiException ex) =>
        ex.StatusCode.HasValue ? $"HTTP {(int)ex.StatusCode.Value}" : "no response";

    private static string Format(TimeSpan wait) =>
        wait.TotalMinutes >= 1 ? $"{wait.TotalMinutes:0.#} minutes" : $"{wait.TotalSeconds:0} seconds";
}
