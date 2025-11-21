using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

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

public sealed class WikidataSeedCommand : AsyncCommand<WikidataSeedSettings> {
    private const string CursorKey = "wikidata_taxa_cursor";

    public override Task<int> ExecuteAsync(CommandContext context, WikidataSeedSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataSeedSettings settings, CancellationToken cancellationToken) {
        var configuration = WikidataConfiguration.FromEnvironment();
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");

        using var store = WikidataCacheStore.Open(cachePath);
        using var client = new WikidataApiClient(configuration);

        var startCursor = DetermineCursor(settings, store);
        var batchSize = Math.Clamp(settings.BatchSize ?? configuration.SparqlBatchSize, 50, 2_000);
        var dynamicBatchSize = batchSize;
        var totalGoal = settings.Limit.HasValue && settings.Limit.Value > 0 ? settings.Limit.Value : int.MaxValue;

        if (settings.ResetCursor && settings.Cursor is null) {
            store.SetSyncCursor(CursorKey, startCursor);
        }

        var cursor = startCursor;
        var totalNew = 0;
        var totalTouched = 0;
        var lastBatch = 0;

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
            catch (WikidataApiException ex) when (ShouldDownshift(ex, dynamicBatchSize)) {
                dynamicBatchSize = Math.Max(50, dynamicBatchSize / 2);
                AnsiConsole.MarkupLineInterpolated($"[yellow]SPARQL request timed out (status {(int?)ex.StatusCode ?? 0}). Reducing batch size to {dynamicBatchSize} and retrying from Q{cursor}.[/]");
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                continue;
            }
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

        var status = lastBatch == 0
            ? "[yellow]No additional Wikidata taxa found for the given cursor.[/]"
            : "[green]Finished fetching Wikidata taxon ids.[/]";

        AnsiConsole.MarkupLine(status);
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
    private static bool ShouldDownshift(WikidataApiException ex, int currentBatch) {
        if (currentBatch <= 50) {
            return false;
        }

        if (!ex.StatusCode.HasValue) {
            return false;
        }

        return ex.StatusCode.Value is System.Net.HttpStatusCode.GatewayTimeout
            or System.Net.HttpStatusCode.RequestTimeout
            or System.Net.HttpStatusCode.ServiceUnavailable;
    }
}
