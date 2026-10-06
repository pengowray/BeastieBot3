using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// `wikidata sweep-taxa`: reads a short record of every Wikidata item with a taxon name (P225) into
// wikidata_taxon_sweep in the Wikidata cache, from the QLever endpoint (WikidataTaxonSweep). The
// public species site lists species that are in Wikidata but not in IUCN from it.
//
// Resumable: each page is stored with the cursor in one transaction, and the next run carries on
// after the cursor. When a pass reaches the end it deletes the rows it did not see and records the
// time it finished; a later run starts a new pass only with --restart or when the last pass is older
// than --refresh-days.

namespace BeastieBot3.Wikidata;

public sealed class WikidataSweepTaxaSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Stop after storing this many items (0 = no limit, the default). The next run carries on from there.")]
    public int? Limit { get; init; }

    [CommandOption("--page-size <N>")]
    [Description("Result rows per query (default 100000). An item with two parent taxa has two rows.")]
    public int? PageSize { get; init; }

    [CommandOption("--endpoint <URL>")]
    [Description("SPARQL endpoint. Defaults to WIKIDATA_QLEVER_ENDPOINT or https://qlever.dev/api/wikidata. The Wikidata Query Service cannot run this query in its time limit.")]
    public string? Endpoint { get; init; }

    [CommandOption("--restart")]
    [Description("Start a new pass from the first item, even when the last pass finished. Items stored by the last pass are kept until the new pass finishes.")]
    public bool Restart { get; init; }

    [CommandOption("--refresh-days <DAYS>")]
    [Description("Start a new pass when the last pass finished more than this many days ago. Without it, a run after a finished pass does nothing.")]
    public int? RefreshDays { get; init; }

    [CommandOption("--status")]
    [Description("Print how far the sweep has got and exit, without querying Wikidata.")]
    public bool Status { get; init; }
}

[CommandInfo("wikidata sweep-taxa", CommandKind.Mutates,
    "Download the taxon name, rank, parent taxon, Catalogue of Life ID, IUCN taxon ID, English Wikipedia article and English label of every Wikidata taxon item (about 4 million) from the QLever Wikidata endpoint, continuing where the last run stopped. site build-db lists species that are in Wikidata but not in IUCN from them. A full pass takes about 10 minutes.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "A run carries on from the last item stored. After a pass reaches the last item, a run does nothing unless --restart or --refresh-days starts a new pass. When a pass finishes, it deletes the stored items it did not see, which Wikidata has deleted or merged.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "wikidata sweep-taxa",
        "wikidata sweep-taxa --limit 200000",
        "wikidata sweep-taxa --refresh-days 30",
        "wikidata sweep-taxa --status",
    })]
public sealed class WikidataSweepTaxaCommand : AsyncCommand<WikidataSweepTaxaSettings> {
    private const int DefaultPageSize = 100_000;
    private const int MinPageSize = 5_000;
    private static readonly TimeSpan[] FailureWaits = {
        TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(4),
    };

    public override Task<int> ExecuteAsync(CommandContext context, WikidataSweepTaxaSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataSweepTaxaSettings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(cachePath)}");
        using var store = WikidataCacheStore.Open(cachePath);

        var state = store.GetTaxonSweepState();
        if (settings.Status) {
            PrintStatus(state);
            return 0;
        }

        var now = DateTime.UtcNow;
        var decision = WikidataSweepPlan.Decide(state, settings.Restart, settings.RefreshDays, now);
        if (decision == WikidataSweepPlan.Action.UpToDate) {
            PrintStatus(state);
            AnsiConsole.MarkupLine("[green]The last pass finished; nothing to do.[/] Use --restart or --refresh-days to start a new pass.");
            return 0;
        }
        DateTime passStarted;
        if (decision == WikidataSweepPlan.Action.StartPass) {
            passStarted = now;
            store.SetSyncCursor(WikidataCacheStore.TaxonSweepCursorKey, 0);
            store.SetSyncText(WikidataCacheStore.TaxonSweepStartedKey, passStarted.ToString("O", CultureInfo.InvariantCulture));
            AnsiConsole.MarkupLine("[grey]Starting a new pass from the first item.[/]");
        } else {
            passStarted = state.PassStartedUtc!.Value;
            AnsiConsole.MarkupLineInterpolated($"[grey]Continuing the pass started {passStarted:yyyy-MM-dd HH:mm} UTC after Q{state.Cursor}.[/]");
        }

        var configuration = WikidataConfiguration.FromEnvironment();
        var endpoint = ResolveEndpoint(settings.Endpoint);
        AnsiConsole.MarkupLine($"[grey]Endpoint:[/] {Markup.Escape(endpoint.ToString())}");
        // A page of 100,000 rows is about 45 MB of JSON and takes QLever about 12 seconds; allow
        // for a slow day.
        using var client = new WikidataApiClient(configuration with {
            SparqlEndpoint = endpoint,
            Timeout = configuration.Timeout < TimeSpan.FromMinutes(5) ? TimeSpan.FromMinutes(5) : configuration.Timeout,
        });

        long? total = null;
        try {
            total = WikidataTaxonSweep.ParseCount(await client.QuerySparqlAsync(WikidataTaxonSweep.CountQuery, cancellationToken).ConfigureAwait(false));
            if (total is { } t) {
                store.SetSyncText(WikidataCacheStore.TaxonSweepTotalKey, t.ToString(CultureInfo.InvariantCulture));
            }
        } catch (WikidataApiException ex) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Could not count the taxon items ({ex.Message}); progress is shown without a total.[/]");
        }
        total ??= state.LastTotal;

        var limit = settings.Limit is > 0 ? settings.Limit.Value : int.MaxValue;
        var pageSize = Math.Clamp(settings.PageSize ?? DefaultPageSize, MinPageSize, 500_000);
        var cursor = store.GetSyncCursor(WikidataCacheStore.TaxonSweepCursorKey);
        var seenThisPass = decision == WikidataSweepPlan.Action.StartPass ? 0 : state.RowsSeenThisPass;
        var storedThisRun = 0;
        var failures = 0;
        var finished = false;
        var clock = Stopwatch.StartNew();

        while (!cancellationToken.IsCancellationRequested && storedThisRun < limit) {
            var pageClock = Stopwatch.StartNew();
            WikidataSweepPage page;
            try {
                var json = await client.QuerySparqlAsync(WikidataTaxonSweep.BuildQuery(cursor, pageSize), cancellationToken).ConfigureAwait(false);
                page = WikidataTaxonSweep.Parse(json);
            } catch (WikidataApiException ex) when (ex.StatusCode is null or >= System.Net.HttpStatusCode.InternalServerError
                                                         or System.Net.HttpStatusCode.TooManyRequests
                                                         or System.Net.HttpStatusCode.RequestTimeout) {
                if (failures >= FailureWaits.Length) {
                    AnsiConsole.MarkupLineInterpolated($"[yellow]The endpoint is not answering ({Markup.Escape(ex.Message)}).[/] Stopping; everything stored so far is kept. Run the command again later to carry on after Q{cursor}.");
                    return 1;
                }
                var wait = FailureWaits[failures++];
                pageSize = Math.Max(MinPageSize, pageSize / 2);
                AnsiConsole.MarkupLineInterpolated($"[yellow]Query failed ({Markup.Escape(ex.Message)}).[/] Waiting {wait.TotalSeconds:0} s, then retrying after Q{cursor} with {pageSize:N0} rows per page.");
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                continue;
            }
            failures = 0;

            var items = WikidataTaxonSweep.TakeComplete(page, pageSize);
            if (storedThisRun + items.Count > limit) {
                items = items.Take(limit - storedThisRun).ToList();
            }
            if (items.Count == 0) {
                finished = page.RowCount < pageSize;
                break;
            }
            store.StoreTaxonSweepPage(items, DateTime.UtcNow);
            cursor = items[^1].Qid;
            storedThisRun += items.Count;
            seenThisPass += items.Count;

            var progress = total is > 0 ? $" ({Math.Min(100.0, 100.0 * seenThisPass / total.Value):0.0}% of about {total.Value:N0})" : string.Empty;
            var eta = Eta(clock.Elapsed, storedThisRun, total is { } tt ? tt - seenThisPass : null);
            AnsiConsole.MarkupLineInterpolated($"[grey]Up to Q{cursor}:[/] {items.Count:N0} items in {pageClock.Elapsed.TotalSeconds:0} s; {seenThisPass:N0} this pass{progress}{eta}");

            if (page.RowCount < pageSize) {
                finished = true;
                break;
            }
        }

        if (finished) {
            var deleted = store.CompleteTaxonSweepPass(passStarted, DateTime.UtcNow);
            AnsiConsole.MarkupLineInterpolated($"[green]Pass finished:[/] {seenThisPass:N0} items read; {deleted:N0} stored items that the pass did not see were deleted.");
        } else {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Stopped after Q{cursor}.[/] Run the command again to carry on.");
        }
        PrintStatus(store.GetTaxonSweepState());
        return 0;
    }

    private static Uri ResolveEndpoint(string? option) {
        var text = option ?? Environment.GetEnvironmentVariable("WIKIDATA_QLEVER_ENDPOINT");
        if (string.IsNullOrWhiteSpace(text)) {
            return WikidataTaxonSweep.DefaultEndpoint;
        }
        return Uri.TryCreate(text.Trim(), UriKind.Absolute, out var uri)
            ? uri
            : throw new InvalidOperationException($"'{text}' is not an absolute URL.");
    }

    private static string Eta(TimeSpan elapsed, long done, long? remaining) {
        if (remaining is not > 0 || done <= 0) {
            return string.Empty;
        }
        var left = TimeSpan.FromSeconds(elapsed.TotalSeconds / done * remaining.Value);
        return left.TotalMinutes >= 1 ? $", about {left.TotalMinutes:0} min left" : $", about {left.TotalSeconds:0} s left";
    }

    private static void PrintStatus(WikidataTaxonSweepState state) {
        var table = new Table().Border(TableBorder.Rounded).AddColumn("").AddColumn("");
        table.AddRow("Items stored", state.Rows.ToString("N0", CultureInfo.InvariantCulture));
        table.AddRow("Species (rank Q7432)", state.Species.ToString("N0", CultureInfo.InvariantCulture));
        table.AddRow("Last pass finished", state.LastCompletedUtc is { } done ? $"{done:yyyy-MM-dd HH:mm} UTC" : "never");
        if (state.PassStartedUtc is { } started) {
            var of = state.LastTotal is > 0 ? $" of about {state.LastTotal.Value:N0}" : string.Empty;
            table.AddRow("Pass in progress", $"started {started:yyyy-MM-dd HH:mm} UTC; {state.RowsSeenThisPass:N0} items read{of}; next after Q{state.Cursor}");
        }
        AnsiConsole.Write(table);
    }
}

/// What a sweep run does, from the stored state and its options.
internal static class WikidataSweepPlan {
    public enum Action { Continue, StartPass, UpToDate }

    public static Action Decide(WikidataTaxonSweepState state, bool restart, int? refreshDays, DateTime nowUtc) {
        if (restart) {
            return Action.StartPass;
        }
        if (state.PassStartedUtc is not null) {
            return Action.Continue;
        }
        if (state.LastCompletedUtc is not { } completed) {
            return Action.StartPass;
        }
        return refreshDays is { } days && nowUtc - completed > TimeSpan.FromDays(Math.Max(0, days))
            ? Action.StartPass
            : Action.UpToDate;
    }
}
