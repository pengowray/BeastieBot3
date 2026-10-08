using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses natureserve-fetch`: downloads the global, US and Canadian ranks and the ESA, COSEWIC and
// SARA codes of every NatureServe Explorer species record into the status lists store.
//
// Resumable: each page is stored together with its prefix's progress in one transaction, and the
// next run carries on with the first prefix not done. See NatureServeSearch for why the download
// goes by scientific name prefix, and NatureServePass for full and refresh passes.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses natureserve-fetch", CommandKind.Mutates,
    "Download the NatureServe global rank (G rank), the US and Canadian national ranks, and the US Endangered Species Act, COSEWIC and SARA statuses of every species, subspecies, variety and population on NatureServe Explorer (about 113,500 records, CC BY 4.0) into the status lists store. A full download (about 1,200 requests) takes about 15 minutes, and a stopped run carries on where it left off.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "A run carries on from the last page stored. After a download finishes, a run does nothing unless --restart starts a new full download or --refresh-days asks for the records changed since the last download. A full download that finishes with every record deletes the stored records it did not see; a refresh deletes the records NatureServe has unpublished.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "statuses natureserve-fetch",
        "statuses natureserve-fetch --limit 3",
        "statuses natureserve-fetch --refresh-days 30",
        "statuses natureserve-fetch --status",
    })]
internal sealed class NatureServeFetchCommand : AsyncCommand<NatureServeFetchCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Status lists store. Default: Datastore:status_lists_sqlite, else status_lists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--limit <PAGES>")]
        [Description("Stop after this many requests for pages of 100 records (0 = no limit, the default). The next run carries on from there.")]
        public int? Limit { get; init; }

        [CommandOption("--restart")]
        [Description("Start a new full download from the first record, even when one is under way or the last one finished. Stored records are kept until the new download finishes.")]
        public bool Restart { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("When the last download finished more than this many days ago, download the records NatureServe changed since then. Without it, a run after a finished download does nothing.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how far the download has got and exit, without asking NatureServe.")]
        public bool Status { get; init; }
    }

    // A prefix is split into longer prefixes only up to this length, so a name list that cannot be
    // split (a name that is all one letter repeated) cannot split for ever.
    private const int MaxPrefixLength = 6;

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = paths.ResolveStatusListsPath(settings.StorePath);
        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        using var store = StatusListStore.Open(storePath);

        var state = NatureServePassState.Read(store.GetState);
        if (settings.Status) {
            PrintStatus(store, state);
            return 0;
        }

        var now = DateTime.UtcNow;
        var decision = NatureServePlan.Decide(state, settings.Restart, settings.RefreshDays, now);
        if (decision == NatureServePlan.Action.UpToDate) {
            PrintStatus(store, state);
            AnsiConsole.MarkupLine("[green]The last download finished; nothing to do.[/] Use --refresh-days to download the records changed since then, or --restart for a new full download.");
            return 0;
        }
        if (decision is NatureServePlan.Action.StartFull or NatureServePlan.Action.StartRefresh) {
            var refresh = decision == NatureServePlan.Action.StartRefresh;
            DateTime? modifiedSince = refresh ? NatureServePlan.RefreshSince(state) : null;
            store.StartNatureServePass(new Dictionary<string, string?> {
                [NatureServePassKeys.Started] = StatusListStore.Stamp(now),
                [NatureServePassKeys.Kind] = refresh ? NatureServePassKeys.Refresh : NatureServePassKeys.Full,
                [NatureServePassKeys.ModifiedSince] = modifiedSince is { } s ? StatusListStore.Stamp(s) : null,
            }, NatureServePassKeys.PassKeys, firstPrefix: "");
            AnsiConsole.MarkupLine(refresh
                ? $"[grey]Starting a refresh: records NatureServe changed since {modifiedSince:yyyy-MM-dd HH:mm} UTC.[/]"
                : "[grey]Starting a full download.[/]");
            state = NatureServePassState.Read(store.GetState);
        } else {
            AnsiConsole.MarkupLineInterpolated($"[grey]Continuing the {(state.PassIsRefresh ? "refresh" : "full download")} started {state.PassStartedUtc:yyyy-MM-dd HH:mm} UTC.[/]");
        }

        var passStarted = state.PassStartedUtc!.Value;
        var since = state.PassModifiedSinceUtc;
        var limit = settings.Limit is > 0 ? settings.Limit.Value : int.MaxValue;
        var requests = 0;
        var clock = Stopwatch.StartNew();
        var storedAtStart = store.CountNatureServeFetchedSince(passStarted);
        using var client = new NatureServeClient(message => AnsiConsole.MarkupLineInterpolated($"[yellow]{message}[/]"));

        try {
            while (!cancellationToken.IsCancellationRequested) {
                var partition = store.GetPartitions().FirstOrDefault(p => !p.Done);
                if (partition is null) {
                    return await FinishPass(store, client, state, passStarted, cancellationToken).ConfigureAwait(false);
                }
                if (requests >= limit) {
                    AnsiConsole.MarkupLineInterpolated($"[yellow]Stopped after {requests:N0} requests (--limit).[/] Run the command again to carry on.");
                    break;
                }

                var page = await client.SearchAsync(partition.Prefix, partition.NextPage, since, cancellationToken).ConfigureAwait(false);
                requests++;
                if (partition.Prefix.Length == 0 && partition.NextPage == 0) {
                    store.SetState(NatureServePassKeys.Total, page.TotalResults.ToString(CultureInfo.InvariantCulture));
                    state = state with { PassTotal = page.TotalResults };
                    AnsiConsole.MarkupLineInterpolated($"[grey]NatureServe has {page.TotalResults:N0} records{(since is null ? "" : " changed since then")}.[/]");
                }

                var label = partition.Prefix.Length == 0 ? (since is null ? "All records" : "All changed records") : $"Names starting {partition.Prefix}";
                if (page.TotalResults > NatureServeSearch.MaxRecordsPerQuery && partition.NextPage == 0 && partition.Prefix.Length < MaxPrefixLength) {
                    var parts = NatureServeSearch.Split(partition.Prefix);
                    store.StoreNatureServePage(partition, page.TotalResults, done: false, page.Species, DateTime.UtcNow, splitInto: parts);
                    AnsiConsole.MarkupLineInterpolated($"[grey]{label}: {page.TotalResults:N0} records. One search returns at most {NatureServeSearch.MaxRecordsPerQuery:N0}, so asking for names starting {parts[0]} to {parts[^1]} instead.[/]");
                    continue;
                }

                var readable = Math.Min(page.TotalResults, NatureServeSearch.MaxRecordsPerQuery);
                var pages = NatureServeSearch.PageCount(readable);
                var done = page.ResultCount == 0 || partition.NextPage + 1 >= pages;
                store.StoreNatureServePage(partition, page.TotalResults, done, page.Species, DateTime.UtcNow);
                if (done) {
                    if (page.TotalResults > NatureServeSearch.MaxRecordsPerQuery) {
                        AnsiConsole.MarkupLineInterpolated($"[yellow]{label}: only the first {NatureServeSearch.MaxRecordsPerQuery:N0} of {page.TotalResults:N0} records can be read.[/]");
                    }
                    var seen = store.CountNatureServeFetchedSince(passStarted);
                    AnsiConsole.MarkupLineInterpolated(
                        $"[grey]{label}: {page.TotalResults:N0} records.[/] {Progress(seen, state.PassTotal, seen - storedAtStart, clock.Elapsed)}");
                } else if ((partition.NextPage + 1) % 25 == 0) {
                    AnsiConsole.MarkupLineInterpolated($"[grey]{label}: page {partition.NextPage + 1:N0} of {pages:N0}.[/]");
                }
            }
        } catch (NatureServeException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]NatureServe did not answer:[/] {ex.Message}. Everything stored so far is kept; run the command again later to carry on.");
            return 1;
        }
        PrintStatus(store, NatureServePassState.Read(store.GetState));
        return 0;
    }

    private static async Task<int> FinishPass(StatusListStore store, NatureServeClient client, NatureServePassState state, DateTime passStarted,
        CancellationToken cancellationToken) {
        var seen = store.CountNatureServeFetchedSince(passStarted);
        var total = state.PassTotal;
        IReadOnlyList<long> unpublished = Array.Empty<long>();
        DateTime? deleteNotSeenSince = null;
        if (state.PassIsRefresh) {
            unpublished = await client.UnpublishedSinceAsync(state.PassModifiedSinceUtc, cancellationToken).ConfigureAwait(false);
        } else if (NatureServePlan.MayDeleteUnseen(seen, total)) {
            deleteNotSeenSince = passStarted;
        }

        var finished = DateTime.UtcNow;
        var completion = new Dictionary<string, string?> {
            [NatureServePassKeys.Completed] = StatusListStore.Stamp(finished),
            [NatureServePassKeys.CompletedPassStarted] = StatusListStore.Stamp(passStarted),
            [NatureServePassKeys.CompletedKind] = state.PassIsRefresh ? NatureServePassKeys.Refresh : NatureServePassKeys.Full,
            [NatureServePassKeys.CompletedSeen] = seen.ToString(CultureInfo.InvariantCulture),
            [NatureServePassKeys.CompletedTotal] = total?.ToString(CultureInfo.InvariantCulture),
        };
        if (!state.PassIsRefresh) {
            completion[NatureServePassKeys.FullCompleted] = StatusListStore.Stamp(finished);
        }
        var deleted = store.CompleteNatureServePass(unpublished, deleteNotSeenSince, completion, NatureServePassKeys.PassKeys,
            rows => new StatusSourceInfo(StatusSources.NatureServe, "NatureServe Explorer", "https://explorer.natureserve.org/", "CC BY 4.0",
                NatureServePlan.Citation(finished), finished.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), finished, rows));

        var of = total is { } t ? $" of the {t:N0} NatureServe gave" : "";
        if (state.PassIsRefresh) {
            AnsiConsole.MarkupLineInterpolated($"[green]Refresh finished:[/] stored {seen:N0} changed records{of}; deleted {deleted:N0} records NatureServe has unpublished.");
        } else if (deleteNotSeenSince is not null) {
            AnsiConsole.MarkupLineInterpolated($"[green]Full download finished:[/] stored {seen:N0} records{of}; deleted {deleted:N0} stored records that are no longer on NatureServe Explorer.");
        } else {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Full download finished, with records missing:[/] stored {seen:N0} records{of}. Records stored before this download were kept, because some records were not read.");
        }
        PrintStatus(store, NatureServePassState.Read(store.GetState));
        return 0;
    }

    private static string Progress(long seen, long? total, long storedThisRun, TimeSpan elapsed) {
        if (total is not > 0) {
            return $"{seen:N0} records stored so far.";
        }
        var text = $"{seen:N0} of {total.Value:N0} records stored ({Math.Min(100.0, 100.0 * seen / total.Value):0.0}%)";
        var left = total.Value - seen;
        if (storedThisRun > 0 && left > 0) {
            var eta = TimeSpan.FromSeconds(elapsed.TotalSeconds / storedThisRun * left);
            text += eta.TotalMinutes >= 1 ? $", about {eta.TotalMinutes:0} min left" : $", about {eta.TotalSeconds:0} s left";
        }
        return text + ".";
    }

    private static void PrintStatus(StatusListStore store, NatureServePassState state) {
        var table = new Table().Border(TableBorder.Rounded).AddColumn("").AddColumn("");
        table.AddRow("NatureServe records stored", store.CountNatureServe().ToString("N0", CultureInfo.InvariantCulture));
        if (state.CompletedUtc is { } completed) {
            var kind = state.CompletedKind == NatureServePassKeys.Refresh ? "Refresh" : "Full download";
            var counts = state.CompletedSeen is { } seen
                ? state.CompletedTotal is { } total ? $"; {seen:N0} records stored of the {total:N0} NatureServe gave" : $"; {seen:N0} records stored"
                : "";
            table.AddRow("Last download finished", $"{completed:yyyy-MM-dd HH:mm} UTC ({kind}{counts})");
        } else {
            table.AddRow("Last download finished", "never");
        }
        if (state.PassStartedUtc is { } started) {
            var partitions = store.GetPartitions();
            var total = state.PassTotal is { } t ? $" of {t:N0}" : "";
            table.AddRow("Download under way",
                $"{(state.PassIsRefresh ? "refresh" : "full download")} started {started:yyyy-MM-dd HH:mm} UTC; {store.CountNatureServeFetchedSince(started):N0}{total} records stored; {partitions.Count(p => p.Done):N0} of {partitions.Count:N0} name prefixes done so far");
        }
        AnsiConsole.Write(table);
    }
}
