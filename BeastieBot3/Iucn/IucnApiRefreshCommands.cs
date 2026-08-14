using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Commands;
using Spectre.Console;
using Spectre.Console.Cli;

// Start, inspect and abandon a refresh of the API cache.
//
// Sessions are created here and nowhere else. The download commands only ever READ the active
// session's cutoff, so they can't disagree about which date they are refreshing to — one command
// owns the decision, the rest follow it. That is also what makes resuming a plain re-run: with a
// session open, `iucn api cache-all` already knows the date.

namespace BeastieBot3.Iucn;

// Shared by every download command: work out which cutoff this run uses and say so on screen.
// A run that silently adopted a session's date would look like an ordinary top-up.
internal static class IucnRefreshRun {
    internal sealed record Plan(DateTime? Threshold, RefreshThresholdSource Source, IucnRefreshSession? Session);

    // Returns null when --refresh-before could not be read; the caller should stop.
    public static Plan? Begin(IucnApiCacheStore store, string? refreshBefore, double? maxAgeHours) {
        DateTime? explicitCutoff = null;
        if (!string.IsNullOrWhiteSpace(refreshBefore)) {
            if (!IucnRefreshMath.TryParseCutoffUtc(refreshBefore, out var parsed)) {
                AnsiConsole.MarkupLineInterpolated($"[red]Could not read --refresh-before:[/] {refreshBefore}");
                AnsiConsole.MarkupLine("Use a date like [bold]2026-06-16[/], a full timestamp, or [bold]now[/].");
                return null;
            }
            explicitCutoff = parsed;
        }

        var session = store.GetActiveRefreshSession();
        var (threshold, source) = IucnRefreshMath.ResolveThreshold(explicitCutoff, maxAgeHours, session, DateTime.UtcNow);
        if (threshold is not null) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{IucnRefreshMath.DescribeThreshold(threshold.Value, source, session)}[/]");
        }
        return new Plan(threshold, source, session);
    }

    // Close the session once every phase it asked for has finished, so the stored cutoff stops
    // applying to later runs and the workflow step stops offering to resume.
    public static void CloseIfFinished(IucnApiCacheStore store, IucnRefreshSession? session) {
        if (session is null || session.CompletedAt is not null) return;
        var current = store.GetActiveRefreshSession();
        if (current is null || current.Id != session.Id) return;

        var progress = IucnApiRefreshStartCommand.ReadProgress(store, current);
        if (!progress.IsFinished) return;

        store.CloseRefreshSession(current.Id);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Refresh finished:[/] {current.DisplayLabel}. Everything fetched before {IucnRefreshMath.Stamp(current.CutoffUtc)} has been downloaded again.");
    }
}

[CommandInfo("iucn api refresh-start", CommandKind.Mutates,
    "Begin a refresh of the API cache: mark everything downloaded before a cutoff date to be fetched again.",
    Reason = "Records the refresh cutoff. Downloads only happen when you then run the cache commands.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Records a cutoff date; the following cache-all run re-downloads everything older than it, and resumes from where it stopped on every later run. Refuses to start while another refresh is open unless you pass --replace.",
    Examples = new[] {
        "iucn api refresh-start --label 2026-1",
        "iucn api refresh-start --cutoff 2026-06-16 --label 2026-1",
        "iucn api refresh-start --label 2026-1 --no-tombstones",
    })]
internal sealed class IucnApiRefreshStartCommand : AsyncCommand<IucnApiRefreshStartCommand.Settings> {
    // Option order is the order the web UI shows the fields in, so the ones an operator actually
    // sets come before the path override.
    public sealed class Settings : CommonSettings {
        [CommandOption("--cutoff <DATE>")]
        [Description("Re-download everything fetched before this date (UTC, e.g. 2026-06-16). Defaults to now, which refreshes the whole cache.")]
        public string? Cutoff { get; init; }

        [CommandOption("--label <NAME>")]
        [Description("What you are refreshing to, shown wherever the refresh is reported (e.g. 2026-1).")]
        public string? Label { get; init; }

        [CommandOption("--no-tombstones")]
        [Description("Skip the final pass that re-checks taxa the API previously said were gone. They are checked by default, because a taxon absent from the last release can exist in the new one.")]
        public bool NoTombstones { get; init; }

        [CommandOption("--no-discovery")]
        [Description("Skip the family-paging sweep that finds taxa missing from the CSV export. Included by default.")]
        public bool NoDiscovery { get; init; }

        [CommandOption("--replace")]
        [Description("Abandon the refresh already in progress and start this one instead.")]
        public bool Replace { get; init; }

        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;

        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        using var store = IucnApiCacheStore.Open(cachePath);

        if (!IucnRefreshMath.TryParseCutoffUtc(settings.Cutoff ?? "now", out var cutoff)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read the cutoff date:[/] {settings.Cutoff}");
            AnsiConsole.MarkupLine("Use a date like [bold]2026-06-16[/], a full timestamp, or [bold]now[/].");
            return Task.FromResult(-1);
        }

        var active = store.GetActiveRefreshSession();
        if (active is not null && !settings.Replace) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]A refresh is already in progress:[/] {active.DisplayLabel}, cutoff {IucnRefreshMath.Stamp(active.CutoffUtc)}.");
            ReportProgress(store, active);
            AnsiConsole.MarkupLine("Run [bold]iucn api cache-all --full[/] to carry on with it, or add [bold]--replace[/] to abandon it and start again.");
            return Task.FromResult(-1);
        }

        if (active is not null) {
            store.CloseRefreshSession(active.Id);
            AnsiConsole.MarkupLineInterpolated($"[yellow]Abandoned the refresh in progress:[/] {active.DisplayLabel}.");
        }

        var session = store.StartRefreshSession(cutoff, settings.Label, !settings.NoTombstones, !settings.NoDiscovery);

        AnsiConsole.MarkupLineInterpolated(
            $"[green]Refresh started:[/] {session.DisplayLabel}, re-downloading everything fetched before {IucnRefreshMath.Stamp(cutoff)}.");
        AnsiConsole.MarkupLineInterpolated(
            $"To re-download: {session.StartTaxaRemaining:N0} taxa and {session.StartAssessmentsRemaining:N0} assessments.");
        if (session.IncludeDiscovery) {
            AnsiConsole.MarkupLine("Includes the family-paging sweep for taxa missing from the CSV export.");
        }
        if (session.IncludeTombstones) {
            AnsiConsole.MarkupLine("Ends by re-checking the taxa the API previously said were gone.");
        }
        AnsiConsole.MarkupLine("Now run [bold]iucn api cache-all --full[/]. Stop it whenever you like: re-running carries on from where it stopped, and you never re-enter the date.");
        return Task.FromResult(0);
    }

    internal static void ReportProgress(IucnApiCacheStore store, IucnRefreshSession session) {
        var progress = ReadProgress(store, session);
        if (session.StartTaxaRemaining == 0 && session.StartAssessmentsRemaining == 0) {
            AnsiConsole.MarkupLine("Nothing in the cache was older than the cutoff, so there was nothing to re-download.");
            return;
        }
        AnsiConsole.MarkupLineInterpolated(
            $"Done so far: {progress.TaxaDone:N0} of {session.StartTaxaRemaining:N0} taxa, {progress.AssessmentsDone:N0} of {session.StartAssessmentsRemaining:N0} assessments ({progress.PercentDone}%).");
    }

    internal static IucnRefreshProgress ReadProgress(IucnApiCacheStore store, IucnRefreshSession session) => new() {
        Session = session,
        TaxaRemaining = store.CountTaxaDownloadedBefore(session.CutoffUtc),
        AssessmentsRemaining = store.CountAssessmentsDownloadedBefore(session.CutoffUtc),
    };
}

[CommandInfo("iucn api refresh-status", CommandKind.ReadOnly,
    "Show how far the API cache refresh has got, and what is left to download.",
    Rerun = RerunEffect.ReadOnly,
    Examples = new[] { "iucn api refresh-status" })]
internal sealed class IucnApiRefreshStatusCommand : Command<IucnApiRefreshStatusCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        using var store = IucnApiCacheStore.Open(cachePath);

        AnsiConsole.MarkupLineInterpolated($"[grey]API cache database:[/] {cachePath}");
        AnsiConsole.MarkupLineInterpolated(
            $"Cached now: {store.CountTaxa():N0} taxa, {store.CountAssessments():N0} assessments.");
        var oldest = store.GetOldestTaxaDownloadedAt();
        if (oldest is not null) {
            AnsiConsole.MarkupLineInterpolated($"Oldest taxon payload was fetched {IucnRefreshMath.Stamp(oldest.Value)}.");
        }

        var active = store.GetActiveRefreshSession();
        if (active is null) {
            var last = store.GetLastRefreshSession();
            AnsiConsole.MarkupLine(last is null
                ? "[grey]No refresh has been run. Start one with[/] [bold]iucn api refresh-start[/][grey].[/]"
                : $"[green]No refresh in progress.[/] The last one ({last.DisplayLabel}) finished {IucnRefreshMath.Stamp(last.CompletedAt ?? last.StartedAt)}.");
            return 0;
        }

        var progress = IucnApiRefreshStartCommand.ReadProgress(store, active);
        AnsiConsole.MarkupLineInterpolated(
            $"[yellow]Refresh in progress:[/] {active.DisplayLabel}, cutoff {IucnRefreshMath.Stamp(active.CutoffUtc)}, started {IucnRefreshMath.Stamp(active.StartedAt)}.");

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Step");
        table.AddColumn(new TableColumn("Done").RightAligned());
        table.AddColumn(new TableColumn("Left").RightAligned());
        table.AddRow("Taxa", $"{progress.TaxaDone:N0}", $"{progress.TaxaRemaining:N0}");
        table.AddRow("Assessments", $"{progress.AssessmentsDone:N0}", $"{progress.AssessmentsRemaining:N0}");
        if (active.IncludeDiscovery) {
            table.AddRow("Family sweep", active.DiscoveryDoneAt is null ? "not yet" : "done", "");
        }
        if (active.IncludeTombstones) {
            table.AddRow("Re-check gone taxa", active.TombstonesDoneAt is null ? "not yet" : "done", "");
        }
        AnsiConsole.Write(table);

        AnsiConsole.MarkupLine(progress.IsFinished
            ? "[green]Everything is downloaded.[/] The refresh closes itself on the next [bold]iucn api cache-all[/] run."
            : "Carry on with [bold]iucn api cache-all --full[/]. The cutoff is remembered, so there is nothing to re-enter.");
        return 0;
    }
}

[CommandInfo("iucn api refresh-abandon", CommandKind.Mutates,
    "Stop the refresh in progress. Nothing already downloaded is lost.",
    Reason = "Closes the refresh record only; no cached payload is deleted.",
    Rerun = RerunEffect.IdempotentAdd,
    Examples = new[] { "iucn api refresh-abandon" })]
internal sealed class IucnApiRefreshAbandonCommand : Command<IucnApiRefreshAbandonCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        using var store = IucnApiCacheStore.Open(cachePath);

        var active = store.GetActiveRefreshSession();
        if (active is null) {
            AnsiConsole.MarkupLine("[grey]No refresh is in progress.[/]");
            return 0;
        }

        IucnApiRefreshStartCommand.ReportProgress(store, active);
        store.CloseRefreshSession(active.Id);
        AnsiConsole.MarkupLineInterpolated(
            $"[yellow]Stopped the refresh:[/] {active.DisplayLabel}. Everything downloaded so far is kept; later runs go back to fetching only what is missing.");
        return 0;
    }
}
