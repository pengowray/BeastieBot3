using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Flows;

// One command for the whole Wikidata/Wikipedia cache ladder. Each step is still its own
// command (they stay runnable one at a time from the workflow page's "Step by step" panel);
// this runs them in priority order, measures the real databases before each one, and skips
// the ones with nothing to do. Re-running never redoes finished work, so the answer to
// "which step am I up to" is always: run it again, or run it with --status to look.
//
// Sub-steps run in-process through Program.BuildApp(), exactly the way the web job runner
// launches commands, so their output, limits and flags are identical to running them by hand.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia update", CommandKind.Mutates,
    "Run all the Wikidata and Wikipedia cache steps in order: sweep, download, search, queue titles, match, fetch pages. Skips steps with nothing to do and stops cleanly, so re-running continues where it left off. --status shows the same plan without running anything.",
    Reason = "Runs the individual cache commands in order; each only adds what is missing.",
    Rerun = RerunEffect.IdempotentAdd,
    Examples = new[] {
        "wikipedia update",
        "wikipedia update --status",
        "wikipedia update --limit 500",
        "wikipedia update --include-rest --limit 0"
    })]
public sealed class WikipediaUpdateCommand : AsyncCommand<WikipediaUpdateCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--status")]
        [Description("Show what each step has left to do and what a run would do, then exit without running anything.")]
        public bool StatusOnly { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Most downloads or searches per step this run (default 2000; 0 = no cap). Whatever is not reached this run is picked up by the next.")]
        public int Limit { get; init; } = 2000;

        [CommandOption("--include-rest")]
        [Description("Also retry failed downloads and work the low-priority queue (higher taxa, synonyms, redirects). Off by default because that queue holds hundreds of thousands of titles.")]
        public bool IncludeRest { get; init; }
    }

    // One rung of the ladder. Gate reads a fresh measurement just before the rung runs, so a
    // count changed by an earlier rung (the search queues items; the download drains them) is
    // seen, not guessed. Null gate = always runs (the step is cheap and finds its own work).
    private sealed record Rung(
        string Title,
        string[] Commands,
        Func<WikiCoverageState, (bool Run, string Why)>? Gate,
        bool StopOnFailure = true
    );

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var state = WikiCoverageStateReader.ReadNow(paths);
        if (!state.IucnExists) {
            AnsiConsole.MarkupLine("[red]No IUCN database found.[/] Import an IUCN release first (the step above this one in the workflow).");
            return -1;
        }

        var limit = Math.Max(0, settings.Limit);
        var rungs = BuildLadder(settings, limit, state);

        PrintPlan(rungs, state, limit, settings.StatusOnly);
        if (settings.StatusOnly) {
            return 0;
        }

        var stepsRun = 0;
        var stepsSkipped = 0;
        for (var i = 0; i < rungs.Count; i++) {
            cancellationToken.ThrowIfCancellationRequested();
            var rung = rungs[i];

            // Measure again now: an earlier rung may have queued or drained the very thing
            // this rung is gated on.
            state = WikiCoverageStateReader.ReadNow(paths);
            var (run, why) = Decide(rung, state);
            if (!run) {
                stepsSkipped++;
                AnsiConsole.MarkupLineInterpolated($"[grey]Step {i + 1} of {rungs.Count} · {rung.Title} — skipped: {why}[/]");
                continue;
            }

            stepsRun++;
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLineInterpolated($"[bold]Step {i + 1} of {rungs.Count} · {rung.Title}[/] [grey]({why})[/]");

            foreach (var command in rung.Commands) {
                var full = WithCommonArgs(command, settings);
                AnsiConsole.MarkupLineInterpolated($"[grey]$ beastiebot3 {full}[/]");
                var exit = await RunSubCommandAsync(full, cancellationToken).ConfigureAwait(false);
                if (exit != 0 && !cancellationToken.IsCancellationRequested) {
                    if (rung.StopOnFailure) {
                        AnsiConsole.WriteLine();
                        AnsiConsole.MarkupLineInterpolated(
                            $"[red]Stopped at step {i + 1} ({rung.Title}):[/] `{command}` exited with code {exit}. Nothing done so far is lost; run `wikipedia update` again to continue from here.");
                        return exit;
                    }
                    AnsiConsole.MarkupLineInterpolated(
                        $"[yellow]`{command}` exited with code {exit}.[/] Continuing; the remaining steps do not depend on it. Run `wikipedia update` again later to retry this step.");
                }
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        // Where things stand now that the run is done, and what a re-run would still find.
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLineInterpolated($"[green]Update finished:[/] {stepsRun} steps ran, {stepsSkipped} had nothing to do.");
        state = WikiCoverageStateReader.ReadNow(paths);
        PrintTotals(state);
        PrintWhatRemains(state, settings, limit);
        return 0;
    }

    private List<Rung> BuildLadder(Settings settings, int limit, WikiCoverageState initial) {
        string Cap(string command) => limit > 0 ? $"{command} --limit {limit}" : command;

        var rungs = new List<Rung> {
            new("Sweep Wikidata for new IUCN-tagged items",
                new[] { "wikidata seed-taxa" },
                Gate: null,
                // Needs query.wikidata.org, a public endpoint that is regularly overloaded. The
                // eight steps below use the Wikidata and Wikipedia web APIs instead, which stay up
                // independently of it, so an outage here must not abandon the run.
                StopOnFailure: false),
            new("Download queued Wikidata items",
                new[] { Cap("wikidata cache-entities") },
                s => s.WikidataEntitiesQueued > 0
                    ? (true, $"{s.WikidataEntitiesQueued:n0} items queued")
                    : (false, "nothing queued")),
            new("Search Wikidata for taxa the sweep missed",
                new[] { Cap("wikidata backfill-iucn") },
                s => Unsearched(s) > 0
                    ? (true, $"{Unsearched(s):n0} taxa never searched for")
                    : (false, "every taxon without an item has been searched for already")),
            new("Download the items the search found",
                new[] { Cap("wikidata cache-entities") },
                s => s.WikidataEntitiesQueued > 0
                    ? (true, $"{s.WikidataEntitiesQueued:n0} items queued")
                    : (false, "the search queued nothing new")),
            new("Queue Wikipedia titles from the cached items",
                new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa" },
                Gate: null),
            new("Check for a new all-titles dump",
                new[] { "wikipedia titles-dump" },
                Gate: null,
                StopOnFailure: false),   // needs dumps.wikimedia.org; the update works without it
            new("Match taxa to articles",
                new[] { "wikipedia match-taxa" },
                s => s.TaxaNeverMatched > 0
                    ? (true, $"{s.TaxaNeverMatched:n0} taxa never checked")
                    : (false, "every taxon has been checked")),
            new("Download the pages taxa are waiting on",
                new[] { Cap("wikipedia fetch-pages --awaited-only --newest-first") },
                s => s.PagesQueuedAwaited > 0
                    ? (true, $"{s.PagesQueuedAwaited:n0} pages awaited")
                    : (false, "no taxon is waiting on a page")),
            new("Settle the matches for the pages that arrived",
                new[] { "wikipedia match-taxa" },
                s => s.TaxaAwaitingPage > 0 || s.TaxaNeverMatched > 0
                    ? (true, $"{s.TaxaAwaitingPage:n0} taxa have a candidate page to settle")
                    : (false, "no matches waiting on a page")),
        };

        if (settings.IncludeRest) {
            rungs.Add(new("Retry failed downloads",
                new[] { Cap("wikidata cache-entities --failed-only"), Cap("wikipedia fetch-pages --failed-only") },
                s => s.WikidataEntitiesFailed > 0 || s.PagesFailed > 0
                    ? (true, $"{s.WikidataEntitiesFailed:n0} Wikidata items and {s.PagesFailed:n0} pages failed before")
                    : (false, "no downloads have failed")));
            // --exists-first only helps once a dump is imported; the rung before this imports it.
            var existsFirst = initial.DumpTitles > 0 ? " --exists-first" : "";
            rungs.Add(new("Download the rest of the queue (low priority)",
                new[] { Cap($"wikipedia fetch-pages{existsFirst}") },
                s => RestOfQueue(s) > 0
                    ? (true, $"{RestOfQueue(s):n0} other titles queued")
                    : (false, "nothing else queued")));
        }

        return rungs;
    }

    private static (bool Run, string Why) Decide(Rung rung, WikiCoverageState state) {
        if (rung.Gate is null) {
            return (true, "always checks for new work; adds only what is missing");
        }
        // The gate counts come from a cross-database read that can be unavailable (a cache
        // created moments ago by an earlier rung). Running the step is the safe default: every
        // step skips work already done.
        if (!state.Known) {
            return (true, "will run (couldn't count what's left)");
        }
        return rung.Gate(state);
    }

    private void PrintPlan(List<Rung> rungs, WikiCoverageState state, int limit, bool statusOnly) {
        var table = new Table().Border(TableBorder.Simple);
        table.AddColumn("#");
        table.AddColumn("Step");
        table.AddColumn(statusOnly ? "What a run would do" : "This run");

        for (var i = 0; i < rungs.Count; i++) {
            var (run, why) = Decide(rungs[i], state);
            table.AddRow(
                (i + 1).ToString(),
                rungs[i].Title,
                run ? why : $"[grey]skip — {why}[/]");
        }
        AnsiConsole.Write(table);

        if (limit > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Downloads and searches are capped at {limit:n0} per step this run (--limit changes this; 0 removes the cap). Re-running continues where this run stops.[/]");
        }

        PrintTotals(state);
    }

    // The standing counts, so the plan and the finish line both say where things stand overall,
    // not just what this run touched.
    private static void PrintTotals(WikiCoverageState s) {
        if (!s.Known) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Overall counts unavailable:[/] {s.UnavailableReason ?? MissingCacheReason(s)}");
            return;
        }

        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Taxa:[/] {s.IucnTaxa:n0} in IUCN · {s.TaxaWithArticle:n0} matched to an article · {s.TaxaWithoutArticle:n0} checked, no article found · {s.TaxaNeverMatched:n0} never checked");
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Wikidata:[/] {s.WikidataEntitiesCached:n0} items downloaded · {s.WikidataEntitiesQueued:n0} queued · {s.WikidataEntitiesFailed:n0} failed · {Unsearched(s):n0} taxa never searched for");
        var dump = s.DumpTitles == 0 ? "no all-titles dump imported" : $"all-titles dump of {s.DumpDate ?? "unknown date"} imported";
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Wikipedia:[/] {s.PagesCached:n0} pages cached · {s.PagesQueued:n0} queued ({s.PagesQueuedAwaited:n0} awaited by a taxon) · {s.PagesFailed:n0} failed · {dump}");
    }

    // The one cause worth naming, because it is the one the reader can act on. Everything else
    // comes back as the reader it failed on, verbatim.
    private static string MissingCacheReason(WikiCoverageState s) {
        var missing = new List<string>();
        if (!s.IucnExists) missing.Add("the IUCN database");
        if (!s.WikidataExists) missing.Add("the Wikidata cache");
        if (!s.WikipediaExists) missing.Add("the Wikipedia cache");
        return missing.Count > 0
            ? $"{string.Join(" and ", missing)} not found. Check the paths in paths.ini."
            : "the counts have not been measured yet.";
    }

    // What a re-run (or a bigger run) would still pick up. Without this, "finished" reads as
    // "done forever", and with queues this size it never is.
    private static void PrintWhatRemains(WikiCoverageState s, Settings settings, int limit) {
        if (!s.Known) return;

        var remains = new List<string>();
        if (s.WikidataEntitiesQueued > 0) remains.Add($"{s.WikidataEntitiesQueued:n0} Wikidata items still queued");
        if (Unsearched(s) > 0) remains.Add($"{Unsearched(s):n0} taxa still to search for");
        if (s.TaxaNeverMatched > 0) remains.Add($"{s.TaxaNeverMatched:n0} taxa still never checked");
        if (s.PagesQueuedAwaited > 0) remains.Add($"{s.PagesQueuedAwaited:n0} awaited pages still to download");
        if (remains.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Still to do:[/] {string.Join(" · ", remains)}. Run `wikipedia update` again to continue{(limit > 0 ? ", or raise --limit" : "")}.");
            return;
        }

        var rest = RestOfQueue(s);
        var failed = s.WikidataEntitiesFailed + s.PagesFailed;
        if (!settings.IncludeRest && (rest > 0 || failed > 0)) {
            AnsiConsole.MarkupLineInterpolated(
                $"[green]Caught up on everything the lists need.[/] Left in the low-priority queue: {rest:n0} titles (higher taxa, synonyms, redirects) and {failed:n0} failed downloads. `wikipedia update --include-rest` works through those too.");
        }
        else if (rest > 0 || failed > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[green]Caught up on everything the lists need.[/] {rest:n0} low-priority titles and {failed:n0} failed downloads remain; re-run with --include-rest to keep working through them.");
        }
        else {
            AnsiConsole.MarkupLine("[green]Everything is downloaded and matched. Nothing is queued.[/]");
        }
    }

    private static long Unsearched(WikiCoverageState s) => Math.Max(0, s.TaxaWithoutWikidata - s.WikidataBackfillMisses);
    private static long RestOfQueue(WikiCoverageState s) => Math.Max(0, s.PagesQueued - s.PagesQueuedAwaited);

    private static string WithCommonArgs(string command, Settings settings) {
        if (settings.SettingsDir is not null) command += $" --settings-dir \"{settings.SettingsDir}\"";
        if (settings.IniFile is not null) command += $" --ini-file \"{settings.IniFile}\"";
        return command;
    }

    private static async Task<int> RunSubCommandAsync(string commandLine, CancellationToken cancellationToken) {
        // Same in-process launch the web job runner uses, so the sub-command's output streams
        // into this run's console (and the web job log) as it happens.
        var argv = SplitArgs(commandLine);
        var app = Program.BuildApp();
        return await app.RunAsync(argv, cancellationToken).ConfigureAwait(false);
    }

    // Minimal argv split: our own command strings only quote paths (--settings-dir "c:\a b").
    private static List<string> SplitArgs(string commandLine) {
        var args = new List<string>();
        var current = new System.Text.StringBuilder();
        var inQuotes = false;
        foreach (var ch in commandLine) {
            if (ch == '"') { inQuotes = !inQuotes; continue; }
            if (ch == ' ' && !inQuotes) {
                if (current.Length > 0) { args.Add(current.ToString()); current.Clear(); }
                continue;
            }
            current.Append(ch);
        }
        if (current.Length > 0) args.Add(current.ToString());
        return args;
    }
}
