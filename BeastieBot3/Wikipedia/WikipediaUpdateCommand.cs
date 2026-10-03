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
    "Update the Wikidata and Wikipedia caches and match IUCN taxa to Wikipedia articles, by running the individual cache commands in order. Steps whose queue is empty are skipped, and the next run continues from where the last run stopped.",
    Reason = "Runs the individual cache commands in order; each only adds what is missing. Also drops queued titles that carry an authority or a note, since no article can have such a title.",
    Rerun = RerunEffect.Discovers,
    RerunNote = "Each run also runs wikipedia titles-dump, to import a newer all-titles dump when one has been published, and wikipedia prune-queue --apply, to delete queued titles that no Wikipedia article can have: titles with an author, a year or a note after the scientific name, such as \"Eumeces schneideri (Daudin, 1802) [orth. error]\".",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "wikipedia update",
        "wikipedia update --status",
        "wikipedia update --limit 500",
        "wikipedia update --until-done",
        "wikipedia update --include-rest"
    })]
public sealed class WikipediaUpdateCommand : AsyncCommand<WikipediaUpdateCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--status")]
        [Description("Show what each step has left to do and what a run would do, then exit without running anything.")]
        public bool StatusOnly { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Maximum number of downloads or Wikidata searches per step (0 = no maximum, the default). Downloads and searches beyond the maximum are left for the next run, or the next round with --until-done.")]
        public int Limit { get; init; }

        [CommandOption("--include-rest")]
        [Description("Also retry failed downloads and work the low-priority queue (higher taxa, synonyms, redirects). Off by default because that queue holds hundreds of thousands of titles.")]
        public bool IncludeRest { get; init; }

        [CommandOption("--until-done")]
        [Description("Repeat all the steps in rounds until nothing is left to do or a round changes nothing. --limit caps each step in each round. Ctrl+C stops safely; the next run continues from there.")]
        public bool UntilDone { get; init; }
    }

    // Whether a rung runs again in a later --until-done round. Sweeping Wikidata or checking for
    // a new dump minutes after the first round finds nothing, and the full re-match only finds
    // something when Wikidata has changed since it last ran.
    private enum Repeat {
        EveryRound,          // the gate decides each round
        FirstRoundOnly,
        WhenWikidataChanged, // first round: the gate decides; later rounds: only after new Wikidata items or links
    }

    // One rung of the ladder. Gate reads a fresh measurement just before the rung runs, so a
    // count changed by an earlier rung (the search queues items; the download drains them) is
    // seen, not guessed. Null gate = always runs (the step is cheap and finds its own work).
    private sealed record Rung(
        string Title,
        string[] Commands,
        Func<WikiCoverageState, (bool Run, string Why)>? Gate,
        bool StopOnFailure = true,
        Repeat Repeat = Repeat.EveryRound
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

        PrintPlan(rungs, state, limit, settings);
        if (settings.StatusOnly) {
            return 0;
        }

        var runStart = state;
        var stepsRun = 0;
        var stepsSkipped = 0;
        var round = 0;
        // The state just after each rung last ran, for Repeat.WhenWikidataChanged.
        var lastRanAt = new Dictionary<Rung, WikiCoverageState>();

        while (true) {
            round++;
            var roundStart = state;
            if (settings.UntilDone) {
                AnsiConsole.WriteLine();
                AnsiConsole.Write(new Rule($"[bold]Round {round}[/]").LeftJustified());
            }

            for (var i = 0; i < rungs.Count; i++) {
                cancellationToken.ThrowIfCancellationRequested();
                var rung = rungs[i];

                // Measured after the previous rung, so a count it queued or drained is seen here.
                var (run, why) = round == 1
                    ? Decide(rung, state)
                    : DecideLaterRound(rung, state, lastRanAt.GetValueOrDefault(rung));
                if (!run) {
                    stepsSkipped++;
                    AnsiConsole.MarkupLineInterpolated($"[grey]Step {i + 1} of {rungs.Count} · {rung.Title}: skipped, {why}[/]");
                    continue;
                }

                stepsRun++;
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLineInterpolated($"[bold]Step {i + 1} of {rungs.Count} · {rung.Title}[/] [grey]({why})[/]");

                var before = state;
                foreach (var command in rung.Commands) {
                    var full = WithCommonArgs(command, settings);
                    AnsiConsole.MarkupLineInterpolated($"[grey]$ beastiebot3 {full}[/]");
                    var exit = await RunSubCommandAsync(full, cancellationToken).ConfigureAwait(false);
                    if (exit != 0 && !cancellationToken.IsCancellationRequested) {
                        if (rung.StopOnFailure) {
                            state = WikiCoverageStateReader.ReadNow(paths);
                            PrintStepResult(before, state);
                            AnsiConsole.WriteLine();
                            AnsiConsole.MarkupLineInterpolated(
                                $"[red]Stopped at step {i + 1} ({rung.Title}):[/] `{command}` exited with code {exit}. Nothing done so far is lost; run `wikipedia update` again to continue from here.");
                            PrintRunChanges(runStart, state);
                            return exit;
                        }
                        AnsiConsole.MarkupLineInterpolated(
                            $"[yellow]`{command}` exited with code {exit}.[/] Continuing; the remaining steps do not depend on it. Run `wikipedia update` again later to retry this step.");
                    }
                    cancellationToken.ThrowIfCancellationRequested();
                }

                state = WikiCoverageStateReader.ReadNow(paths);
                lastRanAt[rung] = state;
                PrintStepResult(before, state);
            }

            if (!settings.UntilDone) {
                break;
            }

            var remains = Remaining(state, settings);
            if (remains.Count == 0) {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLineInterpolated($"[green]Round {round} finished with nothing left to do.[/]");
                break;
            }
            if (!WikiUpdateProgress.MadeProgress(roundStart, state)) {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLineInterpolated(
                    $"[yellow]Stopped after round {round}:[/] it changed nothing, so another round would not either. Usually a step failed (see its message above) or the same downloads failed again.");
                break;
            }
            AnsiConsole.WriteLine();
            var roundChanges = WikiUpdateProgress.Describe(roundStart, state);
            if (roundChanges is null) {
                // MadeProgress lets an unmeasured round through; it did not measure "no change".
                AnsiConsole.MarkupLineInterpolated($"[grey]Round {round} done (couldn't measure what changed). Starting round {round + 1}; Ctrl+C stops safely.[/]");
            } else {
                AnsiConsole.MarkupLineInterpolated($"[grey]Round {round} done: {roundChanges}. Starting round {round + 1}; Ctrl+C stops safely.[/]");
            }
        }

        // Where things stand now that the run is done, and what a re-run would still find.
        AnsiConsole.WriteLine();
        if (settings.UntilDone) {
            // Skips include first-round-only steps, which did have work; "had nothing to do" would be false.
            AnsiConsole.MarkupLineInterpolated($"[green]Update finished after {round} round{(round == 1 ? "" : "s")}:[/] {stepsRun} steps ran, {stepsSkipped} were skipped.");
        } else {
            AnsiConsole.MarkupLineInterpolated($"[green]Update finished:[/] {stepsRun} steps ran, {stepsSkipped} had nothing to do.");
        }
        PrintRunChanges(runStart, state);
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
                // Needs query.wikidata.org, a public endpoint that is regularly overloaded. Only
                // this step and the backfill search below use it; everything else goes through the
                // Wikidata and Wikipedia web APIs, which stay up independently of it. So an outage
                // here must not abandon the run.
                StopOnFailure: false,
                Repeat: Repeat.FirstRoundOnly),
            new("Download queued Wikidata items",
                new[] { Cap("wikidata cache-entities") },
                s => s.WikidataEntitiesQueued > 0
                    ? (true, $"{s.WikidataEntitiesQueued:n0} items queued")
                    : (false, "nothing queued")),
            new("Search Wikidata for taxa the sweep missed",
                new[] { Cap("wikidata backfill-iucn") },
                s => s.TaxaNeverSearched > 0
                    ? (true, $"{s.TaxaNeverSearched:n0} taxa never searched for")
                    : (false, "every taxon without an item has been searched for already"),
                // Same query.wikidata.org endpoint as the sweep, and this is the longest step in
                // the ladder — a bad hour there used to strand every Wikipedia step behind it.
                // What it did find is banked as it goes, and the next step re-measures the queue.
                StopOnFailure: false),
            new("Download the items the search found",
                new[] { Cap("wikidata cache-entities") },
                s => s.WikidataEntitiesQueued > 0
                    ? (true, $"{s.WikidataEntitiesQueued:n0} items queued")
                    : (false, "the search queued nothing new")),
            new("Queue Wikipedia titles from the cached items",
                new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa" },
                Gate: null,
                Repeat: Repeat.WhenWikidataChanged),
            new("Check for a new all-titles dump",
                new[] { "wikipedia titles-dump" },
                Gate: null,
                StopOnFailure: false,    // needs dumps.wikimedia.org; the update works without it
                Repeat: Repeat.FirstRoundOnly),
            // Earlier matcher runs queued IUCN synonyms verbatim, authority and note included
            // ("Eumeces schneideri (Daudin, 1802) [orth. error]"), and no article can have such
            // a title. Dropping them here, before the match pass, means a taxon that was waiting
            // on one is re-matched from clean candidates in this same run.
            new("Drop queued titles no article can have",
                new[] { "wikipedia prune-queue --apply" },
                _ => (true, "always runs; removes only titles that carry an authority or a note"),
                Repeat: Repeat.FirstRoundOnly),
            // The full pass: re-checks taxa already found to have no article too, because a new
            // Wikidata item or synonym can give one a candidate title. This rung's old gate
            // ("any taxon never checked") was always true only because 980 varieties the matcher
            // skips were counted as never checked, so the full pass ran on every update, twice.
            new("Match taxa to articles",
                new[] { "wikipedia match-taxa" },
                s => s.TaxaNeverMatched > 0
                    ? (true, $"{s.TaxaNeverMatched:n0} taxa never checked; also re-checks every taxon with no article")
                    : (true, "every taxon has been checked once; re-checks those with no article (a new name or Wikidata item can give one a title to try)"),
                Repeat: Repeat.WhenWikidataChanged),
            new("Download the pages taxa are waiting on",
                new[] { Cap("wikipedia fetch-pages --awaited-only --newest-first") },
                s => s.PagesQueuedAwaited > 0
                    ? (true, $"{s.PagesQueuedAwaited:n0} pages awaited")
                    : (false, "no taxon is waiting on a page")),
            new("Settle the matches for the pages that arrived",
                new[] { "wikipedia match-taxa --pending-only" },
                s => s.TaxaAwaitingPage > 0 || s.TaxaNeverMatched > 0
                    ? (true, $"{s.TaxaAwaitingPage:n0} taxa have a candidate page to settle")
                    : (false, "no matches waiting on a page")),
        };

        if (settings.IncludeRest) {
            rungs.Add(new("Retry failed downloads",
                new[] { Cap("wikidata cache-entities --failed-only"), Cap("wikipedia fetch-pages --failed-only") },
                s => s.WikidataEntitiesFailed > 0 || s.PagesFailed > 0
                    ? (true, $"{s.WikidataEntitiesFailed:n0} Wikidata items and {s.PagesFailed:n0} pages failed before")
                    : (false, "no downloads have failed"),
                // A retry that fails again is the expected outcome for a share of these, and the
                // low-priority queue below does not depend on it.
                StopOnFailure: false,
                // Once is enough: the same failures retried every round would spend the limit
                // on titles that just failed.
                Repeat: Repeat.FirstRoundOnly));
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

    private static (bool Run, string Why) DecideLaterRound(Rung rung, WikiCoverageState state, WikiCoverageState? lastRan) {
        switch (rung.Repeat) {
            case Repeat.FirstRoundOnly:
                return (false, "only runs in the first round");
            case Repeat.WhenWikidataChanged when lastRan is not null && !WikiUpdateProgress.WikidataChanged(lastRan, state):
                return (false, "no new Wikidata items or taxon links since it last ran");
            default:
                return Decide(rung, state);
        }
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

    private void PrintPlan(List<Rung> rungs, WikiCoverageState state, int limit, Settings settings) {
        var statusOnly = settings.StatusOnly;
        var table = new Table().Border(TableBorder.Simple);
        table.AddColumn("#");
        table.AddColumn("Step");
        table.AddColumn(statusOnly ? "What a run would do" : settings.UntilDone ? "First round" : "This run");

        for (var i = 0; i < rungs.Count; i++) {
            var (run, why) = Decide(rungs[i], state);
            table.AddRow(
                (i + 1).ToString(),
                rungs[i].Title,
                run ? why : $"[grey]skip — {why}[/]");
        }
        AnsiConsole.Write(table);

        if (limit > 0) {
            if (settings.UntilDone) {
                AnsiConsole.MarkupLineInterpolated($"[grey]At most {limit:n0} downloads or searches per step per round (--limit; 0 = no cap). Rounds repeat until nothing is left or a round changes nothing.[/]");
            } else {
                AnsiConsole.MarkupLineInterpolated($"[grey]At most {limit:n0} downloads or searches per step this run (--limit; 0 = no cap). Whatever is not reached is picked up by the next run, or add --until-done to repeat the steps until nothing is left.[/]");
            }
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

        var notCounted = s.VarietiesSkipped > 0
            ? $" [grey](not counting {s.VarietiesSkipped:n0} varieties, which the matcher skips, or subpopulations)[/]"
            : " [grey](not counting subpopulations)[/]";
        var taxa = new List<string> {
            $"{s.TaxaWithArticle:n0} matched to an article",
            $"{s.TaxaWithoutArticle:n0} checked, no article found",
        };
        if (s.TaxaAwaitingPage > 0) taxa.Add($"{s.TaxaAwaitingPage:n0} waiting on a page");
        if (s.TaxaRejected > 0) taxa.Add($"{s.TaxaRejected:n0} with only disambiguation pages or pages about another kingdom");
        taxa.Add($"{s.TaxaNeverMatched:n0} never checked");
        AnsiConsole.MarkupLine($"[grey]Taxa:[/] {s.IucnTaxa:n0} in IUCN{notCounted} · {Markup.Escape(string.Join(" · ", taxa))}");
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Wikidata:[/] {s.WikidataEntitiesCached:n0} items downloaded · {s.WikidataEntitiesQueued:n0} queued · {s.WikidataEntitiesFailed:n0} failed · {s.TaxaNeverSearched:n0} taxa never searched for");
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

    // The work a re-run would pick up, most valuable first. --until-done keeps going while this
    // is non-empty (and rounds keep changing something).
    private static List<string> Remaining(WikiCoverageState s, Settings settings) {
        var remains = new List<string>();
        if (!s.Known) return remains;
        if (s.WikidataEntitiesQueued > 0) remains.Add($"{s.WikidataEntitiesQueued:n0} Wikidata items still queued");
        if (s.TaxaNeverSearched > 0) remains.Add($"{s.TaxaNeverSearched:n0} taxa still to search for");
        if (s.TaxaNeverMatched > 0) remains.Add($"{s.TaxaNeverMatched:n0} taxa still never checked");
        if (s.PagesQueuedAwaited > 0) remains.Add($"{s.PagesQueuedAwaited:n0} awaited pages still to download");
        if (settings.IncludeRest && RestOfQueue(s) > 0) remains.Add($"{RestOfQueue(s):n0} low-priority titles still queued");
        return remains;
    }

    // One line under each step: what it changed. A step that finds nothing new says so, rather
    // than leaving the reader to compare totals.
    private static void PrintStepResult(WikiCoverageState before, WikiCoverageState after) {
        if (!before.Known || !after.Known) {
            AnsiConsole.MarkupLine("[grey]Result: not measured (the counts couldn't be read).[/]");
            return;
        }
        var line = WikiUpdateProgress.Describe(before, after);
        if (line is null) {
            AnsiConsole.MarkupLine("[grey]Result: none of the counts changed.[/]");
        } else {
            AnsiConsole.MarkupLineInterpolated($"[green]Result:[/] {line}");
        }
    }

    // The whole run's effect, start against finish, for every count that moved.
    private static void PrintRunChanges(WikiCoverageState start, WikiCoverageState end) {
        var changes = WikiUpdateProgress.Changes(start, end);
        AnsiConsole.WriteLine();
        if (changes.Count == 0) {
            AnsiConsole.MarkupLine(start.Known && end.Known
                ? "[yellow]This run changed none of the counts.[/]"
                : "[grey]Couldn't measure what this run changed; the counts are unavailable (see below).[/]");
            return;
        }

        var table = new Table().Border(TableBorder.Simple).Title("What this run changed");
        table.AddColumn("");
        table.AddColumn(new TableColumn("Before").RightAligned());
        table.AddColumn(new TableColumn("After").RightAligned());
        table.AddColumn(new TableColumn("Change").RightAligned());
        foreach (var c in changes) {
            table.AddRow(
                Markup.Escape(c.Metric.Label),
                c.Before.ToString("n0"),
                c.After.ToString("n0"),
                (c.Delta > 0 ? "+" : "-") + Math.Abs(c.Delta).ToString("n0"));
        }
        AnsiConsole.Write(table);
    }

    // What a re-run (or a bigger run) would still pick up. Without this, "finished" reads as
    // "done forever", and with queues this size it never is.
    private static void PrintWhatRemains(WikiCoverageState s, Settings settings, int limit) {
        if (!s.Known) return;

        var remains = Remaining(s, settings);
        if (remains.Count > 0) {
            var next = settings.UntilDone
                ? "Run `wikipedia update --until-done` again later to retry"
                : $"Run `wikipedia update` again to continue{(limit > 0 ? ", raise --limit to do more per run" : "")}, or add --until-done to repeat until nothing is left";
            AnsiConsole.MarkupLineInterpolated($"[yellow]Still to do:[/] {string.Join(" · ", remains)}. {next}.");
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
