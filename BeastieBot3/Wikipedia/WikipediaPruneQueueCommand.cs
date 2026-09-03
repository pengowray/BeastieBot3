using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Taxonomy;

// Removes queued titles that carry a taxonomic authority or a nomenclatural note, which no
// Wikipedia article does. They came from IUCN synonyms stored complete ("Eumeces schneideri
// (Daudin, 1802) [orth. error]") being used verbatim as candidate article titles. In the 2026-1
// cache that was 48,000 of the 190,000 titles waiting to be downloaded, none of which can exist.
//
// The matcher no longer produces them (IucnSynonymService reduces a name to its bare form).
// `wikipedia update` runs this with --apply before its match step, so whatever an older run
// queued is gone before the matcher looks. Reports by default; --apply deletes.
//
// The rule is narrower than "BareScientificName.Strip changed it": the queue also holds
// common-name titles from Wikidata sitelinks, which Strip cuts too. See CarriesAuthorityOrNote.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia prune-queue", CommandKind.Destructive,
    "Remove queued Wikipedia titles that carry a taxonomic authority or a nomenclatural note, which no article title does. Reports what it would remove unless --apply is given.",
    Reason = "Deletes queued titles from the Wikipedia cache. Cached pages and settled matches are untouched.",
    Rerun = RerunEffect.Rebuilds,
    Examples = new[] {
        "wikipedia prune-queue",
        "wikipedia prune-queue --apply",
    })]
internal sealed class WikipediaPruneQueueCommand : Command<WikipediaPruneQueueCommand.Settings> {
    private const int ExamplesShown = 15;

    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--apply")]
        [Description("Delete the titles. Without it the command only reports what it would remove.")]
        public bool Apply { get; init; }

        [CommandOption("--report <FILE>")]
        [Description("Write every title that would be (or was) removed to this file, one per line, for checking.")]
        public string? ReportPath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();

        string cachePath;
        try {
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }

        using var store = WikipediaCacheStore.Open(cachePath);
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikipedia cache:[/] {Markup.Escape(cachePath)}");

        var queued = store.ReadQueuedTitles();
        var doomed = new List<long>();
        var doomedTitles = new List<string>();
        foreach (var (id, title) in queued) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!BareScientificName.CarriesAuthorityOrNote(title)) {
                continue;
            }
            doomed.Add(id);
            doomedTitles.Add(title);
        }

        if (!string.IsNullOrWhiteSpace(settings.ReportPath)) {
            var reportPath = Path.GetFullPath(settings.ReportPath);
            Directory.CreateDirectory(Path.GetDirectoryName(reportPath)!);
            File.WriteAllLines(reportPath, doomedTitles);
            AnsiConsole.MarkupLineInterpolated($"[grey]Full list written to[/] {Markup.Escape(reportPath)}");
        }

        if (doomed.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[green]Nothing to remove.[/] All {queued.Count:n0} queued titles read as names.");
            return 0;
        }

        var examples = doomedTitles.GetRange(0, Math.Min(ExamplesShown, doomedTitles.Count));

        AnsiConsole.MarkupLineInterpolated(
            $"{doomed.Count:n0} of {queued.Count:n0} queued titles carry an authority or a note:");
        foreach (var title in examples) {
            AnsiConsole.MarkupLineInterpolated($"  [grey]{Markup.Escape(title)}[/]");
        }
        if (doomed.Count > examples.Count) {
            AnsiConsole.MarkupLineInterpolated($"  [grey]and {doomed.Count - examples.Count:n0} more.[/]");
        }

        if (!settings.Apply) {
            AnsiConsole.MarkupLine("[yellow]Nothing removed. Run again with --apply to delete them.[/]");
            return 0;
        }

        var deleted = store.DeletePages(doomed);
        AnsiConsole.MarkupLineInterpolated($"[green]Removed {deleted:n0} titles.[/]");
        AnsiConsole.MarkupLine("[grey]Taxa that were waiting on one keep their place and are picked up by the next `wikipedia match-taxa` run.[/]");
        return 0;
    }
}
