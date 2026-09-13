using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Web.Flows;

// What a `wikipedia update` step, round or run actually changed, measured as the difference
// between two coverage snapshots. The standing totals alone could not show it: a run that checked
// 1,300 titles and found 81 articles printed "132,906 pages cached" against the previous
// "132,825", and read as a run that did almost nothing.
//
// Pure over two WikiCoverageState records, so the wording and the "did anything move" decision
// that ends an --until-done loop are pinned by WikiUpdateProgressTests.

namespace BeastieBot3.Wikipedia;

internal static class WikiUpdateProgress {
    internal enum Group { Wikidata, Pages, Taxa }

    /// <param name="Label">Row label in the run summary table.</param>
    /// <param name="Up">Step-result phrase when the count rises, with {0} for the amount.</param>
    /// <param name="Down">Step-result phrase when the count falls, with {0} for the amount.</param>
    /// <param name="CountsAsProgress">Movement here means work got done. Queue sizes and failure
    /// counts are excluded: they move when nothing useful happened (a retry that fails again).</param>
    /// <param name="RiseInStepLine">False when a rise only repeats another phrase in the same step
    /// line: every title queued is also a rise in "titles queued".</param>
    internal sealed record Metric(
        Group Group,
        string Label,
        Func<WikiCoverageState, long> Read,
        string Up,
        string Down,
        bool CountsAsProgress,
        bool RiseInStepLine = true);

    internal sealed record Change(Metric Metric, long Before, long After) {
        public long Delta => After - Before;
    }

    // Table order. Step lines put the outcomes (CountsAsProgress) first and queue sizes after,
    // so a match step leads with what matched rather than with the pages it queued.
    internal static readonly IReadOnlyList<Metric> Metrics = new Metric[] {
        new(Group.Wikidata, "Wikidata items downloaded", s => s.WikidataEntitiesCached,
            "{0} Wikidata items downloaded", "{0} Wikidata items removed from the cache", true),
        new(Group.Wikidata, "Wikidata items queued", s => s.WikidataEntitiesQueued,
            "{0} Wikidata items queued", "{0} fewer Wikidata items queued", false),
        new(Group.Wikidata, "Taxa with no Wikidata item", s => s.TaxaWithoutWikidata,
            "{0} more taxa with no Wikidata item", "{0} taxa linked to a Wikidata item", true),
        // Falls under the sweep as well as the search (a linked taxon needs no search), so the
        // phrase can't claim a search happened.
        new(Group.Wikidata, "Taxa never searched for", s => s.TaxaNeverSearched,
            "{0} more taxa to search for", "{0} fewer taxa to search for", true),

        new(Group.Pages, "Titles ever queued", s => s.PagesKnown,
            "{0} new titles", "{0} titles dropped", true, RiseInStepLine: false),
        new(Group.Pages, "Pages downloaded", s => s.PagesCached,
            "{0} pages downloaded", "{0} pages removed from the cache", true),
        new(Group.Pages, "Titles with no article", s => s.PagesMissing,
            "{0} titles have no article", "{0} fewer titles with no article", true),
        new(Group.Pages, "Failed downloads", s => s.PagesFailed,
            "{0} downloads failed", "{0} fewer failed downloads", false),
        new(Group.Pages, "Titles queued", s => s.PagesQueued,
            "{0} titles queued", "{0} fewer titles queued", false),
        new(Group.Pages, "Pages awaited by a taxon", s => s.PagesQueuedAwaited,
            "{0} more pages awaited by a taxon", "{0} fewer pages awaited by a taxon", false),

        new(Group.Taxa, "Taxa matched to an article", s => s.TaxaWithArticle,
            "{0} taxa matched to an article", "{0} fewer taxa matched to an article", true),
        new(Group.Taxa, "Taxa with no article", s => s.TaxaWithoutArticle,
            "{0} taxa found to have no article", "{0} fewer taxa with no article", true),
        new(Group.Taxa, "Taxa waiting on a page", s => s.TaxaAwaitingPage,
            "{0} more taxa waiting on a page", "{0} fewer taxa waiting on a page", true),
        new(Group.Taxa, "Taxa never checked", s => s.TaxaNeverMatched,
            "{0} more taxa never checked", "{0} taxa checked for the first time", true),
    };

    /// Every metric that moved, in table order. Empty when either snapshot is unmeasured, since a
    /// difference against an unknown state is not a measurement.
    public static IReadOnlyList<Change> Changes(WikiCoverageState before, WikiCoverageState after) {
        if (!before.Known || !after.Known) return Array.Empty<Change>();
        return Metrics
            .Select(m => new Change(m, m.Read(before), m.Read(after)))
            .Where(c => c.Delta != 0)
            .ToList();
    }

    /// True when real work moved between the snapshots. An --until-done loop stops on a round
    /// where this is false, because the next round would start from the same place.
    /// An unmeasured snapshot counts as progress: stopping on a failed count would end a loop
    /// that is working.
    public static bool MadeProgress(WikiCoverageState before, WikiCoverageState after) {
        if (!before.Known || !after.Known) return true;
        return Metrics.Any(m => m.CountsAsProgress && m.Read(before) != m.Read(after));
    }

    /// One line for a step's result: "81 pages downloaded · 1,197 titles have no article".
    /// Null when nothing moved (or a snapshot is unmeasured), so the caller chooses how to say that.
    public static string? Describe(WikiCoverageState before, WikiCoverageState after) {
        var phrases = Changes(before, after)
            .Where(c => c.Delta < 0 || c.Metric.RiseInStepLine)
            .OrderBy(c => c.Metric.CountsAsProgress ? 0 : 1)   // stable: table order within each half
            .Select(c => string.Format(c.Delta > 0 ? c.Metric.Up : c.Metric.Down, Math.Abs(c.Delta).ToString("n0")))
            .ToList();
        return phrases.Count == 0 ? null : string.Join(" · ", phrases);
    }

    /// Whether Wikidata gained items or taxon links between the snapshots: the thing that can
    /// give a taxon a new candidate title, and so the reason to queue titles or re-match again.
    public static bool WikidataChanged(WikiCoverageState before, WikiCoverageState after) =>
        !before.Known || !after.Known
        || before.WikidataEntitiesCached != after.WikidataEntitiesCached
        || before.TaxaWithoutWikidata != after.TaxaWithoutWikidata;
}
