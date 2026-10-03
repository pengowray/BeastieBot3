using System;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// Pins the Wikidata/Wikipedia workflow lights. The steps are ordered by priority, so what each
// light has to get right is which kind of work is outstanding: taxa nothing has ever looked at
// (amber, finishable for this release), a queue being worked down (neutral, "more to do"), and
// failures. Getting these the wrong way round is what made the steps confusing: a permanent
// 190,000-page backlog rendered as a warning says the pipeline is broken when it is not.
public class FlowWikiProbeTests {
    private static WikiCoverageState State(Action<WikiCoverageStateBuilder>? configure = null) {
        var b = new WikiCoverageStateBuilder();
        configure?.Invoke(b);
        return b.Build();
    }

    private sealed class WikiCoverageStateBuilder {
        public bool Known = true;
        public long IucnTaxa = 188_485;
        public long TaxaWithoutWikidata;
        public long SweepCursorP627 = 141_278_490;
        public long SweepCursorP141 = 141_278_490;
        public long BackfillMisses;
        // The reader counts this directly; by default the misses are taken to be current taxa.
        public long? NeverSearched;
        public long EntitiesCached = 181_294;
        public long EntitiesQueued;
        public long EntitiesFailed;
        public long PagesKnown = 334_925;
        public long PagesCached = 99_689;
        public long PagesMissing = 45_024;
        public long PagesQueued;
        public long PagesQueuedAwaited;
        public long PagesFailed;
        public long TaxaNeverMatched;
        public long TaxaWithArticle = 67_134;
        public DateTime? OldestCachedPageAt = new(2026, 6, 14, 0, 0, 0, DateTimeKind.Utc);
        public long DumpTitles;
        public string? DumpDate;
        public long PagesQueuedInDump;
        public long PagesQueuedNotInDump;

        public WikiCoverageState Build() => new() {
            Known = Known,
            IucnTaxa = IucnTaxa,
            TaxaWithoutWikidata = TaxaWithoutWikidata,
            WikidataSweepCursorP627 = SweepCursorP627,
            WikidataSweepCursorP141 = SweepCursorP141,
            WikidataBackfillMisses = BackfillMisses,
            TaxaNeverSearched = NeverSearched ?? Math.Max(0, TaxaWithoutWikidata - BackfillMisses),
            WikidataEntitiesCached = EntitiesCached,
            WikidataEntitiesQueued = EntitiesQueued,
            WikidataEntitiesFailed = EntitiesFailed,
            PagesKnown = PagesKnown,
            PagesCached = PagesCached,
            PagesMissing = PagesMissing,
            PagesQueued = PagesQueued,
            PagesQueuedAwaited = PagesQueuedAwaited,
            PagesFailed = PagesFailed,
            TaxaNeverMatched = TaxaNeverMatched,
            TaxaWithArticle = TaxaWithArticle,
            OldestCachedPageAt = OldestCachedPageAt,
            DumpTitles = DumpTitles,
            DumpDate = DumpDate,
            PagesQueuedInDump = PagesQueuedInDump,
            PagesQueuedNotInDump = PagesQueuedNotInDump,
        };
    }

    // Nothing measured yet: the first poll after startup, or a cache that is missing. Reporting
    // "no gaps" then would be a count nobody took.
    [Fact]
    public void Every_probe_is_silent_until_the_counts_have_been_taken() {
        var unknown = State(s => s.Known = false);
        foreach (var probe in new[] {
                     FlowStepProbes.WikidataSweep, FlowStepProbes.WikidataSearch,
                     FlowStepProbes.WikidataDownload, FlowStepProbes.WikipediaQueue,
                     FlowStepProbes.WikipediaMatch, FlowStepProbes.WikipediaFetchAwaited,
                     FlowStepProbes.WikipediaFetchRest, FlowStepProbes.WikiRetryFailed,
                     FlowStepProbes.WikiRefresh }) {
            Assert.Null(FlowStepProbes.EvaluateWiki(probe, unknown));
        }
    }

    // Without this the step could only say when `seed-taxa` last ran, which for anyone who ran
    // `cache-all` or the CLI is never, next to 181,294 downloaded items.
    // A Q-number is an identifier, so it is printed without digit grouping. It used to come out
    // as Q136,591,620.
    [Fact]
    public void The_sweep_reports_what_it_found_and_how_far_it_read() {
        var r = FlowStepProbes.WikiWikidataSweep(State(s => s.EntitiesQueued = 312));
        Assert.Equal("ok", r.Status);
        Assert.Equal("181,606 Wikidata items found. The sweep has read items up to Q141278490.", r.Detail);
        Assert.DoesNotContain("Q141,", r.Detail);
    }

    // seed-taxa sweeps P627 and P141 in separate passes, each with its own cursor.
    [Fact]
    public void The_sweep_names_each_property_when_the_passes_stand_at_different_q_numbers() {
        var r = FlowStepProbes.WikiWikidataSweep(State(s => s.SweepCursorP141 = 136_591_620));
        Assert.Equal("181,294 Wikidata items found. The sweep has read items with P627 up to Q141278490 and items with P141 up to Q136591620.", r.Detail);
    }

    [Fact]
    public void The_sweep_says_which_pass_has_not_started() {
        var r = FlowStepProbes.WikiWikidataSweep(State(s => s.SweepCursorP141 = 0));
        Assert.Contains("items with P627 up to Q141278490", r.Detail);
        Assert.Contains("has not started on items with P141", r.Detail);
    }

    [Fact]
    public void The_sweep_is_todo_when_nothing_has_been_found_yet() {
        var r = FlowStepProbes.WikiWikidataSweep(State(s => { s.EntitiesCached = 0; s.SweepCursorP627 = 0; s.SweepCursorP141 = 0; }));
        Assert.Equal("todo", r.Status);
        Assert.Contains("No Wikidata items found yet", r.Detail);
    }

    [Fact]
    public void The_sweep_leaves_the_cursor_out_before_the_first_run() {
        var r = FlowStepProbes.WikiWikidataSweep(State(s => { s.SweepCursorP627 = 0; s.SweepCursorP141 = 0; }));
        Assert.Equal("181,294 Wikidata items found.", r.Detail);
    }

    [Fact]
    public void The_title_queue_splits_into_downloaded_to_do_and_no_article() {
        var r = FlowStepProbes.WikiWikipediaQueue(State(s => s.PagesQueued = 190_212));
        Assert.Equal("ok", r.Status);
        Assert.Equal("334,925 titles: 99,689 downloaded, 190,212 to download, 45,024 with no article.", r.Detail);
    }

    // The line left failed downloads out, so its parts came up 399 titles short of the total.
    [Fact]
    public void The_title_queue_parts_add_up_to_the_total_when_downloads_failed() {
        var r = FlowStepProbes.WikiWikipediaQueue(State(s => { s.PagesQueued = 189_813; s.PagesFailed = 399; }));
        Assert.Equal("334,925 titles: 99,689 downloaded, 189,813 to download, 399 failed to download, 45,024 with no article.", r.Detail);

        var numbers = System.Text.RegularExpressions.Regex.Matches(r.Detail!, @"\d[\d,]*")
            .Select(m => long.Parse(m.Value.Replace(",", ""))).ToList();
        Assert.Equal(numbers[0], numbers.Skip(1).Sum());
    }

    [Fact]
    public void The_title_queue_is_todo_when_nothing_has_been_queued() {
        var r = FlowStepProbes.WikiWikipediaQueue(State(s => s.PagesKnown = 0));
        Assert.Equal("todo", r.Status);
        Assert.Contains("No titles queued yet", r.Detail);
    }

    [Fact]
    public void Wikidata_search_is_todo_when_taxa_have_never_been_searched_for() {
        var r = FlowStepProbes.WikiWikidataSearch(State(s => s.TaxaWithoutWikidata = 13_694));
        Assert.Equal("todo", r.Status);
        Assert.Contains("13,694", r.Detail);
        Assert.Contains("not been searched for", r.Detail);
    }

    // The point of recording searches that found nothing: once every gap has been searched for,
    // the step is done, even though the gap itself remains.
    [Fact]
    public void Wikidata_search_is_done_once_every_gap_has_been_searched_for() {
        var r = FlowStepProbes.WikiWikidataSearch(State(s => {
            s.TaxaWithoutWikidata = 13_694;
            s.BackfillMisses = 13_694;
        }));
        Assert.Equal("ok", r.Status);
        Assert.Contains("all searched for already", r.Detail);
    }

    [Fact]
    public void Wikidata_search_splits_the_gap_when_only_some_were_searched_for() {
        var r = FlowStepProbes.WikiWikidataSearch(State(s => {
            s.TaxaWithoutWikidata = 13_694;
            s.BackfillMisses = 10_000;
        }));
        Assert.Equal("todo", r.Status);
        Assert.Contains("3,694 not searched for yet", r.Detail);
        Assert.Contains("10,000 searched before", r.Detail);
    }

    // The misses table also holds taxa a later release dropped, so "never searched" is counted
    // directly. Taken as gap minus misses, this case would read as nothing left to search.
    [Fact]
    public void Wikidata_search_uses_the_direct_count_when_misses_include_dropped_taxa() {
        var r = FlowStepProbes.WikiWikidataSearch(State(s => {
            s.TaxaWithoutWikidata = 100;
            s.BackfillMisses = 150;
            s.NeverSearched = 10;
        }));
        Assert.Equal("todo", r.Status);
        Assert.Contains("10 not searched for yet", r.Detail);
        Assert.Contains("90 searched before", r.Detail);
    }

    [Fact]
    public void Wikidata_search_is_done_when_every_taxon_has_an_item() {
        var r = FlowStepProbes.WikiWikidataSearch(State());
        Assert.Equal("ok", r.Status);
        Assert.Contains("Every IUCN taxon", r.Detail);
    }

    // A download queue is worked down over time, so it is "more to do", not a warning.
    [Fact]
    public void A_download_queue_reads_as_backlog_not_as_a_warning() {
        Assert.Equal("backlog", FlowStepProbes.WikiWikidataDownload(State(s => s.EntitiesQueued = 312)).Status);
        Assert.Equal("backlog", FlowStepProbes.WikiFetchAwaited(State(s => {
            s.PagesQueued = 189_816;
            s.PagesQueuedAwaited = 105_237;
        })).Status);
        Assert.Equal("backlog", FlowStepProbes.WikiFetchRest(State(s => {
            s.PagesQueued = 189_816;
            s.PagesQueuedAwaited = 105_237;
        })).Status);
    }

    [Fact]
    public void The_rest_of_the_queue_is_what_no_taxon_is_waiting_on() {
        var r = FlowStepProbes.WikiFetchRest(State(s => {
            s.PagesQueued = 189_816;
            s.PagesQueuedAwaited = 105_237;
        }));
        Assert.Contains("84,579", r.Detail);
    }

    [Fact]
    public void Empty_queues_report_done() {
        Assert.Equal("ok", FlowStepProbes.WikiWikidataDownload(State()).Status);
        Assert.Equal("ok", FlowStepProbes.WikiFetchAwaited(State()).Status);
        Assert.Equal("ok", FlowStepProbes.WikiFetchRest(State()).Status);
    }

    // Taxa the matcher has never looked at are the new release's additions: finishable, and the
    // one thing worth an amber light after an import.
    [Fact]
    public void Taxa_never_checked_for_an_article_are_todo() {
        var r = FlowStepProbes.WikiWikipediaMatch(State(s => s.TaxaNeverMatched = 980));
        Assert.Equal("todo", r.Status);
        Assert.Contains("980", r.Detail);
    }

    [Fact]
    public void Matching_is_done_when_every_taxon_has_been_checked() {
        var r = FlowStepProbes.WikiWikipediaMatch(State());
        Assert.Equal("ok", r.Status);
        Assert.Contains("188,485", r.Detail);
    }

    [Fact]
    public void Failures_name_both_caches_and_only_the_ones_that_failed() {
        var both = FlowStepProbes.WikiFailures(State(s => { s.PagesFailed = 399; s.EntitiesFailed = 12; }));
        Assert.Equal("todo", both.Status);
        Assert.Contains("399 Wikipedia pages and 12 Wikidata items", both.Detail);

        var pagesOnly = FlowStepProbes.WikiFailures(State(s => s.PagesFailed = 399));
        Assert.Equal("399 Wikipedia pages failed to download.", pagesOnly.Detail);

        Assert.Equal("ok", FlowStepProbes.WikiFailures(State()).Status);
    }

    // Re-downloading old copies is never overdue, so it stays green and just reports the age.
    [Fact]
    public void Refreshing_old_copies_reports_the_age_without_flagging_it() {
        var r = FlowStepProbes.WikiRefreshAge(State());
        Assert.Equal("ok", r.Status);
        Assert.Contains("99,689 pages cached", r.Detail);
        Assert.Contains("2026", r.Detail);
    }

    [Fact]
    public void Refreshing_says_so_when_nothing_is_cached_yet() {
        var r = FlowStepProbes.WikiRefreshAge(State(s => { s.PagesCached = 0; s.OldestCachedPageAt = null; }));
        Assert.Equal("ok", r.Status);
        Assert.Contains("No pages cached", r.Detail);
    }

    // The Update button's one light. Amber only for finishable work (new taxa unsearched or
    // unmatched); a standing download queue alone is "backlog"; and when the lists' needs are
    // met it stays green even with a six-figure low-priority queue.
    [Fact]
    public void Update_light_lists_the_outstanding_work_most_valuable_first() {
        var r = FlowStepProbes.WikiUpdate(State(s => {
            s.TaxaWithoutWikidata = 8_000; s.BackfillMisses = 5_000;
            s.TaxaNeverMatched = 2_340;
            s.EntitiesQueued = 4_120;
            s.PagesQueuedAwaited = 61_000;
        }));
        Assert.Equal("todo", r.Status);
        Assert.Contains("3,000 taxa never searched for on Wikidata", r.Detail);
        Assert.Contains("2,340 taxa never checked for an article", r.Detail);
        Assert.Contains("4,120 Wikidata items to download", r.Detail);
        Assert.Contains("61,000 pages to download", r.Detail);
    }

    [Fact]
    public void Update_light_shows_queues_alone_as_backlog_not_overdue() {
        var r = FlowStepProbes.WikiUpdate(State(s => { s.EntitiesQueued = 4_120; s.PagesQueuedAwaited = 61_000; }));
        Assert.Equal("backlog", r.Status);
    }

    [Fact]
    public void Update_light_is_green_when_only_low_priority_work_remains() {
        var r = FlowStepProbes.WikiUpdate(State(s => { s.PagesQueued = 129_000; s.PagesFailed = 399; }));
        Assert.Equal("ok", r.Status);
        Assert.Contains("129,000 titles queued", r.Detail);
        Assert.Contains("399 Wikipedia pages failed to download", r.Detail);

        var done = FlowStepProbes.WikiUpdate(State());
        Assert.Equal("ok", done.Status);
        Assert.Contains("nothing is queued", done.Detail);
    }

    // Without the dump the fetch queue cannot tell a redlink from a real page; with it the light
    // reports the dump's date and how the queue splits, and stays green (importing again before
    // a new dump exists would change nothing).
    [Fact]
    public void Titles_dump_says_what_the_queue_gains_from_importing_one() {
        var r = FlowStepProbes.WikiTitlesDump(State());
        Assert.Equal("todo", r.Status);
        Assert.Contains("No all-titles dump imported", r.Detail);
    }

    [Fact]
    public void Titles_dump_reports_the_dump_date_and_the_queue_split() {
        var r = FlowStepProbes.WikiTitlesDump(State(s => {
            s.DumpTitles = 7_012_345;
            s.DumpDate = "2026-08-20";
            s.PagesQueuedInDump = 61_000;
            s.PagesQueuedNotInDump = 129_000;
        }));
        Assert.Equal("ok", r.Status);
        Assert.Contains("2026-08-20", r.Detail);
        Assert.Contains("61,000 queued titles are in it", r.Detail);
        Assert.Contains("129,000 are not (likely redlinks)", r.Detail);
    }
}
