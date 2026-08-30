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
        public long BackfillMisses;
        public long EntitiesCached = 181_294;
        public long EntitiesQueued;
        public long EntitiesFailed;
        public long PagesCached = 99_689;
        public long PagesQueued;
        public long PagesQueuedAwaited;
        public long PagesFailed;
        public long TaxaNeverMatched;
        public long TaxaWithArticle = 67_134;
        public DateTime? OldestCachedPageAt = new(2026, 6, 14, 0, 0, 0, DateTimeKind.Utc);

        public WikiCoverageState Build() => new() {
            Known = Known,
            IucnTaxa = IucnTaxa,
            TaxaWithoutWikidata = TaxaWithoutWikidata,
            WikidataBackfillMisses = BackfillMisses,
            WikidataEntitiesCached = EntitiesCached,
            WikidataEntitiesQueued = EntitiesQueued,
            WikidataEntitiesFailed = EntitiesFailed,
            PagesCached = PagesCached,
            PagesQueued = PagesQueued,
            PagesQueuedAwaited = PagesQueuedAwaited,
            PagesFailed = PagesFailed,
            TaxaNeverMatched = TaxaNeverMatched,
            TaxaWithArticle = TaxaWithArticle,
            OldestCachedPageAt = OldestCachedPageAt,
        };
    }

    // Nothing measured yet: the first poll after startup, or a cache that is missing. Reporting
    // "no gaps" then would be a count nobody took.
    [Fact]
    public void Every_probe_is_silent_until_the_counts_have_been_taken() {
        var unknown = State(s => s.Known = false);
        foreach (var probe in new[] {
                     FlowStepProbes.WikidataSearch, FlowStepProbes.WikidataDownload,
                     FlowStepProbes.WikipediaMatch, FlowStepProbes.WikipediaFetchAwaited,
                     FlowStepProbes.WikipediaFetchRest, FlowStepProbes.WikiRetryFailed,
                     FlowStepProbes.WikiRefresh }) {
            Assert.Null(FlowStepProbes.EvaluateWiki(probe, unknown));
        }
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
}
