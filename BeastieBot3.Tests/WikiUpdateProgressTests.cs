using BeastieBot3.Web.Flows;
using BeastieBot3.Wikipedia;
using Xunit;

namespace BeastieBot3.Tests;

// `wikipedia update` reports each step and the whole run as the difference between two coverage
// snapshots, and `--until-done` stops on a round where nothing that counts as progress moved.
public class WikiUpdateProgressTests {
    private static readonly WikiCoverageState Start = new() {
        Known = true,
        WikidataEntitiesCached = 187_911,
        TaxaWithoutWikidata = 7_157,
        TaxaNeverSearched = 5_131,
        PagesKnown = 196_000,
        PagesCached = 132_825,
        PagesMissing = 30_000,
        PagesQueued = 34_731,
        PagesQueuedAwaited = 1_276,
        TaxaWithArticle = 98_746,
        TaxaWithoutArticle = 87_740,
        TaxaAwaitingPage = 1_365,
    };

    // The run the report was written for: 1,278 titles checked, 81 of them articles.
    private static readonly WikiCoverageState AfterFetch = Start with {
        PagesCached = 132_906,
        PagesMissing = 31_197,
        PagesQueued = 33_453,
        PagesQueuedAwaited = 624,
    };

    [Fact]
    public void A_download_step_says_what_arrived_and_what_turned_out_missing() {
        var line = WikiUpdateProgress.Describe(Start, AfterFetch);
        Assert.Equal(
            "81 pages downloaded · 1,197 titles have no article · 1,278 fewer titles queued · 652 fewer pages awaited by a taxon",
            line);
    }

    // A match step moves the awaited-page queue as well as the taxa; the outcomes lead.
    [Fact]
    public void Outcomes_come_before_queue_sizes_and_new_titles_are_not_said_twice() {
        var matched = Start with {
            TaxaWithArticle = Start.TaxaWithArticle + 300,
            PagesKnown = Start.PagesKnown + 652,
            PagesQueued = Start.PagesQueued + 652,
            PagesQueuedAwaited = Start.PagesQueuedAwaited + 652,
        };
        Assert.Equal(
            "300 taxa matched to an article · 652 titles queued · 652 more pages awaited by a taxon",
            WikiUpdateProgress.Describe(Start, matched));
        Assert.Contains(WikiUpdateProgress.Changes(Start, matched), c => c.Metric.Label == "Titles ever queued");
    }

    [Fact]
    public void Nothing_moved_describes_as_null_and_is_not_progress() {
        Assert.Null(WikiUpdateProgress.Describe(Start, Start));
        Assert.False(WikiUpdateProgress.MadeProgress(Start, Start));
    }

    // A retry that fails again moves the failure count and nothing else. Counting that as
    // progress would loop --until-done on the same failures forever.
    [Fact]
    public void Queue_and_failure_counts_alone_are_not_progress() {
        var retriedAndFailed = Start with { PagesFailed = 40, PagesQueued = Start.PagesQueued - 40 };
        Assert.False(WikiUpdateProgress.MadeProgress(Start, retriedAndFailed));
        Assert.NotNull(WikiUpdateProgress.Describe(Start, retriedAndFailed));
    }

    [Fact]
    public void Settled_taxa_count_as_progress() {
        var settled = Start with { TaxaAwaitingPage = 674, TaxaWithArticle = 98_819, TaxaWithoutArticle = 88_322 };
        Assert.True(WikiUpdateProgress.MadeProgress(Start, settled));
        Assert.Equal(
            "73 taxa matched to an article · 582 taxa found to have no article · 691 fewer taxa waiting on a page",
            WikiUpdateProgress.Describe(Start, settled));
    }

    // Stopping a working loop because one count read failed would be worse than one extra round.
    [Fact]
    public void An_unmeasured_snapshot_counts_as_progress_but_reports_no_changes() {
        var unknown = new WikiCoverageState();
        Assert.True(WikiUpdateProgress.MadeProgress(Start, unknown));
        Assert.Empty(WikiUpdateProgress.Changes(Start, unknown));
    }

    [Fact]
    public void Wikidata_changes_are_new_items_or_new_links() {
        Assert.False(WikiUpdateProgress.WikidataChanged(Start, AfterFetch));
        Assert.True(WikiUpdateProgress.WikidataChanged(Start, Start with { TaxaWithoutWikidata = 2_529 }));
        Assert.True(WikiUpdateProgress.WikidataChanged(Start, Start with { WikidataEntitiesCached = 187_950 }));
    }
}
