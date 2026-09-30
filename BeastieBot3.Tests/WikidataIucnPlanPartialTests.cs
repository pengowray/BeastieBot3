using System;
using BeastieBot3.WikidataEdits;
using Xunit;

namespace BeastieBot3.Tests;

// `wikidata iucn-status-plan --limit N` called its console summary partial whenever N was set,
// even when the run read every linked item before reaching N, and the Markdown report never said
// partial at all. Partial now means the run actually stopped at the limit.
public class WikidataIucnPlanPartialTests {
    private static string Report(WikidataIucnPlanTally t) =>
        WikidataIucnPlanReport.Write(t, new WikidataIucnEditConfig(), new DateTime(2026, 9, 30, 0, 0, 0, DateTimeKind.Utc),
            "plan.csv", "plan-sample-edits.jsonl", "plan-sample-assessment-items.jsonl");

    [Fact]
    public void A_run_that_stopped_at_its_limit_says_so_in_the_console_and_the_report() {
        var t = new WikidataIucnPlanTally { StoppedAtLimit = 2000 };

        Assert.Equal("Planned changes, pairs (partial: stopped at --limit 2000)", WikidataIucnStatusPlanCommand.SummaryTitle(t));
        Assert.Contains("**Partial plan:** this run stopped after 2,000 of the Wikidata items linked to IUCN taxa (`--limit 2000`). " +
                        "The Coverage section is complete; all other counts cover only those 2,000 items.", Report(t));
    }

    [Fact]
    public void A_run_that_read_every_item_is_not_called_partial() {
        var t = new WikidataIucnPlanTally();

        Assert.DoesNotContain("partial", WikidataIucnStatusPlanCommand.SummaryTitle(t));
        Assert.DoesNotContain("Partial plan", Report(t));
    }

    // A partial run stops before it has seen every item, so it has no count of linked items that
    // are not downloaded; printing its 0 would read as "none missing".
    [Fact]
    public void Only_a_complete_run_reports_linked_items_not_downloaded() {
        var partial = new WikidataIucnPlanTally { StoppedAtLimit = 2000, LinksToTaxaOutsideRelease = 5 };
        Assert.Contains("Links skipped: 5 point at IUCN ids not in 2026-1.", Report(partial));
        Assert.DoesNotContain("not downloaded yet", Report(partial));

        var complete = new WikidataIucnPlanTally { LinksToTaxaOutsideRelease = 5, ItemsLinkedButNotDownloaded = 3 };
        Assert.Contains("Links skipped: 5 point at IUCN ids not in 2026-1; 3 point at items not downloaded yet.", Report(complete));
    }
}
