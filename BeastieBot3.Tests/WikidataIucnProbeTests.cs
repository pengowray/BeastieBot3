using System;
using BeastieBot3.Web.Flows;
using BeastieBot3.WikidataEdits;
using Xunit;

namespace BeastieBot3.Tests;

// Lights for the "Update IUCN statuses on Wikidata" workflow page.
public class WikidataIucnProbeTests {
    private static readonly DateTime Finished = new(2026, 9, 13, 5, 0, 0, DateTimeKind.Utc);

    private static WikidataIucnPlanRun Run(long stale = 0, string? edition = null) => new() {
        FinishedAtUtc = Finished, Release = "2026-1", EditionItem = edition,
        Pairs = 178_000, Editable = 148_000, ForReview = 25_000, StaleItems = stale,
    };

    [Fact]
    public void Release_item_is_to_do_until_configured() {
        Assert.Equal("todo", WikidataIucnProbes.EditionItemStep(new WikidataIucnFlowState { Release = "2026-1" }).Status);
        var ok = WikidataIucnProbes.EditionItemStep(new WikidataIucnFlowState { Release = "2026-1", EditionItem = "Q140000001" });
        Assert.Equal("ok", ok.Status);
        Assert.Contains("Q140000001", ok.Detail);
    }

    [Fact]
    public void Plan_goes_back_to_to_do_when_the_release_item_changes_after_it() {
        var state = new WikidataIucnFlowState { Release = "2026-1", EditionItem = "Q140000001", LastPlan = Run(edition: null) };
        Assert.Equal("todo", WikidataIucnProbes.PlanStep(state).Status);
        Assert.Equal("ok", WikidataIucnProbes.PlanStep(state with { LastPlan = Run(edition: "Q140000001") }).Status);
        Assert.Equal("todo", WikidataIucnProbes.PlanStep(new WikidataIucnFlowState { Release = "2026-1" }).Status);
    }

    // Freshness is only measured by a dry run; before one, the step has nothing to say.
    [Fact]
    public void Item_freshness_comes_from_the_last_dry_run() {
        Assert.Null(WikidataIucnProbes.ItemsFreshStep(new WikidataIucnFlowState()));
        Assert.Equal("todo", WikidataIucnProbes.ItemsFreshStep(new WikidataIucnFlowState { LastPlan = Run(stale: 180_788) })!.Status);
        Assert.Equal("ok", WikidataIucnProbes.ItemsFreshStep(new WikidataIucnFlowState { LastPlan = Run() })!.Status);
    }

    [Fact]
    public void Plan_run_counts_edits_by_tier_and_skips_no_change() {
        var tally = new WikidataIucnPlanTally { StaleItems = 12 };
        tally.TierByCategory["A"] = new() { ["ReferencesOnly"] = 100, ["NoChange"] = 7, ["UnmappedCategory"] = 3 };
        tally.TierByCategory["B"] = new() { ["StatusChanged"] = 5 };
        tally.TierByCategory["C"] = new() { ["StatusAdded"] = 20 };
        tally.TierByCategory["D"] = new() { ["StatusChanged"] = 1 };
        var run = WikidataIucnFlowStateReader.PlanRun(Finished, "2026-1", null, System.Text.Json.JsonSerializer.Serialize<object>(tally));
        Assert.Equal(105, run.Editable);
        Assert.Equal(21, run.ForReview);
        Assert.Equal(12, run.StaleItems);
    }
}
