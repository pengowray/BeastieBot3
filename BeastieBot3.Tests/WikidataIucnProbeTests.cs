using System;
using BeastieBot3.Web.Flows;
using BeastieBot3.WikidataEdits;
using Xunit;

namespace BeastieBot3.Tests;

// Lights for the "Update IUCN statuses on Wikidata" workflow page.
public class WikidataIucnProbeTests {
    private static readonly DateTime Finished = new(2026, 9, 13, 5, 0, 0, DateTimeKind.Utc);

    private static WikidataIucnPlanRun Run(long stale = 0, string? edition = null, int? limit = null) => new() {
        FinishedAtUtc = Finished, Release = "2026-1", EditionItem = edition,
        Pairs = 178_000, Editable = 148_000, ForReview = 25_000, StaleItems = stale, StoppedAtLimit = limit,
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
        Assert.Null(run.StoppedAtLimit);
    }

    // A complete run saves "StoppedAtLimit":null; runs saved before the field existed have no such
    // property. Both read as complete.
    [Fact]
    public void Plan_run_reads_the_limit_a_partial_run_stopped_at() {
        var tally = new WikidataIucnPlanTally { StoppedAtLimit = 100, Pairs = 90 };
        var partial = WikidataIucnFlowStateReader.PlanRun(Finished, "2026-1", null, System.Text.Json.JsonSerializer.Serialize<object>(tally));
        Assert.Equal(100, partial.StoppedAtLimit);
        Assert.Equal(90, partial.Pairs);

        Assert.Null(WikidataIucnFlowStateReader.PlanRun(Finished, "2026-1", null, """{"Pairs":5,"StaleItems":0}""").StoppedAtLimit);
        Assert.Null(WikidataIucnFlowStateReader.PlanRun(Finished, "2026-1", null, null).StoppedAtLimit);
    }

    // A --limit 100 run saw 100 linked items: it can find old copies among them, but it cannot say
    // every cached copy is fresh.
    [Fact]
    public void Partial_dry_run_never_marks_item_freshness_done() {
        var fresh = WikidataIucnProbes.ItemsFreshStep(new WikidataIucnFlowState { LastPlan = Run(limit: 100) })!;
        Assert.Equal("todo", fresh.Status);
        Assert.StartsWith("Partial dry run on ", fresh.Detail);
        Assert.Contains("stopped after 100 linked Wikidata items (--limit 100).", fresh.Detail);
        Assert.Contains("The cached copies of those 100 items were all 30 days old or less; the other linked items were not checked.", fresh.Detail);
        Assert.DoesNotContain("every cached copy", fresh.Detail);

        var stale = WikidataIucnProbes.ItemsFreshStep(new WikidataIucnFlowState { LastPlan = Run(stale: 12, limit: 100) })!;
        Assert.Equal("todo", stale.Status);
        Assert.Contains("12 of those 100 items had cached copies more than 30 days old", stale.Detail);
        Assert.Contains("run the dry run again without --limit", stale.Detail);
    }

    [Fact]
    public void Partial_dry_run_leaves_the_plan_step_to_do() {
        var state = new WikidataIucnFlowState { Release = "2026-1", LastPlan = Run(limit: 2000) };
        var partial = WikidataIucnProbes.PlanStep(state);
        Assert.Equal("todo", partial.Status);
        Assert.StartsWith("Partial dry run on ", partial.Detail);
        Assert.Contains("For those 2,000 items: 148,000 edits planned, 25,000 pairs need a person to confirm the match.", partial.Detail);
        Assert.Contains("without --limit for the complete plan", partial.Detail);

        // The release-changed line still names the last run as partial.
        var changed = WikidataIucnProbes.PlanStep(state with { EditionItem = "Q140000001" });
        Assert.Equal("todo", changed.Status);
        Assert.StartsWith("The release or release item changed", changed.Detail);
        Assert.Contains("(partial dry run on ", changed.Detail);
        Assert.Contains("stopped after 2,000 linked Wikidata items)", changed.Detail);

        Assert.Equal("ok", WikidataIucnProbes.PlanStep(state with { LastPlan = Run() }).Status);
    }
}
