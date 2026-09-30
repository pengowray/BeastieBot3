using System;

// Step lights for the Wikidata IUCN status workflow. Pure over WikidataIucnFlowState, pinned by
// WikidataIucnProbeTests.

namespace BeastieBot3.Web.Flows;

public static class WikidataIucnProbes {
    public const string AssessmentItems = "wd-iucn-assessment-items";
    public const string ItemsFresh = "wd-iucn-items-fresh";
    public const string EditionItem = "wd-iucn-edition-item";
    public const string Plan = "wd-iucn-plan";

    public static bool IsProbe(string probe) => probe is AssessmentItems or ItemsFresh or EditionItem or Plan;

    public static FlowProbeResult? Evaluate(string probe, WikidataIucnFlowState s) => probe switch {
        AssessmentItems => AssessmentItemsStep(s),
        ItemsFresh => ItemsFreshStep(s),
        EditionItem => EditionItemStep(s),
        Plan => PlanStep(s),
        _ => null,
    };

    internal static FlowProbeResult AssessmentItemsStep(WikidataIucnFlowState s) =>
        !s.AssessmentItemTableExists || s.AssessmentItemsCheckedAtUtc is null
            ? new FlowProbeResult("todo", "Not looked up yet.")
            : new FlowProbeResult("ok", $"{s.AssessmentItems:n0} assessment items found on Wikidata, last looked up {s.AssessmentItemsCheckedAtUtc:d MMM yyyy}.");

    // Measured by the last dry run, the only read that walks every linked item.
    internal static FlowProbeResult? ItemsFreshStep(WikidataIucnFlowState s) {
        if (s.LastPlan is not { } plan) return null;
        return plan.StaleItems == 0
            ? new FlowProbeResult("ok", $"Dry run on {plan.FinishedAtUtc:d MMM yyyy}: every cached copy of a linked Wikidata item was 30 days old or less.")
            : new FlowProbeResult("todo", $"Dry run on {plan.FinishedAtUtc:d MMM yyyy}: {plan.StaleItems:n0} linked Wikidata items had cached copies more than 30 days old. Re-download those items, then run the dry run again.");
    }

    internal static FlowProbeResult EditionItemStep(WikidataIucnFlowState s) =>
        s.EditionItem is null
            ? new FlowProbeResult("todo", $"No Wikidata item set for the {s.Release} release: planned edits cite a placeholder until edition_item is set.")
            : new FlowProbeResult("ok", $"{s.Release} release item: {s.EditionItem}.");

    internal static FlowProbeResult PlanStep(WikidataIucnFlowState s) {
        if (s.LastPlan is not { } plan) {
            return new FlowProbeResult("todo", "No dry run yet.");
        }
        var summary = $"{plan.Editable:n0} edits planned, {plan.ForReview:n0} pairs need a person to confirm the match (dry run on {plan.FinishedAtUtc:d MMM yyyy}).";
        if (!string.Equals(plan.Release, s.Release, StringComparison.Ordinal) || plan.EditionItem != s.EditionItem) {
            return new FlowProbeResult("todo", $"The release or release item changed since the last dry run; run it again. Last run: {summary}");
        }
        return new FlowProbeResult("ok", summary);
    }
}
