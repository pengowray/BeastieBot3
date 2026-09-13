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
            : new FlowProbeResult("ok", $"{s.AssessmentItems:n0} found, last checked {s.AssessmentItemsCheckedAtUtc:d MMM yyyy}.");

    // Measured by the last dry run, the only read that walks every linked item.
    internal static FlowProbeResult? ItemsFreshStep(WikidataIucnFlowState s) {
        if (s.LastPlan is not { } plan) return null;
        return plan.StaleItems == 0
            ? new FlowProbeResult("ok", $"All linked items were downloaded in the 30 days before the dry run of {plan.FinishedAtUtc:d MMM yyyy}.")
            : new FlowProbeResult("todo", $"{plan.StaleItems:n0} linked items were more than 30 days old at the dry run of {plan.FinishedAtUtc:d MMM yyyy}.");
    }

    internal static FlowProbeResult EditionItemStep(WikidataIucnFlowState s) =>
        s.EditionItem is null
            ? new FlowProbeResult("todo", $"No item set for the {s.Release} release. Edits cite a placeholder until one is.")
            : new FlowProbeResult("ok", $"{s.Release} release item: {s.EditionItem}.");

    internal static FlowProbeResult PlanStep(WikidataIucnFlowState s) {
        if (s.LastPlan is not { } plan) {
            return new FlowProbeResult("todo", "No dry run yet.");
        }
        var summary = $"{plan.Editable:n0} edits ready, {plan.ForReview:n0} for review (dry run of {plan.FinishedAtUtc:d MMM yyyy}).";
        if (!string.Equals(plan.Release, s.Release, StringComparison.Ordinal) || plan.EditionItem != s.EditionItem) {
            return new FlowProbeResult("todo", $"Settings changed since the last dry run. {summary}");
        }
        return new FlowProbeResult("ok", summary);
    }
}
