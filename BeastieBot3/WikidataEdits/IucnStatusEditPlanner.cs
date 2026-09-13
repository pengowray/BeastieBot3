using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Serialization;

// What to change on one Wikidata taxon item so it states the latest IUCN global assessment, with
// both references: the release reference (edition item + IUCN taxon id + assessment URL + retrieved
// date) and the assessment reference (the item for that assessment publication).
//
// The plan is a short list of actions per rank variant, not edit payloads: payloads are built from
// the actions and the item snapshot when they are exported or applied, so a plan for 190,000 items
// stays small and a payload is never built on a stale copy of an item by accident.
//
// Two rank variants, because Wikidata has two live conventions for a changed status:
//   Preferred: add the new status as a preferred statement and leave the old one at normal rank
//              (history kept; P141's single-best-value constraint satisfied).
//   Replace:   overwrite the current statement's value and references (one statement, no history).
// When the status is unchanged or absent, both variants plan the same actions.

namespace BeastieBot3.WikidataEdits;

internal enum EditVariant { Preferred, Replace }

internal enum PlanCategory {
    /// The item already states this status with both references and the IUCN id. Nothing to do.
    NoChange,
    /// Status agrees; references (and possibly the IUCN id) to add.
    ReferencesOnly,
    /// The item states a different current status.
    StatusChanged,
    /// The item has no current IUCN status.
    StatusAdded,
    /// No P141 value maps to this IUCN category; nothing planned.
    UnmappedCategory,
}

[JsonPolymorphic(TypeDiscriminatorPropertyName = "op")]
[JsonDerivedType(typeof(AddTaxonIdClaim), "add-p627")]
[JsonDerivedType(typeof(DeprecateTaxonIdClaim), "deprecate-p627")]
[JsonDerivedType(typeof(AddStatusStatement), "add-p141")]
[JsonDerivedType(typeof(AddStatusReferences), "add-p141-refs")]
[JsonDerivedType(typeof(SetStatusRank), "set-p141-rank")]
[JsonDerivedType(typeof(ReplaceStatusValue), "replace-p141")]
internal abstract record PlannedAction;

/// Add P627 = taxon id, referenced to the release.
internal sealed record AddTaxonIdClaim(long TaxonId) : PlannedAction;

/// Deprecate an IUCN id the item carries that is not in this release (the taxon was renumbered),
/// with reason for deprecated rank = withdrawn identifier value.
internal sealed record DeprecateTaxonIdClaim(string StatementId, string OldValue) : PlannedAction;

/// Add a P141 statement with both references.
internal sealed record AddStatusStatement(string ValueQid, string Rank) : PlannedAction;

/// Add whichever of the two references the statement lacks.
internal sealed record AddStatusReferences(string StatementId, bool ReleaseReference, bool AssessmentReference) : PlannedAction;

internal sealed record SetStatusRank(string StatementId, string Rank) : PlannedAction;

/// Overwrite a statement's value and references with the new status and both references; rank normal.
internal sealed record ReplaceStatusValue(string StatementId, string ValueQid) : PlannedAction;

internal sealed record StatusEditPlan(
    PlanCategory Category,
    string? TargetValueQid,
    IReadOnlyList<string> CurrentValueQids,
    /// The assessment reference target: an existing item, or "CREATE:iucn-assessment:{id}".
    string AssessmentRef,
    bool CreatesAssessmentItem,
    IReadOnlyDictionary<EditVariant, IReadOnlyList<PlannedAction>> Actions) {
    public bool VariantsDiffer =>
        Actions.Count > 1 && !Actions.Values.Skip(1).All(a => a.SequenceEqual(Actions.Values.First()));
}

internal static class IucnStatusEditPlanner {
    public const string WithdrawnIdentifierValue = "Q21441764";

    public static string AssessmentPlaceholder(long assessmentId) =>
        $"CREATE:iucn-assessment:{assessmentId.ToString(CultureInfo.InvariantCulture)}";

    public static StatusEditPlan Plan(
        IucnGlobalAssessment assessment,
        WdTaxonItem item,
        ExistingAssessmentItem? existingAssessmentItem,
        WikidataIucnEditConfig config,
        IReadOnlySet<long> currentTaxonIds) {
        var assessmentRef = existingAssessmentItem?.Qid ?? AssessmentPlaceholder(assessment.AssessmentId);
        var creates = existingAssessmentItem is null;
        var target = WikidataIucnStatusValues.QidForCode(assessment.CategoryCode);
        var current = item.ConservationStatuses.Where(s => s.Rank != "deprecated").ToList();
        var best = BestRanked(current);
        var currentValues = best.Select(s => s.ValueId).OfType<string>().Distinct(StringComparer.Ordinal).ToList();

        var variants = config.Variants.DefaultIfEmpty(EditVariant.Preferred).Distinct().ToList();
        if (target is null) {
            var none = variants.ToDictionary(v => v, _ => (IReadOnlyList<PlannedAction>)Array.Empty<PlannedAction>());
            return new StatusEditPlan(PlanCategory.UnmappedCategory, null, currentValues, assessmentRef, false, none);
        }

        var idActions = TaxonIdActions(assessment.TaxonId, item, currentTaxonIds);
        var actions = new Dictionary<EditVariant, IReadOnlyList<PlannedAction>>();
        PlanCategory category;

        if (current.Count == 0) {
            category = PlanCategory.StatusAdded;
            foreach (var v in variants) {
                actions[v] = idActions.Append(new AddStatusStatement(target, "normal")).ToList();
            }
        } else if (best.All(s => s.ValueId == target)) {
            // Usually one statement; duplicates of the same value are left as they are.
            var statusActions = MissingReferences(best[0], assessment, config, assessmentRef);
            category = statusActions.Count == 0 && idActions.Count == 0 ? PlanCategory.NoChange : PlanCategory.ReferencesOnly;
            foreach (var v in variants) {
                actions[v] = idActions.Concat(statusActions).ToList();
            }
        } else {
            category = PlanCategory.StatusChanged;
            foreach (var v in variants) {
                actions[v] = idActions.Concat(v == EditVariant.Preferred
                    ? PreferredChange(current, best, target, assessment, config, assessmentRef)
                    : ReplaceChange(best, target)).ToList();
            }
        }

        // An assessment item only needs creating when some action cites it.
        var citesAssessment = actions.Values.SelectMany(a => a).Any(a => a switch {
            AddStatusStatement or ReplaceStatusValue => true,
            AddStatusReferences r => r.AssessmentReference,
            _ => false,
        });
        return new StatusEditPlan(category, target, currentValues, assessmentRef, creates && citesAssessment, actions);
    }

    /// The statements a reader sees as current: preferred ones if any, otherwise normal ones.
    public static List<WdStatement> BestRanked(IReadOnlyList<WdStatement> nonDeprecated) {
        var preferred = nonDeprecated.Where(s => s.Rank == "preferred").ToList();
        return preferred.Count > 0 ? preferred : nonDeprecated.Where(s => s.Rank == "normal").ToList();
    }

    private static List<PlannedAction> TaxonIdActions(long taxonId, WdTaxonItem item, IReadOnlySet<long> currentTaxonIds) {
        var actions = new List<PlannedAction>();
        var id = taxonId.ToString(CultureInfo.InvariantCulture);
        var live = item.IucnTaxonIds.Where(s => s.Rank != "deprecated" && s.ValueString is not null).ToList();
        if (!live.Any(s => s.ValueString!.Trim() == id)) {
            actions.Add(new AddTaxonIdClaim(taxonId));
        }
        foreach (var old in live) {
            var value = old.ValueString!.Trim();
            if (value == id) continue;
            // Only an id this release no longer has. A different id that is still current means the
            // item is (also) another taxon; the classifier sends that pair to review and nothing
            // here should remove it.
            if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var oldId) && !currentTaxonIds.Contains(oldId)) {
                actions.Add(new DeprecateTaxonIdClaim(old.Id, value));
            }
        }
        return actions;
    }

    private static List<PlannedAction> MissingReferences(WdStatement statement, IucnGlobalAssessment assessment, WikidataIucnEditConfig config, string assessmentRef) {
        var hasRelease = HasReleaseReference(statement, assessment.TaxonId, config);
        var hasAssessment = statement.References.Any(r => Cites(r, "P248", assessmentRef));
        return hasRelease && hasAssessment
            ? new List<PlannedAction>()
            : new List<PlannedAction> { new AddStatusReferences(statement.Id, !hasRelease, !hasAssessment) };
    }

    private static IEnumerable<PlannedAction> PreferredChange(
        List<WdStatement> current, List<WdStatement> best, string target,
        IucnGlobalAssessment assessment, WikidataIucnEditConfig config, string assessmentRef) {
        var actions = new List<PlannedAction>();
        // The new status may already be on the item at normal rank (a status that changed back):
        // promote that statement instead of adding a duplicate.
        var existing = current.FirstOrDefault(s => s.ValueId == target && s.Rank == "normal")
                       ?? current.FirstOrDefault(s => s.ValueId == target);
        foreach (var s in best.Where(s => s.Rank == "preferred" && s.ValueId != target)) {
            actions.Add(new SetStatusRank(s.Id, "normal"));
        }
        if (existing is null) {
            actions.Add(new AddStatusStatement(target, "preferred"));
        } else {
            if (existing.Rank != "preferred") actions.Add(new SetStatusRank(existing.Id, "preferred"));
            actions.AddRange(MissingReferences(existing, assessment, config, assessmentRef));
        }
        return actions;
    }

    private static IEnumerable<PlannedAction> ReplaceChange(List<WdStatement> best, string target) {
        // Several current statements with different values: overwrite the first, leave the rest for
        // the report (the classifier's flags already say the item is untidy).
        var first = best[0];
        var actions = new List<PlannedAction> { new ReplaceStatusValue(first.Id, target) };
        if (first.Rank != "normal") actions.Add(new SetStatusRank(first.Id, "normal"));
        return actions;
    }

    public static bool HasReleaseReference(WdStatement statement, long taxonId, WikidataIucnEditConfig config) {
        var id = taxonId.ToString(CultureInfo.InvariantCulture);
        return statement.References.Any(r => Cites(r, "P248", config.EditionRef) && Cites(r, "P627", id));
    }

    private static bool Cites(WdReference reference, string property, string value) =>
        reference.Values.TryGetValue(property, out var values)
        && values.Any(v => string.Equals(v.Trim(), value, StringComparison.Ordinal));
}
