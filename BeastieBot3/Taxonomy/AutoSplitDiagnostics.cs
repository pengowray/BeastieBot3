using System;
using System.Collections.Generic;

namespace BeastieBot3.Taxonomy;

/// <summary>
/// Records the tree builder's auto-split and intermediate-layer decisions for the generation
/// report and structure-metrics.json. Passed through <see cref="TaxonomyTreeOptions{T}.Diagnostics"/>.
/// </summary>
internal interface IAutoSplitDiagnostics {
    void RecordDecision(AutoSplitDecision decision);

    void RecordLayer(IntermediateLayerDecision decision);
}

/// <summary>
/// One auto-split decision. Every attempt ends with exactly one closing record: Outcome "accepted"
/// for the rank used, or CandidateRank "(all)" when no rank was used.
/// </summary>
internal sealed record AutoSplitDecision(
    /// <summary>Taxonomy path to the parent being split, e.g. "Least concern → Rodentia → Cricetidae".</summary>
    string ParentPath,
    /// <summary>Number of items being split.</summary>
    int ItemCount,
    /// <summary>Candidate rank tried, e.g. "genus", or "(all)" for the closing record of a failed attempt.</summary>
    string CandidateRank,
    /// <summary>Outcome: "accepted", or "rejected:" plus single_group, has_unknown, few_meaningful,
    /// groups_too_small, no_meaningful, high_other, too_many_groups, depth_limit, heading_depth or no_candidate.</summary>
    string Outcome,
    /// <summary>Number of groups produced (after lumping). 0 if not applicable.</summary>
    int GroupCount = 0,
    /// <summary>Number of non-Other/Unknown groups.</summary>
    int MeaningfulGroups = 0,
    /// <summary>Fraction of items in Other+Unknown groups (0.0-1.0).</summary>
    double OtherFraction = 0,
    /// <summary>Size of the largest meaningful group.</summary>
    int LargestGroup = 0) {
    /// <summary>True for the one record that closes an attempt.</summary>
    public bool ClosesAttempt => Outcome == "accepted" || CandidateRank == "(all)";
}

/// <summary>One decision on whether to show a layer of Catalogue of Life nodes between two levels.</summary>
/// <param name="ParentPath">Path to the node the layer would go under.</param>
/// <param name="ItemCount">N: items under that node.</param>
/// <param name="Ranks">CoL ranks of the candidate groups, e.g. "suborder".</param>
/// <param name="Level">The configured level the layer sits above, e.g. "family".</param>
/// <param name="Outcome">"accepted", or "rejected:" plus mixed_parents (the entries have different IUCN
/// parents, recorded before any CoL node is read), single_value_groups, few_items, few_anchors,
/// headings_not_reduced, too_many_groups, dominant_group, groups_too_small, little_outside_largest or
/// heading_depth, or "looked_through:dominant_group" when the next record is the same place one node
/// further down.</param>
/// <param name="NamedGroups">CoL groups left after demoting groups with one value of the level.</param>
/// <param name="LooseValues">Distinct values of the level among items in no CoL group.</param>
/// <param name="Anchors">D: distinct values of the level among all N items.</param>
/// <param name="Headings">F: named groups plus loose values; must be below D.</param>
/// <param name="LargestShare">Share of N in the largest named group.</param>
/// <param name="Groups">The named groups, comma-separated.</param>
internal sealed record IntermediateLayerDecision(
    string ParentPath,
    int ItemCount,
    string Ranks,
    string Level,
    string Outcome,
    int NamedGroups = 0,
    int LooseValues = 0,
    int Anchors = 0,
    int Headings = 0,
    double LargestShare = 0,
    string Groups = "") {
    /// <summary>
    /// True for the record that ends the decision at one place. A look-through record is followed by
    /// another record for the same place, and mixed_parents is recorded before any CoL node is read.
    /// </summary>
    public bool ClosesAttempt =>
        !Outcome.StartsWith("looked_through:", StringComparison.Ordinal) && Outcome != "rejected:mixed_parents";
}

/// <summary>
/// Collects decisions into lists for reporting.
/// </summary>
internal sealed class AutoSplitDiagnosticCollector : IAutoSplitDiagnostics {
    private readonly List<AutoSplitDecision> _decisions = new();
    private readonly List<IntermediateLayerDecision> _layers = new();

    public IReadOnlyList<AutoSplitDecision> Decisions => _decisions;

    public IReadOnlyList<IntermediateLayerDecision> Layers => _layers;

    public void RecordDecision(AutoSplitDecision decision) {
        _decisions.Add(decision);
    }

    public void RecordLayer(IntermediateLayerDecision decision) {
        _layers.Add(decision);
    }
}

/// <summary>
/// No-op diagnostics implementation for normal (non-diagnostic) generation.
/// </summary>
internal sealed class NullAutoSplitDiagnostics : IAutoSplitDiagnostics {
    public static readonly NullAutoSplitDiagnostics Instance = new();
    public void RecordDecision(AutoSplitDecision decision) { }
    public void RecordLayer(IntermediateLayerDecision decision) { }
}
