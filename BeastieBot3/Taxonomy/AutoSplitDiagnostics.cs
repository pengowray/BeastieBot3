using System.Collections.Generic;

namespace BeastieBot3.Taxonomy;

/// <summary>
/// Records auto-split decisions for diagnostics and reporting.
/// Passed through AutoSplitOptions; default no-op keeps the tree builder clean.
/// </summary>
internal interface IAutoSplitDiagnostics {
    void RecordDecision(AutoSplitDecision decision);
}

internal sealed record AutoSplitDecision(
    /// <summary>Taxonomy path to the parent being split, e.g. "Rodentia → Cricetidae → Sigmodontinae".</summary>
    string ParentPath,
    /// <summary>Number of items being split.</summary>
    int ItemCount,
    /// <summary>Candidate rank tried, e.g. "genus".</summary>
    string CandidateRank,
    /// <summary>Outcome: "accepted", "rejected:few_meaningful", "rejected:high_other", "rejected:too_many_groups", "rejected:depth_limit", "rejected:single_group", "rejected:no_meaningful".</summary>
    string Outcome,
    /// <summary>Number of groups produced (after lumping). 0 if not applicable.</summary>
    int GroupCount = 0,
    /// <summary>Number of non-Other/Unknown groups.</summary>
    int MeaningfulGroups = 0,
    /// <summary>Fraction of items in Other+Unknown groups (0.0-1.0).</summary>
    double OtherFraction = 0,
    /// <summary>Size of the largest meaningful group.</summary>
    int LargestGroup = 0);

/// <summary>
/// Collects auto-split decisions into a list for reporting.
/// </summary>
internal sealed class AutoSplitDiagnosticCollector : IAutoSplitDiagnostics {
    private readonly List<AutoSplitDecision> _decisions = new();

    public IReadOnlyList<AutoSplitDecision> Decisions => _decisions;

    public void RecordDecision(AutoSplitDecision decision) {
        _decisions.Add(decision);
    }
}

/// <summary>
/// No-op diagnostics implementation for normal (non-diagnostic) generation.
/// </summary>
internal sealed class NullAutoSplitDiagnostics : IAutoSplitDiagnostics {
    public static readonly NullAutoSplitDiagnostics Instance = new();
    public void RecordDecision(AutoSplitDecision decision) { }
}
