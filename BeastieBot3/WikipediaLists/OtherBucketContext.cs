using System;
using System.Collections.Generic;
using BeastieBot3.Iucn;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Context for items within an "Other" bucket, tracking which rank values need annotation.
/// Stores the rank label (e.g., "Subfamily") and per-taxon rank values so that
/// parenthetical annotations reflect the actual grouping rank rather than always "Family".
/// Shared between the tree-rendering orchestrator (which builds it) and the line formatter
/// (which consumes it); promoted from a nested type during the WikipediaListGenerator carve-up.
/// </summary>
internal sealed class OtherBucketContext {
    private readonly HashSet<string> _linkedValues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<long, string>? _valuesByTaxonId;

    public bool IsInOtherBucket { get; }
    public string RankLabel { get; }

    public OtherBucketContext(bool isInOtherBucket, string rankLabel = "Family", Dictionary<long, string>? valuesByTaxonId = null) {
        IsInOtherBucket = isInOtherBucket;
        RankLabel = rankLabel;
        _valuesByTaxonId = valuesByTaxonId;
    }

    /// <summary>
    /// Gets the rank value for a record. Looks up the per-taxon map first,
    /// then falls back to FamilyName only if the rank IS family.
    /// Returns null if the rank-specific value is unknown (avoids redundant annotations).
    /// </summary>
    public string? GetRankValue(IucnSpeciesRecord record) {
        if (_valuesByTaxonId != null && _valuesByTaxonId.TryGetValue(record.TaxonId, out var value)) {
            return value;
        }
        return RankLabel.Equals("Family", StringComparison.OrdinalIgnoreCase) ? record.FamilyName : null;
    }

    /// <summary>
    /// Returns true if this is the first occurrence of the value and it should be wiki-linked.
    /// </summary>
    public bool ShouldLinkValue(string value) {
        if (string.IsNullOrWhiteSpace(value)) return false;
        return _linkedValues.Add(value);
    }
}
