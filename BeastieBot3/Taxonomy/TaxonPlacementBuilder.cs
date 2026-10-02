using System;
using System.Collections.Generic;
using System.Linq;

// Decides which Catalogue of Life nodes to insert between IUCN's ranks (see TaxonPlacement.cs).
// Pure: takes one sample per IUCN species with its CoL lineage and returns a TaxonPlacementIndex
// plus diagnostics. No I/O; TaxonPlacementStore reads the samples and saves the result.
//
// For each span (class to order, order to family, family to genus) every matched species gives the
// CoL nodes of its lineage that lie between its two IUCN ranks (its segment). Each IUCN taxon at the
// lower end of the span (the anchor: an order, a family or a genus) keeps a node when at least
// VoteThreshold of its matched species have it in their segment. With a threshold above one half,
// two nodes that are not ancestor and descendant cannot both pass, so the kept nodes form one chain.
//
// A kept node must also be contained: of all matched species whose CoL lineage includes the node,
// at least ContainmentThreshold must sit under the anchor's IUCN parent (the IUCN class for a
// class-to-order node, the IUCN order for an order-to-family node, the IUCN family for a
// family-to-genus node). This stops CoL's order Perciformes from becoming a heading inside IUCN's
// order Scorpaeniformes when most of CoL's Perciformes are in IUCN's Perciformes.
//
// A species whose lineage has no usable bound for a span (no CoL node at or above the IUCN rank,
// as happens when a chain is cut short by a missing parent) does not vote in that span.

namespace BeastieBot3.Taxonomy;

/// <summary>One CoL node in a species' lineage.</summary>
/// <param name="Id">CoL nameusage ID.</param>
/// <param name="Name">CoL scientific name, in CoL's case.</param>
/// <param name="Rank">CoL rank string as stored (e.g. "suborder", "unranked").</param>
internal sealed record ColLineageNode(string Id, string Name, string Rank);

/// <summary>
/// One IUCN species and its CoL lineage. <paramref name="Lineage"/> runs broad to narrow from the
/// CoL root down to the genus, each node once; the accepted species itself and every node of rank
/// species or below are left out. Null when the species has no CoL match.
/// </summary>
internal sealed record PlacementSample(
    string Kingdom,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string GenusName,
    IReadOnlyList<ColLineageNode>? Lineage);

internal sealed record PlacementBuilderOptions {
    public static readonly PlacementBuilderOptions Default = new();

    private readonly double _voteThreshold = 0.8;
    private readonly double _containmentThreshold = 0.9;

    /// <summary>
    /// Share of an anchor's matched species that must have a node in their segment for the anchor
    /// to keep it. Must be above 0.5 so the kept nodes form one chain.
    /// </summary>
    public double VoteThreshold {
        get => _voteThreshold;
        init {
            if (!(value > 0.5 && value <= 1.0)) {
                throw new ArgumentOutOfRangeException(nameof(VoteThreshold), value, "Must be above 0.5 and at most 1.");
            }
            _voteThreshold = value;
        }
    }

    /// <summary>
    /// Share of a node's matched species (release-wide) that must sit under the anchor's IUCN
    /// parent for the node to be used there.
    /// </summary>
    public double ContainmentThreshold {
        get => _containmentThreshold;
        init {
            if (!(value > 0.0 && value <= 1.0)) {
                throw new ArgumentOutOfRangeException(nameof(ContainmentThreshold), value, "Must be above 0 and at most 1.");
            }
            _containmentThreshold = value;
        }
    }
}

/// <summary>The IUCN taxon a placement path belongs to, with its IUCN rank values as stored.</summary>
internal sealed record PlacementAnchor(
    PlacementSpan Span,
    string Key,
    string Kingdom,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string? GenusName);

internal enum PlacementDropReason {
    /// <summary>Fewer than VoteThreshold of the anchor's matched species have the node.</summary>
    BelowVote,
    /// <summary>Fewer than ContainmentThreshold of the node's species sit under the anchor's IUCN parent.</summary>
    NotContained,
}

/// <summary>A CoL node kept in an anchor's path.</summary>
/// <param name="Votes">Matched species of the anchor with the node in their segment.</param>
/// <param name="Share">Votes divided by the anchor's voting species.</param>
/// <param name="Containment">Share of the node's species (release-wide) under the anchor's IUCN parent.</param>
internal sealed record KeptPlacementNode(PlacementNode Node, string ColId, int Votes, double Share, double Containment);

/// <summary>A CoL node that was a candidate for an anchor but not kept.</summary>
/// <param name="NodeSpecies">Matched species (release-wide) whose lineage includes the node.</param>
/// <param name="MainParent">The IUCN parent holding the most of the node's species (a class, order or family name).</param>
/// <param name="MainParentSpecies">How many of the node's species that parent holds.</param>
internal sealed record DroppedPlacementNode(
    string ColId,
    string Name,
    string ColRank,
    PlacementDropReason Reason,
    int Votes,
    double Share,
    double Containment,
    int NodeSpecies,
    string? MainParent,
    int MainParentSpecies);

/// <summary>What the builder decided for one anchor.</summary>
/// <param name="Species">IUCN species under the anchor, matched or not.</param>
/// <param name="Voting">Species whose lineage gave a segment for this span.</param>
internal sealed record AnchorDiagnostics(
    PlacementAnchor Anchor,
    int Species,
    int Voting,
    IReadOnlyList<KeptPlacementNode> Kept,
    IReadOnlyList<DroppedPlacementNode> Dropped);

internal sealed record PlacementBuildOutput(
    TaxonPlacementIndex Index,
    IReadOnlyList<AnchorDiagnostics> Anchors,
    int SampleCount,
    int MatchedCount,
    PlacementBuilderOptions Options);

internal static class TaxonPlacementBuilder {
    private static readonly PlacementSpan[] Spans = {
        PlacementSpan.ClassToOrder,
        PlacementSpan.OrderToFamily,
        PlacementSpan.FamilyToGenus,
    };

    public static PlacementBuildOutput Build(IEnumerable<PlacementSample> samples, PlacementBuilderOptions? options = null) {
        options ??= PlacementBuilderOptions.Default;
        var list = samples as IReadOnlyList<PlacementSample> ?? samples.ToList();

        var nodes = new Dictionary<string, NodeInfo>(StringComparer.Ordinal);
        var states = Spans.ToDictionary(s => s, _ => new SpanState());
        var matched = 0;

        // Pass 1: segments and votes per anchor.
        foreach (var sample in list) {
            var lineage = sample.Lineage;
            if (lineage is not null) {
                matched++;
                for (var i = 0; i < lineage.Count; i++) {
                    var n = lineage[i];
                    if (!nodes.TryGetValue(n.Id, out var info)) {
                        info = new NodeInfo(n.Name, n.Rank, ColRankOrder.Of(n.Rank));
                        nodes[n.Id] = info;
                    }
                    if (i > info.Depth) {
                        info.Depth = i;
                    }
                }
            }

            foreach (var span in Spans) {
                var state = states[span];
                var key = TaxonPlacementIndex.KeyFor(span, sample.Kingdom, sample.ClassName, sample.OrderName, sample.FamilyName, sample.GenusName);
                if (!state.Anchors.TryGetValue(key, out var anchor)) {
                    anchor = new AnchorState(MakeAnchor(span, key, sample), ParentKey(span, sample));
                    state.Anchors[key] = anchor;
                }
                anchor.Species++;
                if (lineage is null) {
                    continue;
                }
                var segment = Segment(span, sample, lineage);
                if (segment is not { } range) {
                    continue;
                }
                anchor.Voting++;
                for (var i = range.Start; i < range.End; i++) {
                    var id = lineage[i].Id;
                    anchor.Votes[id] = anchor.Votes.GetValueOrDefault(id) + 1;
                    state.Candidates.Add(id);
                }
            }
        }

        // Pass 2: where each candidate node's species sit, by IUCN parent, counted over every
        // matched species whose lineage includes the node (not only those with it in a segment).
        foreach (var sample in list) {
            var lineage = sample.Lineage;
            if (lineage is null) {
                continue;
            }
            foreach (var span in Spans) {
                var state = states[span];
                var parent = ParentKey(span, sample);
                if (!state.ParentLabels.ContainsKey(parent)) {
                    state.ParentLabels[parent] = ParentLabel(span, sample);
                }
                foreach (var n in lineage) {
                    if (!state.Candidates.Contains(n.Id)) {
                        continue;
                    }
                    if (!state.Containment.TryGetValue(n.Id, out var byParent)) {
                        byParent = new Dictionary<string, int>(StringComparer.Ordinal);
                        state.Containment[n.Id] = byParent;
                    }
                    byParent[parent] = byParent.GetValueOrDefault(parent) + 1;
                }
            }
        }

        // Pass 3: decide each anchor's path.
        var paths = new List<PlacementPath>();
        var diagnostics = new List<AnchorDiagnostics>();
        foreach (var span in Spans) {
            var state = states[span];
            foreach (var anchor in state.Anchors.Values) {
                var kept = new List<(KeptPlacementNode Node, int Depth)>();
                var dropped = new List<DroppedPlacementNode>();
                foreach (var (id, votes) in anchor.Votes) {
                    var info = nodes[id];
                    var share = (double)votes / anchor.Voting;
                    var byParent = state.Containment[id];
                    var nodeSpecies = byParent.Values.Sum();
                    var inParent = byParent.GetValueOrDefault(anchor.ParentKey);
                    var containment = nodeSpecies == 0 ? 0 : (double)inParent / nodeSpecies;
                    var rank = ColRankOrder.Clean(info.Rank);

                    PlacementDropReason? reason = share < options.VoteThreshold ? PlacementDropReason.BelowVote
                        : containment < options.ContainmentThreshold ? PlacementDropReason.NotContained
                        : null;
                    if (reason is { } why) {
                        var main = byParent.OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.Ordinal).First();
                        dropped.Add(new DroppedPlacementNode(id, info.Name, rank, why, votes, share, containment,
                            nodeSpecies, state.ParentLabels.GetValueOrDefault(main.Key), main.Value));
                        continue;
                    }
                    var node = new PlacementNode(info.Name, rank, ShowRank(span, info.Ordinal));
                    kept.Add((new KeptPlacementNode(node, id, votes, share, containment), info.Depth));
                }

                var orderedKept = kept.OrderBy(k => k.Depth).Select(k => k.Node).ToList();
                var orderedDropped = dropped
                    .OrderByDescending(d => d.Votes)
                    .ThenBy(d => nodes[d.ColId].Depth)
                    .ThenBy(d => d.ColId, StringComparer.Ordinal)
                    .ToList();
                diagnostics.Add(new AnchorDiagnostics(anchor.Anchor, anchor.Species, anchor.Voting, orderedKept, orderedDropped));
                if (orderedKept.Count > 0) {
                    paths.Add(new PlacementPath(span, anchor.Anchor.Key, orderedKept.Select(k => k.Node).ToList()));
                }
            }
        }

        return new PlacementBuildOutput(new TaxonPlacementIndex(paths), diagnostics, list.Count, matched, options);
    }

    /// <summary>
    /// True when a CoL rank lies strictly between the span's two IUCN ranks, so a heading can name
    /// the rank. False for nodes with no ordinal and for nodes at or beyond either IUCN rank.
    /// </summary>
    public static bool ShowRank(PlacementSpan span, int? ordinal) {
        if (ordinal is not { } o) {
            return false;
        }
        var (upper, lower) = Bounds(span);
        return o > upper && o < lower;
    }

    /// <summary>
    /// The lineage positions [Start, End) of a sample's candidate nodes for one span, or null when
    /// the lineage has no usable bound for that span (the sample then does not vote).
    /// </summary>
    internal static (int Start, int End)? Segment(PlacementSpan span, PlacementSample sample, IReadOnlyList<ColLineageNode> lineage) {
        switch (span) {
            case PlacementSpan.ClassToOrder: {
                var lower = IndexOfName(lineage, sample.OrderName, 0, lineage.Count);
                if (lower < 0) {
                    lower = FirstAtOrBelow(lineage, ColRankOrder.Order, 0);
                }
                if (lower < 0) {
                    return null;
                }
                var upper = IndexOfName(lineage, sample.ClassName, 0, lower);
                if (upper < 0) {
                    upper = LastWhere(lineage, lower, o => o <= ColRankOrder.Class);
                }
                return upper < 0 ? null : (upper + 1, lower);
            }
            case PlacementSpan.OrderToFamily: {
                var family = FirstAtOrBelow(lineage, ColRankOrder.Family, 0);
                if (family < 0) {
                    return null;
                }
                var upper = IndexOfName(lineage, sample.OrderName, 0, family);
                if (upper < 0) {
                    upper = LastWhere(lineage, family, o => o < ColRankOrder.Order);
                }
                return upper < 0 ? null : (upper + 1, family);
            }
            case PlacementSpan.FamilyToGenus: {
                var genus = FirstAtOrBelow(lineage, ColRankOrder.Genus, 0);
                if (genus < 0) {
                    genus = lineage.Count;
                }
                var upper = LastWhere(lineage, genus, o => o <= ColRankOrder.Family);
                return upper < 0 ? null : (upper + 1, genus);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(span), span, null);
        }
    }

    private static (int Upper, int Lower) Bounds(PlacementSpan span) => span switch {
        PlacementSpan.ClassToOrder => (ColRankOrder.Class, ColRankOrder.Order),
        PlacementSpan.OrderToFamily => (ColRankOrder.Order, ColRankOrder.Family),
        PlacementSpan.FamilyToGenus => (ColRankOrder.Family, ColRankOrder.Genus),
        _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
    };

    private static int IndexOfName(IReadOnlyList<ColLineageNode> lineage, string? name, int from, int to) {
        if (string.IsNullOrWhiteSpace(name)) {
            return -1;
        }
        var trimmed = name.Trim();
        for (var i = from; i < to; i++) {
            if (string.Equals(lineage[i].Name, trimmed, StringComparison.OrdinalIgnoreCase)) {
                return i;
            }
        }
        return -1;
    }

    private static int FirstAtOrBelow(IReadOnlyList<ColLineageNode> lineage, int ordinal, int from) {
        for (var i = from; i < lineage.Count; i++) {
            if (ColRankOrder.Of(lineage[i].Rank) is { } o && o >= ordinal) {
                return i;
            }
        }
        return -1;
    }

    private static int LastWhere(IReadOnlyList<ColLineageNode> lineage, int before, Func<int, bool> test) {
        for (var i = before - 1; i >= 0; i--) {
            if (ColRankOrder.Of(lineage[i].Rank) is { } o && test(o)) {
                return i;
            }
        }
        return -1;
    }

    private static PlacementAnchor MakeAnchor(PlacementSpan span, string key, PlacementSample s) => span switch {
        PlacementSpan.ClassToOrder => new PlacementAnchor(span, key, s.Kingdom, s.ClassName, s.OrderName, null, null),
        PlacementSpan.OrderToFamily => new PlacementAnchor(span, key, s.Kingdom, s.ClassName, s.OrderName, s.FamilyName, null),
        PlacementSpan.FamilyToGenus => new PlacementAnchor(span, key, s.Kingdom, s.ClassName, s.OrderName, s.FamilyName, s.GenusName),
        _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
    };

    /// <summary>Key of the IUCN taxon one rank above the anchor: class, order or family.</summary>
    private static string ParentKey(PlacementSpan span, PlacementSample s) => span switch {
        PlacementSpan.ClassToOrder => Join(s.Kingdom, s.ClassName),
        PlacementSpan.OrderToFamily => Join(s.Kingdom, s.ClassName, s.OrderName),
        PlacementSpan.FamilyToGenus => Join(s.Kingdom, s.FamilyName),
        _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
    };

    private static string ParentLabel(PlacementSpan span, PlacementSample s) => span switch {
        PlacementSpan.ClassToOrder => s.ClassName ?? string.Empty,
        PlacementSpan.OrderToFamily => s.OrderName ?? string.Empty,
        PlacementSpan.FamilyToGenus => s.FamilyName ?? string.Empty,
        _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
    };

    private static string Join(params string?[] parts) =>
        string.Join("|", parts.Select(p => (p ?? string.Empty).Trim().ToUpperInvariant()));

    private sealed class NodeInfo {
        public NodeInfo(string name, string rank, int? ordinal) {
            Name = name;
            Rank = rank;
            Ordinal = ordinal;
        }

        public string Name { get; }
        public string Rank { get; }
        public int? Ordinal { get; }

        /// <summary>Largest lineage position seen. Lineages cut short by a missing parent start lower, so the largest is the true depth.</summary>
        public int Depth { get; set; } = -1;
    }

    private sealed class AnchorState {
        public AnchorState(PlacementAnchor anchor, string parentKey) {
            Anchor = anchor;
            ParentKey = parentKey;
        }

        public PlacementAnchor Anchor { get; }
        public string ParentKey { get; }
        public int Species { get; set; }
        public int Voting { get; set; }
        public Dictionary<string, int> Votes { get; } = new(StringComparer.Ordinal);
    }

    private sealed class SpanState {
        public Dictionary<string, AnchorState> Anchors { get; } = new(StringComparer.Ordinal);
        public HashSet<string> Candidates { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, Dictionary<string, int>> Containment { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, string> ParentLabels { get; } = new(StringComparer.Ordinal);
    }
}
