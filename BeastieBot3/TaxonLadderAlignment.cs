using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace BeastieBot3;

internal static class TaxonLadderAlignment {
    private static readonly string[] CanonicalRankOrder = {
        "domain",
        "kingdom",
        "phylum",
        "division",
        "class",
        "order",
        "suborder",
        "infraorder",
        "superfamily",
        "family",
        "subfamily",
        "tribe",
        "subtribe",
        "genus",
        "subgenus",
        "species",
        "subspecies",
        "variety",
        "form"
    };

    private static readonly IReadOnlyDictionary<string, int> RankPosition = BuildRankIndices();

    public static TaxonLadderAlignmentResult Align(params TaxonLadder[] ladders) {
        if (ladders is null || ladders.Length == 0) {
            return new TaxonLadderAlignmentResult(Array.Empty<TaxonLadderAlignmentRow>());
        }

        var table = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        var rankSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var ladder in ladders) {
            if (ladder is null) {
                continue;
            }

            foreach (var node in ladder.Nodes) {
                rankSet.Add(node.Rank);
                if (!table.TryGetValue(node.Rank, out var row)) {
                    row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    table[node.Rank] = row;
                }
                row[ladder.SourceLabel] = node.Name;
            }
        }

        if (rankSet.Count == 0) {
            return new TaxonLadderAlignmentResult(Array.Empty<TaxonLadderAlignmentRow>());
        }

        var orderedRanks = rankSet
            .Select(rank => new { Rank = rank, Position = GetRankPosition(rank) })
            .OrderBy(item => item.Position)
            .ThenBy(item => item.Rank, StringComparer.Ordinal)
            .Select(item => item.Rank)
            .ToList();

        var rows = new List<TaxonLadderAlignmentRow>(orderedRanks.Count);
        foreach (var rank in orderedRanks) {
            var row = table.TryGetValue(rank, out var values)
                ? new TaxonLadderAlignmentRow(rank, new ReadOnlyDictionary<string, string>(values))
                : new TaxonLadderAlignmentRow(rank, new ReadOnlyDictionary<string, string>(new Dictionary<string, string>()));
            rows.Add(row);
        }

        return new TaxonLadderAlignmentResult(rows);
    }

    private static IReadOnlyDictionary<string, int> BuildRankIndices() {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < CanonicalRankOrder.Length; i++) {
            map[CanonicalRankOrder[i]] = i;
        }
        return new ReadOnlyDictionary<string, int>(map);
    }

    private static int GetRankPosition(string rank) {
        if (RankPosition.TryGetValue(rank, out var position)) {
            return position;
        }

        return CanonicalRankOrder.Length + Math.Abs(rank.GetHashCode());
    }
}

internal sealed record TaxonLadderAlignmentRow(string Rank, IReadOnlyDictionary<string, string> Values);

internal sealed record TaxonLadderAlignmentResult(IReadOnlyList<TaxonLadderAlignmentRow> Rows);
