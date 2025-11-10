using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace BeastieBot3;

internal sealed class TaxonLadder {
    private readonly ReadOnlyDictionary<string, TaxonLadderNode> _lookup;

    public TaxonLadder(string sourceLabel, IEnumerable<TaxonLadderNode> nodes) {
        if (string.IsNullOrWhiteSpace(sourceLabel)) {
            throw new ArgumentException("Source label is required.", nameof(sourceLabel));
        }

        SourceLabel = sourceLabel.Trim();

        if (nodes is null) {
            throw new ArgumentNullException(nameof(nodes));
        }

        var list = new List<TaxonLadderNode>();
        var seenRanks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var node in nodes) {
            if (node is null) {
                continue;
            }

            if (string.IsNullOrWhiteSpace(node.Rank) || string.IsNullOrWhiteSpace(node.Name)) {
                continue;
            }

            var normalizedRank = NormalizeRank(node.Rank);
            if (!seenRanks.Add(normalizedRank)) {
                continue;
            }

            list.Add(new TaxonLadderNode(normalizedRank, node.Name.Trim()));
        }

        Nodes = list.AsReadOnly();
        _lookup = new ReadOnlyDictionary<string, TaxonLadderNode>(Nodes.ToDictionary(n => n.Rank, n => n, StringComparer.OrdinalIgnoreCase));
    }

    public string SourceLabel { get; }

    public IReadOnlyList<TaxonLadderNode> Nodes { get; }

    public string? GetValue(string rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return null;
        }

        return _lookup.TryGetValue(NormalizeRank(rank), out var node) ? node.Name : null;
    }

    private static string NormalizeRank(string rank) {
        var trimmed = rank.Trim();
        return trimmed switch {
            "domain" => "domain",
            "superfamily" => "superfamily",
            "subfamily" => "subfamily",
            "tribe" => "tribe",
            "subtribe" => "subtribe",
            "section" => "section",
            "subsection" => "subsection",
            "series" => "series",
            "subseries" => "subseries",
            "subgenus" => "subgenus",
            "genus" => "genus",
            "species" => "species",
            "subspecies" => "subspecies",
            "variety" => "variety",
            "form" => "form",
            _ => trimmed.ToLowerInvariant()
        };
    }
}

internal sealed record TaxonLadderNode(string Rank, string Name);
