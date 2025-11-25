using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3;

internal static class ScientificNameHelper {
    public static string? Normalize(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return null;
        }

        var parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0) {
            return null;
        }

        return string.Join(' ', parts).ToLowerInvariant();
    }

    public static string? BuildFromParts(string? genus, string? species, string? infraName) {
        var pieces = new List<string>(3);
        AppendIfAny(pieces, genus);
        AppendIfAny(pieces, species);
        AppendIfAny(pieces, infraName);
        return pieces.Count == 0 ? null : string.Join(' ', pieces);
    }

    public static string? BuildWithRankLabel(string? genus, string? species, string? rankLabel, string? infraName) {
        if (string.IsNullOrWhiteSpace(rankLabel) || string.IsNullOrWhiteSpace(infraName)) {
            return null;
        }

        var pieces = new List<string>(4);
        AppendIfAny(pieces, genus);
        AppendIfAny(pieces, species);
        AppendIfAny(pieces, rankLabel);
        AppendIfAny(pieces, infraName);
        return pieces.Count < 4 ? null : string.Join(' ', pieces);
    }

    public static string? BuildWithSubgenus(string? genus, string? subgenus, string? species, string? infraName) {
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(subgenus) || string.IsNullOrWhiteSpace(species)) {
            return null;
        }

        var pieces = new List<string>(4) {
            $"{genus.Trim()} ({subgenus.Trim()})",
            species.Trim()
        };

        if (!string.IsNullOrWhiteSpace(infraName)) {
            pieces.Add(infraName.Trim());
        }

        return string.Join(' ', pieces);
    }

    public static IReadOnlyList<string> BuildInfraRankTokens(string? infraType) {
        if (string.IsNullOrWhiteSpace(infraType)) {
            return Array.Empty<string>();
        }

        var trimmed = infraType.Trim();
        if (trimmed.Length == 0) {
            return Array.Empty<string>();
        }

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { trimmed };
        var normalized = trimmed.ToLowerInvariant();

        if (normalized.Contains("subspecies", StringComparison.Ordinal) || normalized.Contains("subsp", StringComparison.Ordinal) || normalized.Contains("ssp", StringComparison.Ordinal)) {
            set.Add("subsp.");
            set.Add("ssp.");
        }
        else if (normalized.Contains("variety", StringComparison.Ordinal) || normalized.StartsWith("var", StringComparison.Ordinal)) {
            set.Add("var.");
        }
        else if (normalized.StartsWith("form", StringComparison.Ordinal) || normalized.StartsWith("f", StringComparison.Ordinal)) {
            set.Add("f.");
        }

        return set.Where(token => !string.IsNullOrWhiteSpace(token)).Select(token => token.Trim()).ToList();
    }

    private static void AppendIfAny(ICollection<string> parts, string? value) {
        if (!string.IsNullOrWhiteSpace(value)) {
            parts.Add(value.Trim());
        }
    }
}
