using System;
using System.Collections.Generic;

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

    private static void AppendIfAny(ICollection<string> parts, string? value) {
        if (!string.IsNullOrWhiteSpace(value)) {
            parts.Add(value.Trim());
        }
    }
}
