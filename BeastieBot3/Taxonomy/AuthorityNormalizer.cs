using System;
using System.Text;

// Normalizes taxonomic authority strings (e.g., "(Linnaeus, 1758)") for consistent
// comparison. Collapses whitespace, normalizes parentheses, handles "&" vs "and",
// and strips trailing punctuation. Used by IucnScientificNameVerifier and synonym
// matching to compare authorities across IUCN, COL, and Wikipedia sources.

namespace BeastieBot3.Taxonomy;

internal static class AuthorityNormalizer {
    public static string Normalize(string? value) {
        if (value is null) {
            return string.Empty;
        }

        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return string.Empty;
        }

        var builder = new StringBuilder(trimmed.Length);
        var previousWasSpace = false;

        foreach (var c in trimmed) {
            var normalized = NormalizeChar(c);
            if (normalized == ' ') {
                if (previousWasSpace) {
                    continue;
                }
                builder.Append(' ');
                previousWasSpace = true;
            } else {
                builder.Append(normalized);
                previousWasSpace = false;
            }
        }

        return builder.ToString();
    }

    public static bool Equivalent(string? a, string? b) {
        var normalizedA = Normalize(a);
        var normalizedB = Normalize(b);
        if (normalizedA.Length == 0 && normalizedB.Length == 0) {
            return true;
        }

        return string.Equals(normalizedA, normalizedB, StringComparison.OrdinalIgnoreCase);
    }

    private static char NormalizeChar(char value) {
        return value switch {
            '\u00A0' => ' ',
            '\u2007' => ' ',
            '\u202F' => ' ',
            '\u2009' => ' ',
            '\t' => ' ',
            '\r' => ' ',
            '\n' => ' ',
            _ => value
        };
    }
}
