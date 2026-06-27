using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

// Classifies why two scientific-name strings that are not byte-for-byte equal fail to match,
// and how far apart they are. Used by the CoL crosscheck audit to turn a bare "no exact match"
// into an actionable hint: a near-identical CoL name plus the reason it differs (whitespace,
// punctuation, Unicode encoding, diacritics, letter case, or a combination) or, when nothing
// lines up that cleanly, the edit distance to the closest candidate.
//
// All comparison happens on decoded text (the caller HTML-decodes first). The transforms are
// applied as a cascade from least to most aggressive so the result names the single innocuous
// reason when there is one, and falls back to a Levenshtein distance otherwise.

namespace BeastieBot3.Taxonomy;

internal static class ScientificNameDifference {
    internal enum Kind {
        Exact,        // byte-for-byte equal (callers usually filter this out)
        Whitespace,   // differ only in spacing
        Punctuation,  // differ only in punctuation (hyphens, periods, apostrophes, hybrid markers)
        Unicode,      // differ only in Unicode encoding (NFC vs NFD of the same text)
        Diacritic,    // differ only in diacritics (Muller vs Müller)
        Case,         // differ only in letter case
        Formatting,   // same name up to a combination of the above
        Fuzzy,        // a genuine spelling difference within the edit-distance threshold
        Unrelated,    // too far apart to plausibly be the same name
    }

    internal readonly record struct Result(Kind Kind, int Distance, string Description) {
        // Formatting-equivalent matches (same name, only encoding/formatting differs) are the most
        // useful suggestion; among genuine spelling differences a smaller edit distance wins.
        public bool IsFormattingEquivalent => Kind is Kind.Whitespace or Kind.Punctuation
            or Kind.Unicode or Kind.Diacritic or Kind.Case or Kind.Formatting;
    }

    private static readonly Regex MultiSpace = new(@"\s+", RegexOptions.Compiled);

    public static Result Classify(string a, string b) {
        if (a is null) throw new ArgumentNullException(nameof(a));
        if (b is null) throw new ArgumentNullException(nameof(b));

        if (string.Equals(a, b, StringComparison.Ordinal)) {
            return new Result(Kind.Exact, 0, "identical");
        }

        // Encoding-only: same text, different Unicode normalization (e.g. precomposed vs combining).
        var na = a.Normalize(NormalizationForm.FormC);
        var nb = b.Normalize(NormalizationForm.FormC);
        if (string.Equals(na, nb, StringComparison.Ordinal)) {
            return new Result(Kind.Unicode, 0, "differs only in Unicode encoding (normalization)");
        }

        // Work on NFC forms from here so combining-mark noise doesn't masquerade as a real difference.
        if (string.Equals(Canonical(na), Canonical(nb), StringComparison.Ordinal)) {
            return ClassifyFormatting(na, nb);
        }

        // Genuine spelling difference: measure on the aggressively normalized forms so we count
        // letter changes, not formatting.
        var distance = Levenshtein(Canonical(na), Canonical(nb));
        var longer = Math.Max(Canonical(na).Length, Canonical(nb).Length);
        // Allow more slack for longer names, but keep it tight: a one or two character typo in a
        // binomial, scaling to ~1 edit per 6 characters for longer trinomials.
        var threshold = Math.Max(2, longer / 6);
        return distance <= threshold
            ? new Result(Kind.Fuzzy, distance, distance == 1 ? "1-character spelling difference" : $"{distance}-character spelling difference")
            : new Result(Kind.Unrelated, distance, "no close match");
    }

    // Among formatting-equivalent strings, report which dimension(s) actually contribute by
    // removing one transform at a time and seeing whether equality breaks.
    private static Result ClassifyFormatting(string a, string b) {
        var dims = new List<(Kind Kind, string Label)>();
        if (!Eq(a, b, ws: false, lower: true, punct: true, diac: true)) dims.Add((Kind.Whitespace, "spacing"));
        if (!Eq(a, b, ws: true, lower: false, punct: true, diac: true)) dims.Add((Kind.Case, "letter case"));
        if (!Eq(a, b, ws: true, lower: true, punct: false, diac: true)) dims.Add((Kind.Punctuation, "punctuation"));
        if (!Eq(a, b, ws: true, lower: true, punct: true, diac: false)) dims.Add((Kind.Diacritic, "diacritics"));

        if (dims.Count == 1) {
            return new Result(dims[0].Kind, 0, $"differs only in {dims[0].Label}");
        }
        if (dims.Count == 0) {
            // The cascade already proved Canonical(a) == Canonical(b) but no single dimension is
            // responsible on its own — treat as generic formatting.
            return new Result(Kind.Formatting, 0, "differs only in formatting");
        }
        return new Result(Kind.Formatting, 0, $"differs only in {JoinAnd(dims.Select(d => d.Label))}");
    }

    // Equality of a, b under a chosen subset of normalizations applied.
    private static bool Eq(string a, string b, bool ws, bool lower, bool punct, bool diac) =>
        string.Equals(Apply(a, ws, lower, punct, diac), Apply(b, ws, lower, punct, diac), StringComparison.Ordinal);

    private static string Canonical(string s) => Apply(s, ws: true, lower: true, punct: true, diac: true);

    private static string Apply(string s, bool ws, bool lower, bool punct, bool diac) {
        var result = ws ? MultiSpace.Replace(s.Trim(), " ") : s;
        if (lower) result = result.ToLowerInvariant();
        if (punct) result = StripPunctuation(result);
        if (diac) result = StripDiacritics(result);
        return result;
    }

    private static string StripPunctuation(string s) {
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s) {
            if (char.IsLetterOrDigit(ch) || char.IsWhiteSpace(ch)) {
                sb.Append(ch);
            }
        }
        return sb.ToString();
    }

    private static string StripDiacritics(string s) {
        var decomposed = s.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed) {
            if (CharUnicodeInfo.GetUnicodeCategory(ch) != UnicodeCategory.NonSpacingMark) {
                sb.Append(ch);
            }
        }
        return sb.ToString().Normalize(NormalizationForm.FormC);
    }

    private static string JoinAnd(IEnumerable<string> items) {
        var list = items.ToList();
        return list.Count switch {
            0 => "",
            1 => list[0],
            2 => $"{list[0]} and {list[1]}",
            _ => string.Join(", ", list.Take(list.Count - 1)) + ", and " + list[^1],
        };
    }

    public static int Levenshtein(string a, string b) {
        if (a.Length == 0) return b.Length;
        if (b.Length == 0) return a.Length;

        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++) previous[j] = j;

        for (var i = 1; i <= a.Length; i++) {
            current[0] = i;
            for (var j = 1; j <= b.Length; j++) {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                current[j] = Math.Min(Math.Min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
            }
            (previous, current) = (current, previous);
        }
        return previous[b.Length];
    }
}
