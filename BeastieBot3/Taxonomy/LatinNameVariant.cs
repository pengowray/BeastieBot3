using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

// Answers one narrow question: are these two scientific names the same name, rendered differently?
//
// The nomenclature codes let a single taxon's name be written more than one way. A species epithet
// that is a Latin adjective agrees in gender with its genus, so a genus transfer changes the ending
// (Schistura striatus / Schistura striata); a patronym may be formed with one -i or two
// (Ancistrocheirus lesueuri / lesueurii); and transliterated Greek roots vary (rhithymna /
// rithymna, lydgatei / lidgatei). All of these are the same name.
//
// This is deliberately stricter than ScientificNameDifference's fuzzy tier, which measures edit
// distance and cannot tell "Schistura striatus vs striata" (same taxon) from "Elater turcicus vs
// suecicus" (two different beetles). Two rules do the separating:
//
//   1. The genus must match exactly. Cordia/Cora, Sorex/Shorea, and Dacne/Daphne are one or two
//      edits apart and belong to different kingdoms.
//   2. The epithets must be equal once the endings and spellings above are folded away. Anything
//      that survives that folding is a different word, not a different rendering.
//
// Callers use this to offer an article title or a cross-reference, so a false positive is a wrong
// link on a published page. When in doubt this says no.

namespace BeastieBot3.Taxonomy;

internal static class LatinNameVariant {
    /// True when the two names are the same name under Latin gender agreement and the standard
    /// orthographic variants. False for anything else, including names that differ in word count,
    /// in genus, or by a genuine change of epithet.
    public static bool SameName(string? a, string? b) {
        if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b)) {
            return false;
        }

        var left = Parts(a!);
        var right = Parts(b!);
        if (left.Length < 2 || left.Length != right.Length) {
            return false;
        }

        // The genus is the anchor and is never folded: a taxon that moved genus has a different
        // name in the sense that matters here, and near-identical genus names in different
        // kingdoms are exactly the trap this guards against.
        if (!string.Equals(left[0], right[0], StringComparison.OrdinalIgnoreCase)) {
            return false;
        }

        for (var i = 1; i < left.Length; i++) {
            if (!string.Equals(Stem(left[i]), Stem(right[i]), StringComparison.Ordinal)) {
                return false;
            }
        }

        // Identical strings are not a variant of each other; callers want a name to offer.
        return !string.Equals(string.Join(' ', left), string.Join(' ', right), StringComparison.OrdinalIgnoreCase);
    }

    private static readonly Regex Space = new(@"\s+", RegexOptions.Compiled);

    // Rank abbreviations ("ssp.", "subsp.", "var.", "f.") are labels, not name parts, and IUCN and
    // CoL disagree about which to use. Dropping them lets a trinomial compare against a trinomial.
    private static string[] Parts(string name) =>
        Space.Split(StripDiacritics(name).Trim())
            .Where(p => p.Length > 0 && !p.EndsWith('.') && !p.Equals("x", StringComparison.OrdinalIgnoreCase))
            .ToArray();

    // Folds an epithet to the part that carries its meaning: the gendered ending comes off, and the
    // spellings that vary without changing the word are normalised. Order matters, so the digraphs
    // are folded before the endings.
    private static string Stem(string epithet) {
        var s = epithet.ToLowerInvariant();
        s = new string(s.Where(char.IsLetter).ToArray());   // drops hyphens: montis-everesti / montiseveresti

        // Greek transliterations and Latin spellings that vary freely.
        s = s.Replace("ph", "f").Replace("rh", "r").Replace("th", "t").Replace("ch", "c")
             .Replace("ae", "e").Replace("oe", "e").Replace("y", "i").Replace("k", "c")
             .Replace("j", "i").Replace("v", "u").Replace("w", "u");

        // Doubled consonants only: bellieri / bellierii is one patronym written two ways. Doubled
        // vowels are left alone, because "leoo" is a typo of "leo" rather than a second spelling of
        // it, and folding them would make the two the same name.
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s) {
            if (sb.Length == 0 || sb[^1] != ch || IsVowel(ch)) {
                sb.Append(ch);
            }
        }
        s = sb.ToString();

        // The gendered and genitive endings. Stripped once (an epithet is not two endings long) and
        // by the same rule on both sides, so a name that carries an ending is never compared
        // against one that was left with its own ending on.
        foreach (var ending in Endings) {
            if (s.Length >= ending.Length + 2 && s.EndsWith(ending, StringComparison.Ordinal)) {
                return s[..^ending.Length];
            }
        }
        return s;
    }

    private static bool IsVowel(char c) => c is 'a' or 'e' or 'i' or 'o' or 'u';

    // Longest first, so "orum" is tried before "um" and the stem does not keep a stray "or".
    // "ei" and "ii" earn their place next to "i": a patronym is formed either way (haullevillei /
    // haullevillii, haynei / haynii, lidgatei / lydgatei) and both are the same name.
    private static readonly string[] Endings = {
        "orum", "arum", "ium", "eus", "eum", "ea", "us", "um", "is", "es", "os", "on", "ae", "ei", "ii",
        "a", "e", "i", "o",
    };

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
}
