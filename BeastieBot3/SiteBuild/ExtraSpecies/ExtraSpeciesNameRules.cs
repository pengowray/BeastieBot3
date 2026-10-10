using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

// The name tests that decide whether a species from the Catalogue of Life or Wikidata may be the
// same as an IUCN taxon or as a species from the other source, when no id links them (see
// SiteExtraSpeciesBuild for what is compared with what). Pure, so the rules are pinned by tests.

namespace BeastieBot3.SiteBuild.ExtraSpecies;

/// Why two entries may be the same species. Likely reasons make the site leave out the entry from
/// the less preferred source; possible reasons only add a notice.
internal static class OverlapReason {
    /// The entry's name is one of the IUCN taxon's IUCN synonyms.
    public const string IucnSynonym = "iucn-synonym";
    /// The entry's name is a Catalogue of Life synonym of the other entry (or of the IUCN taxon's CoL usage).
    public const string ColSynonym = "col-synonym";
    /// The entry's name is a Wikidata synonym (P1420) of the IUCN taxon.
    public const string WikidataSynonym = "wikidata-synonym";
    /// Same genus, epithets that differ only by a Latin gender ending ("albus", "alba").
    public const string GenderEnding = "gender-ending";
    /// Same genus, and the epithets are one name spelled two ways (SpellingMatch).
    public const string Spelling = "spelling";
    /// Same epithet in another genus of the same family, with the same author and year, or an
    /// epithet no other species in the family has.
    public const string OtherGenus = "other-genus";
    /// The IUCN taxon has a provisional name ("Notogomphus sp. nov. 'lateralis'") and the entry's name
    /// is the one built from its quoted epithet ("Notogomphus lateralis"): it may have been described since.
    public const string ProvisionalName = ExtraOverlapReasons.ProvisionalName;
    /// The IUCN taxon's name is the entry's name with "_new" after it ("Aquilegia ottonis_new"), a
    /// record IUCN made for a national assessment. Not likely: the list keeps the entry, because the
    /// IUCN taxon has no global assessment.
    public const string WorkingName = ExtraOverlapReasons.WorkingName;

    public static bool IsLikely(string reason) =>
        reason is IucnSynonym or ColSynonym or WikidataSynonym or GenderEnding;
}

internal static class ExtraSpeciesNameRules {
    // Endings that change with the genus's gender. Each group is one adjective's forms.
    private static readonly string[][] GenderGroups = [
        ["us", "a", "um"],
        ["er", "ra", "rum"],
        ["er", "era", "erum"],
        ["is", "e"],
        ["os", "on"],
    ];

    /// "albus" and "alba", "niger" and "nigrum", "brevis" and "breve"; not "alba" and "albida".
    public static bool IsGenderVariant(string a, string b) {
        if (a.Length < 3 || b.Length < 3 || string.Equals(a, b, StringComparison.Ordinal)) {
            return false;
        }
        foreach (var group in GenderGroups) {
            foreach (var ending in group) {
                if (!a.EndsWith(ending, StringComparison.Ordinal) || a.Length - ending.Length < 2) {
                    continue;
                }
                var stem = a[..^ending.Length];
                foreach (var other in group) {
                    if (other != ending && string.Equals(b, stem + other, StringComparison.Ordinal)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /// Why two epithets of one genus are one name spelled two ways, or null when they are not (or
    /// are gender variants, counted apart). rare: one of the two epithets is used in no other genus
    /// of the IUCN species and extra entries; sameAuthor: SameAuthority of the two names.
    ///
    /// The rule (October 2026) replaced "one letter apart, or two from 7 letters", which matched about
    /// 17,900 pairs of which a reading of 280 judged about 35% the same name: most pairs that differ
    /// in the first letters, or by a consonant, are different words (pubescens and rubescens, striata
    /// and stricta, microcarpa and macrocarpa), and so are most pairs 3 or 4 letters apart near the end
    /// (densiflora and densifolia, thomsonii and thomsoniana). This one keeps about 5,400 of those
    /// pairs, judged about 95% the same name. In order:
    ///   A  the same after Normalize ("x canescens", "le-testui");
    ///   B  the same after Canonical spellings (coeruleum and caeruleum, hakeaeformis and hakeiformis,
    ///      joponensis and yoponensis);
    ///   C  one stem with two of the endings i, ii, ae, iae (richardsii and richardsiae);
    ///   D  one edit (a swap of neighbouring letters counts as one) after the first 3 letters, when
    ///      one epithet is rare, the authors are the same, or the edit is soft (a vowel for a vowel; a
    ///      vowel, "h" or doubled letter added or removed; a swap): lowei and lowii, froggatti and frogatti;
    ///   E  one edit in the first 3 letters, when one epithet is rare or the authors are the same
    ///      (kusumba and kasumba, unica and uncia), never micro- for macro-;
    ///   F  two edits in epithets of 7 or more letters, with the same authors, when one epithet is
    ///      rare or the edits come after the first 3 letters (tnaculatus and maculatus).
    public static string? SpellingMatch(string a, string b, bool rare, bool sameAuthor) {
        if (string.Equals(a, b, StringComparison.Ordinal) || IsGenderVariant(a, b)) {
            return null;
        }
        var x = Normalize(a);
        var y = Normalize(b);
        if (Math.Min(x.Length, y.Length) < 4) {
            return null;
        }
        if (x == y) {
            return "A";
        }
        if (Canonical(x).Overlaps(Canonical(y))) {
            return "B";
        }
        if (IsPatronymPair(x, y)) {
            return "C";
        }
        var distance = Osa(x, y);
        var prefix = CommonPrefix(x, y);
        if (distance == 1) {
            if (prefix >= 3) {
                return rare || sameAuthor ? "D" : IsSoftEdit(x, y) ? "D" : null;
            }
            if (IsMicroMacro(x) && IsMicroMacro(y)) {
                return null;
            }
            return rare || sameAuthor ? "E" : null;
        }
        if (distance == 2 && Math.Min(x.Length, y.Length) >= 7 && sameAuthor && (rare || prefix >= 3)) {
            return "F";
        }
        return null;
    }

    /// Lower case, without diacritics, ligatures (NFKC: "ﬂ" is "fl"), the hybrid marker and hyphens,
    /// spaces and apostrophes.
    internal static string Normalize(string epithet) {
        var text = epithet.Normalize(NormalizationForm.FormKC).Replace("×", "").Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(text.Length);
        foreach (var c in text) {
            if (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark) {
                sb.Append(char.ToLowerInvariant(c));
            }
        }
        var s = sb.ToString();
        if (s.StartsWith("x ", StringComparison.Ordinal)) {
            s = s[2..];
        }
        return s.Replace("-", "").Replace("'", "").Replace(" ", "");
    }

    // A connecting vowel before a second word ("hakeaeformis", "hakeiformis").
    private static readonly Regex ConnectingVowel = new(@"(?<=[a-z])(iae|ae|ii|i|e|o)(?=(fol|flor|form|fer|ger|carp|phyl|col|spor))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex DoubledVowel = new(@"([aeiou])\1+", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// The forms an epithet has when spellings that Latin names write two ways are made one: a
    /// connecting vowel before -folia, -flora, -formis and the like as "i"; "ae" as "e" or "a", "oe"
    /// as "e" or "o" (all four ways); "ue" as "u"; "ph" as "f"; "y" and "j" as "i"; "k" as "c"; "w"
    /// and "v" as "u"; a final "-os" as "-us" and "-on" as "-um"; a doubled vowel as one. Doubled
    /// consonants stay, or roosi and rossii would be one.
    internal static HashSet<string> Canonical(string normalized) {
        var forms = new HashSet<string>(StringComparer.Ordinal);
        foreach (var ae in new[] { "e", "a" }) {
            foreach (var oe in new[] { "e", "o" }) {
                var s = ConnectingVowel.Replace(normalized, "i")
                    .Replace("ae", ae).Replace("oe", oe).Replace("ue", "u")
                    .Replace("ph", "f").Replace("y", "i").Replace("j", "i").Replace("k", "c").Replace("w", "v").Replace("v", "u");
                if (s.EndsWith("os", StringComparison.Ordinal)) {
                    s = s[..^2] + "us";
                }
                if (s.EndsWith("on", StringComparison.Ordinal)) {
                    s = s[..^2] + "um";
                }
                forms.Add(DoubledVowel.Replace(s, "$1"));
            }
        }
        return forms;
    }

    private static readonly string[] GenitiveEndings = ["iae", "ae", "ii", "i"];

    // One stem of 3 or more letters with two different endings of i, ii, ae and iae.
    private static bool IsPatronymPair(string a, string b) {
        (string Stem, string Ending)? Split(string s) {
            foreach (var ending in GenitiveEndings) {
                if (s.EndsWith(ending, StringComparison.Ordinal) && s.Length - ending.Length >= 3) {
                    return (s[..^ending.Length], ending);
                }
            }
            return null;
        }
        return Split(a) is { } x && Split(b) is { } y && x.Stem == y.Stem && x.Ending != y.Ending;
    }

    // For epithets one edit apart: a swap of neighbouring letters, a vowel for a vowel, or a vowel,
    // "h" or doubled letter added or removed.
    private static bool IsSoftEdit(string a, string b) {
        const string vowelish = "aeiouyh";
        var p = CommonPrefix(a, b);
        var x = a[p..];
        var y = b[p..];
        var suffix = 0;
        while (suffix < Math.Min(x.Length, y.Length) && x[^(suffix + 1)] == y[^(suffix + 1)]) {
            suffix++;
        }
        var dx = x[..^suffix];
        var dy = y[..^suffix];
        if (dx.Length == 2 && dy.Length == 2 && dx[0] == dy[1] && dx[1] == dy[0]) {
            return true;
        }
        if (dx.Length == 1 && dy.Length == 1) {
            return vowelish.Contains(dx[0]) && vowelish.Contains(dy[0]) && dx[0] != 'h' && dy[0] != 'h';
        }
        var added = dx.Length > 0 ? dx : dy;
        var full = dx.Length > 0 ? a : b;
        if (added.Length != 1) {
            return false;
        }
        var c = added[0];
        return vowelish.Contains(c) || (p > 0 && full[p - 1] == c) || (p + 1 < full.Length && full[p + 1] == c);
    }

    private static bool IsMicroMacro(string s) => s.StartsWith("micro", StringComparison.Ordinal) || s.StartsWith("macro", StringComparison.Ordinal);

    private static int CommonPrefix(string a, string b) {
        var n = Math.Min(a.Length, b.Length);
        var i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    // Optimal string alignment distance: Levenshtein, with a swap of neighbouring letters counted as one.
    internal static int Osa(string a, string b) {
        var d = new int[a.Length + 1, b.Length + 1];
        for (var i = 0; i <= a.Length; i++) {
            d[i, 0] = i;
        }
        for (var j = 0; j <= b.Length; j++) {
            d[0, j] = j;
        }
        for (var i = 1; i <= a.Length; i++) {
            for (var j = 1; j <= b.Length; j++) {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                d[i, j] = Math.Min(Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1), d[i - 1, j - 1] + cost);
                if (i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1]) {
                    d[i, j] = Math.Min(d[i, j], d[i - 2, j - 2] + 1);
                }
            }
        }
        return d[a.Length, b.Length];
    }

    /// Authorities that name the same author and year once brackets, punctuation, "&"/"and",
    /// "ex" parts and diacritics are ignored: "(Linnaeus, 1758)" and "Linnaeus 1758". Both must have
    /// a year.
    public static bool SameAuthority(string? a, string? b) {
        var x = AuthorityKey(a);
        var y = AuthorityKey(b);
        return x is not null && x == y;
    }

    internal static string? AuthorityKey(string? authority) {
        if (string.IsNullOrWhiteSpace(authority)) {
            return null;
        }
        var text = authority.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(text.Length);
        var hasYear = false;
        foreach (var c in text) {
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark) {
                continue;
            }
            if (char.IsLetter(c)) {
                sb.Append(char.ToLowerInvariant(c));
            } else if (char.IsDigit(c)) {
                sb.Append(c);
                hasYear = true;
            } else {
                sb.Append(' ');
            }
        }
        var words = sb.ToString().Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Where(w => w is not ("and" or "et" or "in" or "ex"));
        var key = string.Join(' ', words);
        return hasYear && key.Length > 0 ? key : null;
    }

    /// "Genus epithet" as two parts, from a species name: no subgenus in brackets, no author, no
    /// infraspecific part. Null when the name is not a plain binomial.
    public static (string Genus, string Epithet)? SplitBinomial(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }
        var parts = name.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Where(p => !(p.StartsWith('(') && p.EndsWith(')')))
            .ToList();
        if (parts.Count != 2) {
            return null;
        }
        var (genus, epithet) = (parts[0], parts[1]);
        if (genus.Length < 2 || !char.IsUpper(genus[0]) || !genus.Skip(1).All(char.IsLower)) {
            return null;
        }
        if (epithet.Length < 2 || !epithet.All(c => char.IsLower(c) || c == '-')) {
            return null;
        }
        return (genus, epithet);
    }
}
