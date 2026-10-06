using System.Globalization;
using System.Text;

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
    /// Same genus, epithets one or two letters apart.
    public const string Spelling = "spelling";
    /// Same epithet in another genus of the same family, with the same author and year, or an
    /// epithet no other species in the family has.
    public const string OtherGenus = "other-genus";

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

    /// Epithets one letter apart when the shorter has 4 or more letters, or two letters apart when
    /// it has 7 or more ("smithi" and "smithii", "wallacei" and "wallacii"). Gender variants are not
    /// counted here.
    public static bool IsSpellingVariant(string a, string b) {
        if (string.Equals(a, b, StringComparison.Ordinal) || IsGenderVariant(a, b)) {
            return false;
        }
        var shorter = Math.Min(a.Length, b.Length);
        var allowed = shorter >= 7 ? 2 : shorter >= 4 ? 1 : 0;
        return allowed > 0 && Math.Abs(a.Length - b.Length) <= allowed && Distance(a, b, allowed) <= allowed;
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

    // Levenshtein distance, stopping early once it passes the limit.
    private static int Distance(string a, string b, int limit) {
        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++) {
            previous[j] = j;
        }
        for (var i = 1; i <= a.Length; i++) {
            current[0] = i;
            var rowMin = current[0];
            for (var j = 1; j <= b.Length; j++) {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                current[j] = Math.Min(Math.Min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
                rowMin = Math.Min(rowMin, current[j]);
            }
            if (rowMin > limit) {
                return limit + 1;
            }
            (previous, current) = (current, previous);
        }
        return previous[b.Length];
    }
}
