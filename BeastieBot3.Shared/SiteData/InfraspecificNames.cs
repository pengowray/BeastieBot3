namespace BeastieBot3.Shared.SiteData;

/// The names of a species' subspecies and varieties as the IUCN Red List, the Catalogue of Life and
/// Wikidata write them ("Panthera pardus ssp. orientalis", "Panthera leo melanochaita",
/// "Abies alba var. acutifolia"): their ranks, their parts, and the key under which the names from
/// different sources are one row on the species page. `site build-db` keeps only the names Split
/// reads, and the site merges the rows by Key.
public static class InfraspecificNames {
    public const string Subspecies = "subspecies";
    public const string Variety = "variety";

    // Rank markers, left out of the parts and the key.
    private static readonly HashSet<string> Markers = new(StringComparer.OrdinalIgnoreCase) {
        "subsp.", "ssp.", "var.", "subsp", "ssp", "var",
    };

    /// The genus, species epithet and infraspecific epithet of a name; null unless the name is those
    /// three words with at most one rank marker before the last: a capitalised genus and two epithets
    /// that start with a lower-case letter.
    public static (string Genus, string Species, string Infra)? Split(string name) {
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (words.Length == 4 && Markers.Contains(words[2])) {
            words = [words[0], words[1], words[3]];
        }
        if (words.Length != 3 || !char.IsUpper(words[0][0]) || !IsEpithet(words[1]) || !IsEpithet(words[2])) {
            return null;
        }
        return (words[0], words[1], words[2]);
    }

    /// The key of a name: the rank, then genus, species and infraspecific epithet folded
    /// (SiteNameKey.Fold), so names that differ only in the rank marker, case or spacing share it and
    /// a subspecies and a variety of the same name do not. Null when Split gives null.
    public static string? Key(string rank, string name) =>
        Split(name) is { } parts ? rank + ":" + SiteNameKey.Fold($"{parts.Genus} {parts.Species} {parts.Infra}") : null;

    /// The rank of IUCN's taxon kind, or null for a kind that is neither ("species").
    public static string? RankOfKind(string kind) => kind switch {
        Subspecies => Subspecies,
        Variety => Variety,
        _ => null,
    };

    // A lower-case word of letters and hyphens ("melanochaita", "st-hilairei"); accents allowed.
    private static bool IsEpithet(string word) => char.IsLower(word[0]) && word.All(c => char.IsLetter(c) || c == '-'
        || System.Globalization.CharUnicodeInfo.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.NonSpacingMark);
}
