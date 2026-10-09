namespace BeastieBot3.Shared.SiteData;

/// The names of a species' subspecies and varieties as the IUCN Red List, the Catalogue of Life and
/// Wikidata write them ("Panthera pardus ssp. orientalis", "Panthera leo melanochaita",
/// "Abies alba var. acutifolia"): their ranks, their parts, and the key under which the names from
/// different sources are one row on the species page. `site build-db` keeps only the names Split
/// reads, and the site merges a species' rows by MergeKey.
public static class InfraspecificNames {
    public const string Subspecies = "subspecies";
    public const string Variety = "variety";

    // Rank markers, left out of the parts and the key.
    private static readonly HashSet<string> Markers = new(StringComparer.OrdinalIgnoreCase) {
        "subsp.", "ssp.", "var.", "subsp", "ssp", "var",
    };

    /// The genus, species epithet and infraspecific epithet of a name; null unless the name is those
    /// three words with at most one rank marker before the last: a capitalised genus and two epithets
    /// that start with a lower-case letter. A subgenus in brackets after the genus, as the Catalogue of
    /// Life writes many insect names ("Stenus (Hypostenus) obconicus obconicus"), is left out.
    public static (string Genus, string Species, string Infra)? Split(string name) {
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (words.Length >= 4 && words[1].Length > 2 && words[1][0] == '(' && words[1][^1] == ')' && char.IsUpper(words[1][1])) {
            words = [words[0], .. words[2..]];
        }
        if (words.Length == 4 && Markers.Contains(words[2])) {
            words = [words[0], words[1], words[3]];
        }
        if (words.Length != 3 || !char.IsUpper(words[0][0]) || !IsEpithet(words[1]) || !IsEpithet(words[2])) {
            return null;
        }
        return (words[0], words[1], words[2]);
    }

    /// The key under which the names in one species' list are one row: the rank and the infraspecific
    /// epithet's stem (LatinNameVariant.Stem), so a spelling with another Latin ending or another
    /// species part is the same row: "Panthera leo melanochaitus" with "Panthera leo melanochaita", and
    /// the Catalogue of Life's "Acerodon macklotii floresii" with IUCN's "Acerodon mackloti floresii".
    /// Only for names already listed under one species, whose species part says nothing more. Null when
    /// Split gives null.
    public static string? MergeKey(string rank, string name) =>
        Split(name) is { } parts ? rank + ":" + LatinNameVariant.Stem(SiteNameKey.Fold(parts.Infra)) : null;

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
