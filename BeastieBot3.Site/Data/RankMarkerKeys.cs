using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Data;

/// The name_key keys (SiteNameKey.Fold) a trinomial may be stored under: IUCN writes the same name
/// with "ssp.", "subsp." or "var." in one record and without a rank marker in another (a taxon
/// "Sarotherodon tournieri ssp. liberiensis", a synonym "Sarotherodon tournieri liberiensis").
/// A name of three words gets the three marked forms too, and a name of four words whose third word
/// is a marker gets the other markers and the unmarked form. Any other name is its own key.
public static class RankMarkerKeys {
    private static readonly string[] Markers = ["ssp.", "subsp.", "var."];

    public static IReadOnlyList<string> For(string name) {
        var key = SiteNameKey.Fold(name);
        if (key.Length == 0) {
            return [];
        }
        var words = key.Split(' ');
        var keys = new List<string> { key };
        if (words.Length == 3 && IsEpithet(words[1]) && IsEpithet(words[2])) {
            keys.AddRange(Markers.Select(m => $"{words[0]} {words[1]} {m} {words[2]}"));
        } else if (words.Length == 4 && Markers.Contains(words[2]) && IsEpithet(words[1]) && IsEpithet(words[3])) {
            keys.Add($"{words[0]} {words[1]} {words[3]}");
            keys.AddRange(Markers.Where(m => m != words[2]).Select(m => $"{words[0]} {words[1]} {m} {words[3]}"));
        }
        return keys;
    }

    // A folded epithet: letters and hyphens only ("pygmaea", "novae-angliae").
    private static bool IsEpithet(string word) => word.Length > 0 && word.All(c => char.IsLetter(c) || c == '-');
}
