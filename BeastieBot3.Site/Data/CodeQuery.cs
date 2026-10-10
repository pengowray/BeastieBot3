using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Data;

/// A search for a code given among a taxon's English names: "bird code: CROW", "bird code CROW",
/// "code: SBT", "species code: Po" (IUCN's seagrass codes, written that way). Such a search finds
/// only codes, and an exact code is a strong match. Without the words a code is found too, but after
/// every name, and never goes straight to a taxon: "crow" must not open Lophostrix cristata.
public static partial class CodeQuery {
    [GeneratedRegex(@"^\s*(?:(?:bird|species|plant)\s+)?code\b\s*:?\s*(?<code>[\p{L}\p{N}]{1,8})\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Pattern();

    /// The code ("CROW"); null when the text is not a code search.
    public static string? Parse(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var match = Pattern().Match(text);
        return match.Success ? match.Groups["code"].Value : null;
    }
}
