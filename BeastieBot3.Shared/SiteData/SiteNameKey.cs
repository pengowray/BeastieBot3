using System.Globalization;
using System.Text;

namespace BeastieBot3.Shared.SiteData;

/// The folded form of a name used for exact lookups in the site database (name_key.key):
/// lower case, diacritics removed, runs of whitespace collapsed to one space, trimmed.
/// The builder and the site must fold with this one function or lookups miss.
public static class SiteNameKey {
    public static string Fold(string name) {
        var decomposed = name.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(decomposed.Length);
        var pendingSpace = false;
        foreach (var c in decomposed) {
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark) {
                continue;
            }
            if (char.IsWhiteSpace(c)) {
                pendingSpace = sb.Length > 0;
                continue;
            }
            if (pendingSpace) {
                sb.Append(' ');
                pendingSpace = false;
            }
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.ToString().Normalize(NormalizationForm.FormC);
    }
}
