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

    /// The form two common names in a language other than English must share to be one name:
    /// Unicode compatibility normalisation (NFKC), lower case, runs of whitespace collapsed to one
    /// space, trimmed. Accents and other marks are kept, so "Ñandú" and "Nandu", or the Japanese
    /// "ガエル" and "カエル", stay two names; Fold would make each pair one.
    public static string CaseFold(string name) {
        var normalised = name.Normalize(NormalizationForm.FormKC);
        var sb = new StringBuilder(normalised.Length);
        var pendingSpace = false;
        foreach (var c in normalised) {
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
        return sb.ToString();
    }
}
