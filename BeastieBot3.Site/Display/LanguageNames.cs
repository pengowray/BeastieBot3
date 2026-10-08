using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Display;

/// English names of the ISO 639 language codes common names carry ("fr" -> "French"), and the
/// code a name is grouped and tagged by.
public static partial class LanguageNames {
    // Codes that say nothing about the language: undetermined, uncoded, no linguistic content.
    // Names with them are listed with the names that have no code.
    private static readonly HashSet<string> NotGiven = new(StringComparer.OrdinalIgnoreCase) { "und", "mis", "zxx" };

    /// The code names are grouped by: trimmed and lower case, "eng" (ISO 639-2) as "en", and an
    /// empty string when no language is given ("und", "mis", "zxx", blank or missing).
    public static string Key(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return string.Empty;
        }
        var key = code.Trim().ToLowerInvariant();
        if (NotGiven.Contains(key)) {
            return string.Empty;
        }
        return key == "eng" ? "en" : key;
    }

    /// English, in any region: "en", "en-GB" or IUCN's ISO 639-2 "eng".
    public static bool IsEnglish(string? code) {
        var key = Key(code);
        return key == "en" || key.StartsWith("en-", StringComparison.Ordinal);
    }

    /// The code for a lang attribute, or null when no language is given or the code is not a
    /// well-formed BCP 47 language tag.
    public static string? LangAttribute(string? code) {
        var key = Key(code);
        return key.Length > 0 && LanguageTag().IsMatch(key) ? key : null;
    }

    /// The English name of the language, SiteText.LanguageNotGiven when there is none, or the code
    /// itself when no name is known. Names come from LanguageNameTable (the ISO 639 codes, with
    /// CLDR's English names), so they do not depend on the server's ICU data; ICU is asked only
    /// about a code the table does not have.
    public static string Name(string? code) {
        var key = Key(code);
        if (key.Length == 0) {
            return SiteText.LanguageNotGiven;
        }
        if (LanguageNameTable.Name(key) is { } name) {
            return name;
        }
        try {
            var culture = CultureInfo.GetCultureInfo(key);
            // Codes ICU does not know can resolve to the invariant culture, whose English name is
            // "Invariant Language (Invariant Country)".
            if (culture.Name.Length == 0 || culture.Equals(CultureInfo.InvariantCulture)) {
                return key;
            }
            if (!string.IsNullOrEmpty(culture.EnglishName) && !culture.EnglishName.StartsWith("Unknown", StringComparison.Ordinal)) {
                return culture.EnglishName;
            }
        } catch (CultureNotFoundException) {
            // Fall through to the code itself.
        }
        return key;
    }

    [GeneratedRegex("^[a-z]{2,3}(-[a-z0-9]{2,8})*$")]
    private static partial Regex LanguageTag();
}
