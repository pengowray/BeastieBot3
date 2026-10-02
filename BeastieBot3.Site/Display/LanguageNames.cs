using System.Globalization;

namespace BeastieBot3.Site.Display;

/// English names of the ISO 639 language codes common names carry ("fr" -> "French").
public static class LanguageNames {
    // The languages IUCN and Wikidata give most names in, so the common cases do not depend on the
    // server's ICU data.
    private static readonly Dictionary<string, string> Known = new(StringComparer.OrdinalIgnoreCase) {
        ["en"] = "English",
        ["fr"] = "French",
        ["es"] = "Spanish",
        ["pt"] = "Portuguese",
        ["de"] = "German",
        ["it"] = "Italian",
        ["nl"] = "Dutch",
        ["sv"] = "Swedish",
        ["da"] = "Danish",
        ["no"] = "Norwegian",
        ["nb"] = "Norwegian Bokmål",
        ["fi"] = "Finnish",
        ["is"] = "Icelandic",
        ["pl"] = "Polish",
        ["cs"] = "Czech",
        ["ru"] = "Russian",
        ["uk"] = "Ukrainian",
        ["zh"] = "Chinese",
        ["ja"] = "Japanese",
        ["ko"] = "Korean",
        ["ar"] = "Arabic",
        ["tr"] = "Turkish",
        ["id"] = "Indonesian",
        ["ms"] = "Malay",
        ["vi"] = "Vietnamese",
        ["th"] = "Thai",
        ["hi"] = "Hindi",
        ["sw"] = "Swahili",
        ["af"] = "Afrikaans",
        ["la"] = "Latin",
    };

    public static string Name(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return "?";
        }
        var trimmed = code.Trim();
        if (Known.TryGetValue(trimmed, out var name)) {
            return name;
        }
        try {
            var culture = CultureInfo.GetCultureInfo(trimmed);
            if (!string.IsNullOrEmpty(culture.EnglishName) && !culture.EnglishName.StartsWith("Unknown", StringComparison.Ordinal)) {
                return culture.EnglishName;
            }
        } catch (CultureNotFoundException) {
            // Fall through to the code itself.
        }
        return trimmed;
    }
}
