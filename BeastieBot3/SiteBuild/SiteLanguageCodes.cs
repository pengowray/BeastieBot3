using BeastieBot3.Shared.SiteData;

// Language codes of the common names that site build-db reads from the Catalogue of Life (ISO 639-3,
// plus a few Wikimedia codes and some wrong ones), from Wikidata (Wikimedia's language codes:
// "zh-hans", "pt-br", "be-tarask", "sr-el") and from the language codes of Wikipedia sites
// ("zh-yue" for zh_yuewiki, "bat-smg", "simple"), put into the form the site stores: the ISO 639-1
// code when the language has one, else its ISO 639-3 code. A code is kept only when
// LanguageNameTable has an English name for it, so every language the species page lists has one.
// IUCN's codes go through IucnLanguageCodes alone and are kept as they are.

namespace BeastieBot3.SiteBuild;

internal static class SiteLanguageCodes {
    // Codes that need more than taking the part before the first hyphen: Wikimedia's own codes,
    // codes the Catalogue of Life gets wrong, and individual languages stored as their
    // macrolanguage. null: the names are left out.
    private static readonly Dictionary<string, string?> Special = new(StringComparer.Ordinal) {
        // Wikimedia codes that are not ISO 639 codes of the same language.
        ["zh-yue"] = "yue",
        ["zh-min-nan"] = "nan",
        ["zh-classical"] = "lzh",
        ["be-x-old"] = "be",
        ["bat-smg"] = "sgs",
        ["fiu-vro"] = "vro",
        ["roa-rup"] = "rup",
        ["cbk-zam"] = "cbk",
        ["als"] = "gsw",   // Alemannic Wikipedia; the ISO code als is Tosk Albanian
        ["bh"] = "bho",    // Bhojpuri Wikipedia
        ["sh"] = "hbs",    // Serbo-Croatian; sh was withdrawn from ISO 639-1
        ["mo"] = "ro",     // Moldovan, written as Romanian
        ["ike"] = "iu",    // Eastern Canadian Inuktitut, Wikidata's "ike-cans" and "ike-latn"
        ["simple"] = null, // Simple English Wikipedia
        ["nrm"] = null,    // Norman Wikipedia; Norman as a whole has no ISO 639 code (nrm is Narom)
        ["roa-tara"] = null, // Tarantino, no ISO 639 code
        ["map-bms"] = null,  // Banyumasan, no ISO 639 code
        ["eml"] = null,      // Emilian-Romagnol, an ISO 639 code that has been withdrawn
        // Kotava Wikipedia titles its species articles with a word for the group and the scientific
        // name in brackets ("Vesnol (Myotis horsfieldii)"), so the title without the brackets is one
        // word for all 960 bats, and its Wikidata labels repeat the titles.
        ["avk"] = null,
        // Codes the Catalogue of Life gives to names in another language: its source 2036 (and a few
        // others) has Danish names as "dnj" (Dan), Thai as "thy" (Tha), Malayalam as "mlf" (Mal) and
        // Persian as "fqs" (Fas), as the names' scripts and words show.
        ["dnj"] = "da",
        ["thy"] = "th",
        ["mlf"] = "ml",
        ["fqs"] = "fa",
        // Individual languages stored as their macrolanguage, which is how Wikipedia and Wikidata
        // label the same names: Mandarin as Chinese, Standard Malay as Malay, and so on.
        ["cmn"] = "zh",
        ["zlm"] = "ms",
        ["zsm"] = "ms",
        ["swh"] = "sw",
        ["arb"] = "ar",
        ["pes"] = "fa",
        ["ekk"] = "et",
        ["lvs"] = "lv",
        ["npi"] = "ne",
        ["khk"] = "mn",
        ["uzn"] = "uz",
        ["azj"] = "az",
        ["plt"] = "mg",
        ["ydd"] = "yi",
        ["ory"] = "or",
        ["pbu"] = "ps",
    };

    /// The code to store for a common name from the Catalogue of Life, Wikidata or Wikipedia, or
    /// null when its names are left out: English (out of scope: the English names come from the
    /// common names store), no language or several, a code with no ISO 639 language, or a language
    /// LanguageNameTable has no name for.
    public static string? Normalise(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return null;
        }
        var lower = code.Trim().ToLowerInvariant().Replace('_', '-');
        if (Special.TryGetValue(lower, out var special)) {
            return Known(special);
        }
        var hyphen = lower.IndexOf('-');
        var primary = hyphen < 0 ? lower : lower[..hyphen];
        if (Special.TryGetValue(primary, out special)) {
            return Known(special);
        }
        return Known(IucnLanguageCodes.Normalise(primary));
    }

    // Interned: a build keeps about a million of these names until they are written.
    private static string? Known(string? code) =>
        code is null || code == "en" || !LanguageNameTable.Contains(code) ? null : string.Intern(code);
}
