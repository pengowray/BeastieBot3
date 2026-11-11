using System.Text.RegularExpressions;

// Repairs for the common names in the Catalogue of Life's chapters of the Handbook of the Mammals of
// the World (82 source datasets in COL 26.7 XR, from Plazi's digitised treatments). Text extraction
// damaged about 1 in 7 of their French and German names; reported at
// https://github.com/plazi/community/issues/583. Only the kinds of damage a pattern can repair
// without changing a correct name are repaired:
//   - a trailing column heading after the "I" read for the book's "|": "Rhinolophe du Cap I German"
//     is "Rhinolophe du Cap" (without the "I", OtherLanguageNameRules still leaves the name out);
//   - French: "à" read as "a" ("Souris a queue fragile"), the apostrophe lost before a capital
//     ("Pélomys dAngola"), "è" read as "é" at the end of a word ("Minioptére") and "tête" read
//     as "téte";
//   - German: "ß" read as "R", "RR" or "lR" after a vowel ("WeiRRbauch", "SchwarzfulR-Baumratte").
// Of the 1,014 names these repairs change, 191 become a name that Wikipedia, Wikidata or IUCN gives
// for the same species. Only 4 were such a name before, French names where IUCN or Wikidata has also
// lost the "à" ("Phoque a capuchon"). Lost hyphens
// ("BerberStreifengrasmaus") are left alone: the missing mark is a hyphen in some names and a space
// in others ("GemeineDickschwanzratte").

namespace BeastieBot3.SiteBuild;

internal static class HandbookNameRepair {
    /// Text in the description (source_metadata.description) of each Handbook chapter dataset.
    public const string DatasetDescription = "Handbook of the Mammals of the World";

    /// The name repaired; the name itself when nothing needs repair. language: the Catalogue of
    /// Life's language code ("fra", "deu").
    public static string Repair(string name, string? language) {
        var repaired = TrailingHeading.Replace(name, string.Empty);
        return language?.Trim().ToLowerInvariant() switch {
            "fra" or "fr" => French(repaired),
            "deu" or "de" => German(repaired),
            _ => repaired,
        };
    }

    private static string French(string name) {
        name = FrenchA.Replace(name, "à");
        name = FrenchE.Replace(name, "ère");
        name = name.Replace("téte", "tête", StringComparison.Ordinal);
        return FrenchApostrophe.Replace(name, "d'");
    }

    private static string German(string name) => GermanSharpS.Replace(name, "ß");

    private static readonly Regex TrailingHeading = new(@"\s+[I|l]\s+(?:English|French|German|Spanish)$", RegexOptions.CultureInvariant);
    // "a" alone between spaces or hyphens ("Baleine-a-bec").
    private static readonly Regex FrenchA = new(@"(?<=[ -])a(?=[ -])", RegexOptions.CultureInvariant);
    private static readonly Regex FrenchE = new(@"ére\b", RegexOptions.CultureInvariant);
    private static readonly Regex FrenchApostrophe = new(@"\bd(?=[A-ZÀ-Ý])", RegexOptions.CultureInvariant);
    // After a vowel, before a lower-case letter, a hyphen, a space or the end.
    private static readonly Regex GermanSharpS = new(@"(?<=[aeiouäöü])l?RR?(?=[a-zäöüß]|[-\s]|$)", RegexOptions.CultureInvariant);
}
