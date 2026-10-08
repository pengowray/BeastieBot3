using System.Globalization;
using System.Text.RegularExpressions;

// The remark (RQ_STATUT) of a French red list row in PatriNat's BDC Statuts. In the national (LRN)
// and regional (LRR) red lists it is short and made of codes, in this order, each part optional:
//
//   [the IUCN criteria, or the category and criteria before a regional adjustment, or the letter of
//    the reason for NA] [" - " the population or presence the row assesses]
//
//   "B2ab(iii)"                   criteria
//   "pr. D2"                      an NT taxon that came close to meeting D2 ("proche")
//   "VU D1 (-1) - Nicheur"        met VU D1, moved down one category for the region; breeding birds
//   "NT (pr. D1) (-1)"            the same form with the criteria in brackets
//   "b - Visiteur"                NA, reason b; birds on passage (the 2011 bird list's "de passage")
//   "Hivernant"                   wintering birds, no criteria
//
// Parse reads these parts where the remark has one of these forms and leaves the others NULL; the
// remark itself is stored as given. A few regional remarks are sentences ("Espèce erratique non
// autochtone dans la région"); those are not stored, because the status lists store holds no
// narrative text. The remarks of the other status types (protection lists) are notes and comments
// and are never stored.

namespace BeastieBot3.StatusLists;

/// The parts of a red list remark. Text is the remark to store: as given, or null when it is empty
/// or when the part before the population is a sentence (IsSentence). PopulationFr is the population or presence as the BDC writes it
/// (Nicheur, Hivernant, Visiteur régulier ...), Population the same as breeding, wintering or
/// visiting (FranceBdc sets Population from the document's title for a bird row whose remark has
/// none).
internal sealed record FranceRemark(
    string? Text,
    string? Criteria,
    string? AdjustedFrom,
    int? Adjustment,
    string? NaReason,
    string? PopulationFr,
    string? Population,
    bool IsSentence) {
    public static readonly FranceRemark None = new(null, null, null, null, null, null, null, false);
}

internal static partial class FranceRedListRemark {
    public const string Breeding = "breeding";
    public const string Wintering = "wintering";
    public const string Visiting = "visiting";

    /// Every population or presence term found in the remarks of BDC version 18, with its key.
    /// "Visiteur" alone is the 2011 bird list's "de passage" (birds on passage). "Inconnu"
    /// (presence unknown, 9 rows of the French Guiana list) has no key: the row is about the
    /// whole taxon.
    private static readonly Dictionary<string, string?> Populations = new(StringComparer.OrdinalIgnoreCase) {
        ["Nicheur"] = Breeding,
        ["Nicheur certain"] = Breeding,
        ["Nicheur probable"] = Breeding,
        ["Reproducteur certain"] = Breeding,
        ["Reproducteur probable"] = Breeding,
        ["Hivernant"] = Wintering,
        ["Visiteur"] = Visiting,
        ["Visiteur régulier"] = Visiting,
        ["Visiteur occasionnel"] = Visiting,
        ["Visiteur et possiblement nicheur"] = Visiting,
        ["Visiteur régulier et reproducteur probable"] = Visiting,
        ["Visiteur régulier et nicheur probable"] = Visiting,
        ["Inconnu"] = null,
    };

    /// The key of a population term (breeding, wintering, visiting); null for Inconnu, for no term
    /// and for a term not in the list.
    public static string? PopulationKey(string? populationFr) =>
        populationFr is not null && Populations.TryGetValue(populationFr, out var key) ? key : null;

    /// Reads a red list remark. <paramref name="code"/> is the row's category (CODE_STATUT): the
    /// letter of a reason is read only for NA.
    public static FranceRemark Parse(string code, string? remark) {
        if (string.IsNullOrWhiteSpace(remark)) {
            return FranceRemark.None;
        }
        // Two LRN remarks start with a stray quote: "\"B1a,B1biii,...".
        var text = remark.Trim().Trim('"').Trim();

        string? populationFr = null;
        var front = text;
        if (Populations.ContainsKey(text)) {
            populationFr = Canonical(text);
            front = "";
        } else {
            var dash = text.LastIndexOf(" - ", StringComparison.Ordinal);
            if (dash >= 0 && Populations.ContainsKey(text[(dash + 3)..].Trim())) {
                populationFr = Canonical(text[(dash + 3)..].Trim());
                front = text[..dash].Trim();
            }
        }
        var population = PopulationKey(populationFr);

        string? criteria = null, adjustedFrom = null, naReason = null;
        int? adjustment = null;
        if (front.Length == 1 && front[0] is >= 'a' and <= 'd' && code == "NA") {
            naReason = front;
        } else if (Adjusted().Match(front) is { Success: true } adjusted) {
            adjustedFrom = adjusted.Groups["category"].Value;
            criteria = (adjusted.Groups["bracketed"].Success ? adjusted.Groups["bracketed"].Value : adjusted.Groups["criteria"].Value).Trim();
            adjustment = int.Parse(adjusted.Groups["steps"].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        } else if (Criteria().IsMatch(front)) {
            criteria = front;
        } else if (IsSentence(front)) {
            return new FranceRemark(null, null, null, null, null, populationFr, population, true);
        }
        return new FranceRemark(remark, criteria, adjustedFrom, adjustment, naReason, populationFr, population, false);
    }

    // The term as the dictionary writes it, whatever its case in the remark.
    private static string Canonical(string term) => Populations.Keys.First(k => k.Equals(term, StringComparison.OrdinalIgnoreCase));

    // Three or more words of three or more letters, none of them a criteria code: a sentence such as
    // "Espèce non connue dans la région au moment de l'évaluation".
    private static bool IsSentence(string text) => Words().Matches(text).Count >= 3;

    /// IUCN criteria as the lists write them: "D2", "B2ab(iii,v) C2a(i)", "B1a,B1biii,B1bv", "A2ac+4ac",
    /// "B(1+2)ab(iii)", with "pr." in front for an NT taxon.
    [GeneratedRegex(@"^(?:pr\.\s*)?[A-E][A-Ea-e0-9iv(),+;.\s]*$")]
    private static partial Regex Criteria();

    /// The category and criteria before a regional adjustment, and the number of categories it moved:
    /// "VU D1 (-1)", "VU (D1) (-1)", "NT (pr. D1) (-1)", "EN (B2ab(iii) D1) (+1)".
    [GeneratedRegex(@"^(?<category>EX|EW|RE|CR|EN|VU|NT|LC|DD)\s*(?:\((?<bracketed>.+)\)|(?<criteria>[^()].*?))\s*\((?<steps>[+-]\d)\)$")]
    private static partial Regex Adjusted();

    [GeneratedRegex(@"\p{L}{3,}")]
    private static partial Regex Words();
}
