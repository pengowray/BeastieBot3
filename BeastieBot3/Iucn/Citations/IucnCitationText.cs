using System.Text.RegularExpressions;

// Reads the text of IUCN's citation of an assessment, as the API's `citation` field gives it:
//
//   <authors> <year>. <title>. The IUCN Red List of Threatened Species <year>: e.T<taxon>A<assessment>.
//   [https://dx.doi.org/<doi>.] Accessed on <date>.
//
// Used by the Wikidata dry run (IucnAssessmentCitationParser) and the public site's citation parser
// (IucnCitationPartsParser).

namespace BeastieBot3.Iucn.Citations;

internal static class IucnCitationText {
    private static readonly Regex AccessedOnPattern = new(
        @"\s*Accessed on\b[^.]*\.?\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex DoiPattern = new(
        @"doi\.org/(10\.\d{4,9}/\S+)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// Removes IUCN's trailing "Accessed on 18 August 2026." (the date the payload was fetched).
    public static string StripAccessedOn(string citation) =>
        AccessedOnPattern.Replace(citation ?? string.Empty, string.Empty).Trim();

    /// The DOI a citation links to ("10.2305/IUCN.UK.2025-2.RLTS.T22732931A250382422.en"), or null.
    /// The DOI's own last segment is ".en"/".es"; a dot after that is the sentence's.
    public static string? ExtractDoi(string? citation) {
        if (string.IsNullOrWhiteSpace(citation)) return null;
        var match = DoiPattern.Match(citation);
        if (!match.Success) return null;
        var doi = match.Groups[1].Value.TrimEnd('.', ',', ';', ')');
        return doi.Length > "10.1/".Length ? doi : null;
    }
}
