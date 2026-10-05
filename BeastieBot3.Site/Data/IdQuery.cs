using System.Globalization;
using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Data;

/// IUCN ids in search text. Accepted, ignoring case and surrounding punctuation:
///   T22823A14871490, e.T22823A14871490, e.T22823A14871490.en (the end of an IUCN DOI)
///   T22823 (taxon id), A14871490 (assessment id)
///   22823 (either: Number is set, and the search looks for both)
///   a DOI, with or without "doi:" or a doi.org address: 10.2305/IUCN.UK.2016-3.RLTS.T22823A14871490.en
///   a Red List page address: iucnredlist.org/species/22823/14871490, or /species/22823
/// An IUCN DOI is read for the ids in it, so an errata version whose DOI names the assessment it
/// corrects finds that assessment, whose history row links the errata version.
public sealed record IdQuery(long? TaxonId, long? AssessmentId, long? Number) {
    // Longer numbers than this are not ids and would overflow long.
    private const int MaxDigits = 15;

    private static readonly Regex TaxonAndAssessment = new(
        @"^(?:e\.)?t(\d{1,15})a(\d{1,15})(?:\.en)?$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex TaxonOnly = new(@"^t(\d{1,15})$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex AssessmentOnly = new(@"^a(\d{1,15})$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex Digits = new(@"^\d{1,15}$", RegexOptions.CultureInvariant);

    // An IUCN DOI anywhere in the text: "10.2305/IUCN.UK.2016-3.RLTS.T22823A14871490.en".
    private static readonly Regex Doi = new(
        @"10\.2305/[^\s]*?\bT(\d{1,15})A(\d{1,15})\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    // A Red List page: "https://www.iucnredlist.org/species/22823/14871490".
    private static readonly Regex RedListUrl = new(
        @"iucnredlist\.org/species/(\d{1,15})(?:/(\d{1,15}))?", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// The ids in the text, or null when the text is not one of the accepted forms.
    public static IdQuery? Parse(string? text) {
        var query = ParseForms(text);
        return query is { TaxonId: null, AssessmentId: null, Number: null } ? null : query;
    }

    private static IdQuery? ParseForms(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var trimmed = text.Trim().Trim('.', ',', ';', ':', '(', ')', '[', ']', '<', '>', '"', '\'');
        if (trimmed.Length == 0) {
            return null;
        }

        if (Doi.Match(trimmed) is { Success: true } doi) {
            return new IdQuery(ToId(doi.Groups[1].Value), ToId(doi.Groups[2].Value), null);
        }
        if (RedListUrl.Match(trimmed) is { Success: true } url) {
            var assessment = url.Groups[2].Success ? ToId(url.Groups[2].Value) : null;
            return new IdQuery(ToId(url.Groups[1].Value), assessment, null);
        }
        if (TaxonAndAssessment.Match(trimmed) is { Success: true } both) {
            return new IdQuery(ToId(both.Groups[1].Value), ToId(both.Groups[2].Value), null);
        }
        if (TaxonOnly.Match(trimmed) is { Success: true } taxon) {
            return new IdQuery(ToId(taxon.Groups[1].Value), null, null);
        }
        if (AssessmentOnly.Match(trimmed) is { Success: true } assessmentOnly) {
            return new IdQuery(null, ToId(assessmentOnly.Groups[1].Value), null);
        }
        if (Digits.IsMatch(trimmed)) {
            return new IdQuery(null, null, ToId(trimmed));
        }
        return null;
    }

    private static long? ToId(string digits) =>
        digits.Length <= MaxDigits && long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0
            ? n
            : null;
}
