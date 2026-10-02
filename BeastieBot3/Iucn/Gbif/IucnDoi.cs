using System.Globalization;
using System.Text.RegularExpressions;

// DOIs and assessment page URLs of IUCN Red List assessments, as they appear in citation text:
// "... The IUCN Red List of Threatened Species 2013: https://doi.org/10.2305/IUCN.UK.2013-1.RLTS.T133722A512509.en"
// and "https://www.iucnredlist.org/species/133722/512509". The DOI names the taxon id and the
// assessment id after "RLTS.T"; for an errata version published 2015 to 2018 the assessment id is
// the predecessor's, so the two ids can differ from the assessment page's.

namespace BeastieBot3.Iucn.Gbif;

/// <summary>The parts of an IUCN assessment DOI.</summary>
/// <param name="Release">"2013-1", or a bare year ("2012") for some releases. Null when the DOI does not have the usual "IUCN.UK.&lt;release&gt;.RLTS" form.</param>
/// <param name="Language">The two-letter language suffix ("en", "es"), or null.</param>
internal sealed record IucnDoiParts(long TaxonId, long AssessmentId, string? Release, string? Language);

internal static class IucnDoi {
    // A DOI with or without a resolver prefix (https://doi.org/, https://dx.doi.org/, doi:). DOIs
    // end at whitespace; a full stop or bracket after the DOI belongs to the sentence.
    private static readonly Regex DoiPattern = new(
        @"(?:https?://(?:dx\.)?doi\.org/|\bdoi:\s*)?(?<doi>10\.\d{4,9}/[^\s""<>]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex IdsPattern = new(
        @"RLTS\.T(?<taxon>\d+)A(?<assessment>\d+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex ReleasePattern = new(
        @"IUCN\.UK\.(?<release>\d{4}(?:-\d)?)\.RLTS\.",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex LanguagePattern = new(
        @"\.(?<language>[a-z]{2})$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex AssessmentUrlPattern = new(
        @"iucnredlist\.org/species/(?<taxon>\d+)/(?<assessment>\d+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// The first DOI in <paramref name="text"/>, without its resolver prefix:
    /// "10.2305/IUCN.UK.2013-1.RLTS.T133722A512509.en". Null when the text has none.
    /// </summary>
    public static string? Extract(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var match = DoiPattern.Match(text);
        if (!match.Success) {
            return null;
        }
        var doi = match.Groups["doi"].Value.TrimEnd('.', ',', ';', ':', ')', ']');
        return doi.Length > "10.1234/".Length ? doi : null;
    }

    /// <summary>
    /// The taxon and assessment ids named in an IUCN DOI ("...RLTS.T133722A512509.en"), with the
    /// release and language when the DOI has the usual form. Null when the DOI names no ids.
    /// </summary>
    public static IucnDoiParts? Parse(string? doi) {
        if (string.IsNullOrWhiteSpace(doi)) {
            return null;
        }
        var ids = IdsPattern.Match(doi);
        if (!ids.Success
            || !long.TryParse(ids.Groups["taxon"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
            || !long.TryParse(ids.Groups["assessment"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var assessmentId)) {
            return null;
        }
        var release = ReleasePattern.Match(doi);
        var language = LanguagePattern.Match(doi);
        return new IucnDoiParts(
            taxonId,
            assessmentId,
            release.Success ? release.Groups["release"].Value : null,
            language.Success ? language.Groups["language"].Value.ToLowerInvariant() : null);
    }

    /// <summary>
    /// The taxon and assessment ids in an assessment page URL
    /// ("https://www.iucnredlist.org/species/133722/512509"). Null for any other text.
    /// </summary>
    public static (long TaxonId, long AssessmentId)? ParseAssessmentUrl(string? url) {
        if (string.IsNullOrWhiteSpace(url)) {
            return null;
        }
        var match = AssessmentUrlPattern.Match(url);
        if (!match.Success
            || !long.TryParse(match.Groups["taxon"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
            || !long.TryParse(match.Groups["assessment"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var assessmentId)) {
            return null;
        }
        return (taxonId, assessmentId);
    }
}
