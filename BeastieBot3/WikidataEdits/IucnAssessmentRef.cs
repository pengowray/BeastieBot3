using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

// Reads the IUCN taxon and assessment ids out of an assessment DOI or a Red List URL.
//
//   10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en   (taxon 15951, assessment 107265605)
//   https://www.iucnredlist.org/species/15951/107265605
//
// Wikidata stores DOIs upper-cased (".EN"), items also carry them as doi.org URLs in P953/P856,
// and older releases use a bare year as the release token ("IUCN.UK.2008.RLTS"), so the release
// segment is taken as whatever sits between "IUCN.UK." and ".RLTS". The old site's
// /details/{taxon}/0 form names no assessment, so it yields a taxon id only.

namespace BeastieBot3.WikidataEdits;

internal enum IucnAssessmentRefSource {
    Doi,
    Url,
}

/// Ids parsed from one DOI or URL. Release is the DOI's Red List version token ("2016-3", "2008")
/// and Language its suffix upper-cased ("EN"); both are null for a URL.
internal sealed record IucnAssessmentRef(
    long TaxonId,
    long? AssessmentId,
    string? Release,
    string? Language,
    IucnAssessmentRefSource Source);

internal static class IucnAssessmentRefParser {
    public const string DoiPrefix = "10.2305/IUCN.UK.";

    private static readonly Regex DoiPattern = new(
        @"^10\.2305/IUCN\.UK\.(?<release>[^./\s]+)\.RLTS\.T(?<taxon>\d+)A(?<assessment>\d+)(?:\.(?<lang>[A-Za-z]{2,3}))?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex RedListUrlPattern = new(
        @"^https?://(?:www\.)?iucnredlist\.org/(?:species|details)/(?<taxon>\d+)(?:/(?<assessment>\d+))?/?(?:[?#].*)?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly string[] DoiUrlPrefixes = {
        "https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "doi:",
    };

    /// A DOI in Wikidata's form: prefix and resolver stripped, percent-escapes decoded, upper-cased.
    /// Null when what is left doesn't start with "10.".
    public static string? NormalizeDoi(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var text = value.Trim();
        foreach (var prefix in DoiUrlPrefixes) {
            if (text.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) {
                text = text[prefix.Length..];
                break;
            }
        }

        if (text.Contains('%')) {
            try {
                text = Uri.UnescapeDataString(text);
            }
            catch (UriFormatException) {
                return null;
            }
        }

        text = text.Trim().TrimEnd('.');
        return text.StartsWith("10.", StringComparison.Ordinal) ? text.ToUpperInvariant() : null;
    }

    /// True for any DOI under IUCN's Red List assessment prefix, whether or not the ids parse.
    public static bool IsIucnAssessmentDoi(string? value) =>
        NormalizeDoi(value)?.StartsWith(DoiPrefix, StringComparison.Ordinal) == true;

    public static IucnAssessmentRef? TryParseDoi(string? value) {
        var doi = NormalizeDoi(value);
        if (doi is null) {
            return null;
        }

        var match = DoiPattern.Match(doi);
        if (!match.Success
            || !TryReadId(match.Groups["taxon"].Value, out var taxonId)
            || !TryReadId(match.Groups["assessment"].Value, out var assessmentId)) {
            return null;
        }

        var language = match.Groups["lang"].Success ? match.Groups["lang"].Value.ToUpperInvariant() : null;
        return new IucnAssessmentRef(taxonId, assessmentId, match.Groups["release"].Value, language, IucnAssessmentRefSource.Doi);
    }

    /// A Red List species URL, or a doi.org URL wrapping an assessment DOI.
    public static IucnAssessmentRef? TryParseUrl(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var text = value.Trim();
        var match = RedListUrlPattern.Match(text);
        if (match.Success) {
            if (!TryReadId(match.Groups["taxon"].Value, out var taxonId)) {
                return null;
            }

            long? assessmentId = match.Groups["assessment"].Success
                && TryReadId(match.Groups["assessment"].Value, out var parsed)
                ? parsed
                : null;
            return new IucnAssessmentRef(taxonId, assessmentId, null, null, IucnAssessmentRefSource.Url);
        }

        foreach (var prefix in DoiUrlPrefixes) {
            if (text.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) && prefix != "doi:") {
                return TryParseDoi(text);
            }
        }

        return null;
    }

    /// The ids for an item: the first DOI that parses, else the first URL that names an assessment,
    /// else the first URL that names a taxon.
    public static IucnAssessmentRef? FromItem(IEnumerable<string> dois, IEnumerable<string> urls) {
        foreach (var doi in dois) {
            if (TryParseDoi(doi) is { } parsed) {
                return parsed;
            }
        }

        IucnAssessmentRef? taxonOnly = null;
        foreach (var url in urls) {
            if (TryParseUrl(url) is not { } parsed) {
                continue;
            }

            if (parsed.AssessmentId is not null) {
                return parsed;
            }

            taxonOnly ??= parsed;
        }

        return taxonOnly;
    }

    // An id of 0 is the old site's "no assessment" placeholder, not an id.
    private static bool TryReadId(string text, out long value) =>
        long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value) && value > 0;
}
