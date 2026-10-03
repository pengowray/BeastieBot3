using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;

// Picks the DOI for one assessment's {{cite iucn}} from the DOIs other sources offer, and checks
// each against the assessment before trusting it.
//
// An IUCN Red List DOI names a release, a taxon id and an assessment id:
// 10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en. The release ("2016-2", sometimes a bare "2012") is
// not in the API payload and cannot be worked out from the year: assessment ids overlap between the
// releases of one year, and a missing DOI does not mean there is none. So a DOI is never built from
// a year here; it is only taken from a source that states it, and accepted when its ids fit:
//
//   - the taxon id equals the assessment's taxon id, and
//   - the assessment id equals the assessment's own id, or the assessment is an errata version
//     (ErrataYear set) and the id is one of the assessments it replaced. IUCN gave errata versions
//     published 2015 to 2018 the DOI of the assessment they correct, which then redirects to the
//     errata page (giant panda: page e.T712A121745669, DOI ...2016-2.RLTS.T712A45033386.en), and
//     Module:Cite IUCN accepts a DOI with another assessment id only when |errata= is set.
//
// The release part is not checked: an amended version can carry the release of the assessment it
// amends (Dugong 2019, "amended version of 2015 assessment", own id, release 2015-4).
//
// Sources in priority order: IUCN's own citation text, GBIF's copy of the IUCN checklist, Wikidata
// (P356 on the assessment's item), then the DOI `iucn resolve-dois` found by checking candidate DOIs
// at doi.org. The first DOI that passes wins.

namespace BeastieBot3.SiteBuild;

/// Whether a DOI fits the assessment it is offered for.
internal enum DoiVerdict {
    /// The DOI names this assessment.
    Accepted,
    /// The DOI names an assessment this errata version replaced.
    AcceptedPredecessor,
    /// No DOI offered.
    Missing,
    /// Not an IUCN Red List DOI of the form 10.2305/IUCN.UK.<release>.RLTS.T<taxon>A<assessment>.<lang>.
    Malformed,
    /// The DOI names another taxon.
    TaxonMismatch,
    /// The DOI names another assessment of the taxon, and not one this errata version replaced.
    AssessmentMismatch,
    /// The DOI names an earlier assessment of the taxon, but this assessment is not an errata version.
    PredecessorWithoutErrata,
}

/// The parts of an IUCN Red List DOI.
internal readonly record struct IucnDoi(string Release, long TaxonId, long AssessmentId, string Language) {
    /// The canonical spelling: 10.2305/IUCN.UK.{release}.RLTS.T{taxon}A{assessment}.{lang}.
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"10.2305/IUCN.UK.{Release}.RLTS.T{TaxonId}A{AssessmentId}.{Language}");
}

internal readonly record struct DoiChoice(string? Doi, DoiSource Source);

internal static class IucnDoiSelector {
    private static readonly Regex DoiPattern = new(
        @"^10\.2305/IUCN\.UK\.(?<rel>\d{4}(?:-\d)?)\.RLTS\.T(?<t>\d+)A(?<a>\d+)\.(?<lang>[a-z]{2})$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly string[] ResolverPrefixes = {
        "https://dx.doi.org/", "http://dx.doi.org/", "https://doi.org/", "http://doi.org/", "dx.doi.org/", "doi.org/", "doi:",
    };

    /// Reads an IUCN Red List DOI written with or without a resolver prefix ("https://dx.doi.org/",
    /// "doi:") and in any letter case. Null when it is not one.
    public static IucnDoi? TryParse(string? doi) {
        if (string.IsNullOrWhiteSpace(doi)) return null;
        var text = doi.Trim();
        foreach (var prefix in ResolverPrefixes) {
            if (text.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) {
                text = text[prefix.Length..].Trim();
                break;
            }
        }
        text = text.TrimEnd('.', ',', ';', ')');
        var match = DoiPattern.Match(text);
        if (!match.Success) return null;
        if (!long.TryParse(match.Groups["t"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
            || !long.TryParse(match.Groups["a"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var assessmentId)) {
            return null;
        }
        return new IucnDoi(match.Groups["rel"].Value, taxonId, assessmentId, match.Groups["lang"].Value.ToLowerInvariant());
    }

    /// The canonical spelling of an IUCN Red List DOI ("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en"),
    /// or null when the text is not one.
    public static string? Normalise(string? doi) => TryParse(doi)?.ToString();

    /// Checks one DOI against the assessment. Only TaxonId, AssessmentId and ErrataYear of parts are read.
    public static DoiVerdict Check(IucnCitationParts parts, string? doi, IReadOnlyCollection<long>? predecessorAssessmentIds) {
        if (string.IsNullOrWhiteSpace(doi)) return DoiVerdict.Missing;
        if (TryParse(doi) is not { } parsed) return DoiVerdict.Malformed;
        if (parsed.TaxonId != parts.TaxonId) return DoiVerdict.TaxonMismatch;
        if (parsed.AssessmentId == parts.AssessmentId) return DoiVerdict.Accepted;
        if (predecessorAssessmentIds is null || !predecessorAssessmentIds.Contains(parsed.AssessmentId)) {
            return DoiVerdict.AssessmentMismatch;
        }
        return parts.ErrataYear is null ? DoiVerdict.PredecessorWithoutErrata : DoiVerdict.AcceptedPredecessor;
    }

    public static bool IsAccepted(DoiVerdict verdict) =>
        verdict is DoiVerdict.Accepted or DoiVerdict.AcceptedPredecessor;

    /// The first acceptable DOI in priority order (citation, GBIF, Wikidata, found at doi.org), in
    /// canonical spelling, with its source; (null, None) when none passes. parts.Doi is ignored.
    public static DoiChoice Select(
        IucnCitationParts parts,
        string? citationDoi,
        string? gbifDoi,
        IEnumerable<string>? wikidataDois,
        IReadOnlyCollection<long>? predecessorAssessmentIds,
        string? resolvedDoi = null) {
        if (TryAccept(citationDoi) is { } fromCitation) return new DoiChoice(fromCitation, DoiSource.Citation);
        if (TryAccept(gbifDoi) is { } fromGbif) return new DoiChoice(fromGbif, DoiSource.Gbif);
        foreach (var doi in wikidataDois ?? Array.Empty<string>()) {
            if (TryAccept(doi) is { } fromWikidata) return new DoiChoice(fromWikidata, DoiSource.Wikidata);
        }
        if (TryAccept(resolvedDoi) is { } fromDoiOrg) return new DoiChoice(fromDoiOrg, DoiSource.Resolved);
        return new DoiChoice(null, DoiSource.None);

        string? TryAccept(string? doi) =>
            IsAccepted(Check(parts, doi, predecessorAssessmentIds)) ? Normalise(doi) : null;
    }
}
