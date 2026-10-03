using System.Globalization;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using GbifDoi = BeastieBot3.Iucn.Gbif.IucnDoi;

// Decides one assessment's DOI from Crossref's list, or by checking candidate DOIs at doi.org.
//
// Every DOI accepted here passes IucnDoiSelector.Check: its taxon id is the assessment's, and its
// assessment id is the assessment's own or, for an errata version, one of the assessments it
// replaced. A DOI with a predecessor's id is accepted only when it points to this assessment's
// page: IUCN gave errata versions published 2015 to 2018 the DOI of the assessment they correct
// and pointed that DOI at the errata page (giant panda: 10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en
// points to /species/712/121745669). A predecessor's DOI that still points to the predecessor's
// own page is that assessment's DOI, not this one's.
//
// From Crossref, a DOI naming the assessment's own id wins; among several, the one pointing to its
// page, then the one in its language, then the most likely release. At doi.org the candidates are
// checked in order (IucnDoiCandidates) and the first acceptable DOI that exists wins. An answer
// doi.org gives that is neither "exists" nor "not found" stops the assessment without a result, so
// a failed lookup is never saved as "no DOI".

namespace BeastieBot3.Iucn.Doi;

/// One assessment that has no DOI from IUCN's citation text, GBIF or Wikidata.
internal sealed record DoiTarget {
    public required long AssessmentId { get; init; }
    public required long TaxonId { get; init; }
    public int? YearPublished { get; init; }
    public int? ErrataYear { get; init; }
    public int? AmendsYear { get; init; }
    public IReadOnlyList<long> PredecessorIds { get; init; } = Array.Empty<long>();
    /// Two-letter DOI suffix: en, es, pt or fr.
    public string Language { get; init; } = "en";
    /// The release the local CSV exports show the assessment was new in ("2026-1"), if any.
    public string? NewInRelease { get; init; }
    /// "global", "regional", "no scope" or "history".
    public required string Scope { get; init; }
    /// "species", "subspecies", "variety", "subpopulation" or "unknown".
    public string Kind { get; init; } = "unknown";
    /// Whether the cached API payload was read (it gives the errata and amended years).
    public bool HasPayload { get; init; }

    public DoiCandidateRequest CandidateRequest() => new() {
        TaxonId = TaxonId,
        AssessmentId = AssessmentId,
        YearPublished = YearPublished,
        ErrataYear = ErrataYear,
        AmendsYear = AmendsYear,
        PredecessorIds = PredecessorIds,
        Language = Language,
        NewInRelease = NewInRelease,
    };
}

/// A DOI chosen from Crossref's list (or null), with a note when the list had a DOI for this
/// assessment's page that could not be used.
internal sealed record CrossrefChoice(string? Doi, string? Note);

/// The outcome of checking candidates at doi.org. Complete is false when a lookup gave an answer
/// that is neither "exists" nor "not found"; such a result must not be saved.
internal sealed record DoiProbeResult(string? Doi, int Tried, bool Complete, IReadOnlyList<DoiLookupLogRow> Lookups, string? Note);

internal static class DoiLookupVerdicts {
    public const string Accepted = "accepted";
    public const string NotFound = "not found";
    public const string Unexpected = "unexpected answer";
    public const string PointsElsewhere = "exists, points to another assessment's page";
    public const string FailsIdCheck = "exists, ids do not fit";
}

internal static class IucnDoiResolution {
    /// Whether the DOI fits the assessment by IucnDoiSelector's id rules.
    public static bool FitsIds(DoiTarget target, string doi) =>
        IucnDoiSelector.IsAccepted(IucnDoiSelector.Check(PartsFor(target), doi, target.PredecessorIds));

    /// The DOI to use from Crossref's works for this assessment (those whose DOI names it or whose
    /// page URL is its page). Pure.
    public static CrossrefChoice ChooseFromCrossref(DoiTarget target, IReadOnlyList<CrossrefIucnWork> works) {
        var own = works
            .Where(w => w.AssessmentId == target.AssessmentId && w.TaxonId == target.TaxonId && FitsIds(target, w.Doi))
            .OrderBy(w => w.UrlAssessmentId == target.AssessmentId ? 0 : 1)
            .ThenBy(w => string.Equals(w.Language, target.Language, StringComparison.OrdinalIgnoreCase) ? 0 : 1)
            .ThenBy(w => ReleaseRank(target, w.Release))
            .ThenBy(w => w.Doi, StringComparer.Ordinal)
            .ToList();
        if (own.Count > 0) {
            return new CrossrefChoice(own[0].Doi, own.Count > 1 ? $"Crossref lists {own.Count} DOIs for this assessment: {string.Join(", ", own.Select(w => w.Doi))}" : null);
        }

        var redirected = works
            .Where(w => w.UrlAssessmentId == target.AssessmentId && w.AssessmentId != target.AssessmentId)
            .OrderBy(w => w.Doi, StringComparer.Ordinal)
            .ToList();
        var accepted = redirected.Where(w => FitsIds(target, w.Doi)).ToList();
        if (accepted.Count > 0) {
            return new CrossrefChoice(accepted[0].Doi, accepted.Count > 1 ? $"Crossref lists {accepted.Count} DOIs pointing to this page: {string.Join(", ", accepted.Select(w => w.Doi))}" : null);
        }
        // An errata version whose replaced assessment IucnTaxaHeaders.PredecessorIds does not find
        // (published in another year, or missing from the taxon record): Crossref's link from the DOI
        // to this page shows which assessment it replaced. 5 of the 2026-1 latest global assessments
        // (Pinus pinea, errata 2018 of 2013: 10.2305/IUCN.UK.2013-1.RLTS.T42391A2977175.en).
        if (target.ErrataYear is not null) {
            var linked = redirected
                .Where(w => w.TaxonId == target.TaxonId
                    && IucnDoiSelector.IsAccepted(IucnDoiSelector.Check(PartsFor(target), w.Doi, target.PredecessorIds.Append(w.AssessmentId).ToList())))
                .ToList();
            if (linked.Count > 0) {
                return new CrossrefChoice(linked[0].Doi,
                    $"Crossref links {linked[0].Doi} to this errata version's page; it names assessment {linked[0].AssessmentId.ToString(CultureInfo.InvariantCulture)}, which this errata version replaced.");
            }
        }
        if (redirected.Count > 0) {
            return new CrossrefChoice(null,
                $"Crossref lists {string.Join(", ", redirected.Select(w => w.Doi))} pointing to this assessment's page, but its ids do not fit this assessment (not an errata version, or not one it replaced)");
        }
        return new CrossrefChoice(null, null);
    }

    /// Checks candidates at doi.org in order and stops at the first acceptable DOI that exists.
    public static async Task<DoiProbeResult> ProbeAsync(DoiTarget target, IReadOnlyList<DoiCandidate> candidates,
        IDoiHandleLookup lookup, Func<DateTime> utcNow, CancellationToken cancellationToken) {
        var log = new List<DoiLookupLogRow>();
        var tried = 0;
        string? note = null;
        foreach (var candidate in candidates) {
            cancellationToken.ThrowIfCancellationRequested();
            var answer = await lookup.LookupAsync(candidate.Doi, cancellationToken).ConfigureAwait(false);
            tried++;
            var verdict = Judge(target, candidate, answer);
            log.Add(new DoiLookupLogRow(target.AssessmentId, candidate.Doi, utcNow(), answer.HttpStatus, answer.ResponseCode, answer.Url, verdict));
            switch (verdict) {
                case DoiLookupVerdicts.Accepted:
                    return new DoiProbeResult(IucnDoiSelector.Normalise(candidate.Doi) ?? candidate.Doi, tried, true, log, note);
                case DoiLookupVerdicts.Unexpected:
                    return new DoiProbeResult(null, tried, false, log,
                        $"doi.org gave an unexpected answer for {candidate.Doi}: HTTP {answer.HttpStatus.ToString(CultureInfo.InvariantCulture)}, responseCode {answer.ResponseCode?.ToString(CultureInfo.InvariantCulture) ?? "none"}");
                case DoiLookupVerdicts.PointsElsewhere:
                    note ??= $"{candidate.Doi} exists but points to {answer.Url}";
                    break;
            }
        }
        return new DoiProbeResult(null, tried, true, log, note);
    }

    /// What one doi.org answer means for this assessment. Pure.
    public static string Judge(DoiTarget target, DoiCandidate candidate, DoiHandleResult answer) {
        switch (answer.Status) {
            case DoiHandleStatus.NotFound:
                return DoiLookupVerdicts.NotFound;
            case DoiHandleStatus.Unexpected:
                return DoiLookupVerdicts.Unexpected;
        }
        if (!FitsIds(target, candidate.Doi)) {
            return DoiLookupVerdicts.FailsIdCheck;
        }
        if (candidate.Kind == DoiCandidateKind.Predecessor) {
            var page = GbifDoi.ParseAssessmentUrl(answer.Url);
            if (page?.AssessmentId != target.AssessmentId) {
                return DoiLookupVerdicts.PointsElsewhere;
            }
        }
        return DoiLookupVerdicts.Accepted;
    }

    private static int ReleaseRank(DoiTarget target, string? release) {
        if (release is null || target.YearPublished is not { } year) {
            return int.MaxValue;
        }
        var order = IucnDoiCandidates.ReleasesFor(year);
        for (var i = 0; i < order.Count; i++) {
            if (string.Equals(order[i], release, StringComparison.OrdinalIgnoreCase)) {
                return i;
            }
        }
        return int.MaxValue - 1;
    }

    // IucnDoiSelector.Check reads only TaxonId, AssessmentId and ErrataYear.
    private static IucnCitationParts PartsFor(DoiTarget target) => new() {
        TaxonId = target.TaxonId,
        AssessmentId = target.AssessmentId,
        Year = target.YearPublished ?? 0,
        ScientificName = string.Empty,
        ErrataYear = target.ErrataYear,
    };
}
