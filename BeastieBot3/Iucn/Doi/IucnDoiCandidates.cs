using System.Globalization;

// The DOIs an assessment may have, most likely first, for checking one by one at doi.org.
//
// An IUCN Red List DOI is 10.2305/IUCN.UK.<release>.RLTS.T<taxon id>A<assessment id>.<language>.
// The release is the Red List version the assessment was first published in, which is not in the
// API payload or the CSV export, and assessment ids overlap between the releases of one year. So
// the candidates are the releases of the assessment's year published, ordered by how many known
// DOIs of that year use each one (counted over 64,484 assessments whose DOI is known from IUCN's
// citations and Wikidata, October 2026):
//
//   1996 to 2008  the bare year ("2004")
//   2009          2009-2 1,236; 2009 226; 2009-1 1
//   2010          2010-3 3,063; 2010-4 1,659; 2010-2 610; 2010-1 30; 2010 1
//   2011          2011-1 693; 2011-2 647
//   2012          2012-1 7,722; 2012 398; 2012-2 5
//   2013          2013-1 2,836; 2013-2 1,192; 2013 1
//   2014          2014-1 1,154; 2014-2 885; 2014-3 885
//   2015          2015-4 1,046; 2015-2 271; 2015-1 162; 2015 114; 2015-3 5
//   2016          2016-3 11,616; 2016-1 820; 2016-2 393
//   2017          2017-3 1,754; 2017-1 1,662; 2017-2 486
//   2018          2018-2 1,382; 2018-1 349; 2018 22
//   2019          2019-3 572; 2019-2 362; 2019-1 198
//   2020          2020-3 853; 2020-2 419; 2020-1 416
//   2021          2021-3 292; 2021-1 285; 2021-2 163
//   2022          2022-1 294; 2022-2 165
//   2023          2023-1 330
//   2024          2024-2 1,087; 2024-1 381
//   2025          2025-2 752; 2025-1 351
//   2026          2026-1 753
//
// For 2026 and later years the releases not yet seen are added after the known ones (up to -3),
// since a new release can be published at any time.
//
// Which assessment id, and which years:
//   - Own id, releases of the year published. An amended version then tries the releases of the
//     year it amends (Dugong 2019, "amended version of 2015 assessment": own id, 2015-4).
//   - An errata version published 2015 to 2018 kept the DOI of the assessment it corrects, with
//     that assessment's release (the DOI now points to the errata version's page), so its
//     predecessors are tried first. Errata versions from 2019 have their own id with the original
//     release; their predecessors are tried last. An errata version also tries the releases of
//     the year it was published in.
//   - A release the local CSV exports pin goes first: an assessment id in this release's CSV but
//     not the previous release's was new in this release.
//
// The language suffix is the CSV export's language for the assessment: English en, Spanish es,
// Portuguese pt, French fr. It matched the DOI's suffix for all 24,538 assessments checked.

namespace BeastieBot3.Iucn.Doi;

internal enum DoiCandidateKind {
    /// The DOI names this assessment.
    Own,
    /// The DOI names an assessment this errata version replaced.
    Predecessor,
}

internal sealed record DoiCandidate(string Doi, string Release, long AssessmentId, DoiCandidateKind Kind);

/// What candidate generation needs to know about one assessment.
internal sealed record DoiCandidateRequest {
    public required long TaxonId { get; init; }
    public required long AssessmentId { get; init; }
    public int? YearPublished { get; init; }
    /// From "(errata version published in YYYY)".
    public int? ErrataYear { get; init; }
    /// From "(amended version of YYYY assessment)".
    public int? AmendsYear { get; init; }
    /// The assessments this one may have replaced (IucnTaxaHeaders.PredecessorIds). Only used for an errata version.
    public IReadOnlyList<long> PredecessorIds { get; init; } = Array.Empty<long>();
    /// Two-letter suffix: en, es, pt or fr.
    public string Language { get; init; } = "en";
    /// The release the local CSV exports show the assessment was new in ("2026-1"), if any.
    public string? NewInRelease { get; init; }
}

internal static class IucnDoiCandidates {
    /// The newest year the release table covers. Releases of this year and later years that the
    /// table does not list are tried after the listed ones.
    public const int NewestKnownYear = 2026;

    private static readonly Dictionary<int, string[]> KnownReleases = new() {
        [2009] = ["2009-2", "2009", "2009-1"],
        [2010] = ["2010-3", "2010-4", "2010-2", "2010-1", "2010"],
        [2011] = ["2011-1", "2011-2"],
        [2012] = ["2012-1", "2012", "2012-2"],
        [2013] = ["2013-1", "2013-2", "2013"],
        [2014] = ["2014-1", "2014-2", "2014-3"],
        [2015] = ["2015-4", "2015-2", "2015-1", "2015", "2015-3"],
        [2016] = ["2016-3", "2016-1", "2016-2"],
        [2017] = ["2017-3", "2017-1", "2017-2"],
        [2018] = ["2018-2", "2018-1", "2018"],
        [2019] = ["2019-3", "2019-2", "2019-1"],
        [2020] = ["2020-3", "2020-2", "2020-1"],
        [2021] = ["2021-3", "2021-1", "2021-2"],
        [2022] = ["2022-1", "2022-2"],
        [2023] = ["2023-1"],
        [2024] = ["2024-2", "2024-1"],
        [2025] = ["2025-2", "2025-1"],
        [2026] = ["2026-1"],
    };

    /// The release tokens of one year, most likely first.
    public static IReadOnlyList<string> ReleasesFor(int year) {
        var y = year.ToString(CultureInfo.InvariantCulture);
        if (year <= 2008) {
            return [y];
        }
        var releases = KnownReleases.TryGetValue(year, out var known) ? known.ToList() : new List<string>();
        if (year >= NewestKnownYear) {
            for (var n = 1; n <= 3; n++) {
                var token = $"{y}-{n.ToString(CultureInfo.InvariantCulture)}";
                if (!releases.Contains(token)) {
                    releases.Add(token);
                }
            }
        }
        return releases;
    }

    /// The year of a release token: "2016-3" and "2016" are both 2016. Null when it is not one.
    public static int? YearOf(string? release) {
        if (string.IsNullOrEmpty(release) || release.Length < 4
            || !int.TryParse(release.AsSpan(0, 4), NumberStyles.None, CultureInfo.InvariantCulture, out var year)) {
            return null;
        }
        return release.Length == 4 || release[4] == '-' ? year : null;
    }

    /// The DOI suffix for the CSV export's language column: English en, "Spanish; Castilian" es,
    /// Portuguese pt, French fr. Null for an empty or unknown language.
    public static string? LanguageCode(string? csvLanguage) {
        if (string.IsNullOrWhiteSpace(csvLanguage)) {
            return null;
        }
        var text = csvLanguage.Trim();
        if (text.StartsWith("English", StringComparison.OrdinalIgnoreCase)) return "en";
        if (text.StartsWith("Spanish", StringComparison.OrdinalIgnoreCase) || text.StartsWith("Castilian", StringComparison.OrdinalIgnoreCase)) return "es";
        if (text.StartsWith("Portuguese", StringComparison.OrdinalIgnoreCase)) return "pt";
        if (text.StartsWith("French", StringComparison.OrdinalIgnoreCase)) return "fr";
        return null;
    }

    public static string Doi(string release, long taxonId, long assessmentId, string language) =>
        string.Create(CultureInfo.InvariantCulture, $"10.2305/IUCN.UK.{release}.RLTS.T{taxonId}A{assessmentId}.{language}");

    /// Whether an errata version published this year kept its predecessor's DOI (2015 to 2018).
    public static bool ErrataKeepsPredecessorDoi(int? errataYear) => errataYear is >= 2015 and <= 2018;

    /// Every candidate DOI, most likely first, without repeats. Empty when the year published is unknown.
    public static IReadOnlyList<DoiCandidate> For(DoiCandidateRequest request) {
        if (request.YearPublished is not { } year) {
            return Array.Empty<DoiCandidate>();
        }
        var language = string.IsNullOrWhiteSpace(request.Language) ? "en" : request.Language.Trim().ToLowerInvariant();
        var own = request.AssessmentId;
        var predecessors = request.ErrataYear is null
            ? Array.Empty<long>()
            : request.PredecessorIds.Where(id => id != own).Distinct().ToArray();
        var predecessorsFirst = ErrataKeepsPredecessorDoi(request.ErrataYear) && predecessors.Length > 0;

        var candidates = new List<DoiCandidate>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        void Add(string release, long assessmentId, DoiCandidateKind kind) {
            var doi = Doi(release, request.TaxonId, assessmentId, language);
            if (seen.Add(doi)) {
                candidates.Add(new DoiCandidate(doi, release, assessmentId, kind));
            }
        }

        var ownYears = new List<int> { year };
        if (request.AmendsYear is { } amends && !ownYears.Contains(amends)) ownYears.Add(amends);
        if (request.ErrataYear is { } errata && !ownYears.Contains(errata)) ownYears.Add(errata);

        if (request.NewInRelease is { } pinned && YearOf(pinned) is { } pinnedYear && ownYears.Contains(pinnedYear)) {
            Add(pinned, own, DoiCandidateKind.Own);
        }
        if (predecessorsFirst) {
            foreach (var predecessor in predecessors) {
                foreach (var release in ReleasesFor(year)) Add(release, predecessor, DoiCandidateKind.Predecessor);
            }
        }
        foreach (var ownYear in ownYears) {
            foreach (var release in ReleasesFor(ownYear)) Add(release, own, DoiCandidateKind.Own);
        }
        if (!predecessorsFirst) {
            foreach (var predecessor in predecessors) {
                foreach (var release in ReleasesFor(year)) Add(release, predecessor, DoiCandidateKind.Predecessor);
            }
        }
        return candidates;
    }
}
