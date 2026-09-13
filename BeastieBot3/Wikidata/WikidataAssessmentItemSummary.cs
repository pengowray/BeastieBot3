using System;
using System.Collections.Generic;
using System.Linq;

// Counts over the stored assessment items, for `wikidata iucn-assessment-items` and its --status.
// Pure: the IUCN sides are passed in as id sets, so the command decides where they come from.

namespace BeastieBot3.Wikidata;

/// Assessment and taxon ids of one IUCN CSV release database (assessments_html).
internal sealed record IucnReleaseIds(string Label, HashSet<long> AssessmentIds, HashSet<long> TaxonIds);

/// Assessment ids in the IUCN API cache's taxa_assessment_backlog: all of them, and those flagged latest.
internal sealed record IucnApiBacklogIds(HashSet<long> LatestAssessmentIds, HashSet<long> AllAssessmentIds);

internal sealed record AssessmentIdDuplicate(long AssessmentId, IReadOnlyList<string> Qids);

internal sealed record WikidataAssessmentItemSummary {
    public int Total { get; init; }
    public IReadOnlyList<(string Endpoint, int Count)> ByEndpoint { get; init; } = Array.Empty<(string, int)>();
    public IReadOnlyList<(string Route, int Count, int OnlyThisRoute)> ByRoute { get; init; } = Array.Empty<(string, int, int)>();

    public int WithTaxonAndAssessmentId { get; init; }
    public int IdsFromDoi { get; init; }
    public int IdsFromUrl { get; init; }
    public int TaxonIdOnly { get; init; }
    public int IucnDoiNotParsed { get; init; }
    public int NoIucnDoiOrUrl { get; init; }
    public int DoiNotUpperCase { get; init; }
    public string? EarliestRelease { get; init; }
    public string? LatestRelease { get; init; }

    public IReadOnlyList<(string Qid, int Count)> InstanceOf { get; init; } = Array.Empty<(string, int)>();
    public int NoInstanceOf { get; init; }
    public IReadOnlyList<string> TaxonItems { get; init; } = Array.Empty<string>();
    public IReadOnlyList<(string Qid, int Count)> PublishedIn { get; init; } = Array.Empty<(string, int)>();
    public int NoPublishedIn { get; init; }

    public int MainSubjectPresent { get; init; }
    public int MainSubjectAbsent { get; init; }
    public int WithAuthorItems { get; init; }
    public int WithAuthorStrings { get; init; }
    public int WithNoAuthorStatements { get; init; }

    /// Publication items (not taxon items) with an assessment id, checked against a CSV release.
    public int? LatestInRelease { get; init; }
    public int? OlderAssessmentOfTaxonInRelease { get; init; }
    public int? TaxonNotInRelease { get; init; }

    public int? LatestInApiCache { get; init; }
    public int? SupersededInApiCache { get; init; }
    public int? NotInApiCache { get; init; }

    public IReadOnlyList<AssessmentIdDuplicate> Duplicates { get; init; } = Array.Empty<AssessmentIdDuplicate>();

    public static WikidataAssessmentItemSummary Build(
        IReadOnlyList<WikidataAssessmentItemRow> rows,
        IucnReleaseIds? release,
        IucnApiBacklogIds? api) {
        var publications = rows.Where(r => !r.IsTaxonItem).ToList();
        var withAssessment = publications.Where(r => r.AssessmentId is not null).ToList();

        var routes = rows.SelectMany(r => r.FoundBy).Distinct(StringComparer.Ordinal).OrderBy(r => r, StringComparer.Ordinal);

        var releases = rows.Select(r => r.DoiRelease).Where(r => r is not null).Select(r => r!)
            .OrderBy(r => r, StringComparer.Ordinal).ToList();

        var summary = new WikidataAssessmentItemSummary {
            Total = rows.Count,
            ByEndpoint = rows.GroupBy(r => r.SourceEndpoint ?? "unknown")
                .Select(g => (g.Key, g.Count())).OrderByDescending(x => x.Item2).ToList(),
            ByRoute = routes.Select(route => (
                    route,
                    rows.Count(r => r.FoundBy.Contains(route, StringComparer.Ordinal)),
                    rows.Count(r => r.FoundBy.Count == 1 && r.FoundBy[0] == route)))
                .ToList(),
            WithTaxonAndAssessmentId = rows.Count(r => r.TaxonId is not null && r.AssessmentId is not null),
            IdsFromDoi = rows.Count(r => r.AssessmentId is not null && r.IdSource == "doi"),
            IdsFromUrl = rows.Count(r => r.AssessmentId is not null && r.IdSource == "url"),
            TaxonIdOnly = rows.Count(r => r.TaxonId is not null && r.AssessmentId is null),
            IucnDoiNotParsed = rows.Count(r => r.TaxonId is null && r.AllDois.Any(WikidataEdits.IucnAssessmentRefParser.IsIucnAssessmentDoi)),
            NoIucnDoiOrUrl = rows.Count(r => r.TaxonId is null && !r.AllDois.Any(WikidataEdits.IucnAssessmentRefParser.IsIucnAssessmentDoi)),
            DoiNotUpperCase = rows.Count(r => r.AllDois.Any(d => !string.Equals(d, d.ToUpperInvariant(), StringComparison.Ordinal))),
            EarliestRelease = releases.FirstOrDefault(),
            LatestRelease = releases.LastOrDefault(),
            InstanceOf = Distribution(rows.SelectMany(r => r.InstanceOf)),
            NoInstanceOf = rows.Count(r => r.InstanceOf.Count == 0),
            TaxonItems = rows.Where(r => r.IsTaxonItem).Select(r => r.Qid).ToList(),
            PublishedIn = Distribution(rows.SelectMany(r => r.PublishedIn)),
            NoPublishedIn = rows.Count(r => r.PublishedIn.Count == 0),
            MainSubjectPresent = rows.Count(r => r.MainSubjects.Count > 0),
            MainSubjectAbsent = rows.Count(r => r.MainSubjects.Count == 0),
            WithAuthorItems = rows.Count(r => r.AuthorItemCount > 0),
            WithAuthorStrings = rows.Count(r => r.AuthorStringCount > 0),
            WithNoAuthorStatements = rows.Count(r => r.AuthorItemCount == 0 && r.AuthorStringCount == 0),
            Duplicates = withAssessment
                .GroupBy(r => r.AssessmentId!.Value)
                .Where(g => g.Count() > 1)
                .Select(g => new AssessmentIdDuplicate(g.Key, g.Select(r => r.Qid).ToList()))
                .OrderBy(d => d.AssessmentId)
                .ToList(),
        };

        if (release is not null) {
            summary = summary with {
                LatestInRelease = withAssessment.Count(r => release.AssessmentIds.Contains(r.AssessmentId!.Value)),
                OlderAssessmentOfTaxonInRelease = withAssessment.Count(r =>
                    !release.AssessmentIds.Contains(r.AssessmentId!.Value)
                    && r.TaxonId is { } t && release.TaxonIds.Contains(t)),
                TaxonNotInRelease = withAssessment.Count(r =>
                    !release.AssessmentIds.Contains(r.AssessmentId!.Value)
                    && !(r.TaxonId is { } t && release.TaxonIds.Contains(t))),
            };
        }

        if (api is not null) {
            summary = summary with {
                LatestInApiCache = withAssessment.Count(r => api.LatestAssessmentIds.Contains(r.AssessmentId!.Value)),
                SupersededInApiCache = withAssessment.Count(r =>
                    !api.LatestAssessmentIds.Contains(r.AssessmentId!.Value) && api.AllAssessmentIds.Contains(r.AssessmentId!.Value)),
                NotInApiCache = withAssessment.Count(r => !api.AllAssessmentIds.Contains(r.AssessmentId!.Value)),
            };
        }

        return summary;
    }

    private static IReadOnlyList<(string Qid, int Count)> Distribution(IEnumerable<string> values) =>
        values.GroupBy(v => v, StringComparer.Ordinal)
            .Select(g => (g.Key, g.Count()))
            .OrderByDescending(x => x.Item2)
            .ThenBy(x => x.Key, StringComparer.Ordinal)
            .ToList();
}
