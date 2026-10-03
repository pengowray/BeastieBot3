using System;
using System.Collections.Generic;
using System.Threading;
using Spectre.Console;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikidata;

// One taxon's pass of `wikipedia match-taxa`: builds the taxon's candidate titles, tries them in
// order against the Wikipedia cache, and writes the result (taxon_wiki_matches) and one attempt row
// per candidate (taxon_wiki_match_attempts). WikipediaMatchTaxaCommand reads the IUCN rows, the
// Wikidata cache and the synonym sources and calls this once per taxon.

namespace BeastieBot3.Wikipedia;

internal static class TaxonPageMatcher {
    /// <summary>
    /// Matches one taxon. <paramref name="buildCandidates"/> is called only when the taxon is
    /// checked: a taxon already matched (without <paramref name="reprocessMatched"/>), or checked
    /// before and not waiting on a page (with <paramref name="pendingOnly"/>), is left as it is.
    /// </summary>
    public static TaxonProcessResult ProcessTaxon(
        string taxonId,
        TaxonWikiMatch? existing,
        WikipediaCacheStore cacheStore,
        Func<IReadOnlyList<WikipediaMatchCandidate>> buildCandidates,
        bool reprocessMatched,
        bool pendingOnly,
        CancellationToken cancellationToken) {
        if (!reprocessMatched && existing is not null && string.Equals(existing.MatchStatus, TaxonWikiMatchStatus.Matched, StringComparison.OrdinalIgnoreCase)) {
            return TaxonProcessResult.AlreadyMatched;
        }

        // `wikipedia update` settles taxa after a page download this way: only a taxon that was
        // waiting on a page (or was never checked) can change because a page arrived.
        if (pendingOnly && existing is not null && !string.Equals(existing.MatchStatus, TaxonWikiMatchStatus.Pending, StringComparison.OrdinalIgnoreCase)) {
            return TaxonProcessResult.NotRechecked;
        }

        // Re-evaluating this taxon: drop its prior attempt rows so the attempt log holds
        // only the latest run instead of appending unbounded history on every re-run.
        cacheStore.ClearTaxonAttempts(TaxonSources.Iucn, taxonId);

        // One line per taxon whose result changed. Printing every re-checked taxon put 89,000
        // "Missing" lines in each run's log, burying the few that moved.
        bool Unchanged(string status) => string.Equals(existing?.MatchStatus, status, StringComparison.OrdinalIgnoreCase);

        var candidates = buildCandidates();
        if (candidates.Count == 0) {
            cacheStore.UpsertTaxonMatch(new TaxonWikiMatch(
                TaxonSources.Iucn,
                taxonId,
                TaxonWikiMatchStatus.Missing,
                null,
                null,
                null,
                null,
                null,
                null,
                "No candidate names available",
                DateTime.UtcNow));
            if (!Unchanged(TaxonWikiMatchStatus.Missing)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]No candidates[/] for SIS {Markup.Escape(taxonId)}");
            }
            return TaxonProcessResult.NoCandidates;
        }

        var attemptOrder = cacheStore.GetNextAttemptOrder(TaxonSources.Iucn, taxonId);
        PendingCandidate? pending = null;
        var sawRejection = false;

        foreach (var candidate in candidates) {
            cancellationToken.ThrowIfCancellationRequested();
            var evaluation = EvaluateCandidate(candidate, cacheStore);
            cacheStore.RecordTaxonAttempt(new TaxonWikiMatchAttempt(
                TaxonSources.Iucn,
                taxonId,
                attemptOrder++,
                candidate.DisplayTitle,
                candidate.NormalizedTitle,
                candidate.SourceHint,
                evaluation.AttemptOutcome,
                evaluation.PageSummary.PageRowId,
                evaluation.FinalTitle,
                evaluation.Notes,
                DateTime.UtcNow));

            if (evaluation.Status == CandidateEvaluationStatus.Matched) {
                cacheStore.UpsertTaxonMatch(new TaxonWikiMatch(
                    TaxonSources.Iucn,
                    taxonId,
                    TaxonWikiMatchStatus.Matched,
                    evaluation.PageSummary.PageRowId,
                    candidate.DisplayTitle,
                    candidate.NormalizedTitle,
                    candidate.IsSynonym ? candidate.SynonymValue : null,
                    evaluation.FinalTitle,
                    candidate.MatchMethod,
                    evaluation.Notes,
                    DateTime.UtcNow));
                AnsiConsole.MarkupLineInterpolated($"[green]Matched[/] SIS {Markup.Escape(taxonId)} -> {Markup.Escape(evaluation.FinalTitle ?? candidate.DisplayTitle)} ({Markup.Escape(candidate.MatchMethod)})");
                return TaxonProcessResult.Matched;
            }

            if (evaluation.Status == CandidateEvaluationStatus.Pending && pending is null) {
                pending = new PendingCandidate(candidate, evaluation);
            }

            if (evaluation.Status == CandidateEvaluationStatus.Rejected) {
                sawRejection = true;
            }
        }

        if (pending is not null) {
            var pendingCandidate = pending.Candidate;
            var state = pending.Evaluation;
            cacheStore.UpsertTaxonMatch(new TaxonWikiMatch(
                TaxonSources.Iucn,
                taxonId,
                TaxonWikiMatchStatus.Pending,
                state.PageSummary.PageRowId,
                pendingCandidate.DisplayTitle,
                pendingCandidate.NormalizedTitle,
                pendingCandidate.IsSynonym ? pendingCandidate.SynonymValue : null,
                state.FinalTitle,
                pendingCandidate.MatchMethod,
                state.Notes ?? "Awaiting download",
                DateTime.UtcNow));
            if (!Unchanged(TaxonWikiMatchStatus.Pending)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Pending[/] SIS {Markup.Escape(taxonId)} waiting on {Markup.Escape(pendingCandidate.DisplayTitle)}");
            }
            return TaxonProcessResult.Pending;
        }

        if (sawRejection) {
            // Every candidate that resolved to a real page was a disambiguation/set-index
            // page — distinct from "no article exists at all".
            cacheStore.UpsertTaxonMatch(new TaxonWikiMatch(
                TaxonSources.Iucn,
                taxonId,
                TaxonWikiMatchStatus.Rejected,
                null,
                null,
                null,
                null,
                null,
                null,
                "All candidate pages were disambiguation or set-index pages",
                DateTime.UtcNow));
            if (!Unchanged(TaxonWikiMatchStatus.Rejected)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Rejected[/] SIS {Markup.Escape(taxonId)} (disambiguation/set-index only)");
            }
            return TaxonProcessResult.Rejected;
        }

        cacheStore.UpsertTaxonMatch(new TaxonWikiMatch(
            TaxonSources.Iucn,
            taxonId,
            TaxonWikiMatchStatus.Missing,
            null,
            null,
            null,
            null,
            null,
            null,
            "All candidates missing or invalid",
            DateTime.UtcNow));
        if (!Unchanged(TaxonWikiMatchStatus.Missing)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Missing[/] SIS {Markup.Escape(taxonId)} (no valid articles)");
        }
        return TaxonProcessResult.Missing;
    }

    internal static CandidateEvaluation EvaluateCandidate(WikipediaMatchCandidate candidate, WikipediaCacheStore cacheStore) {
        var now = DateTime.UtcNow;
        var summary = cacheStore.GetPageByNormalizedTitle(candidate.NormalizedTitle);
        WikiPageSummary effectiveSummary;
        if (summary is null) {
            var upsert = cacheStore.UpsertPageCandidate(new WikiPageCandidate(candidate.DisplayTitle, candidate.NormalizedTitle, null, now, now));
            effectiveSummary = new WikiPageSummary(upsert.PageRowId, candidate.DisplayTitle, candidate.NormalizedTitle, WikiPageDownloadStatus.Pending, false, null, false, false, false, now);
        }
        else {
            effectiveSummary = summary;
        }

        return effectiveSummary.DownloadStatus switch {
            WikiPageDownloadStatus.Pending => CandidateEvaluation.Pending(effectiveSummary, "Page not downloaded yet"),
            WikiPageDownloadStatus.Failed => CandidateEvaluation.Failed(effectiveSummary, "Last fetch attempt failed"),
            WikiPageDownloadStatus.Missing => CandidateEvaluation.Missing(effectiveSummary, "Wikipedia reports the page as missing"),
            WikiPageDownloadStatus.Cached => EvaluateCached(effectiveSummary, cacheStore),
            _ => CandidateEvaluation.Failed(effectiveSummary, $"Unknown status {effectiveSummary.DownloadStatus}")
        };
    }

    private static CandidateEvaluation EvaluateCached(WikiPageSummary summary, WikipediaCacheStore cacheStore) {
        // A real cached page that is unusable as a taxon article is "rejected", distinct
        // from a page that genuinely doesn't exist ("missing") — so the taxon's match row
        // can record which it was.
        if (summary.IsDisambiguation) {
            return CandidateEvaluation.Rejected(summary, "Disambiguation page");
        }

        if (summary.IsSetIndex) {
            return CandidateEvaluation.Rejected(summary, "Set index page");
        }

        if (summary.IsRedirect && !string.IsNullOrWhiteSpace(summary.RedirectTarget)) {
            // Re-validate the redirect DESTINATION: a scientific name that redirects to a
            // disambiguation / set-index page is not a real match, and the recorded match must
            // reference the target page, not the redirect stub (whose own flags say nothing about
            // where it points).
            var target = cacheStore.GetPageByNormalizedTitle(WikipediaTitleHelper.Normalize(summary.RedirectTarget));
            if (target is null || target.DownloadStatus == WikiPageDownloadStatus.Pending) {
                return CandidateEvaluation.Pending(summary, "Redirect target not downloaded yet");
            }
            if (target.DownloadStatus == WikiPageDownloadStatus.Missing) {
                return CandidateEvaluation.Missing(summary, "Redirect target is missing");
            }
            if (target.IsDisambiguation) {
                return CandidateEvaluation.Rejected(summary, "Redirects to a disambiguation page");
            }
            if (target.IsSetIndex) {
                return CandidateEvaluation.Rejected(summary, "Redirects to a set-index page");
            }
            // Valid redirect: match the TARGET page (carry its PageRowId), flagged as redirect-resolved.
            return CandidateEvaluation.Redirected(target, target.PageTitle);
        }

        return CandidateEvaluation.Matched(summary, summary.PageTitle);
    }

    /// <summary>
    /// The titles to try for a taxon, in order: the enwiki sitelink of its Wikidata item, then its
    /// names from <see cref="IucnSynonymService.GetCandidates"/> (the IUCN names first, then
    /// synonyms). A title is tried once.
    /// </summary>
    public static IReadOnlyList<WikipediaMatchCandidate> BuildCandidates(
        WikidataIucnMatchCandidate? wikidata,
        IReadOnlyList<TaxonNameCandidate> names) {
        var list = new List<WikipediaMatchCandidate>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void AddCandidate(string? title, string sourceHint, string matchMethod, bool isSynonym, string? synonymValue) {
            if (string.IsNullOrWhiteSpace(title)) {
                return;
            }

            var normalized = WikipediaTitleHelper.Normalize(title);
            if (normalized.Length == 0 || !seen.Add(normalized)) {
                return;
            }

            list.Add(new WikipediaMatchCandidate(title.Trim(), normalized, sourceHint, matchMethod, isSynonym, synonymValue));
        }

        if (wikidata is not null) {
            AddCandidate(wikidata.Title, "wikidata", wikidata.MatchMethod, wikidata.IsSynonym, wikidata.MatchedName);
        }

        foreach (var candidate in names) {
            var method = MethodFor(candidate.Source);
            AddCandidate(candidate.Name, method, method, candidate.IsSynonym, candidate.Name);
        }

        return list;
    }

    private static string MethodFor(TaxonNameSource source) => source switch {
        TaxonNameSource.IucnTaxonomy => "iucn-taxonomy",
        TaxonNameSource.IucnAssessments => "iucn-assessment",
        TaxonNameSource.IucnConstructed => "iucn-constructed",
        TaxonNameSource.IucnInfraRanked => "iucn-infra-rank",
        TaxonNameSource.IucnSynonym => "iucn-synonym",
        TaxonNameSource.ColSynonym => "col-synonym",
        TaxonNameSource.ColAccepted => "col-accepted",
        TaxonNameSource.ColCorrected => "col-corrected",
        TaxonNameSource.ColVariant => "col-variant",
        TaxonNameSource.ColAcceptedViaSynonym => "col-accepted-via-synonym",
        _ => "scientific-name"
    };

    private sealed record PendingCandidate(WikipediaMatchCandidate Candidate, CandidateEvaluation Evaluation);
}

internal enum TaxonProcessResult {
    Matched,
    Pending,
    Missing,
    Rejected,
    NoCandidates,
    Skipped,
    AlreadyMatched,
    NotRechecked
}

internal enum CandidateEvaluationStatus {
    Matched,
    Pending,
    Failed,
    Missing,
    Rejected
}

internal sealed record CandidateEvaluation(
    CandidateEvaluationStatus Status,
    WikiPageSummary PageSummary,
    string AttemptOutcome,
    string? Notes,
    string? FinalTitle
) {
    public static CandidateEvaluation Matched(WikiPageSummary summary, string? finalTitle) => new(CandidateEvaluationStatus.Matched, summary, TaxonWikiAttemptOutcome.Matched, null, finalTitle ?? summary.PageTitle);
    // A match reached via a redirect: status is still Matched, but the per-attempt outcome records the redirect.
    public static CandidateEvaluation Redirected(WikiPageSummary summary, string? finalTitle) => new(CandidateEvaluationStatus.Matched, summary, TaxonWikiAttemptOutcome.Redirected, null, finalTitle ?? summary.PageTitle);
    public static CandidateEvaluation Pending(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Pending, summary, TaxonWikiAttemptOutcome.PendingFetch, notes, summary.PageTitle);
    public static CandidateEvaluation Failed(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Failed, summary, TaxonWikiAttemptOutcome.Failed, notes, summary.PageTitle);
    public static CandidateEvaluation Missing(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Missing, summary, TaxonWikiAttemptOutcome.Missing, notes, summary.PageTitle);
    // A real cached page deliberately rejected (disambiguation/set-index). Falls through like Failed in the
    // candidate loop, but lets the taxon record 'rejected' rather than 'missing'.
    public static CandidateEvaluation Rejected(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Rejected, summary, TaxonWikiAttemptOutcome.Failed, notes, summary.PageTitle);
}

internal sealed record WikipediaMatchCandidate(
    string DisplayTitle,
    string NormalizedTitle,
    string SourceHint,
    string MatchMethod,
    bool IsSynonym,
    string? SynonymValue
);
