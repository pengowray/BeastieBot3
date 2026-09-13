using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikidata;

// Links IUCN taxa to Wikipedia articles using multiple strategies:
// 1. Exact match: IUCN scientific_name = Wikipedia page title
// 2. Wikidata sitelinks: enwiki title from wikidata_sitelinks
// 3. Synonyms: IucnSynonymService provides alternate names
// 4. Taxobox parsing: scientific name extracted from cached wikitext
// Writes matches to taxon_matches table. Run via: wikipedia match-taxa

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia match-taxa", CommandKind.Mutates,
    "Attempt to match IUCN taxa to cached Wikipedia pages using Wikidata sitelinks and synonyms.",
    Reason = "Writes IUCN taxon -> Wikipedia page matches into the cache.",
    Examples = new[] {
        "wikipedia match-taxa",
        "wikipedia match-taxa --limit 500",
        "wikipedia match-taxa --resume-after 12345"
    })]
public sealed class WikipediaMatchTaxaCommand : AsyncCommand<WikipediaMatchTaxaCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--iucn-db <PATH>")]
        [Description("Override path to the IUCN taxonomy SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--iucn-api-cache <PATH>")]
        [Description("Override path to the IUCN API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? IucnApiCache { get; init; }

        [CommandOption("--col-db <PATH>")]
        [Description("Override path to the Catalogue of Life SQLite database (defaults to Datastore:COL_sqlite).")]
        public string? ColDatabase { get; init; }

        [CommandOption("--wikipedia-cache <PATH>")]
        [Description("Override path to the Wikipedia cache SQLite database (defaults to Datastore:enwiki_cache_sqlite).")]
        public string? WikipediaCache { get; init; }

        [CommandOption("--wikidata-cache <PATH>")]
        [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite). Optional but enables enwiki sitelinks.")]
        public string? WikidataCache { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Limit the number of taxa evaluated (0 = all).")]
        public int Limit { get; init; }

        [CommandOption("--resume-after <ID>")]
        [Description("Skip taxa whose SIS ID is less than or equal to the specified value (string compare).")]
        public string? ResumeAfter { get; init; }

        [CommandOption("--include-subpopulations")]
        [Description("Include regional/subpopulation assessments (skipped by default).")]
        public bool IncludeSubpopulations { get; init; }

        [CommandOption("--reprocess-matched")]
        [Description("Re-evaluate taxa already marked as matched.")]
        public bool ReprocessMatched { get; init; }

        [CommandOption("--pending-only")]
        [Description("Check only taxa never checked and taxa waiting on a page download. Skips re-checking taxa already found to have no article, which takes most of a full run.")]
        public bool PendingOnly { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(Settings settings, CancellationToken cancellationToken) {
        if (settings.Limit < 0) {
            AnsiConsole.MarkupLine("[red]--limit must be zero or greater.[/]");
            return -1;
        }

        var paths = settings.CreatePaths();
        string iucnPath;
        string wikipediaCachePath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            wikipediaCachePath = paths.ResolveWikipediaCachePath(settings.WikipediaCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN SQLite database not found:[/] {Markup.Escape(iucnPath)}");
            return -3;
        }

        string? wikidataCachePath = null;
        try {
            wikidataCachePath = paths.ResolveWikidataCachePath(settings.WikidataCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Wikidata cache disabled:[/] {Markup.Escape(ex.Message)}");
            wikidataCachePath = null;
        }

        if (!string.IsNullOrWhiteSpace(wikidataCachePath) && !File.Exists(wikidataCachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Wikidata cache not found at {Markup.Escape(wikidataCachePath)}; sitelink candidates disabled.[/]");
            wikidataCachePath = null;
        }

        var colPath = TryResolveOptional(settings.ColDatabase, paths.GetColSqlitePath(), "Catalogue of Life SQLite database");
        var iucnApiCachePath = TryResolveOptional(settings.IucnApiCache, paths.GetIucnApiCachePath(), "IUCN API cache SQLite database");

        using var iucnConnection = OpenReadOnlyConnection(iucnPath);
        using var wikipediaStore = WikipediaCacheStore.Open(wikipediaCachePath);
        using var wikidataConnection = string.IsNullOrWhiteSpace(wikidataCachePath) ? null : OpenReadOnlyConnection(wikidataCachePath);
        using var synonymService = new IucnSynonymService(iucnApiCachePath, colPath);
        var wikidataLookup = new WikidataIucnMatchLookup(wikidataConnection);
        var repository = new IucnTaxonomyRepository(iucnConnection);
        var stats = new WikipediaMatchStats();
        var resumeToken = string.IsNullOrWhiteSpace(settings.ResumeAfter) ? null : settings.ResumeAfter!.Trim();
        var limit = settings.Limit > 0 ? settings.Limit : int.MaxValue;
        var processed = 0;
        const int progressInterval = 250;
        var nextProgress = progressInterval;
        // ReadRows yields one row per assessment, and a taxon with a regional assessment as well as
        // its global one comes back more than once (197,315 rows for 188,792 taxa in 2026-1). Each
        // taxon is matched once; the repeat rows used to redo the work and count it twice.
        var seenTaxa = new HashSet<long>();

        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN DB:[/] {Markup.Escape(iucnPath)}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikipedia cache:[/] {Markup.Escape(wikipediaCachePath)}");
        if (!string.IsNullOrWhiteSpace(wikidataCachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Wikidata cache:[/] {Markup.Escape(wikidataCachePath!)}");
        }
        if (!string.IsNullOrWhiteSpace(iucnApiCachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]IUCN API cache:[/] {Markup.Escape(iucnApiCachePath!)}");
        }
        if (!string.IsNullOrWhiteSpace(colPath)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]COL DB:[/] {Markup.Escape(colPath!)}");
        }

        foreach (var row in repository.ReadRows(0, cancellationToken)) {
            cancellationToken.ThrowIfCancellationRequested();

            var rowTaxonId = row.TaxonId.ToString(CultureInfo.InvariantCulture);
            if (resumeToken is not null && string.Compare(rowTaxonId, resumeToken, StringComparison.Ordinal) <= 0) {
                continue;
            }

            if (!seenTaxa.Add(row.TaxonId)) {
                continue;
            }

            if (!settings.IncludeSubpopulations && ShouldSkip(row)) {
                stats.Skipped++;
                continue;
            }

            processed++;
            stats.Evaluated++;

            var existing = wikipediaStore.GetTaxonMatch(TaxonSources.Iucn, rowTaxonId);
            var result = ProcessTaxon(row, existing, wikipediaStore, wikidataLookup, synonymService, settings, cancellationToken);
            stats.Record(result, existing?.MatchStatus);

            if (processed >= limit) {
                break;
            }

            if (stats.Evaluated >= nextProgress) {
                AnsiConsole.MarkupLineInterpolated($"[grey]Evaluated {stats.Evaluated:n0} taxa (matched {stats.Matched.Total:n0}, pending {stats.Pending.Total:n0}, missing {stats.Missing.Total:n0}).[/]");
                nextProgress += progressInterval;
            }
        }

        RenderSummary(stats);
        return 0;
    }

    private static TaxonProcessResult ProcessTaxon(
        IucnTaxonomyRow row,
        TaxonWikiMatch? existing,
        WikipediaCacheStore cacheStore,
        WikidataIucnMatchLookup wikidataLookup,
        IucnSynonymService synonymService,
        Settings settings,
        CancellationToken cancellationToken) {
        var taxonId = row.TaxonId.ToString(CultureInfo.InvariantCulture);
        if (!settings.ReprocessMatched && existing is not null && string.Equals(existing.MatchStatus, TaxonWikiMatchStatus.Matched, StringComparison.OrdinalIgnoreCase)) {
            return TaxonProcessResult.AlreadyMatched;
        }

        // `wikipedia update` settles taxa after a page download this way: only a taxon that was
        // waiting on a page (or was never checked) can change because a page arrived.
        if (settings.PendingOnly && existing is not null && !string.Equals(existing.MatchStatus, TaxonWikiMatchStatus.Pending, StringComparison.OrdinalIgnoreCase)) {
            return TaxonProcessResult.NotRechecked;
        }

        // Re-evaluating this taxon: drop its prior attempt rows so the attempt log holds
        // only the latest run instead of appending unbounded history on every re-run.
        cacheStore.ClearTaxonAttempts(TaxonSources.Iucn, taxonId);

        // One line per taxon whose result changed. Printing every re-checked taxon put 89,000
        // "Missing" lines in each run's log, burying the few that moved.
        bool Unchanged(string status) => string.Equals(existing?.MatchStatus, status, StringComparison.OrdinalIgnoreCase);

        var candidates = BuildCandidates(row, wikidataLookup, synonymService, cancellationToken);
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

    private static CandidateEvaluation EvaluateCandidate(WikipediaMatchCandidate candidate, WikipediaCacheStore cacheStore) {
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

    private static IReadOnlyList<WikipediaMatchCandidate> BuildCandidates(
        IucnTaxonomyRow row,
        WikidataIucnMatchLookup wikidataLookup,
        IucnSynonymService synonymService,
        CancellationToken cancellationToken) {
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

        var wikidata = wikidataLookup.GetCandidate(row.TaxonId.ToString(CultureInfo.InvariantCulture));
        if (wikidata is not null) {
            AddCandidate(wikidata.Title, "wikidata", wikidata.MatchMethod, wikidata.IsSynonym, wikidata.MatchedName);
        }

        foreach (var candidate in synonymService.GetCandidates(row, cancellationToken)) {
            var method = candidate.Source switch {
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
            AddCandidate(candidate.Name, method, method, candidate.IsSynonym, candidate.Name);
        }

        return list;
    }

    private static bool ShouldSkip(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.SubpopulationName)) {
            return true;
        }

        var infraType = row.InfraType?.Trim();
        if (string.IsNullOrWhiteSpace(row.InfraName)) {
            return LooksPopulation(infraType) || LooksVariety(infraType);
        }

        if (LooksPopulation(infraType) || LooksVariety(infraType)) {
            return true;
        }

        return !(string.IsNullOrWhiteSpace(infraType) || LooksSubspecies(infraType));
    }

    private static bool LooksPopulation(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return false;
        }

        var normalized = text.Trim().ToLowerInvariant();
        return normalized.Contains("population", StringComparison.Ordinal)
            || normalized.Contains("subpopulation", StringComparison.Ordinal)
            || normalized.Contains("regional", StringComparison.Ordinal);
    }

    private static bool LooksVariety(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return false;
        }

        var normalized = text.Trim().ToLowerInvariant();
        return normalized.Contains("variety", StringComparison.Ordinal)
            || normalized.Contains("var.", StringComparison.Ordinal)
            || normalized.Contains("form", StringComparison.Ordinal);
    }

    private static bool LooksSubspecies(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return true;
        }

        var normalized = text.Trim().ToLowerInvariant();
        return normalized.Contains("subspecies", StringComparison.Ordinal)
            || normalized.Contains("subsp", StringComparison.Ordinal)
            || normalized.Contains("ssp", StringComparison.Ordinal);
    }

    private static string? TryResolveOptional(string? overrideValue, string? configuredValue, string description) {
        string? candidate = null;
        if (!string.IsNullOrWhiteSpace(overrideValue)) {
            candidate = overrideValue;
        }
        else if (!string.IsNullOrWhiteSpace(configuredValue)) {
            candidate = configuredValue;
        }

        if (string.IsNullOrWhiteSpace(candidate)) {
            return null;
        }

        try {
            var path = Path.GetFullPath(candidate);
            if (File.Exists(path)) {
                return path;
            }

            AnsiConsole.MarkupLineInterpolated($"[yellow]{Markup.Escape(description)} not found at {Markup.Escape(path)}; feature disabled.[/]");
            return null;
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{Markup.Escape(description)} path '{Markup.Escape(candidate)}' could not be resolved: {Markup.Escape(ex.Message)}[/]");
            return null;
        }
    }

    private static SqliteConnection OpenReadOnlyConnection(string path) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();
        return connection;
    }

    // Each outcome is split into "new this run" and "same as last time": a re-run re-checks
    // tens of thousands of taxa and lands most of them where they were, so a bare total (89,645
    // missing) hid the handful that actually moved.
    private static void RenderSummary(WikipediaMatchStats stats) {
        var table = new Table().Border(TableBorder.Rounded).Title("Wikipedia match summary");
        table.AddColumn("Result");
        table.AddColumn(new TableColumn("Taxa").RightAligned());
        table.AddColumn(new TableColumn("New this run").RightAligned());
        void Row(string label, TransitionCount count) =>
            table.AddRow(label, count.Total.ToString("n0"), count.Changed == 0 ? "[grey]0[/]" : count.Changed.ToString("n0"));
        Row("Matched to an article", stats.Matched);
        Row("Waiting on a page download", stats.Pending);
        Row("No article found", stats.Missing);
        Row("Only disambiguation pages", stats.Rejected);
        Row("No names to look up", stats.NoCandidates);
        table.AddRow("[grey]Already matched, not re-checked[/]", $"[grey]{stats.AlreadyMatched:n0}[/]", "");
        if (stats.NotRechecked > 0) {
            table.AddRow("[grey]Checked before, skipped (--pending-only)[/]", $"[grey]{stats.NotRechecked:n0}[/]", "");
        }
        table.AddRow("[grey]Skipped (subpopulations, varieties)[/]", $"[grey]{stats.Skipped:n0}[/]", "");
        AnsiConsole.Write(table);
    }

    private sealed class TransitionCount {
        public long Total { get; private set; }
        public long Changed { get; private set; }
        public void Add(bool changed) {
            Total++;
            if (changed) Changed++;
        }
    }

    private sealed class WikipediaMatchStats {
        public long Evaluated { get; set; }
        public TransitionCount Matched { get; } = new();
        public TransitionCount Pending { get; } = new();
        public TransitionCount Missing { get; } = new();
        public TransitionCount Rejected { get; } = new();
        public TransitionCount NoCandidates { get; } = new();
        public long Skipped { get; set; }
        public long AlreadyMatched { get; set; }
        public long NotRechecked { get; set; }

        // previousStatus is the taxon's match row before this run touched it (null: never checked).
        // "No names to look up" is stored as 'missing', so it counts as new only from another status.
        public void Record(TaxonProcessResult result, string? previousStatus) {
            bool Was(string status) => string.Equals(previousStatus, status, StringComparison.OrdinalIgnoreCase);
            switch (result) {
                case TaxonProcessResult.Matched:
                    Matched.Add(!Was(TaxonWikiMatchStatus.Matched));
                    break;
                case TaxonProcessResult.Pending:
                    Pending.Add(!Was(TaxonWikiMatchStatus.Pending));
                    break;
                case TaxonProcessResult.Missing:
                    Missing.Add(!Was(TaxonWikiMatchStatus.Missing));
                    break;
                case TaxonProcessResult.Rejected:
                    Rejected.Add(!Was(TaxonWikiMatchStatus.Rejected));
                    break;
                case TaxonProcessResult.NoCandidates:
                    NoCandidates.Add(!Was(TaxonWikiMatchStatus.Missing));
                    break;
                case TaxonProcessResult.Skipped:
                    Skipped++;
                    break;
                case TaxonProcessResult.AlreadyMatched:
                    AlreadyMatched++;
                    break;
                case TaxonProcessResult.NotRechecked:
                    NotRechecked++;
                    break;
            }
        }
    }

    private enum TaxonProcessResult {
        Matched,
        Pending,
        Missing,
        Rejected,
        NoCandidates,
        Skipped,
        AlreadyMatched,
        NotRechecked
    }

    private enum CandidateEvaluationStatus {
        Matched,
        Pending,
        Failed,
        Missing,
        Rejected
    }

    private sealed record CandidateEvaluation(
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

    private sealed record WikipediaMatchCandidate(
        string DisplayTitle,
        string NormalizedTitle,
        string SourceHint,
        string MatchMethod,
        bool IsSynonym,
        string? SynonymValue
    );

    private sealed record PendingCandidate(WikipediaMatchCandidate Candidate, CandidateEvaluation Evaluation);
}
