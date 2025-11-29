using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

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

        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
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

            if (resumeToken is not null && string.Compare(row.InternalTaxonId, resumeToken, StringComparison.Ordinal) <= 0) {
                continue;
            }

            if (!settings.IncludeSubpopulations && ShouldSkip(row)) {
                stats.Skipped++;
                continue;
            }

            processed++;
            stats.Evaluated++;

            var result = ProcessTaxon(row, wikipediaStore, wikidataLookup, synonymService, settings, cancellationToken);
            stats.Record(result);

            if (processed >= limit) {
                break;
            }

            if (stats.Evaluated >= nextProgress) {
                AnsiConsole.MarkupLineInterpolated($"[grey]Evaluated {stats.Evaluated:n0} taxa (matched {stats.Matched:n0}, pending {stats.Pending:n0}, missing {stats.Missing:n0}).[/]");
                nextProgress += progressInterval;
            }
        }

        RenderSummary(stats);
        return 0;
    }

    private static TaxonProcessResult ProcessTaxon(
        IucnTaxonomyRow row,
        WikipediaCacheStore cacheStore,
        WikidataIucnMatchLookup wikidataLookup,
        IucnSynonymService synonymService,
        Settings settings,
        CancellationToken cancellationToken) {
        var taxonId = row.InternalTaxonId?.Trim();
        if (string.IsNullOrWhiteSpace(taxonId)) {
            return TaxonProcessResult.Skipped;
        }

        var existing = cacheStore.GetTaxonMatch(TaxonSources.Iucn, taxonId);
        if (!settings.ReprocessMatched && existing is not null && string.Equals(existing.MatchStatus, TaxonWikiMatchStatus.Matched, StringComparison.OrdinalIgnoreCase)) {
            return TaxonProcessResult.AlreadyMatched;
        }

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
            AnsiConsole.MarkupLineInterpolated($"[yellow]No candidates[/] for SIS {Markup.Escape(taxonId)}");
            return TaxonProcessResult.NoCandidates;
        }

        var attemptOrder = cacheStore.GetNextAttemptOrder(TaxonSources.Iucn, taxonId);
        PendingCandidate? pending = null;

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
            AnsiConsole.MarkupLineInterpolated($"[yellow]Pending[/] SIS {Markup.Escape(taxonId)} waiting on {Markup.Escape(pendingCandidate.DisplayTitle)}");
            return TaxonProcessResult.Pending;
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
        AnsiConsole.MarkupLineInterpolated($"[red]Missing[/] SIS {Markup.Escape(taxonId)} (no valid articles)");
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
            WikiPageDownloadStatus.Cached => EvaluateCached(effectiveSummary),
            _ => CandidateEvaluation.Failed(effectiveSummary, $"Unknown status {effectiveSummary.DownloadStatus}")
        };
    }

    private static CandidateEvaluation EvaluateCached(WikiPageSummary summary) {
        if (summary.IsDisambiguation) {
            return CandidateEvaluation.Failed(summary, "Disambiguation page");
        }

        if (summary.IsSetIndex) {
            return CandidateEvaluation.Failed(summary, "Set index page");
        }

        var finalTitle = summary.IsRedirect && !string.IsNullOrWhiteSpace(summary.RedirectTarget)
            ? summary.RedirectTarget
            : summary.PageTitle;
        return CandidateEvaluation.Matched(summary, finalTitle);
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

        var wikidata = wikidataLookup.GetCandidate(row.InternalTaxonId);
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

    private static void RenderSummary(WikipediaMatchStats stats) {
        var table = new Table().Border(TableBorder.Rounded).Title("Wikipedia match summary");
        table.AddColumn("Category");
        table.AddColumn("Count");
        table.AddRow("Evaluated", stats.Evaluated.ToString("n0"));
        table.AddRow("Matched", stats.Matched.ToString("n0"));
        table.AddRow("Pending", stats.Pending.ToString("n0"));
        table.AddRow("Missing", stats.Missing.ToString("n0"));
        table.AddRow("No candidates", stats.NoCandidates.ToString("n0"));
        table.AddRow("Skipped", stats.Skipped.ToString("n0"));
        table.AddRow("Already matched", stats.AlreadyMatched.ToString("n0"));
        AnsiConsole.Write(table);
    }

    private sealed class WikipediaMatchStats {
        public long Evaluated { get; set; }
        public long Matched { get; set; }
        public long Pending { get; set; }
        public long Missing { get; set; }
        public long NoCandidates { get; set; }
        public long Skipped { get; set; }
        public long AlreadyMatched { get; set; }

        public void Record(TaxonProcessResult result) {
            switch (result) {
                case TaxonProcessResult.Matched:
                    Matched++;
                    break;
                case TaxonProcessResult.Pending:
                    Pending++;
                    break;
                case TaxonProcessResult.Missing:
                    Missing++;
                    break;
                case TaxonProcessResult.NoCandidates:
                    NoCandidates++;
                    break;
                case TaxonProcessResult.Skipped:
                    Skipped++;
                    break;
                case TaxonProcessResult.AlreadyMatched:
                    AlreadyMatched++;
                    break;
            }
        }
    }

    private enum TaxonProcessResult {
        Matched,
        Pending,
        Missing,
        NoCandidates,
        Skipped,
        AlreadyMatched
    }

    private enum CandidateEvaluationStatus {
        Matched,
        Pending,
        Failed,
        Missing
    }

    private sealed record CandidateEvaluation(
        CandidateEvaluationStatus Status,
        WikiPageSummary PageSummary,
        string AttemptOutcome,
        string? Notes,
        string? FinalTitle
    ) {
        public static CandidateEvaluation Matched(WikiPageSummary summary, string? finalTitle) => new(CandidateEvaluationStatus.Matched, summary, TaxonWikiAttemptOutcome.Matched, null, finalTitle ?? summary.PageTitle);
        public static CandidateEvaluation Pending(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Pending, summary, TaxonWikiAttemptOutcome.PendingFetch, notes, summary.PageTitle);
        public static CandidateEvaluation Failed(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Failed, summary, TaxonWikiAttemptOutcome.Failed, notes, summary.PageTitle);
        public static CandidateEvaluation Missing(WikiPageSummary summary, string notes) => new(CandidateEvaluationStatus.Missing, summary, TaxonWikiAttemptOutcome.Missing, notes, summary.PageTitle);
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
