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
// Reads the IUCN rows and the name sources; TaxonPageMatcher matches each taxon and writes
// taxon_wiki_matches. Run via: wikipedia match-taxa

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia match-taxa", CommandKind.Mutates,
    "Attempt to match IUCN taxa to cached Wikipedia pages using Wikidata sitelinks and synonyms.",
    Reason = "Writes IUCN taxon -> Wikipedia page matches into the cache.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Taxa already matched to an article are skipped, and every other taxon is checked again. --pending-only also skips taxa already found to have no article.",
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
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase, "--iucn-db");
            wikipediaCachePath = paths.ResolveWikipediaCachePath(settings.WikipediaCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN Red List database not found:[/] {Markup.Escape(iucnPath)}");
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
            var result = TaxonPageMatcher.ProcessTaxon(rowTaxonId, existing, wikipediaStore,
                () => TaxonPageMatcher.BuildCandidates(wikidataLookup.GetCandidate(rowTaxonId), synonymService.GetCandidates(row, cancellationToken)),
                settings.ReprocessMatched, settings.PendingOnly, cancellationToken);
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
}
