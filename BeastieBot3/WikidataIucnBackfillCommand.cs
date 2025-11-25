using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataIucnBackfillSettings : CommonSettings {
    [CommandOption("--iucn-db <PATH>")]
    [Description("Override path to the IUCN taxonomy SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? IucnDatabase { get; init; }

    [CommandOption("--iucn-api-cache <PATH>")]
    [Description("Override path to the IUCN API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? IucnApiCache { get; init; }

    [CommandOption("--col-db <PATH>")]
    [Description("Override path to the Catalogue of Life SQLite database (defaults to Datastore:COL_sqlite).")]
    public string? ColDatabase { get; init; }

    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of IUCN taxa to evaluate (0 = all)." )]
    public int Limit { get; init; }

    [CommandOption("--queue-all-synonyms")]
    [Description("When supplied, queue/download matches for every synonym even if the primary name matched.")]
    public bool QueueAllSynonyms { get; init; }
}

public sealed class WikidataIucnBackfillCommand : AsyncCommand<WikidataIucnBackfillSettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, WikidataIucnBackfillSettings settings, CancellationToken cancellationToken) {
        _ = context;

        if (settings.Limit < 0) {
            AnsiConsole.MarkupLine("[red]--limit must be zero or greater.[/]");
            return -1;
        }

        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        string iucnPath;
        string wikidataCachePath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            wikidataCachePath = paths.ResolveWikidataCachePath(settings.WikidataCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        var colPath = TryResolveOptionalPath(settings.ColDatabase, paths.GetColSqlitePath(), "Catalogue of Life SQLite database");
        var iucnApiCachePath = TryResolveOptionalPath(settings.IucnApiCache, paths.GetIucnApiCachePath(), "IUCN API cache SQLite database");

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN SQLite database not found:[/] {Markup.Escape(iucnPath)}");
            return -3;
        }

        if (!File.Exists(wikidataCachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikidata cache SQLite database not found:[/] {Markup.Escape(wikidataCachePath)}");
            return -4;
        }

        if (colPath is not null && !File.Exists(colPath)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Catalogue of Life database not found at {Markup.Escape(colPath)}; COL synonyms disabled.[/]");
            colPath = null;
        }

        if (iucnApiCachePath is not null && !File.Exists(iucnApiCachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]IUCN API cache database not found at {Markup.Escape(iucnApiCachePath)}; API synonyms disabled.[/]");
            iucnApiCachePath = null;
        }

        using var iucnConnection = OpenReadOnlyConnection(iucnPath);
        using var wikidataIndexConnection = OpenReadOnlyConnection(wikidataCachePath);
        using var store = WikidataCacheStore.Open(wikidataCachePath);
        using var synonymService = new IucnSynonymService(iucnApiCachePath, colPath);
        using var wikidataClient = new WikidataApiClient(WikidataConfiguration.FromEnvironment());

        if (!synonymService.HasIucnApiCache) {
            AnsiConsole.MarkupLine("[yellow]IUCN API cache not available; only local synonyms will be used.[/]");
        }

        if (!synonymService.HasColDatabase) {
            AnsiConsole.MarkupLine("[yellow]COL SQLite database not available; COL synonyms will be skipped.[/]");
        }

        var repository = new IucnTaxonomyRepository(iucnConnection);
        var existingTaxonIds = LoadCachedTaxonIds(wikidataIndexConnection);
        var existingNames = LoadScientificNames(wikidataIndexConnection);
        var cachedEntities = LoadCachedEntityIds(wikidataIndexConnection);
        var stats = new BackfillStats();
        var rowLimit = settings.Limit > 0 ? settings.Limit : int.MaxValue;

        foreach (var row in repository.ReadRows(0, cancellationToken)) {
            cancellationToken.ThrowIfCancellationRequested();

            if (!IsEligible(row)) {
                continue;
            }

            var sisId = row.InternalTaxonId?.Trim();
            if (string.IsNullOrWhiteSpace(sisId)) {
                continue;
            }

            if (existingTaxonIds.Contains(sisId)) {
                continue;
            }

            var primaryName = row.ScientificNameTaxonomy
                ?? row.ScientificNameAssessments
                ?? ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName);
            var normalizedPrimary = ScientificNameHelper.Normalize(primaryName);
            if (!string.IsNullOrEmpty(normalizedPrimary) && existingNames.Contains(normalizedPrimary)) {
                continue;
            }

            stats.Evaluated++;

            var candidates = synonymService.GetCandidates(row, cancellationToken);
            if (candidates.Count == 0) {
                stats.Missing++;
                continue;
            }

            var matches = await FindMatchesAsync(candidates, settings.QueueAllSynonyms, wikidataClient, cancellationToken).ConfigureAwait(false);
            if (matches.Count == 0) {
                stats.Missing++;
                continue;
            }

            foreach (var match in matches) {
                stats.RecordMatch(match);

                if (cachedEntities.Contains(match.Result.NumericId)) {
                    stats.AlreadyCached++;
                    continue;
                }

                store.UpsertSeeds(new[] { new WikidataSeedRow(match.Result.NumericId, match.Result.EntityId, false, false) });
                var item = new WikidataEntityWorkItem(match.Result.NumericId, match.Result.EntityId, null, 0);
                if (await WikidataEntityDownloader.DownloadSingleAsync(wikidataClient, store, item, cancellationToken).ConfigureAwait(false)) {
                    cachedEntities.Add(match.Result.NumericId);
                    stats.Downloaded++;
                }
                else {
                    stats.Failures++;
                }
            }

            existingTaxonIds.Add(sisId);

            if (stats.Evaluated >= rowLimit) {
                break;
            }
        }

        RenderSummary(stats, iucnPath, wikidataCachePath, iucnApiCachePath, colPath, settings.QueueAllSynonyms);
        return 0;
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

    private static string? TryResolveOptionalPath(string? overrideValue, string? configuredValue, string description) {
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
            return Path.GetFullPath(candidate);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{Markup.Escape(description)} path '{Markup.Escape(candidate)}' could not be resolved: {Markup.Escape(ex.Message)}[/]");
            return null;
        }
    }

    private static bool IsEligible(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.SubpopulationName)) {
            return false;
        }

        var infraType = row.InfraType?.Trim();
        if (string.IsNullOrWhiteSpace(row.InfraName)) {
            return !LooksPopulation(infraType) && !LooksVariety(infraType);
        }

        if (LooksPopulation(infraType) || LooksVariety(infraType)) {
            return false;
        }

        return string.IsNullOrWhiteSpace(infraType) || LooksSubspecies(infraType);
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

    private static HashSet<string> LoadCachedTaxonIds(SqliteConnection connection) {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM wikidata_p627_values";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var value = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (!string.IsNullOrWhiteSpace(value)) {
                set.Add(value.Trim());
            }
        }

        return set;
    }

    private static HashSet<string> LoadScientificNames(SqliteConnection connection) {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT LOWER(name) FROM wikidata_scientific_names";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (!reader.IsDBNull(0)) {
                var name = reader.GetString(0)?.Trim();
                if (!string.IsNullOrWhiteSpace(name)) {
                    set.Add(name);
                }
            }
        }

        return set;
    }

    private static HashSet<long> LoadCachedEntityIds(SqliteConnection connection) {
        var set = new HashSet<long>();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT entity_numeric_id FROM wikidata_entities WHERE json_downloaded = 1";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (!reader.IsDBNull(0)) {
                set.Add(reader.GetInt64(0));
            }
        }

        return set;
    }

    private static async Task<IReadOnlyList<WikidataMatch>> FindMatchesAsync(IReadOnlyList<TaxonNameCandidate> candidates, bool includeAllSynonyms, WikidataApiClient client, CancellationToken cancellationToken) {
        var results = new List<WikidataMatch>();
        var seen = new HashSet<long>();

        var primaries = candidates.Where(c => !c.IsSynonym).ToList();
        var synonyms = candidates.Where(c => c.IsSynonym).ToList();

        foreach (var candidate in primaries) {
            var match = await SearchSingleAsync(candidate, client, cancellationToken).ConfigureAwait(false);
            if (match is null) {
                continue;
            }

            if (seen.Add(match.Result.NumericId)) {
                results.Add(match);
            }

            if (!includeAllSynonyms) {
                break;
            }
        }

        if (results.Count == 0 || includeAllSynonyms) {
            foreach (var candidate in synonyms) {
                var match = await SearchSingleAsync(candidate, client, cancellationToken).ConfigureAwait(false);
                if (match is null) {
                    continue;
                }

                if (seen.Add(match.Result.NumericId)) {
                    results.Add(match);
                    if (!includeAllSynonyms) {
                        break;
                    }
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Searches for a single Wikidata match for the given taxon name candidate, using P225 (taxon name) first, then falling back to label search.
    /// </summary>
    /// <param name="candidate">The taxon name candidate to search for.</param>
    /// <param name="client">The Wikidata API client to use for searching.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous search operation. The task result contains the matched Wikidata entry or null if no match is found.</returns>
    private static async Task<WikidataMatch?> SearchSingleAsync(TaxonNameCandidate candidate, WikidataApiClient client, CancellationToken cancellationToken) {
        var byP225 = await client.SearchTaxaByP225Async(candidate.Name, cancellationToken).ConfigureAwait(false);
        var direct = byP225.FirstOrDefault();
        if (direct is not null) {
            return new WikidataMatch(candidate, SearchMatchMethod.TaxonName, direct);
        }

        var byLabel = await client.SearchTaxaByLabelAsync(candidate.Name, cancellationToken).ConfigureAwait(false);
        var labelResult = byLabel.FirstOrDefault();
        if (labelResult is not null) {
            return new WikidataMatch(candidate, SearchMatchMethod.Label, labelResult);
        }

        return null;
    }

    private static void RenderSummary(BackfillStats stats, string iucnPath, string wikidataPath, string? apiCache, string? colPath, bool queueAllSynonyms) {
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN DB:[/] {Markup.Escape(iucnPath)}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikidata cache:[/] {Markup.Escape(wikidataPath)}");
        if (!string.IsNullOrWhiteSpace(apiCache)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]IUCN API cache:[/] {Markup.Escape(apiCache!)}");
        }

        if (!string.IsNullOrWhiteSpace(colPath)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]COL DB:[/] {Markup.Escape(colPath!)}");
        }

        AnsiConsole.MarkupLineInterpolated($"[grey]Queue all synonyms:[/] {(queueAllSynonyms ? "yes" : "no")}");
        var table = new Table().Border(TableBorder.Minimal);
        table.AddColumn("Metric");
        table.AddColumn("Value");
        table.AddRow("Eligible taxa", stats.Evaluated.ToString());
        table.AddRow("Matches", stats.Matches.ToString());
        table.AddRow("Synonym matches", stats.SynonymMatches.ToString());
        table.AddRow("Already cached", stats.AlreadyCached.ToString());
        table.AddRow("Downloaded", stats.Downloaded.ToString());
        table.AddRow("Failures", stats.Failures.ToString());
        table.AddRow("Missing", stats.Missing.ToString());
        AnsiConsole.Write(table);
    }

    private sealed class BackfillStats {
        public long Evaluated { get; set; }
        public long Matches { get; private set; }
        public long SynonymMatches { get; private set; }
        public long AlreadyCached { get; set; }
        public long Downloaded { get; set; }
        public long Failures { get; set; }
        public long Missing { get; set; }

        public void RecordMatch(WikidataMatch match) {
            Matches++;
            if (match.Candidate.IsSynonym) {
                SynonymMatches++;
            }
        }
    }

    private sealed record WikidataMatch(TaxonNameCandidate Candidate, SearchMatchMethod Method, WikidataSearchResult Result);

    private enum SearchMatchMethod {
        TaxonName,
        Label
    }
}
