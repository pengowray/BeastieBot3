using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

/// <summary>
/// Aggregates common names from all sources (IUCN API, Wikidata, Wikipedia) into the common name store.
/// </summary>
internal sealed class CommonNameAggregateCommand : AsyncCommand<CommonNameAggregateCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--iucn-api-cache <PATH>")]
        [Description("Path to the IUCN API cache SQLite database.")]
        public string? IucnApiCachePath { get; init; }

        [CommandOption("--wikidata-cache <PATH>")]
        [Description("Path to the Wikidata cache SQLite database.")]
        public string? WikidataCachePath { get; init; }

        [CommandOption("--wikipedia-cache <PATH>")]
        [Description("Path to the Wikipedia cache SQLite database.")]
        public string? WikipediaCachePath { get; init; }

        [CommandOption("--source <SOURCE>")]
        [Description("Only aggregate from specific source: iucn, wikidata, wikipedia, or all (default).")]
        public string Source { get; init; } = "all";

        [CommandOption("--limit <N>")]
        [Description("Limit number of records to process per source.")]
        public int? Limit { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile);
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath);

        AnsiConsole.MarkupLine($"[blue]Common name store:[/] {commonNameDbPath}");

        using var store = CommonNameStore.Open(commonNameDbPath);

        var source = settings.Source.ToLowerInvariant();

        if (source is "all" or "iucn") {
            var iucnApiPath = settings.IucnApiCachePath ?? paths.GetIucnApiCachePath();
            if (!string.IsNullOrWhiteSpace(iucnApiPath) && File.Exists(iucnApiPath)) {
                await AggregateIucnCommonNamesAsync(store, iucnApiPath, settings.Limit, cancellationToken);
            } else {
                AnsiConsole.MarkupLine("[yellow]Skipping IUCN:[/] API cache not found");
            }
        }

        if (source is "all" or "wikidata") {
            var wikidataPath = settings.WikidataCachePath ?? paths.GetWikidataCachePath();
            if (!string.IsNullOrWhiteSpace(wikidataPath) && File.Exists(wikidataPath)) {
                await AggregateWikidataCommonNamesAsync(store, wikidataPath, settings.Limit, cancellationToken);
            } else {
                AnsiConsole.MarkupLine("[yellow]Skipping Wikidata:[/] cache not found");
            }
        }

        if (source is "all" or "wikipedia") {
            var wikipediaPath = settings.WikipediaCachePath ?? paths.GetWikipediaCachePath();
            if (!string.IsNullOrWhiteSpace(wikipediaPath) && File.Exists(wikipediaPath)) {
                await AggregateWikipediaCommonNamesAsync(store, wikipediaPath, settings.Limit, cancellationToken);
            } else {
                AnsiConsole.MarkupLine("[yellow]Skipping Wikipedia:[/] cache not found");
            }
        }

        // Show final statistics
        var stats = store.GetStatistics();
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]Common name aggregation complete:[/]");
        var table = new Table();
        table.AddColumn("Metric");
        table.AddColumn(new TableColumn("Count").RightAligned());
        table.AddRow("Taxa", stats.TaxaCount.ToString("N0"));
        table.AddRow("Synonyms", stats.SynonymCount.ToString("N0"));
        table.AddRow("Common Names", stats.CommonNameCount.ToString("N0"));
        table.AddRow("Conflicts", stats.ConflictCount.ToString("N0"));
        AnsiConsole.Write(table);

        return 0;
    }

    private static Task AggregateIucnCommonNamesAsync(CommonNameStore store, string iucnApiPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating IUCN common names...[/]");
            AnsiConsole.MarkupLine($"[blue]IUCN API cache:[/] {iucnApiPath}");

            var runId = store.BeginImportRun("common_names_iucn");
            var processed = 0;
            var added = 0;
            var errors = 0;
            var skippedNoTaxon = 0;

            using var iucnConnection = new SqliteConnection($"Data Source={iucnApiPath};Mode=ReadOnly");
            iucnConnection.Open();

            // Query IUCN API cached assessments (which contain common names in taxon.common_names)
            // We use assessments table because it has the full taxon data including common_names
            using var command = iucnConnection.CreateCommand();
            command.CommandText = limit.HasValue
                ? "SELECT sis_id, json FROM assessments WHERE json IS NOT NULL LIMIT @limit"
                : "SELECT sis_id, json FROM assessments WHERE json IS NOT NULL";
            if (limit.HasValue) {
                command.Parameters.AddWithValue("@limit", limit.Value);
            }

            // Track which taxa we've already processed to avoid duplicates
            var processedTaxa = new HashSet<long>();

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]IUCN common names[/]", autoStart: true);
                    task.IsIndeterminate = !limit.HasValue;
                    if (limit.HasValue) task.MaxValue = limit.Value;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        if (limit.HasValue) task.Increment(1);

                        var sisId = reader.GetInt64(0);
                        var json = reader.IsDBNull(1) ? null : reader.GetString(1);

                        if (string.IsNullOrWhiteSpace(json)) continue;

                        // Skip if we've already processed this taxon (multiple assessments per taxon)
                        if (!processedTaxa.Add(sisId)) continue;

                        // Find the taxon in our store
                        var taxonId = store.FindTaxonBySourceId("iucn", sisId.ToString());
                        if (!taxonId.HasValue) {
                            skippedNoTaxon++;
                            continue;
                        }

                        try {
                            var names = ExtractIucnCommonNames(json);
                            foreach (var (name, isPreferred, language) in names) {
                                var normalized = CommonNameNormalizer.NormalizeForMatching(name);
                                if (normalized == null) continue;

                                store.InsertCommonName(
                                    taxonId.Value,
                                    name,
                                    normalized,
                                    displayName: null, // Will be computed later with caps rules
                                    language,
                                    "iucn",
                                    sisId.ToString(),
                                    isPreferred
                                );
                                added++;
                            }
                        } catch (Exception ex) {
                            errors++;
                            if (errors <= 5) {
                                AnsiConsole.MarkupLine($"[red]Error parsing IUCN JSON for SIS {sisId}:[/] {ex.Message}");
                            }
                        }
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, errors,
                $"Skipped {skippedNoTaxon} records with no matching taxon, processed {processedTaxa.Count} unique taxa");
            AnsiConsole.MarkupLine($"[green]IUCN:[/] {added:N0} common names from {processedTaxa.Count:N0} taxa ({errors} errors, {skippedNoTaxon} skipped)");
        }, cancellationToken);
    }

    private static IReadOnlyList<(string Name, bool IsPreferred, string Language)> ExtractIucnCommonNames(string json) {
        var results = new List<(string, bool, string)>();

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            // Common names are in the assessments JSON under taxon.common_names
            // Structure: {"taxon": {"common_names": [{"main": true, "name": "...", "language": "eng"}]}}
            JsonElement commonNamesElement = default;

            if (root.TryGetProperty("taxon", out var taxon) && taxon.TryGetProperty("common_names", out var tcn)) {
                commonNamesElement = tcn;
            } else if (root.TryGetProperty("common_names", out var cn)) {
                commonNamesElement = cn;
            }

            if (commonNamesElement.ValueKind == JsonValueKind.Array) {
                foreach (var entry in commonNamesElement.EnumerateArray()) {
                    if (entry.ValueKind != JsonValueKind.Object) continue;

                    var name = GetStringProperty(entry, "name");
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    // Get language (IUCN uses "eng", "fra", "spa", etc.)
                    var language = GetStringProperty(entry, "language") ?? "eng";
                    // Normalize language codes to ISO 639-1 style
                    language = NormalizeLanguageCode(language);

                    // Check if this is the "main" (preferred) common name
                    var isMain = entry.TryGetProperty("main", out var mainProp) &&
                                 mainProp.ValueKind == JsonValueKind.True;

                    results.Add((name.Trim(), isMain, language));
                }
            }
        } catch (JsonException) {
            // Ignore malformed JSON
        }

        return results;
    }

    private static string NormalizeLanguageCode(string language) {
        // Convert ISO 639-2/3 codes to ISO 639-1 where possible
        return language.ToLowerInvariant() switch {
            "eng" => "en",
            "fra" or "fre" => "fr",
            "spa" => "es",
            "deu" or "ger" => "de",
            "por" => "pt",
            "ita" => "it",
            "nld" or "dut" => "nl",
            "rus" => "ru",
            "zho" or "chi" => "zh",
            "jpn" => "ja",
            "ara" => "ar",
            _ => language.ToLowerInvariant()
        };
    }

    private static Task AggregateWikidataCommonNamesAsync(CommonNameStore store, string wikidataPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating Wikidata common names...[/]");
            AnsiConsole.MarkupLine($"[blue]Wikidata cache:[/] {wikidataPath}");

            var runId = store.BeginImportRun("common_names_wikidata");
            var processed = 0;
            var added = 0;
            var errors = 0;
            var matched = 0;

            using var wikidataConnection = new SqliteConnection($"Data Source={wikidataPath};Mode=ReadOnly");
            wikidataConnection.Open();

            // Query Wikidata entities with JSON that have P627 (IUCN taxon ID) values
            using var command = wikidataConnection.CreateCommand();
            command.CommandText = limit.HasValue
                ? @"SELECT e.entity_id, e.json, GROUP_CONCAT(p.value) as iucn_ids
                    FROM wikidata_entities e
                    LEFT JOIN wikidata_p627_values p ON p.entity_numeric_id = e.entity_numeric_id
                    WHERE e.json IS NOT NULL
                    GROUP BY e.entity_numeric_id
                    LIMIT @limit"
                : @"SELECT e.entity_id, e.json, GROUP_CONCAT(p.value) as iucn_ids
                    FROM wikidata_entities e
                    LEFT JOIN wikidata_p627_values p ON p.entity_numeric_id = e.entity_numeric_id
                    WHERE e.json IS NOT NULL
                    GROUP BY e.entity_numeric_id";
            if (limit.HasValue) {
                command.Parameters.AddWithValue("@limit", limit.Value);
            }

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Wikidata common names[/]", autoStart: true);
                    task.IsIndeterminate = !limit.HasValue;
                    if (limit.HasValue) task.MaxValue = limit.Value;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        if (limit.HasValue) task.Increment(1);

                        var entityId = reader.GetString(0);
                        var json = reader.IsDBNull(1) ? null : reader.GetString(1);
                        var iucnIds = reader.IsDBNull(2) ? null : reader.GetString(2);

                        if (string.IsNullOrWhiteSpace(json)) continue;

                        // Try to find matching taxon via IUCN ID
                        long? taxonId = null;
                        if (!string.IsNullOrWhiteSpace(iucnIds)) {
                            foreach (var sisIdStr in iucnIds.Split(',')) {
                                taxonId = store.FindTaxonBySourceId("iucn", sisIdStr.Trim());
                                if (taxonId.HasValue) break;
                            }
                        }

                        // If no match via IUCN ID, try to match via scientific name
                        if (!taxonId.HasValue) {
                            try {
                                var record = WikidataEntityParser.Parse(json);
                                foreach (var sciName in record.ScientificNames.Where(n => !string.IsNullOrWhiteSpace(n.Value))) {
                                    var normalized = ScientificNameNormalizer.Normalize(sciName.Value);
                                    if (normalized == null) continue;

                                    taxonId = store.FindTaxonByCanonicalName(normalized);
                                    if (taxonId.HasValue) break;

                                    taxonId = store.FindTaxonBySynonym(normalized);
                                    if (taxonId.HasValue) break;
                                }
                            } catch {
                                // Continue without matching
                            }
                        }

                        if (!taxonId.HasValue) continue;
                        matched++;

                        try {
                            var record = WikidataEntityParser.Parse(json);

                            // Add English common names from P1843
                            foreach (var commonName in record.CommonNames.Where(n =>
                                n.Language.StartsWith("en", StringComparison.OrdinalIgnoreCase))) {

                                var normalized = CommonNameNormalizer.NormalizeForMatching(commonName.Value);
                                if (normalized == null) continue;

                                store.InsertCommonName(
                                    taxonId.Value,
                                    commonName.Value.Trim(),
                                    normalized,
                                    displayName: null,
                                    "en",
                                    "wikidata",
                                    entityId,
                                    isPreferred: false // Wikidata doesn't have preferred flag
                                );
                                added++;
                            }

                            // Also add the English label as a common name candidate
                            if (!string.IsNullOrWhiteSpace(record.LabelEn)) {
                                var normalized = CommonNameNormalizer.NormalizeForMatching(record.LabelEn);
                                if (normalized != null) {
                                    store.InsertCommonName(
                                        taxonId.Value,
                                        record.LabelEn.Trim(),
                                        normalized,
                                        displayName: null,
                                        "en",
                                        "wikidata_label",
                                        entityId,
                                        isPreferred: false
                                    );
                                    added++;
                                }
                            }
                        } catch (Exception ex) {
                            errors++;
                            if (errors <= 5) {
                                AnsiConsole.MarkupLine($"[red]Error parsing Wikidata JSON for {entityId}:[/] {ex.Message}");
                            }
                        }
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, errors,
                $"Matched {matched} entities to taxa");
            AnsiConsole.MarkupLine($"[green]Wikidata:[/] {added:N0} common names from {matched:N0} matched entities ({errors} errors)");
        }, cancellationToken);
    }

    private static Task AggregateWikipediaCommonNamesAsync(CommonNameStore store, string wikipediaPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating Wikipedia common names...[/]");
            AnsiConsole.MarkupLine($"[blue]Wikipedia cache:[/] {wikipediaPath}");

            var runId = store.BeginImportRun("common_names_wikipedia");
            var processed = 0;
            var added = 0;
            var matched = 0;

            using var wikiConnection = new SqliteConnection($"Data Source={wikipediaPath};Mode=ReadOnly");
            wikiConnection.Open();

            // Query matched taxa from Wikipedia cache
            using var command = wikiConnection.CreateCommand();
            command.CommandText = limit.HasValue
                ? @"SELECT m.taxon_identifier, p.page_title, p.normalized_title
                    FROM taxon_wiki_matches m
                    JOIN wiki_pages p ON p.id = m.page_row_id
                    WHERE m.taxon_source = 'IUCN' AND m.match_status = 'matched'
                    LIMIT @limit"
                : @"SELECT m.taxon_identifier, p.page_title, p.normalized_title
                    FROM taxon_wiki_matches m
                    JOIN wiki_pages p ON p.id = m.page_row_id
                    WHERE m.taxon_source = 'IUCN' AND m.match_status = 'matched'";
            if (limit.HasValue) {
                command.Parameters.AddWithValue("@limit", limit.Value);
            }

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Wikipedia titles[/]", autoStart: true);
                    task.IsIndeterminate = !limit.HasValue;
                    if (limit.HasValue) task.MaxValue = limit.Value;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        if (limit.HasValue) task.Increment(1);

                        var taxonIdentifier = reader.GetString(0); // IUCN SIS ID
                        var pageTitle = reader.GetString(1);

                        // Find the taxon
                        var taxonId = store.FindTaxonBySourceId("iucn", taxonIdentifier);
                        if (!taxonId.HasValue) continue;
                        matched++;

                        // The Wikipedia page title might be a common name
                        // We need to check it's not a scientific name
                        var cleanTitle = CommonNameNormalizer.RemoveDisambiguationSuffix(pageTitle);
                        var normalized = CommonNameNormalizer.NormalizeForMatching(cleanTitle);

                        if (normalized == null) continue;

                        // Skip if it looks like a scientific name (contains only Latin binomial pattern)
                        // This is a heuristic - proper check would need the genus/species
                        if (cleanTitle.Split(' ').Length == 2 &&
                            cleanTitle.Split(' ').All(p => p.Length > 0 && char.IsLetter(p[0]))) {
                            // Could be scientific or common - we'll add it and let conflict detection handle it
                        }

                        store.InsertCommonName(
                            taxonId.Value,
                            cleanTitle,
                            normalized,
                            displayName: null,
                            "en",
                            "wikipedia_title",
                            pageTitle,
                            isPreferred: true // Wikipedia title is a strong signal
                        );
                        added++;
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, 0,
                $"Matched {matched} pages to taxa");
            AnsiConsole.MarkupLine($"[green]Wikipedia:[/] {added:N0} titles from {matched:N0} matched pages");
        }, cancellationToken);
    }

    private static string? GetStringProperty(JsonElement element, string propertyName) {
        if (element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String) {
            return prop.GetString();
        }
        return null;
    }
}
