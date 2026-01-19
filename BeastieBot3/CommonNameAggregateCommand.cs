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

        [CommandOption("--col-sqlite <PATH>")]
        [Description("Path to the Catalogue of Life SQLite database.")]
        public string? ColSqlitePath { get; init; }

        [CommandOption("--include-synonyms")]
        [Description("Also import scientific name synonyms from IUCN API.")]
        public bool IncludeSynonyms { get; init; }

        [CommandOption("--source <SOURCE>")]
        [Description("Only aggregate from specific source: iucn, wikidata, wikipedia, col, or all (default).")]
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

                // Also import IUCN synonyms if requested
                if (settings.IncludeSynonyms) {
                    await AggregateIucnSynonymsAsync(store, iucnApiPath, settings.Limit, cancellationToken);
                }
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

        if (source is "all" or "col") {
            var colPath = settings.ColSqlitePath ?? paths.GetColSqlitePath();
            if (!string.IsNullOrWhiteSpace(colPath) && File.Exists(colPath)) {
                await AggregateColVernacularNamesAsync(store, colPath, settings.Limit, cancellationToken);
                await AggregateColSynonymsAsync(store, colPath, settings.Limit, cancellationToken);
            } else {
                AnsiConsole.MarkupLine("[yellow]Skipping COL:[/] database not found");
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
                            var extraction = ExtractIucnCommonNamesWithScientific(json);
                            foreach (var (name, isPreferred, language) in extraction.CommonNames) {
                                // Skip names that match the scientific name (case-insensitive)
                                if (IsScientificNameMatch(name, extraction.ScientificName, extraction.GenusName, extraction.SpeciesEpithet)) continue;

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

    private static Task AggregateIucnSynonymsAsync(CommonNameStore store, string iucnApiPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating IUCN scientific name synonyms...[/]");

            var runId = store.BeginImportRun("synonyms_iucn");
            var processed = 0;
            var added = 0;
            var errors = 0;

            using var iucnConnection = new SqliteConnection($"Data Source={iucnApiPath};Mode=ReadOnly");
            iucnConnection.Open();

            using var command = iucnConnection.CreateCommand();
            command.CommandText = limit.HasValue
                ? "SELECT sis_id, json FROM assessments WHERE json IS NOT NULL LIMIT @limit"
                : "SELECT sis_id, json FROM assessments WHERE json IS NOT NULL";
            if (limit.HasValue) {
                command.Parameters.AddWithValue("@limit", limit.Value);
            }

            var processedTaxa = new HashSet<long>();

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]IUCN synonyms[/]", autoStart: true);
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
                        if (!processedTaxa.Add(sisId)) continue;

                        var taxonId = store.FindTaxonBySourceId("iucn", sisId.ToString());
                        if (!taxonId.HasValue) continue;

                        try {
                            var synonyms = ExtractIucnSynonyms(json);
                            foreach (var (originalName, genusName, speciesName) in synonyms) {
                                // Build the canonical synonym name
                                var synonymScientific = string.IsNullOrWhiteSpace(genusName) || string.IsNullOrWhiteSpace(speciesName)
                                    ? originalName
                                    : $"{genusName} {speciesName}";

                                var normalized = ScientificNameNormalizer.Normalize(synonymScientific);
                                if (normalized == null) continue;

                                store.InsertSynonym(
                                    taxonId.Value,
                                    normalized,
                                    originalName,
                                    "iucn",
                                    "synonym"
                                );
                                added++;
                            }
                        } catch (Exception ex) {
                            errors++;
                            if (errors <= 5) {
                                AnsiConsole.MarkupLine($"[red]Error parsing IUCN synonyms for SIS {sisId}:[/] {ex.Message}");
                            }
                        }
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, errors,
                $"Processed {processedTaxa.Count} unique taxa");
            AnsiConsole.MarkupLine($"[green]IUCN synonyms:[/] {added:N0} synonyms from {processedTaxa.Count:N0} taxa ({errors} errors)");
        }, cancellationToken);
    }

    private static IReadOnlyList<(string OriginalName, string? GenusName, string? SpeciesName)> ExtractIucnSynonyms(string json) {
        var results = new List<(string, string?, string?)>();

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            // Synonyms are in taxon.synonyms
            JsonElement synonymsElement = default;

            if (root.TryGetProperty("taxon", out var taxon) && taxon.TryGetProperty("synonyms", out var syn)) {
                synonymsElement = syn;
            }

            if (synonymsElement.ValueKind == JsonValueKind.Array) {
                foreach (var entry in synonymsElement.EnumerateArray()) {
                    if (entry.ValueKind != JsonValueKind.Object) continue;

                    var name = GetStringProperty(entry, "name");
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    var genusName = GetStringProperty(entry, "genus_name");
                    var speciesName = GetStringProperty(entry, "species_name");

                    results.Add((name.Trim(), genusName?.Trim(), speciesName?.Trim()));
                }
            }
        } catch (JsonException) {
            // Ignore malformed JSON
        }

        return results;
    }

    /// <summary>
    /// Result of extracting common names and scientific name data from IUCN JSON.
    /// </summary>
    private sealed record IucnCommonNameExtraction(
        IReadOnlyList<(string Name, bool IsPreferred, string Language)> CommonNames,
        string? ScientificName,
        string? GenusName,
        string? SpeciesEpithet
    );

    /// <summary>
    /// Extracts common names along with the scientific name from IUCN JSON.
    /// This allows filtering common names that are actually the scientific name.
    /// </summary>
    private static IucnCommonNameExtraction ExtractIucnCommonNamesWithScientific(string json) {
        var commonNames = new List<(string, bool, string)>();
        string? scientificName = null;
        string? genusName = null;
        string? speciesEpithet = null;

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            // Extract scientific name parts from taxon
            JsonElement taxonElement = default;
            if (root.TryGetProperty("taxon", out var taxon)) {
                taxonElement = taxon;
            } else {
                taxonElement = root;
            }

            scientificName = GetStringProperty(taxonElement, "scientific_name")
                ?? GetStringProperty(taxonElement, "scientificName");
            genusName = GetStringProperty(taxonElement, "genus_name")
                ?? GetStringProperty(taxonElement, "genusName");
            speciesEpithet = GetStringProperty(taxonElement, "species_name")
                ?? GetStringProperty(taxonElement, "speciesName");

            // Extract common names
            JsonElement commonNamesElement = default;
            if (taxonElement.TryGetProperty("common_names", out var tcn)) {
                commonNamesElement = tcn;
            } else if (root.TryGetProperty("common_names", out var cn)) {
                commonNamesElement = cn;
            }

            if (commonNamesElement.ValueKind == JsonValueKind.Array) {
                foreach (var entry in commonNamesElement.EnumerateArray()) {
                    if (entry.ValueKind != JsonValueKind.Object) continue;

                    var name = GetStringProperty(entry, "name");
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    var trimmedName = name.Trim();

                    // Skip "Species code:" entries (e.g., "Species code: Zp")
                    if (trimmedName.StartsWith("Species code", StringComparison.OrdinalIgnoreCase)) continue;

                    // Get language (IUCN uses "eng", "fra", "spa", etc.)
                    var language = GetStringProperty(entry, "language") ?? "eng";
                    language = NormalizeLanguageCode(language);

                    // Check if this is the "main" (preferred) common name
                    var isMain = entry.TryGetProperty("main", out var mainProp) &&
                                 mainProp.ValueKind == JsonValueKind.True;

                    commonNames.Add((trimmedName, isMain, language));
                }
            }
        } catch (JsonException) {
            // Ignore malformed JSON
        }

        return new IucnCommonNameExtraction(commonNames, scientificName, genusName, speciesEpithet);
    }

    /// <summary>
    /// Checks if a common name is actually a scientific name by comparing against known scientific name parts.
    /// This is more accurate than heuristic-based detection.
    /// </summary>
    private static bool IsScientificNameMatch(string commonName, string? scientificName, string? genusName, string? speciesEpithet) {
        if (string.IsNullOrWhiteSpace(commonName)) return false;

        var trimmed = commonName.Trim();

        // Direct match with full scientific name
        if (!string.IsNullOrWhiteSpace(scientificName) &&
            trimmed.Equals(scientificName, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        // Match with just the species epithet (e.g., "afer" for "Alphester afer")
        if (!string.IsNullOrWhiteSpace(speciesEpithet) &&
            trimmed.Equals(speciesEpithet, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        // Match with genus name alone
        if (!string.IsNullOrWhiteSpace(genusName) &&
            trimmed.Equals(genusName, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        // If we have both genus and epithet, check for "Genus species" pattern match
        if (!string.IsNullOrWhiteSpace(genusName) && !string.IsNullOrWhiteSpace(speciesEpithet)) {
            var expectedBinomial = $"{genusName} {speciesEpithet}";
            if (trimmed.Equals(expectedBinomial, StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }

        return false;
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
                            // BUT skip if it looks like a scientific name (matches P225 or is Genus species format)
                            if (!string.IsNullOrWhiteSpace(record.LabelEn)) {
                                var labelTrimmed = record.LabelEn.Trim();
                                
                                // Check if label matches any scientific name from P225
                                var isScientificName = record.ScientificNames
                                    .Any(sn => sn.Value.Equals(labelTrimmed, StringComparison.OrdinalIgnoreCase));
                                
                                // Also check if it looks like a scientific name (Genus species format)
                                // Scientific names: start with capital, second word lowercase, typically 2 words
                                if (!isScientificName) {
                                    isScientificName = LooksLikeScientificName(labelTrimmed);
                                }
                                
                                if (!isScientificName) {
                                    var normalized = CommonNameNormalizer.NormalizeForMatching(labelTrimmed);
                                    if (normalized != null) {
                                        store.InsertCommonName(
                                            taxonId.Value,
                                            labelTrimmed,
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
            var titleAdded = 0;
            var taxoboxAdded = 0;
            var matched = 0;

            using var wikiConnection = new SqliteConnection($"Data Source={wikipediaPath};Mode=ReadOnly");
            wikiConnection.Open();

            // First, try to use taxon_wiki_matches if available
            var useTaxonMatches = TableHasRows(wikiConnection, "taxon_wiki_matches", "match_status = 'matched'");

            using var command = wikiConnection.CreateCommand();
            if (useTaxonMatches) {
                // Use pre-computed matches
                command.CommandText = limit.HasValue
                    ? @"SELECT m.taxon_identifier, p.page_title, p.normalized_title, t.data_json
                        FROM taxon_wiki_matches m
                        JOIN wiki_pages p ON p.id = m.page_row_id
                        LEFT JOIN wiki_taxobox_data t ON t.page_row_id = p.id
                        WHERE m.taxon_source = 'IUCN' AND m.match_status = 'matched'
                        LIMIT @limit"
                    : @"SELECT m.taxon_identifier, p.page_title, p.normalized_title, t.data_json
                        FROM taxon_wiki_matches m
                        JOIN wiki_pages p ON p.id = m.page_row_id
                        LEFT JOIN wiki_taxobox_data t ON t.page_row_id = p.id
                        WHERE m.taxon_source = 'IUCN' AND m.match_status = 'matched'";
                if (limit.HasValue) {
                    command.Parameters.AddWithValue("@limit", limit.Value);
                }
            } else {
                // Fall back to matching via taxobox scientific names
                AnsiConsole.MarkupLine("[grey]No pre-computed matches found, using taxobox scientific names...[/]");
                command.CommandText = limit.HasValue
                    ? @"SELECT t.scientific_name, p.page_title, t.data_json
                        FROM wiki_taxobox_data t
                        JOIN wiki_pages p ON p.id = t.page_row_id
                        WHERE t.scientific_name IS NOT NULL AND t.scientific_name != ''
                        LIMIT @limit"
                    : @"SELECT t.scientific_name, p.page_title, t.data_json
                        FROM wiki_taxobox_data t
                        JOIN wiki_pages p ON p.id = t.page_row_id
                        WHERE t.scientific_name IS NOT NULL AND t.scientific_name != ''";
                if (limit.HasValue) {
                    command.Parameters.AddWithValue("@limit", limit.Value);
                }
            }

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Wikipedia pages[/]", autoStart: true);
                    task.IsIndeterminate = !limit.HasValue;
                    if (limit.HasValue) task.MaxValue = limit.Value;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        if (limit.HasValue) task.Increment(1);

                        string? taxonIdentifier;
                        string pageTitle;
                        string? taxoboxJson;

                        if (useTaxonMatches) {
                            taxonIdentifier = reader.GetString(0); // IUCN SIS ID
                            pageTitle = reader.GetString(1);
                            taxoboxJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                        } else {
                            // Using taxobox scientific name - need to match to our taxa
                            var scientificName = reader.GetString(0);
                            pageTitle = reader.GetString(1);
                            taxoboxJson = reader.IsDBNull(2) ? null : reader.GetString(2);

                            // Clean up scientific name (may contain wiki markup)
                            scientificName = CleanWikiScientificName(scientificName);
                            taxonIdentifier = scientificName; // Use as identifier for matching
                        }

                        // Find the taxon
                        long? taxonId;
                        if (useTaxonMatches) {
                            taxonId = store.FindTaxonBySourceId("iucn", taxonIdentifier!);
                        } else {
                            // Match by scientific name
                            taxonId = store.FindTaxonByScientificName(taxonIdentifier!);
                        }

                        if (!taxonId.HasValue) continue;
                        matched++;

                        // Add page title as common name (cleaned)
                        // BUT skip if the title looks like a scientific name
                        var cleanTitle = CommonNameNormalizer.RemoveDisambiguationSuffix(pageTitle);
                        
                        if (!LooksLikeScientificName(cleanTitle)) {
                            var normalized = CommonNameNormalizer.NormalizeForMatching(cleanTitle);

                            if (normalized != null) {
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
                                titleAdded++;
                            }
                        }

                        // Extract common name from taxobox "name" field
                        if (!string.IsNullOrWhiteSpace(taxoboxJson)) {
                            var taxoboxName = ExtractTaxoboxName(taxoboxJson);
                            if (!string.IsNullOrWhiteSpace(taxoboxName) && !LooksLikeScientificName(taxoboxName)) {
                                var taxoboxNormalized = CommonNameNormalizer.NormalizeForMatching(taxoboxName);
                                var cleanTitleNormalized = CommonNameNormalizer.NormalizeForMatching(cleanTitle);
                                if (taxoboxNormalized != null && taxoboxNormalized != cleanTitleNormalized) {
                                    store.InsertCommonName(
                                        taxonId.Value,
                                        taxoboxName,
                                        taxoboxNormalized,
                                        displayName: null,
                                        "en",
                                        "wikipedia_taxobox",
                                        pageTitle,
                                        isPreferred: false
                                    );
                                    taxoboxAdded++;
                                }
                            }
                        }
                    }
                });

            var totalAdded = titleAdded + taxoboxAdded;
            store.CompleteImportRun(runId, processed, totalAdded, 0, 0,
                $"Matched {matched} pages to taxa, {titleAdded} titles, {taxoboxAdded} taxobox names");
            AnsiConsole.MarkupLine($"[green]Wikipedia:[/] {titleAdded:N0} titles + {taxoboxAdded:N0} taxobox names from {matched:N0} matched pages");
        }, cancellationToken);
    }

    private static string? ExtractTaxoboxName(string json) {
        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            // The taxobox "name" field typically contains the common name
            if (root.TryGetProperty("name", out var nameProp) && nameProp.ValueKind == JsonValueKind.String) {
                var name = nameProp.GetString()?.Trim();
                if (string.IsNullOrWhiteSpace(name)) return null;

                // Clean up wiki markup if present
                // Remove things like [[...]], {{...}}, <ref>...</ref>
                name = System.Text.RegularExpressions.Regex.Replace(name, @"\[\[([^\]|]*\|)?([^\]]*)\]\]", "$2");
                name = System.Text.RegularExpressions.Regex.Replace(name, @"\{\{[^}]*\}\}", "");
                name = System.Text.RegularExpressions.Regex.Replace(name, @"<ref[^>]*>.*?</ref>", "", System.Text.RegularExpressions.RegexOptions.Singleline);
                name = System.Text.RegularExpressions.Regex.Replace(name, @"<[^>]+>", "");
                name = name.Trim();

                // Skip if it looks like a scientific name (italic binomial)
                if (name.Contains("''")) return null;

                // Skip if empty after cleanup
                if (string.IsNullOrWhiteSpace(name)) return null;

                return name;
            }
        } catch (JsonException) {
            // Ignore malformed JSON
        }

        return null;
    }

    private static bool TableHasRows(SqliteConnection connection, string tableName, string? whereClause = null) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = string.IsNullOrWhiteSpace(whereClause)
            ? $"SELECT 1 FROM {tableName} LIMIT 1"
            : $"SELECT 1 FROM {tableName} WHERE {whereClause} LIMIT 1";
        return cmd.ExecuteScalar() != null;
    }

    private static string CleanWikiScientificName(string name) {
        // Remove wiki markup from scientific names
        // e.g., "Panthera leo<ref name=MSW3>...</ref>" -> "Panthera leo"
        name = System.Text.RegularExpressions.Regex.Replace(name, @"<ref[^>]*>.*?</ref>", "", System.Text.RegularExpressions.RegexOptions.Singleline);
        name = System.Text.RegularExpressions.Regex.Replace(name, @"<ref[^/]*/?>", "");
        name = System.Text.RegularExpressions.Regex.Replace(name, @"\[\[([^\]|]*\|)?([^\]]*)\]\]", "$2");
        name = System.Text.RegularExpressions.Regex.Replace(name, @"\{\{[^}]*\}\}", "");
        name = System.Text.RegularExpressions.Regex.Replace(name, @"<[^>]+>", "");
        name = System.Text.RegularExpressions.Regex.Replace(name, @"''", ""); // Remove italic markers
        return name.Trim();
    }

    private static string? GetStringProperty(JsonElement element, string propertyName) {
        if (element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String) {
            return prop.GetString();
        }
        return null;
    }

    /// <summary>
    /// Aggregates English vernacular (common) names from the Catalogue of Life database.
    /// 
    /// This imports common names from COL's vernacularname table, matching them to our existing
    /// IUCN-based taxa by normalized scientific name. Only species/subspecies/variety ranks
    /// with accepted status are imported.
    /// 
    /// Data flow:
    /// 1. Query COL vernacularname joined to nameusage for English names
    /// 2. Normalize the scientific name to match our canonical format
    /// 3. Find matching taxon in our database by canonical name or synonym
    /// 4. Insert the vernacular name with source="col"
    /// </summary>
    private static Task AggregateColVernacularNamesAsync(CommonNameStore store, string colPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating COL vernacular names...[/]");
            AnsiConsole.MarkupLine($"[blue]COL database:[/] {colPath}");

            var runId = store.BeginImportRun("common_names_col");
            var processed = 0;
            var added = 0;
            var matched = 0;
            var skippedNoTaxon = 0;

            using var colConnection = new SqliteConnection($"Data Source={colPath};Mode=ReadOnly");
            colConnection.Open();

            // Query vernacular names with their associated scientific names
            // Join to nameusage to get the scientific name and status
            using var command = colConnection.CreateCommand();
            var sql = @"
                SELECT v.taxonID, v.name, v.language, v.preferred, n.scientificName, n.status, n.rank
                FROM vernacularname v
                JOIN nameusage n ON v.taxonID = n.ID
                WHERE v.language LIKE 'en%'
                  AND v.name IS NOT NULL
                  AND v.name != ''
                  AND n.status = 'accepted'";
            if (limit.HasValue) {
                sql += $" LIMIT {limit.Value}";
            }
            command.CommandText = sql;
            command.CommandTimeout = 0;

            // Count for progress (approximate)
            using var countCommand = colConnection.CreateCommand();
            countCommand.CommandText = "SELECT COUNT(*) FROM vernacularname WHERE language LIKE 'en%'";
            var totalCount = limit.HasValue ? limit.Value : Convert.ToInt32(countCommand.ExecuteScalar() ?? 0);

            AnsiConsole.Progress()
                .AutoClear(true)
                .HideCompleted(true)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]COL vernacular names[/]", autoStart: true);
                    task.MaxValue = totalCount;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        task.Increment(1);

                        var colTaxonId = reader.GetString(0);
                        var vernacularName = reader.GetString(1);
                        var language = reader.IsDBNull(2) ? "en" : reader.GetString(2);
                        var preferred = reader.IsDBNull(3) ? null : reader.GetString(3);
                        var scientificName = reader.GetString(4);
                        var status = reader.IsDBNull(5) ? null : reader.GetString(5);
                        var rank = reader.IsDBNull(6) ? null : reader.GetString(6);

                        // Skip non-species ranks (we want species and subspecies)
                        if (rank != null && rank != "species" && rank != "subspecies" && rank != "variety") {
                            continue;
                        }

                        // Normalize the scientific name
                        var normalizedScientific = ScientificNameNormalizer.Normalize(scientificName);
                        if (normalizedScientific == null) continue;

                        // Try to find the taxon by scientific name (COL uses different IDs than IUCN)
                        var taxonId = store.FindTaxonByScientificName(normalizedScientific);

                        // If not found, try by COL ID
                        if (!taxonId.HasValue) {
                            taxonId = store.FindTaxonBySourceId("col", colTaxonId);
                        }

                        if (!taxonId.HasValue) {
                            skippedNoTaxon++;
                            continue;
                        }
                        matched++;

                        // Normalize the vernacular name
                        var normalized = CommonNameNormalizer.NormalizeForMatching(vernacularName);
                        if (normalized == null) continue;

                        // Normalize language code (col uses "eng", "en", etc.)
                        var normalizedLang = NormalizeLanguageCode(language);

                        // Determine if preferred (COL uses "true"/"false" string or null)
                        var isPreferred = preferred?.Equals("true", StringComparison.OrdinalIgnoreCase) == true;

                        store.InsertCommonName(
                            taxonId.Value,
                            vernacularName,
                            normalized,
                            displayName: null,
                            normalizedLang,
                            "col",
                            colTaxonId,
                            isPreferred
                        );
                        added++;
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, skippedNoTaxon,
                $"Matched {matched} to existing taxa, {skippedNoTaxon} skipped (no matching taxon)");
            AnsiConsole.MarkupLine($"[green]COL:[/] {added:N0} vernacular names from {matched:N0} matched taxa ({skippedNoTaxon:N0} skipped)");
        }, cancellationToken);
    }

    /// <summary>
    /// Aggregates scientific name synonyms from the Catalogue of Life database.
    /// 
    /// COL stores synonyms in the nameusage table with status='synonym' or 'ambiguous synonym'.
    /// The parentID field links synonyms to their accepted taxon. This method imports these
    /// synonyms for taxa that exist in our database (matched from IUCN).
    /// 
    /// Data flow:
    /// 1. Query COL nameusage for synonym records joined to their accepted names
    /// 2. Normalize both synonym and accepted scientific names
    /// 3. Find matching taxon in our database by the accepted name
    /// 4. Insert the synonym with source="col" and appropriate synonym_type
    /// 
    /// Note: The number of imported synonyms depends on overlap between COL's accepted
    /// names and our IUCN-based taxa. Many COL species aren't assessed by IUCN.
    /// </summary>
    private static Task AggregateColSynonymsAsync(CommonNameStore store, string colPath, int? limit, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Aggregating COL scientific name synonyms...[/]");
            AnsiConsole.MarkupLine($"[blue]COL database:[/] {colPath}");

            var runId = store.BeginImportRun("synonyms_col");
            var processed = 0;
            var added = 0;
            var skippedNoTaxon = 0;
            var matched = 0;

            using var colConnection = new SqliteConnection($"Data Source={colPath};Mode=ReadOnly");
            colConnection.Open();

            using var command = colConnection.CreateCommand();
            // Get synonyms: join synonym records to their accepted name (via parentID)
            // We need the synonym scientific name and the accepted name's scientific name to match to our taxa
            var sql = """
                SELECT 
                    s.ID as synonym_id,
                    s.scientificName as synonym_name,
                    s.status as synonym_status,
                    s.rank as synonym_rank,
                    a.scientificName as accepted_name,
                    a.ID as accepted_id
                FROM nameusage s
                JOIN nameusage a ON s.parentID = a.ID
                WHERE s.status IN ('synonym', 'ambiguous synonym')
                AND a.status = 'accepted'
                AND s.rank IN ('species', 'subspecies', 'variety')
                """;

            if (limit.HasValue) {
                sql += $" LIMIT {limit.Value}";
            }
            command.CommandText = sql;
            command.CommandTimeout = 0;

            // Count for progress (approximate)
            using var countCommand = colConnection.CreateCommand();
            countCommand.CommandText = "SELECT COUNT(*) FROM nameusage WHERE status IN ('synonym', 'ambiguous synonym')";
            var totalCount = limit.HasValue ? limit.Value : Convert.ToInt32(countCommand.ExecuteScalar() ?? 0);

            AnsiConsole.Progress()
                .AutoClear(true)
                .HideCompleted(true)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]COL synonyms[/]", autoStart: true);
                    task.MaxValue = totalCount;

                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processed++;
                        task.Increment(1);

                        var synonymId = reader.GetString(0);
                        var synonymName = reader.GetString(1);
                        var synonymStatus = reader.IsDBNull(2) ? null : reader.GetString(2);
                        var synonymRank = reader.IsDBNull(3) ? null : reader.GetString(3);
                        var acceptedName = reader.GetString(4);
                        var acceptedId = reader.GetString(5);

                        // Normalize both names
                        var normalizedSynonym = ScientificNameNormalizer.Normalize(synonymName);
                        var normalizedAccepted = ScientificNameNormalizer.Normalize(acceptedName);

                        if (normalizedSynonym == null || normalizedAccepted == null) continue;

                        // Try to find the accepted taxon in our database
                        var taxonId = store.FindTaxonByScientificName(normalizedAccepted);
                        if (!taxonId.HasValue) {
                            taxonId = store.FindTaxonBySourceId("col", acceptedId);
                        }

                        if (!taxonId.HasValue) {
                            skippedNoTaxon++;
                            continue;
                        }
                        matched++;

                        // Determine synonym type
                        var synonymType = synonymStatus == "ambiguous synonym" ? "ambiguous_synonym" : "synonym";

                        store.InsertSynonym(
                            taxonId.Value,
                            normalizedSynonym,
                            synonymName,
                            "col",
                            synonymType
                        );
                        added++;
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, skippedNoTaxon,
                $"Matched {matched} synonyms to existing taxa, {skippedNoTaxon} skipped (no matching taxon)");
            AnsiConsole.MarkupLine($"[green]COL synonyms:[/] {added:N0} synonyms from {matched:N0} matched taxa ({skippedNoTaxon:N0} skipped)");
        }, cancellationToken);
    }

    /// <summary>
    /// Heuristic to detect if a string looks like a scientific name (binomial nomenclature).
    /// Scientific names: "Genus species" or "Genus species subspecies" with first word capitalized,
    /// subsequent words lowercase, typically Latin/Greek roots.
    /// </summary>
    private static bool LooksLikeScientificName(string name) {
        if (string.IsNullOrWhiteSpace(name)) return false;
        
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        
        // Scientific names are typically 2-4 words
        if (words.Length < 2 || words.Length > 4) return false;
        
        // First word (genus) should be capitalized
        if (!char.IsUpper(words[0][0])) return false;
        
        // Second word (species epithet) should be all lowercase
        if (words.Length >= 2 && words[1].Any(char.IsUpper)) return false;
        
        // If 3+ words, check if they follow scientific name patterns
        // (subspecies, variety markers like "var.", "subsp.")
        if (words.Length >= 3) {
            var third = words[2];
            // If third word is all lowercase or a taxonomic marker, likely scientific
            if (third.All(c => char.IsLower(c) || c == '.')) return true;
            // If third word has capitals, probably not scientific (e.g., "American Black Bear")
            if (third.Any(char.IsUpper)) return false;
        }
        
        // Check for common Latin/Greek species epithet endings
        var epithet = words[1].ToLowerInvariant();
        var latinEndings = new[] { "ii", "ae", "is", "us", "um", "a", "ensis", "oides", "ica", "icum", "icus" };
        if (latinEndings.Any(ending => epithet.EndsWith(ending))) return true;
        
        // If first word ends in common genus patterns and second is lowercase, likely scientific
        var genus = words[0].ToLowerInvariant();
        var genusEndings = new[] { "us", "a", "um", "is", "on", "ia", "ops", "yx", "ax" };
        if (genusEndings.Any(ending => genus.EndsWith(ending)) && words[1].All(char.IsLower)) return true;
        
        return false;
    }

    /// <summary>
    /// Checks if a single word looks like a species epithet (the second word of a binomial name).
    /// Used to filter out IUCN data entry errors where only the epithet was entered as a "common name".
    /// Examples: "afer", "affinis", "bilobatus", "zrmanjae"
    /// </summary>
    private static bool IsLikelySpeciesEpithet(string word) {
        if (string.IsNullOrWhiteSpace(word)) return false;
        if (word.Length < 3) return false;

        // Must be all lowercase (or all uppercase which we'll normalize)
        var lower = word.ToLowerInvariant();
        if (word != lower && word != word.ToUpperInvariant()) {
            // Mixed case is likely a real common name
            return false;
        }

        // Check for common Latin/Greek species epithet endings
        var latinEndings = new[] {
            "ii", "ae", "is", "us", "um", "ensis", "oides", "ica", "icum", "icus",
            "atus", "ata", "atum", "inus", "ina", "inum", "alis", "ale", "ilis", "ile",
            "osus", "osa", "osum", "eus", "ea", "eum", "ifer", "ifera", "iferum",
            "anus", "ana", "anum", "ensis", "ense"
        };

        if (latinEndings.Any(ending => lower.EndsWith(ending) && lower.Length > ending.Length + 2)) {
            return true;
        }

        // Single lowercase word that's 4+ letters and looks Latin is suspicious
        // But we need to avoid filtering real common names like "toad", "frog", etc.
        // So we only flag words with clearly Latin patterns
        return false;
    }}