using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

/// <summary>
/// Generates reports about common name conflicts and capitalization issues.
/// </summary>
internal sealed class CommonNameReportCommand : AsyncCommand<CommonNameReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--iucn-db <PATH>")]
        [Description("Path to the IUCN SQLite database (for trace sampling and taxonomy context).")]
        public string? IucnDatabasePath { get; init; }

        [CommandOption("--report <TYPE>")]
        [Description("Report type: ambiguous, ambiguous-iucn, caps, summary, wiki-disambig, iucn-preferred, trace, all (default: summary)")]
        public string ReportType { get; init; } = "summary";

        [CommandOption("-o|--output <PATH>")]
        [Description("Output file path for Markdown report.")]
        public string? OutputPath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Limit number of items in report.")]
        public int? Limit { get; init; }

        [CommandOption("--trace-per-group <N>")]
        [Description("Number of taxa to include per major group for trace report (default: 20).")]
        public int? TracePerGroup { get; init; }

        [CommandOption("--kingdom <KINGDOM>")]
        [Description("Filter by kingdom (Animalia, Plantae, etc.)")]
        public string? Kingdom { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile);
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath);

        AnsiConsole.MarkupLine($"[blue]Common name store:[/] {commonNameDbPath}");

        using var store = CommonNameStore.Open(commonNameDbPath);

        var reportType = settings.ReportType.ToLowerInvariant();

        return reportType switch {
            "ambiguous" => await GenerateAmbiguousReportAsync(store, settings, paths, cancellationToken),
            "ambiguous-iucn" => await GenerateAmbiguousIucnReportAsync(store, settings, paths, cancellationToken),
            "caps" => await GenerateCapsReportAsync(store, settings, paths, cancellationToken),
            "wiki-disambig" => await GenerateWikiDisambigReportAsync(store, settings, paths, cancellationToken),
            "iucn-preferred" => await GenerateIucnPreferredConflictReportAsync(store, settings, paths, cancellationToken),
            "trace" => await GenerateCommonNameTraceReportAsync(store, settings, paths, cancellationToken),
            "all" => await GenerateAllReportsAsync(store, settings, paths, cancellationToken),
            "summary" or _ => await GenerateSummaryReportAsync(store, settings, cancellationToken)
        };
    }

    private static Task<int> GenerateSummaryReportAsync(CommonNameStore store, Settings settings, CancellationToken cancellationToken) {
        return Task.Run(() => {
            var stats = store.GetStatistics();

            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[yellow]Common Name Store Summary[/]"));
            AnsiConsole.WriteLine();

            var table = new Table();
            table.AddColumn("Metric");
            table.AddColumn(new TableColumn("Count").RightAligned());
            table.AddRow("Taxa", stats.TaxaCount.ToString("N0"));
            table.AddRow("Scientific Name Synonyms", stats.SynonymCount.ToString("N0"));
            table.AddRow("Common Names (total)", stats.CommonNameCount.ToString("N0"));
            table.AddRow("Detected Conflicts", stats.ConflictCount.ToString("N0"));
            table.AddRow("Caps Rules", store.GetCapsRuleCount().ToString("N0"));
            AnsiConsole.Write(table);

            return 0;
        }, cancellationToken);
    }

    private static async Task<int> GenerateAllReportsAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        AnsiConsole.MarkupLine("[blue]Generating all reports...[/]");
        AnsiConsole.WriteLine();

        var results = new List<(string Name, int Result)>();

        results.Add(("Ambiguous", await GenerateAmbiguousReportAsync(store, settings, paths, cancellationToken)));
        results.Add(("Ambiguous-IUCN", await GenerateAmbiguousIucnReportAsync(store, settings, paths, cancellationToken)));
        results.Add(("Caps", await GenerateCapsReportAsync(store, settings, paths, cancellationToken)));
        results.Add(("Wiki-Disambig", await GenerateWikiDisambigReportAsync(store, settings, paths, cancellationToken)));
        results.Add(("IUCN-Preferred", await GenerateIucnPreferredConflictReportAsync(store, settings, paths, cancellationToken)));
        results.Add(("Trace", await GenerateCommonNameTraceReportAsync(store, settings, paths, cancellationToken)));

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]All reports completed![/]");

        var failedCount = results.Count(r => r.Result != 0);
        return failedCount > 0 ? 1 : 0;
    }

    private static Task<int> GenerateAmbiguousReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating ambiguous common names report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Ambiguous Common Names Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();

            // Use efficient SQL query to find ambiguous names directly (no default limit)
            var ambiguousNames = store.GetAmbiguousCommonNames(settings.Limit, settings.Kingdom);
            var conflictingNames = new List<(string NormalizedName, List<CommonNameRecord> Records)>();

            AnsiConsole.Progress()
                .AutoClear(true)
                .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Loading conflicts[/]", maxValue: ambiguousNames.Count);
                    foreach (var normalizedName in ambiguousNames) {
                        cancellationToken.ThrowIfCancellationRequested();
                        task.Increment(1);

                        var records = store.GetCommonNamesByNormalized(normalizedName, "en")
                            .Where(r => r.TaxonValidityStatus == "valid" && !r.TaxonIsFossil)
                            .ToList();

                        if (!string.IsNullOrWhiteSpace(settings.Kingdom)) {
                            records = records.Where(r =>
                                r.TaxonKingdom?.Equals(settings.Kingdom, StringComparison.OrdinalIgnoreCase) == true).ToList();
                        }

                        if (records.Select(r => r.TaxonId).Distinct().Count() > 1) {
                            conflictingNames.Add((normalizedName, records));
                        }
                    }
                });

            sb.AppendLine($"## Ambiguous Names ({conflictingNames.Count} found)");
            sb.AppendLine();

            foreach (var (normalizedName, records) in conflictingNames) {
                var displayName = records.FirstOrDefault()?.RawName ?? normalizedName;
                sb.AppendLine($"### {displayName}");
                sb.AppendLine();
                sb.AppendLine("| Scientific Name | Kingdom | Source | Preferred |");
                sb.AppendLine("|-----------------|---------|--------|-----------|");

                var groupedByTaxon = records.GroupBy(r => r.TaxonId);
                foreach (var taxonGroup in groupedByTaxon) {
                    var first = taxonGroup.First();
                    var sources = string.Join(", ", taxonGroup.Select(r => r.Source).Distinct());
                    var isPreferred = taxonGroup.Any(r => r.IsPreferred) ? "Yes" : "No";
                    var scientificName = CapitalizeFirst(first.TaxonCanonicalName);
                    sb.AppendLine($"| {scientificName} | {first.TaxonKingdom ?? "?"} | {sources} | {isPreferred} |");
                }
                sb.AppendLine();
            }

            // Output - save to disk by default
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-ambiguous-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            AnsiConsole.MarkupLine($"[green]Found {conflictingNames.Count} ambiguous common names[/]");
            return 0;
        }, cancellationToken);
    }

    private static Task<int> GenerateCapsReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Checking for missing capitalization rules...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Missing Capitalization Rules Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();

            // Load all caps rules into memory once (instead of querying per word)
            AnsiConsole.MarkupLine("[grey]Loading caps rules...[/]");
            var capsRules = store.GetAllCapsRules();
            AnsiConsole.MarkupLine($"[grey]Loaded {capsRules.Count:N0} caps rules[/]");

            // Get all distinct raw common names (much faster than iterating normalized -> records)
            AnsiConsole.MarkupLine("[grey]Loading common names...[/]");
            var rawNames = store.GetDistinctRawCommonNames("en", settings.Limit);
            AnsiConsole.MarkupLine($"[grey]Loaded {rawNames.Count:N0} distinct raw names[/]");

            var missingWords = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            // Process with progress bar
            AnsiConsole.Progress()
                .AutoClear(true)
                .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn(), new SpinnerColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Scanning for missing caps[/]", maxValue: rawNames.Count);

                    foreach (var rawName in rawNames) {
                        cancellationToken.ThrowIfCancellationRequested();

                        // Check each word against in-memory caps rules (fast!)
                        var words = CommonNameNormalizer.FindMissingCapsWords(
                            rawName,
                            word => capsRules.ContainsKey(word.ToLowerInvariant())
                        );

                        foreach (var word in words) {
                            var lower = word.ToLowerInvariant();
                            if (!missingWords.TryGetValue(lower, out var examples)) {
                                examples = new List<string>();
                                missingWords[lower] = examples;
                            }
                            if (examples.Count < 3) { // Keep up to 3 examples
                                examples.Add(rawName);
                            }
                        }

                        task.Increment(1);
                    }
                });

            // Sort by frequency (most common missing words first)
            var sortedMissing = missingWords
                .OrderByDescending(kvp => kvp.Value.Count)
                .ThenBy(kvp => kvp.Key)
                .ToList();

            sb.AppendLine($"## Missing Words ({sortedMissing.Count} found)");
            sb.AppendLine();
            sb.AppendLine("Words that appear in common names but have no capitalization rule in caps.txt:");
            sb.AppendLine();
            sb.AppendLine("| Word | Example Names |");
            sb.AppendLine("|------|---------------|");

            foreach (var (word, examples) in sortedMissing.Take(settings.Limit ?? 200)) {
                var exampleStr = string.Join("; ", examples.Take(2));
                sb.AppendLine($"| {word} | {exampleStr} |");
            }

            sb.AppendLine();
            sb.AppendLine("### Suggested caps.txt entries");
            sb.AppendLine();
            sb.AppendLine("```");
            foreach (var (word, examples) in sortedMissing) {
                // Guess capitalization based on patterns
                var guessedForm = GuessCapitalization(word, examples);
                var exampleStr = examples.FirstOrDefault() ?? "";
                sb.AppendLine($"{guessedForm} // {exampleStr}");
            }
            sb.AppendLine("```");

            // Output - save to disk by default
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-caps-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            AnsiConsole.MarkupLine($"[green]Found {sortedMissing.Count} words missing caps rules[/]");
            return 0;
        }, cancellationToken);
    }

    private sealed record TaxonGroup(string Name, string? ClassName, string? Kingdom);

    private sealed record TaxonTraceRow(
        long TaxonId,
        string ScientificName,
        string? Kingdom,
        string? ClassName,
        string? OrderName,
        string? FamilyName,
        string GenusName,
        string SpeciesName
    );

    private sealed record TraceCandidate(
        CommonNameRecord Record,
        string CleanedName,
        int Priority,
        bool IsEnglish,
        bool IsAmbiguous,
        bool MatchesScientific,
        bool HasDisambigSuffix
    );

    private sealed record TraceIssue(string GroupName, string ScientificName, string IssueType, string Details);

    private static Task<int> GenerateCommonNameTraceReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating common name trace report...[/]");

            var iucnDbPath = paths.ResolveIucnDatabasePath(settings.IucnDatabasePath);
            AnsiConsole.MarkupLine($"[blue]IUCN database:[/] {iucnDbPath}");

            using var iucnConnection = new SqliteConnection($"Data Source={iucnDbPath};Mode=ReadOnly");
            iucnConnection.Open();

            if (!ObjectExists(iucnConnection, "view_assessments_html_taxonomy_html", "view")) {
                var sbMissing = new StringBuilder();
                sbMissing.AppendLine("# Common Name Trace Report");
                sbMissing.AppendLine();
                sbMissing.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
                sbMissing.AppendLine();
                sbMissing.AppendLine("**Error:** Missing view `view_assessments_html_taxonomy_html` in the IUCN database.");
                sbMissing.AppendLine("Re-run the IUCN importer to rebuild the view, then re-run this report.");

                var timestampMissing = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
                var defaultFileNameMissing = $"common-name-trace-{timestampMissing}.md";
                var outputPathMissing = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileNameMissing);
                File.WriteAllText(outputPathMissing, sbMissing.ToString());
                AnsiConsole.MarkupLine($"[red]Missing view view_assessments_html_taxonomy_html.[/] Report written to: {outputPathMissing}");
                return 1;
            }

            var perGroup = settings.TracePerGroup ?? settings.Limit ?? 20;

            var groups = new List<TaxonGroup> {
                new("Mammals", "Mammalia", null),
                new("Birds", "Aves", null),
                new("Reptiles", "Reptilia", null),
                new("Amphibians", "Amphibia", null),
                new("Ray-finned fishes", "Actinopterygii", null),
                new("Cartilaginous fishes", "Chondrichthyes", null),
                new("Insects", "Insecta", null),
                new("Arachnids", "Arachnida", null),
                new("Molluscs", "Mollusca", null),
                new("Plants", null, "Plantae"),
                new("Fungi", null, "Fungi")
            };

            var sb = new StringBuilder();
            sb.AppendLine("# Common Name Trace Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine($"Per-group sample size: {perGroup}");
            sb.AppendLine();
            sb.AppendLine("This report traces common-name selection for representative taxa, showing candidate names, rejection reasons, and source priority.");
            sb.AppendLine();

            var issues = new List<TraceIssue>();
            var ambiguousSet = store.GetAmbiguousNames("en");
            var capsRules = store.GetAllCapsRules();

            foreach (var group in groups) {
                cancellationToken.ThrowIfCancellationRequested();

                var taxa = LoadTraceTaxa(iucnConnection, group, perGroup);
                if (taxa.Count == 0) {
                    continue;
                }

                sb.AppendLine($"## {group.Name} ({taxa.Count} taxa)");
                sb.AppendLine();

                foreach (var taxon in taxa) {
                    cancellationToken.ThrowIfCancellationRequested();

                    var storeTaxonId = store.FindTaxonBySourceId("iucn", taxon.TaxonId.ToString());
                    if (!storeTaxonId.HasValue) {
                        continue;
                    }

                    var candidates = store.GetCommonNamesForTaxonAllLanguages(storeTaxonId.Value);
                    var scientificNames = store.GetScientificNamesForTaxon(storeTaxonId.Value);
                    var scientificNormalized = BuildScientificNormalizedSet(scientificNames);

                    var traceCandidates = candidates
                        .Select(record => BuildTraceCandidate(record, taxon, ambiguousSet, scientificNormalized))
                        .ToList();

                    var englishCandidates = traceCandidates.Where(c => c.IsEnglish).ToList();
                    var selected = SelectBestCandidate(englishCandidates);

                    sb.AppendLine($"### {taxon.ScientificName}");
                    sb.AppendLine();
                    sb.AppendLine($"- IUCN taxon id: {taxon.TaxonId}");
                    if (!string.IsNullOrWhiteSpace(taxon.ClassName)) sb.AppendLine($"- Class: {taxon.ClassName}");
                    if (!string.IsNullOrWhiteSpace(taxon.OrderName)) sb.AppendLine($"- Order: {taxon.OrderName}");
                    if (!string.IsNullOrWhiteSpace(taxon.FamilyName)) sb.AppendLine($"- Family: {taxon.FamilyName}");

                    if (selected != null) {
                        var displayName = GetDisplayName(selected.Record, capsRules);
                        sb.AppendLine($"- Selected common name: **{selected.Record.RawName}** (source: {selected.Record.Source}, preferred: {(selected.Record.IsPreferred ? "yes" : "no")}, priority: {selected.Priority})");
                        sb.AppendLine($"- Selected display name: **{displayName}**");
                    } else {
                        sb.AppendLine("- Selected common name: **(none)**");
                    }
                    sb.AppendLine();

                    sb.AppendLine("**English candidates**");
                    sb.AppendLine();
                    sb.AppendLine("| Candidate | Source | Preferred | Priority | Ambiguous | Rejected | Reason | Notes |");
                    sb.AppendLine("|----------|--------|-----------|----------|-----------|----------|--------|-------|");

                    foreach (var candidate in englishCandidates.OrderBy(c => c.Priority).ThenByDescending(c => c.Record.IsPreferred).ThenBy(c => c.Record.RawName, StringComparer.OrdinalIgnoreCase)) {
                        var rejected = candidate.MatchesScientific || candidate.IsAmbiguous;
                        var reason = new List<string>();
                        if (candidate.MatchesScientific) reason.Add("matches scientific name");
                        if (candidate.IsAmbiguous) reason.Add("ambiguous");

                        var notes = new List<string>();
                        if (candidate.HasDisambigSuffix) notes.Add("disambiguation suffix removed");

                        sb.AppendLine($"| {candidate.CleanedName} | {candidate.Record.Source} | {(candidate.Record.IsPreferred ? "Yes" : "No")} | {candidate.Priority} | {(candidate.IsAmbiguous ? "Yes" : "No")} | {(rejected ? "Yes" : "No")} | {string.Join("; ", reason)} | {string.Join("; ", notes)} |");
                    }
                    sb.AppendLine();

                    var nonEnglish = traceCandidates.Where(c => !c.IsEnglish).ToList();
                    if (nonEnglish.Count > 0) {
                        sb.AppendLine("**Non-English names (for debugging)**");
                        sb.AppendLine();
                        foreach (var candidate in nonEnglish.OrderBy(c => c.Record.Language).ThenBy(c => c.Record.RawName, StringComparer.OrdinalIgnoreCase)) {
                            sb.AppendLine($"- {candidate.CleanedName} [{candidate.Record.Language}] (source: {candidate.Record.Source})");
                        }
                        sb.AppendLine();
                    }

                    AddTraceIssues(issues, group.Name, taxon.ScientificName, englishCandidates, selected);
                }
            }

            if (issues.Count > 0) {
                sb.AppendLine("## Trace issues to investigate");
                sb.AppendLine();
                sb.AppendLine("| Group | Scientific name | Issue | Details |");
                sb.AppendLine("|-------|-----------------|-------|---------|");
                foreach (var issue in issues) {
                    sb.AppendLine($"| {issue.GroupName} | {issue.ScientificName} | {issue.IssueType} | {issue.Details} |");
                }
                sb.AppendLine();
            }

            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-trace-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            return 0;
        }, cancellationToken);
    }

    private static IReadOnlyList<TaxonTraceRow> LoadTraceTaxa(SqliteConnection connection, TaxonGroup group, int perGroup) {
        using var command = connection.CreateCommand();
        var whereClause = new List<string> {
            "(infraType IS NULL OR infraType = '')",
            "(subpopulationName IS NULL OR subpopulationName = '')"
        };

        if (!string.IsNullOrWhiteSpace(group.ClassName)) {
            whereClause.Add("TRIM(className) = @class COLLATE NOCASE");
            command.Parameters.AddWithValue("@class", group.ClassName);
        }

        if (!string.IsNullOrWhiteSpace(group.Kingdom)) {
            whereClause.Add("TRIM(kingdomName) = @kingdom COLLATE NOCASE");
            command.Parameters.AddWithValue("@kingdom", group.Kingdom);
        }

        var where = string.Join(" AND ", whereClause);
        command.CommandText = $@"
            SELECT DISTINCT taxonId, scientificName_taxonomy, kingdomName, className, orderName, familyName, genusName, speciesName
            FROM view_assessments_html_taxonomy_html
            WHERE {where}
            ORDER BY taxonId
            LIMIT @limit;";
        command.Parameters.AddWithValue("@limit", perGroup);

        var results = new List<TaxonTraceRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var taxonId = reader.GetInt64(0);
            var sci = reader.IsDBNull(1) ? null : reader.GetString(1);
            var kingdom = reader.IsDBNull(2) ? null : reader.GetString(2);
            var className = reader.IsDBNull(3) ? null : reader.GetString(3);
            var orderName = reader.IsDBNull(4) ? null : reader.GetString(4);
            var familyName = reader.IsDBNull(5) ? null : reader.GetString(5);
            var genusName = reader.GetString(6);
            var speciesName = reader.GetString(7);

            var scientificName = sci ?? $"{genusName} {speciesName}";

            results.Add(new TaxonTraceRow(
                taxonId,
                scientificName,
                kingdom,
                className,
                orderName,
                familyName,
                genusName,
                speciesName
            ));
        }

        return results;
    }

    private static bool ObjectExists(SqliteConnection connection, string name, string type) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = @type AND name = @name LIMIT 1";
        command.Parameters.AddWithValue("@type", type);
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    private static HashSet<string> BuildScientificNormalizedSet(IReadOnlyList<string> scientificNames) {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in scientificNames) {
            var normalized = ScientificNameNormalizer.Normalize(name) ?? name;
            var key = CommonNameNormalizer.NormalizeForMatching(normalized);
            if (!string.IsNullOrWhiteSpace(key)) {
                set.Add(key);
            }
        }
        return set;
    }

    private static TraceCandidate BuildTraceCandidate(
        CommonNameRecord record,
        TaxonTraceRow taxon,
        IReadOnlySet<string> ambiguousSet,
        HashSet<string> scientificNormalized) {
        var isEnglish = record.Language.Equals("en", StringComparison.OrdinalIgnoreCase);
        var cleaned = CommonNameNormalizer.RemoveDisambiguationSuffix(record.RawName);
        var hasDisambigSuffix = !string.Equals(cleaned, record.RawName, StringComparison.Ordinal);

        var normalizedCandidate = record.NormalizedName;
        var matchesScientific = scientificNormalized.Contains(normalizedCandidate)
            || CommonNameNormalizer.LooksLikeScientificName(cleaned, taxon.GenusName, taxon.SpeciesName);

        var isAmbiguous = isEnglish && ambiguousSet.Contains(normalizedCandidate);
        var priority = CommonNameStore.GetSourcePriority(record.Source, record.IsPreferred);

        return new TraceCandidate(
            record,
            cleaned,
            priority,
            isEnglish,
            isAmbiguous,
            matchesScientific,
            hasDisambigSuffix
        );
    }

    private static TraceCandidate? SelectBestCandidate(IReadOnlyList<TraceCandidate> candidates) {
        return candidates
            .Where(c => !c.MatchesScientific && !c.IsAmbiguous)
            .OrderBy(c => c.Priority)
            .ThenByDescending(c => c.Record.IsPreferred)
            .ThenBy(c => c.Record.RawName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
    }

    private static string GetDisplayName(CommonNameRecord record, IReadOnlyDictionary<string, string> capsRules) {
        var baseName = record.DisplayName ?? record.RawName;
        return CommonNameNormalizer.ApplyCapitalization(baseName, word =>
            capsRules.TryGetValue(word, out var value) ? value : null);
    }

    private static void AddTraceIssues(
        List<TraceIssue> issues,
        string groupName,
        string scientificName,
        IReadOnlyList<TraceCandidate> englishCandidates,
        TraceCandidate? selected) {
        var rejectedScientific = englishCandidates.Count(c => c.MatchesScientific);
        var rejectedAmbiguous = englishCandidates.Count(c => c.IsAmbiguous);
        var acceptableCount = englishCandidates.Count(c => !c.MatchesScientific && !c.IsAmbiguous);

        if (acceptableCount == 0 && englishCandidates.Count > 0) {
            issues.Add(new TraceIssue(groupName, scientificName, "No acceptable English name", $"{englishCandidates.Count} candidates, all rejected"));
        }

        if (rejectedScientific >= 3) {
            issues.Add(new TraceIssue(groupName, scientificName, "Many scientific-name-like candidates", $"{rejectedScientific} rejected"));
        }

        if (rejectedAmbiguous >= 3) {
            issues.Add(new TraceIssue(groupName, scientificName, "Many ambiguous candidates", $"{rejectedAmbiguous} rejected"));
        }

        if (selected != null && selected.Priority >= 6) {
            issues.Add(new TraceIssue(groupName, scientificName, "Selected from low-priority source", $"priority {selected.Priority} ({selected.Record.Source})"));
        }
    }

    private static string GuessCapitalization(string word, List<string> examples) {
        // Check if word appears capitalized in examples
        foreach (var example in examples) {
            var words = example.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            foreach (var w in words.Skip(1)) { // Skip first word (always capitalized)
                if (w.Equals(word, StringComparison.OrdinalIgnoreCase)) {
                    // Found the word - check its case
                    if (char.IsUpper(w[0])) {
                        return w; // Return as found (capitalized)
                    }
                }
            }
        }

        // Default to lowercase if found lowercase or not found
        return word.ToLowerInvariant();
    }

    private static string CapitalizeFirst(string s) {
        if (string.IsNullOrEmpty(s)) return s;
        return char.ToUpperInvariant(s[0]) + s.Substring(1);
    }

    /// <summary>
    /// Report ambiguous common names that have at least one IUCN preferred source.
    /// </summary>
    private static Task<int> GenerateAmbiguousIucnReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating ambiguous IUCN-preferred common names report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Ambiguous Common Names Report (IUCN Preferred)");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();
            sb.AppendLine("This report lists common names that are ambiguous (map to multiple species)");
            sb.AppendLine("where at least one usage is marked as IUCN preferred (main=true).");
            sb.AppendLine();

            // Use efficient SQL query to find ambiguous names (no default limit)
            var ambiguousNames = store.GetAmbiguousCommonNames(settings.Limit, settings.Kingdom);
            var conflictingNames = new List<(string NormalizedName, List<CommonNameRecord> Records)>();

            AnsiConsole.Progress()
                .AutoClear(true)
                .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Loading conflicts[/]", maxValue: ambiguousNames.Count);
                    foreach (var normalizedName in ambiguousNames) {
                        cancellationToken.ThrowIfCancellationRequested();
                        task.Increment(1);

                        var records = store.GetCommonNamesByNormalized(normalizedName, "en")
                            .Where(r => r.TaxonValidityStatus == "valid" && !r.TaxonIsFossil)
                            .ToList();

                        if (!string.IsNullOrWhiteSpace(settings.Kingdom)) {
                            records = records.Where(r =>
                                r.TaxonKingdom?.Equals(settings.Kingdom, StringComparison.OrdinalIgnoreCase) == true).ToList();
                        }

                        // Only include if there's at least one IUCN preferred record
                        var hasIucnPreferred = records.Any(r => r.Source == "iucn" && r.IsPreferred);
                        if (!hasIucnPreferred) continue;

                        if (records.Select(r => r.TaxonId).Distinct().Count() > 1) {
                            conflictingNames.Add((normalizedName, records));
                        }
                    }
                });

            sb.AppendLine($"## Ambiguous Names with IUCN Preferred ({conflictingNames.Count} found)");
            sb.AppendLine();

            foreach (var (normalizedName, records) in conflictingNames) {
                var displayName = records.FirstOrDefault()?.RawName ?? normalizedName;
                sb.AppendLine($"### {displayName}");
                sb.AppendLine();
                sb.AppendLine("| Scientific Name | Kingdom | Source | Preferred |");
                sb.AppendLine("|-----------------|---------|--------|-----------|");

                var groupedByTaxon = records.GroupBy(r => r.TaxonId);
                foreach (var taxonGroup in groupedByTaxon) {
                    var first = taxonGroup.First();
                    var sources = string.Join(", ", taxonGroup.Select(r => r.Source).Distinct());
                    var isPreferred = taxonGroup.Any(r => r.IsPreferred) ? "Yes" : "No";
                    var scientificName = CapitalizeFirst(first.TaxonCanonicalName);
                    sb.AppendLine($"| {scientificName} | {first.TaxonKingdom ?? "?"} | {sources} | {isPreferred} |");
                }
                sb.AppendLine();
            }

            // Output - save to disk by default
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-ambiguous-iucn-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            AnsiConsole.MarkupLine($"[green]Found {conflictingNames.Count} ambiguous common names with IUCN preferred[/]");
            return 0;
        }, cancellationToken);
    }

    /// <summary>
    /// Report Wikipedia titles that are ambiguous (same title maps to multiple species).
    /// </summary>
    private static Task<int> GenerateWikiDisambigReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating Wikipedia disambiguation report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Wikipedia Disambiguation Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();
            sb.AppendLine("This report identifies Wikipedia page titles that could refer to multiple species.");
            sb.AppendLine("These may need disambiguation on Wikipedia.");
            sb.AppendLine();

            // Use efficient SQL query to find Wikipedia ambiguous names directly (no default limit)
            var wikiAmbiguousNames = store.GetWikipediaAmbiguousNames(settings.Limit, settings.Kingdom);
            var ambiguousWikiTitles = new List<(string NormalizedName, List<CommonNameRecord> Records)>();

            AnsiConsole.Progress()
                .AutoClear(true)
                .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn())
                .Start(ctx => {
                    var task = ctx.AddTask("[green]Loading Wikipedia conflicts[/]", maxValue: wikiAmbiguousNames.Count);
                    foreach (var normalizedName in wikiAmbiguousNames) {
                        cancellationToken.ThrowIfCancellationRequested();
                        task.Increment(1);

                        var records = store.GetCommonNamesByNormalized(normalizedName, "en")
                            .Where(r => r.TaxonValidityStatus == "valid" && !r.TaxonIsFossil)
                            .ToList();

                        if (!string.IsNullOrWhiteSpace(settings.Kingdom)) {
                            records = records.Where(r =>
                                r.TaxonKingdom?.Equals(settings.Kingdom, StringComparison.OrdinalIgnoreCase) == true).ToList();
                        }

                        if (records.Select(r => r.TaxonId).Distinct().Count() > 1) {
                            ambiguousWikiTitles.Add((normalizedName, records));
                        }
                    }
                });

            sb.AppendLine($"## Ambiguous Wikipedia Titles ({ambiguousWikiTitles.Count} found)");
            sb.AppendLine();

            foreach (var (normalizedName, records) in ambiguousWikiTitles) {
                var displayName = records.FirstOrDefault(r => r.Source.StartsWith("wikipedia"))?.RawName
                    ?? records.FirstOrDefault()?.RawName
                    ?? normalizedName;
                sb.AppendLine($"### {displayName}");
                sb.AppendLine();
                sb.AppendLine("| Scientific Name | Kingdom | Source |");
                sb.AppendLine("|-----------------|---------|--------|");

                var groupedByTaxon = records.GroupBy(r => r.TaxonId);
                foreach (var taxonGroup in groupedByTaxon) {
                    var first = taxonGroup.First();
                    var sources = string.Join(", ", taxonGroup.Select(r => r.Source).Distinct());
                    var scientificName = CapitalizeFirst(first.TaxonCanonicalName);
                    sb.AppendLine($"| {scientificName} | {first.TaxonKingdom ?? "?"} | {sources} |");
                }
                sb.AppendLine();
            }

            // Output
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-wiki-disambig-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            AnsiConsole.MarkupLine($"[green]Found {ambiguousWikiTitles.Count} ambiguous Wikipedia titles[/]");
            return 0;
        }, cancellationToken);
    }

    /// <summary>
    /// Report IUCN preferred common names that are marked preferred for multiple species.
    /// </summary>
    private static Task<int> GenerateIucnPreferredConflictReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating IUCN preferred name conflict report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# IUCN Preferred Name Conflicts Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();
            sb.AppendLine("This report identifies common names that are marked as 'preferred' (main=true) by IUCN");
            sb.AppendLine("for multiple different species. These may indicate data quality issues.");
            sb.AppendLine();

            // Find preferred IUCN common names using efficient SQL query (no default limit)
            var conflictNames = store.GetIucnPreferredConflictNames(settings.Limit, settings.Kingdom);

            // Get full records for each conflict
            var preferredConflicts = new List<(string NormalizedName, List<CommonNameRecord> Records)>();
            foreach (var normalizedName in conflictNames) {
                var records = store.GetCommonNamesByNormalized(normalizedName, "en")
                    .Where(r => r.Source == "iucn" && r.IsPreferred)
                    .Where(r => r.TaxonValidityStatus == "valid" && !r.TaxonIsFossil)
                    .ToList();

                // Filter by kingdom if specified
                if (!string.IsNullOrWhiteSpace(settings.Kingdom)) {
                    records = records.Where(r =>
                        r.TaxonKingdom?.Equals(settings.Kingdom, StringComparison.OrdinalIgnoreCase) == true).ToList();
                }

                if (records.Count > 0) {
                    preferredConflicts.Add((normalizedName, records));
                }
            }

            sb.AppendLine($"## Preferred Name Conflicts ({preferredConflicts.Count} found)");
            sb.AppendLine();
            sb.AppendLine("These common names are marked as the 'main' (preferred) common name for multiple species:");
            sb.AppendLine();

            foreach (var (normalizedName, records) in preferredConflicts) {
                var displayName = records.FirstOrDefault()?.RawName ?? normalizedName;
                sb.AppendLine($"### {displayName}");
                sb.AppendLine();
                sb.AppendLine("| Scientific Name | Kingdom | IUCN ID |");
                sb.AppendLine("|-----------------|---------|---------|");

                foreach (var record in records.DistinctBy(r => r.TaxonId)) {
                    var scientificName = CapitalizeFirst(record.TaxonCanonicalName);
                    sb.AppendLine($"| {scientificName} | {record.TaxonKingdom ?? "?"} | {record.SourceIdentifier} |");
                }
                sb.AppendLine();
            }

            // Output
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var defaultFileName = $"common-name-iucn-preferred-{timestamp}.md";
            var outputPath = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, null, null, defaultFileName);

            File.WriteAllText(outputPath, sb.ToString());
            AnsiConsole.MarkupLine($"[green]Report written to:[/] {outputPath}");
            AnsiConsole.MarkupLine($"[green]Found {preferredConflicts.Count} IUCN preferred name conflicts[/]");
            return 0;
        }, cancellationToken);
    }
}
