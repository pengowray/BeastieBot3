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

        [CommandOption("--report <TYPE>")]
        [Description("Report type: ambiguous, caps, summary, wiki-disambig, iucn-preferred (default: summary)")]
        public string ReportType { get; init; } = "summary";

        [CommandOption("-o|--output <PATH>")]
        [Description("Output file path for Markdown report.")]
        public string? OutputPath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Limit number of items in report.")]
        public int? Limit { get; init; }

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
            "caps" => await GenerateCapsReportAsync(store, settings, paths, cancellationToken),
            "wiki-disambig" => await GenerateWikiDisambigReportAsync(store, settings, paths, cancellationToken),
            "iucn-preferred" => await GenerateIucnPreferredConflictReportAsync(store, settings, paths, cancellationToken),
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

    private static Task<int> GenerateAmbiguousReportAsync(CommonNameStore store, Settings settings, PathsService paths, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating ambiguous common names report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Ambiguous Common Names Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();

            var limit = settings.Limit ?? 100;

            // Use efficient SQL query to find ambiguous names directly
            var ambiguousNames = store.GetAmbiguousCommonNames(limit, settings.Kingdom);
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
                    sb.AppendLine($"| {first.TaxonCanonicalName} | {first.TaxonKingdom ?? "?"} | {sources} | {isPreferred} |");
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

            // Get all English common names and check for missing caps rules
            var distinctNames = store.GetDistinctNormalizedCommonNames("en");
            var missingWords = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var processedNames = 0;

            foreach (var normalizedName in distinctNames) {
                var records = store.GetCommonNamesByNormalized(normalizedName, "en");
                foreach (var record in records) {
                    var words = CommonNameNormalizer.FindMissingCapsWords(
                        record.RawName,
                        word => store.GetCorrectCapitalization(word) != null
                    );

                    foreach (var word in words) {
                        var lower = word.ToLowerInvariant();
                        if (!missingWords.ContainsKey(lower)) {
                            missingWords[lower] = new List<string>();
                        }
                        if (missingWords[lower].Count < 3) { // Keep up to 3 examples
                            missingWords[lower].Add(record.RawName);
                        }
                    }
                }
                processedNames++;

                if (settings.Limit.HasValue && processedNames >= settings.Limit.Value) {
                    break;
                }
            }

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
            foreach (var (word, examples) in sortedMissing.Take(50)) {
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

            var limit = settings.Limit ?? 100;

            // Use efficient SQL query to find Wikipedia ambiguous names directly
            var wikiAmbiguousNames = store.GetWikipediaAmbiguousNames(limit, settings.Kingdom);
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
                    sb.AppendLine($"| {first.TaxonCanonicalName} | {first.TaxonKingdom ?? "?"} | {sources} |");
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

            // Find preferred IUCN common names using efficient SQL query
            var limit = settings.Limit ?? 100;
            var conflictNames = store.GetIucnPreferredConflictNames(limit, settings.Kingdom);

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
                    sb.AppendLine($"| {record.TaxonCanonicalName} | {record.TaxonKingdom ?? "?"} | {record.SourceIdentifier} |");
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
