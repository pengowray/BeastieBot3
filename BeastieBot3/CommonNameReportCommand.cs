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
        [Description("Report type: ambiguous, caps, summary (default: summary)")]
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
            "ambiguous" => await GenerateAmbiguousReportAsync(store, settings, cancellationToken),
            "caps" => await GenerateCapsReportAsync(store, settings, cancellationToken),
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

    private static Task<int> GenerateAmbiguousReportAsync(CommonNameStore store, Settings settings, CancellationToken cancellationToken) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Generating ambiguous common names report...[/]");

            var sb = new StringBuilder();
            sb.AppendLine("# Ambiguous Common Names Report");
            sb.AppendLine();
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine();

            // Query conflicts
            using var command = ((IDisposable)store).GetType()
                .GetField("_connection", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                ?.GetValue(store) is SqliteConnection conn ? conn.CreateCommand() : throw new InvalidOperationException("Could not access connection");

            // We need to get the connection differently - let's add a method to the store
            // For now, let's just show a summary from the store methods
            var distinctNames = store.GetDistinctNormalizedCommonNames("en");
            var conflictingNames = new List<(string NormalizedName, List<CommonNameRecord> Records)>();

            var limit = settings.Limit ?? 100;
            var count = 0;

            foreach (var normalizedName in distinctNames) {
                if (count >= limit) break;

                var records = store.GetCommonNamesByNormalized(normalizedName, "en")
                    .Where(r => r.TaxonValidityStatus == "valid" && !r.TaxonIsFossil)
                    .ToList();

                // Check if multiple distinct taxa share this name
                var distinctTaxa = records.Select(r => r.TaxonId).Distinct().Count();
                if (distinctTaxa > 1) {
                    // Filter by kingdom if specified
                    if (!string.IsNullOrWhiteSpace(settings.Kingdom)) {
                        records = records.Where(r =>
                            r.TaxonKingdom?.Equals(settings.Kingdom, StringComparison.OrdinalIgnoreCase) == true).ToList();
                        if (records.Select(r => r.TaxonId).Distinct().Count() <= 1) continue;
                    }

                    conflictingNames.Add((normalizedName, records));
                    count++;
                }
            }

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

            // Output
            if (!string.IsNullOrWhiteSpace(settings.OutputPath)) {
                var outputDir = Path.GetDirectoryName(settings.OutputPath);
                if (!string.IsNullOrWhiteSpace(outputDir)) {
                    Directory.CreateDirectory(outputDir);
                }
                File.WriteAllText(settings.OutputPath, sb.ToString());
                AnsiConsole.MarkupLine($"[green]Report written to:[/] {settings.OutputPath}");
            } else {
                AnsiConsole.WriteLine(sb.ToString());
            }

            AnsiConsole.MarkupLine($"[green]Found {conflictingNames.Count} ambiguous common names[/]");
            return 0;
        }, cancellationToken);
    }

    private static Task<int> GenerateCapsReportAsync(CommonNameStore store, Settings settings, CancellationToken cancellationToken) {
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

            // Output
            if (!string.IsNullOrWhiteSpace(settings.OutputPath)) {
                var outputDir = Path.GetDirectoryName(settings.OutputPath);
                if (!string.IsNullOrWhiteSpace(outputDir)) {
                    Directory.CreateDirectory(outputDir);
                }
                File.WriteAllText(settings.OutputPath, sb.ToString());
                AnsiConsole.MarkupLine($"[green]Report written to:[/] {settings.OutputPath}");
            } else {
                AnsiConsole.WriteLine(sb.ToString());
            }

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
}
