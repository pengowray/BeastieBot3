using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.CommonNames;
using BeastieBot3.Configuration;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// CLI command for Wikipedia list generation. Loads list definitions from YAML,
/// generates wikitext files, writes a generation report + JSON metrics, and
/// optionally compares against a previous run via <c>--compare</c>.
/// </summary>
public sealed class WikipediaListCommand : Command<WikipediaListCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--database <PATH>")]
        public string? DatabasePath { get; init; }

        [CommandOption("--config <FILE>")]
        public string? ConfigPath { get; init; }

        [CommandOption("--templates <DIR>")]
        public string? TemplatesDirectory { get; init; }

        [CommandOption("--output-dir <DIR>")]
        public string? OutputDirectory { get; init; }

        [CommandOption("--rules <FILE>")]
        public string? RulesPath { get; init; }

        [CommandOption("--list <ID>")]
        [System.ComponentModel.Description("Filter to specific list IDs (repeatable). Use 'wikipedia show-lists' to see available IDs.")]
        public string[]? ListIds { get; init; }

        [CommandOption("--limit <N>")]
        public int? Limit { get; init; }

        [CommandOption("--use-legacy-names")]
        [System.ComponentModel.Description("Use legacy common name provider instead of the aggregated common names store.")]
        public bool UseLegacyNames { get; init; }

        [CommandOption("--common-names-db <PATH>")]
        [System.ComponentModel.Description("Path to the common names SQLite database.")]
        public string? CommonNamesDbPath { get; init; }

        [CommandOption("--col-database <PATH>")]
        [System.ComponentModel.Description("Path to the Catalogue of Life SQLite database for enriched taxonomy grouping.")]
        public string? ColDatabasePath { get; init; }

        [CommandOption("--no-col-enrichment")]
        [System.ComponentModel.Description("Disable COL-based taxonomy enrichment even if database is available.")]
        public bool NoColEnrichment { get; init; }

        [CommandOption("--compare <FILE>")]
        [System.ComponentModel.Description("Compare against a previous structure-metrics.json file for A/B testing.")]
        public string? CompareFile { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var configPath = ResolveConfigPath(paths, settings.ConfigPath);
        var templatesDir = ResolveTemplatesDir(paths, settings.TemplatesDirectory);
        var rulesPath = ResolveRulesPath(paths, settings.RulesPath);
        var outputDir = ResolveOutputDir(paths, settings.OutputDirectory);
        var databasePath = paths.ResolveIucnDatabasePath(settings.DatabasePath);

        var loader = new WikipediaListDefinitionLoader();
        var config = loader.Load(configPath);
        var definitions = FilterDefinitions(config.Lists, settings.ListIds);
        if (definitions.Count == 0) {
            if (settings.ListIds is { Length: > 0 }) {
                var requested = string.Join(", ", settings.ListIds);
                AnsiConsole.MarkupLine($"[yellow]No lists matched:[/] {Markup.Escape(requested)}");
            } else {
                AnsiConsole.MarkupLine("[yellow]No lists found in the configuration.[/]");
            }
            AnsiConsole.MarkupLine("[grey]Run[/] [white]wikipedia show-lists[/] [grey]to see all available list IDs.[/]");
            return 0;
        }

        using var query = new IucnListQueryService(databasePath);
        var templates = new WikipediaTemplateRenderer(templatesDir);
        var rules = new Legacy.LegacyTaxaRuleList(rulesPath);

        // Load YAML-based taxon rules (optional)
        var taxonRulesPath = ResolveTaxonRulesPath(paths, rulesPath);
        TaxonRulesService? taxonRules = taxonRulesPath != null ? TaxonRulesService.Load(taxonRulesPath) : null;

        // Determine which common name provider to use
        var commonNamesDbPath = settings.CommonNamesDbPath ?? paths.ResolveCommonNameStorePath(null);
        var useStoreBackedProvider = !settings.UseLegacyNames && File.Exists(commonNamesDbPath);

        // Determine COL enricher availability
        var colDbPath = settings.ColDatabasePath ?? paths.GetColSqlitePath();
        var useColEnrichment = !settings.NoColEnrichment && !string.IsNullOrWhiteSpace(colDbPath) && File.Exists(colDbPath);
        ColTaxonomyEnricher? colEnricher = null;

        if (useColEnrichment) {
            AnsiConsole.MarkupLine($"[grey]Using COL taxonomy enrichment from:[/] {colDbPath}");
            colEnricher = new ColTaxonomyEnricher(colDbPath!);
        }

        WikipediaListGenerator generator;
        IDisposable? providerToDispose = null;

        try {
            if (useStoreBackedProvider) {
                AnsiConsole.MarkupLine($"[grey]Using aggregated common names from:[/] {commonNamesDbPath}");
                var wikipediaCachePath = paths.GetWikipediaCachePath();
                if (!string.IsNullOrWhiteSpace(wikipediaCachePath) && File.Exists(wikipediaCachePath)) {
                    AnsiConsole.MarkupLine($"[grey]Using Wikipedia cache from:[/] {wikipediaCachePath}");
                }
                var storeProvider = new StoreBackedCommonNameProvider(commonNamesDbPath, wikipediaCachePath);
                providerToDispose = storeProvider;
                generator = new WikipediaListGenerator(query, templates, rules, storeProvider, colEnricher, taxonRules);
            } else {
                if (!settings.UseLegacyNames) {
                    AnsiConsole.MarkupLine("[yellow]Common names store not found, using legacy provider.[/]");
                }
                var legacyProvider = new CommonNameProvider(paths.GetWikidataCachePath(), paths.GetIucnApiCachePath());
                providerToDispose = legacyProvider;
                generator = new WikipediaListGenerator(query, templates, rules, legacyProvider, taxonRules);
            }

            var results = new List<(WikipediaListDefinition Definition, WikipediaListResult Result)>();
            foreach (var definition in definitions) {
                AnsiConsole.MarkupLine($"[grey]Generating[/] [white]{definition.Title}[/]...");
                var result = generator.Generate(definition, config.Defaults, outputDir, settings.Limit);
                results.Add((definition, result));
                var problemFlag = result.Metrics?.Problems.Count > 0 ? $" [yellow]({result.Metrics.Problems.Count} problems)[/]" : "";
                AnsiConsole.MarkupLine($"  [green]saved[/] {result.OutputPath} ([cyan]{result.TotalEntries}[/] taxa, [cyan]{result.HeadingCount}[/] headings, dataset {result.DatasetVersion}).{problemFlag}");
            }

            // Write reports
            WriteReport(outputDir, results);
            WriteMetricsJson(outputDir, results);

            // Show problems summary
            ShowProblemsSummary(results);

            // Comparison mode
            if (!string.IsNullOrWhiteSpace(settings.CompareFile)) {
                RunComparison(settings.CompareFile, results);
            }

            return 0;
        }
        finally {
            providerToDispose?.Dispose();
            colEnricher?.Dispose();
        }
    }

    /// <summary>
    /// Writes a timestamped text report with two tables (by generation order and by size)
    /// plus a problems section listing any structural issues detected in the generated lists.
    /// </summary>
    private static void WriteReport(string outputDir, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        if (results.Count == 0) {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var timestamp = now.ToString("yyyyMMdd-HHmmss", System.Globalization.CultureInfo.InvariantCulture);
        var reportPath = Path.Combine(outputDir, $"generation-report-{timestamp}.txt");
        var datasetVersion = results[0].Result.DatasetVersion;
        var generatedAt = now.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

        using var writer = new StreamWriter(reportPath);
        writer.WriteLine($"Wikipedia List Generation Report");
        writer.WriteLine($"================================");
        writer.WriteLine($"Generated: {generatedAt} UTC");
        writer.WriteLine($"Dataset: {datasetVersion}");
        writer.WriteLine($"Lists: {results.Count}");
        writer.WriteLine();

        // Section 1: By generation order
        writer.WriteLine("BY GENERATION ORDER");
        writer.WriteLine("-------------------");
        writer.WriteLine();
        WriteReportTable(writer, results);

        // Section 2: Sorted by taxa desc, then headings desc
        writer.WriteLine();
        writer.WriteLine("BY SIZE (LARGEST FIRST)");
        writer.WriteLine("-----------------------");
        writer.WriteLine();
        var sorted = results
            .OrderByDescending(r => r.Result.TotalEntries)
            .ThenByDescending(r => r.Result.HeadingCount)
            .ToList();
        WriteReportTable(writer, sorted);

        // Section 3: Problems
        var problemLists = results
            .Where(r => r.Result.Metrics?.Problems.Count > 0)
            .OrderByDescending(r => r.Result.Metrics!.Problems.Count)
            .ToList();
        if (problemLists.Count > 0) {
            writer.WriteLine();
            writer.WriteLine("PROBLEMS DETECTED");
            writer.WriteLine("-----------------");
            writer.WriteLine();
            foreach (var (definition, result) in problemLists) {
                var fileName = Path.GetFileName(result.OutputPath);
                writer.WriteLine($"{fileName}:");
                foreach (var problem in result.Metrics!.Problems) {
                    writer.WriteLine($"  - {problem}");
                }
                writer.WriteLine();
            }
        }

        AnsiConsole.MarkupLine($"[grey]Report saved to[/] {reportPath}");
    }

    private static void WriteReportTable(StreamWriter writer, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        writer.WriteLine($"{"File",-55} {"Taxa",7} {"Hdgs",6} {"Ratio",7} {"1-item",7} {"Empty",6} {"Other",6} {"MaxLf",6}");
        writer.WriteLine(new string('-', 102));

        var totalTaxa = 0;
        var totalHeadings = 0;
        foreach (var (definition, result) in results) {
            var fileName = Path.GetFileName(result.OutputPath);
            if (fileName.Length > 55) fileName = fileName[..52] + "...";
            var m = result.Metrics;
            var ratio = result.TotalEntries > 0 ? (double)result.HeadingCount / result.TotalEntries : 0;
            writer.WriteLine($"{fileName,-55} {result.TotalEntries,7} {result.HeadingCount,6} {ratio,7:F3} {m?.SingleItemHeadings ?? 0,7} {m?.EmptyHeadings ?? 0,6} {m?.OtherUnknownHeadings ?? 0,6} {m?.MaxLeafSize ?? 0,6}");
            totalTaxa += result.TotalEntries;
            totalHeadings += result.HeadingCount;
        }

        writer.WriteLine(new string('-', 102));
        writer.WriteLine($"{"TOTAL",-55} {totalTaxa,7} {totalHeadings,6}");
    }

    /// <summary>
    /// Writes <c>structure-metrics.json</c> containing per-list metrics and auto-split decisions.
    /// This file can be passed to <c>--compare</c> on a future run for A/B testing.
    /// </summary>
    private static void WriteMetricsJson(string outputDir, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        if (results.Count == 0) {
            return;
        }

        var report = new GenerationMetricsReport {
            GeneratedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture),
            DatasetVersion = results[0].Result.DatasetVersion,
            Lists = results
                .Where(r => r.Result.Metrics != null)
                .Select(r => r.Result.Metrics!)
                .ToList()
        };

        var jsonPath = Path.Combine(outputDir, "structure-metrics.json");
        var options = new JsonSerializerOptions { WriteIndented = true };
        File.WriteAllText(jsonPath, JsonSerializer.Serialize(report, options));
        AnsiConsole.MarkupLine($"[grey]Metrics saved to[/] {jsonPath}");
    }

    /// <summary>
    /// Prints a console summary of lists with structural problems (FRAGMENTED, OVER-SPLIT, etc.),
    /// limited to the first 10 problematic lists.
    /// </summary>
    private static void ShowProblemsSummary(List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        var problemLists = results
            .Where(r => r.Result.Metrics?.Problems.Count > 0)
            .ToList();

        if (problemLists.Count == 0) {
            return;
        }

        AnsiConsole.MarkupLine($"\n[yellow]Problems detected in {problemLists.Count} list(s):[/]");
        foreach (var (definition, result) in problemLists.Take(10)) {
            var fileName = Path.GetFileName(result.OutputPath);
            foreach (var problem in result.Metrics!.Problems) {
                AnsiConsole.MarkupLine($"  [grey]{fileName}:[/] [yellow]{Markup.Escape(problem)}[/]");
            }
        }
        if (problemLists.Count > 10) {
            AnsiConsole.MarkupLine($"  [grey]... and {problemLists.Count - 10} more (see report)[/]");
        }
    }

    /// <summary>
    /// Loads a previous <c>structure-metrics.json</c> and displays a side-by-side comparison
    /// table showing heading count and single-item heading deltas per list, with IMPROVED/DEGRADED/UNCHANGED status.
    /// </summary>
    private static void RunComparison(string compareFile, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        if (!File.Exists(compareFile)) {
            AnsiConsole.MarkupLine($"[red]Comparison file not found:[/] {compareFile}");
            return;
        }

        GenerationMetricsReport? previous;
        try {
            var json = File.ReadAllText(compareFile);
            previous = JsonSerializer.Deserialize<GenerationMetricsReport>(json);
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to parse comparison file:[/] {ex.Message}");
            return;
        }

        if (previous?.Lists == null || previous.Lists.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]Comparison file contains no list data.[/]");
            return;
        }

        var previousByFile = previous.Lists.ToDictionary(m => m.FileName, StringComparer.OrdinalIgnoreCase);
        AnsiConsole.MarkupLine($"\n[bold]COMPARISON[/] (vs {previous.GeneratedAt})");

        var table = new Table();
        table.AddColumn(new TableColumn("File").Width(50));
        table.AddColumn(new TableColumn("Taxa").RightAligned());
        table.AddColumn(new TableColumn("Headings").RightAligned());
        table.AddColumn(new TableColumn("Delta Hdg").RightAligned());
        table.AddColumn(new TableColumn("1-item").RightAligned());
        table.AddColumn(new TableColumn("Delta 1-item").RightAligned());
        table.AddColumn(new TableColumn("Status"));

        foreach (var (definition, result) in results) {
            var fileName = Path.GetFileName(result.OutputPath);
            var m = result.Metrics;
            if (m == null) continue;

            if (previousByFile.TryGetValue(fileName, out var prev)) {
                var deltaH = m.HeadingCount - prev.HeadingCount;
                var delta1 = m.SingleItemHeadings - prev.SingleItemHeadings;
                var status = deltaH < 0 ? "[green]IMPROVED[/]"
                    : deltaH > 0 ? "[red]DEGRADED[/]"
                    : "[grey]UNCHANGED[/]";
                var deltaHStr = deltaH == 0 ? "0" : deltaH > 0 ? $"[red]+{deltaH}[/]" : $"[green]{deltaH}[/]";
                var delta1Str = delta1 == 0 ? "0" : delta1 > 0 ? $"[red]+{delta1}[/]" : $"[green]{delta1}[/]";

                table.AddRow(
                    Markup.Escape(fileName.Length > 50 ? fileName[..47] + "..." : fileName),
                    m.TotalTaxa.ToString(),
                    m.HeadingCount.ToString(),
                    deltaHStr,
                    m.SingleItemHeadings.ToString(),
                    delta1Str,
                    status);
            } else {
                table.AddRow(
                    Markup.Escape(fileName.Length > 50 ? fileName[..47] + "..." : fileName),
                    m.TotalTaxa.ToString(),
                    m.HeadingCount.ToString(),
                    "[grey]NEW[/]",
                    m.SingleItemHeadings.ToString(),
                    "[grey]NEW[/]",
                    "[blue]NEW[/]");
            }
        }

        AnsiConsole.Write(table);
    }

    private static string ResolveConfigPath(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }

        return Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
    }

    private static string ResolveTemplatesDir(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }

        return Path.Combine(paths.BaseDirectory, "rules", "wikipedia", "templates");
    }

    private static string ResolveRulesPath(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }

        return Path.Combine(paths.BaseDirectory, "rules", "rules-list.txt");
    }

    private static string? ResolveTaxonRulesPath(PathsService paths, string? rulesPath) {
        // Look for taxon-rules.yml in the same directory as the legacy rules file
        var rulesDir = Path.GetDirectoryName(rulesPath) ?? Path.Combine(paths.BaseDirectory, "rules");
        var taxonRulesPath = Path.Combine(rulesDir, "taxon-rules.yml");
        return File.Exists(taxonRulesPath) ? taxonRulesPath : null;
    }

    private static string ResolveOutputDir(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }

        // Use dedicated wikipedia output dir from paths.ini, fallback to datastore subdir
        var configuredPath = paths.GetWikipediaOutputDirectory();
        if (!string.IsNullOrWhiteSpace(configuredPath)) {
            return Path.GetFullPath(configuredPath);
        }

        // Fallback: use datastore_dir/wikipedia-lists if datastore is configured
        var datastoreDir = paths.GetDatastoreDir();
        if (!string.IsNullOrWhiteSpace(datastoreDir)) {
            return Path.Combine(Path.GetFullPath(datastoreDir), "wikipedia-lists");
        }

        // Last resort: relative to the base directory
        return Path.Combine(paths.BaseDirectory, "output", "wikipedia");
    }

    private static IReadOnlyList<WikipediaListDefinition> FilterDefinitions(IReadOnlyList<WikipediaListDefinition> definitions, string[]? ids) {
        if (ids is null || ids.Length == 0) {
            return definitions;
        }

        var wanted = new HashSet<string>(ids.Select(id => id.Trim()), StringComparer.OrdinalIgnoreCase);
        return definitions.Where(def => wanted.Contains(def.Id)).ToList();
    }
}
