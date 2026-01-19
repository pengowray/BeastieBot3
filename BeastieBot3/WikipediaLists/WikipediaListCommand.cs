using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.WikipediaLists;

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
            AnsiConsole.MarkupLine("[yellow]No matching lists found in the configuration.[/]");
            return 0;
        }

        using var query = new IucnListQueryService(databasePath);
        var templates = new WikipediaTemplateRenderer(templatesDir);
        var rules = new Legacy.LegacyTaxaRuleList(rulesPath);
        
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
                var storeProvider = new StoreBackedCommonNameProvider(commonNamesDbPath);
                providerToDispose = storeProvider;
                generator = new WikipediaListGenerator(query, templates, rules, storeProvider, colEnricher);
            } else {
                if (!settings.UseLegacyNames) {
                    AnsiConsole.MarkupLine("[yellow]Common names store not found, using legacy provider.[/]");
                }
                var legacyProvider = new CommonNameProvider(paths.GetWikidataCachePath(), paths.GetIucnApiCachePath());
                providerToDispose = legacyProvider;
                generator = new WikipediaListGenerator(query, templates, rules, legacyProvider);
            }

            var results = new List<(WikipediaListDefinition Definition, WikipediaListResult Result)>();
            foreach (var definition in definitions) {
                AnsiConsole.MarkupLine($"[grey]Generating[/] [white]{definition.Title}[/]...");
                var result = generator.Generate(definition, config.Defaults, outputDir, settings.Limit);
                results.Add((definition, result));
                AnsiConsole.MarkupLine($"  [green]saved[/] {result.OutputPath} ([cyan]{result.TotalEntries}[/] taxa, [cyan]{result.HeadingCount}[/] headings, dataset {result.DatasetVersion}).");
            }

            // Write report file
            WriteReport(outputDir, results);

            return 0;
        }
        finally {
            providerToDispose?.Dispose();
            colEnricher?.Dispose();
        }
    }

    private static void WriteReport(string outputDir, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        if (results.Count == 0) {
            return;
        }

        var reportPath = Path.Combine(outputDir, "generation-report.txt");
        var datasetVersion = results[0].Result.DatasetVersion;
        var generatedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

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

        AnsiConsole.MarkupLine($"[grey]Report saved to[/] {reportPath}");
    }

    private static void WriteReportTable(StreamWriter writer, List<(WikipediaListDefinition Definition, WikipediaListResult Result)> results) {
        writer.WriteLine($"{"File",-60} {"Taxa",8} {"Headings",10}");
        writer.WriteLine(new string('-', 80));

        var totalTaxa = 0;
        var totalHeadings = 0;
        foreach (var (definition, result) in results) {
            var fileName = Path.GetFileName(result.OutputPath);
            writer.WriteLine($"{fileName,-60} {result.TotalEntries,8} {result.HeadingCount,10}");
            totalTaxa += result.TotalEntries;
            totalHeadings += result.HeadingCount;
        }

        writer.WriteLine(new string('-', 80));
        writer.WriteLine($"{"TOTAL",-60} {totalTaxa,8} {totalHeadings,10}");
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
