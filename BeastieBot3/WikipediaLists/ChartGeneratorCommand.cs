using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using Spectre.Console;
using Spectre.Console.Cli;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// CLI command that generates IUCN Red List bar chart data files for Wikipedia.
// For each chart group defined in chart-groups.yml, produces three files:
//   .tab   — Wikimedia Commons tabular data (JSON)
//   .chart — Extension:Chart bar chart definition (JSON)
//   .wikitext — wikitext snippet to embed the chart on Wikipedia
//
// Usage:
//   wikipedia generate-charts
//   wikipedia generate-charts --group mammals --group birds
//   wikipedia generate-charts --output-dir charts/

namespace BeastieBot3.WikipediaLists;

[CommandInfo("wikipedia generate-charts", CommandKind.ReadOnly,
    "Generate IUCN Red List bar chart data files (.tab, .chart, .wikitext) for Wikipedia.",
    Reason = "Generates chart output files (.tab/.chart/.wikitext) only.",
    Examples = new[] {
        "wikipedia generate-charts",
        "wikipedia generate-charts --group mammals --group birds",
        "wikipedia generate-charts --output-dir charts/"
    })]
internal sealed class ChartGeneratorCommand : Command<ChartGeneratorCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--database <PATH>")]
        [Description("Path to IUCN SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--dataset <SOURCE>")]
        [Description("Which IUCN dataset to read: 'csv' (default, the imported CSV release) or 'api' (the CSV-shaped projection of the API cache built by 'iucn api project-view').")]
        public string? Dataset { get; init; }

        [CommandOption("--output-dir <DIR>")]
        [Description("Output directory for generated chart files. Defaults to report output directory.")]
        public string? OutputDirectory { get; init; }

        [CommandOption("--chart-config <FILE>")]
        [Description("Path to chart-groups.yml. Defaults to rules/chart-groups.yml.")]
        public string? ChartConfigPath { get; init; }

        [CommandOption("--taxa-config <FILE>")]
        [Description("Path to taxa-groups.yml. Defaults to rules/taxa-groups.yml.")]
        public string? TaxaConfigPath { get; init; }

        [CommandOption("--group <ID>")]
        [Description("Generate only specific groups (repeatable). Omit to generate all.")]
        public string[]? GroupIds { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var databasePath = IucnDatasetResolver.Resolve(paths, settings.Dataset, settings.DatabasePath);

        var chartConfigPath = ResolveChartConfigPath(paths, settings.ChartConfigPath);
        var taxaConfigPath = ResolveTaxaConfigPath(paths, settings.TaxaConfigPath);
        var outputDir = ResolveOutputDir(paths, settings.OutputDirectory);

        // Load YAML configs
        var chartGroups = LoadChartGroups(chartConfigPath);
        var taxaGroups = LoadTaxaGroups(taxaConfigPath);

        if (chartGroups.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No chart groups found in configuration.[/]");
            return 0;
        }

        // Filter to requested groups
        var groupsToGenerate = FilterGroups(chartGroups, settings.GroupIds);
        if (groupsToGenerate.Count == 0) {
            if (settings.GroupIds is { Length: > 0 }) {
                var requested = string.Join(", ", settings.GroupIds);
                AnsiConsole.MarkupLine($"[yellow]No chart groups matched:[/] {Markup.Escape(requested)}");
                AnsiConsole.MarkupLine("[grey]Available groups:[/] " + string.Join(", ", chartGroups.Keys.OrderBy(k => k)));
            }
            return 0;
        }

        AnsiConsole.MarkupLine($"[grey]Database:[/] {databasePath}");
        AnsiConsole.MarkupLine($"[grey]Output:[/] {outputDir}");
        AnsiConsole.MarkupLine($"[grey]Groups:[/] {groupsToGenerate.Count}");
        AnsiConsole.WriteLine();

        using var builder = new IucnChartDataBuilder(databasePath);
        var version = builder.GetDatasetVersion();
        AnsiConsole.MarkupLine($"[grey]Dataset version:[/] {version}");
        AnsiConsole.WriteLine();

        // Write the single shared .chart definition
        ChartOutputWriter.WriteSharedChart(outputDir);
        AnsiConsole.MarkupLine($"  [green]✓[/] {Markup.Escape(ChartOutputWriter.SharedChartFileName)} (shared chart definition)");

        // Summary table for console output
        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("Group")
            .AddColumn(new TableColumn("Total").RightAligned())
            .AddColumn(new TableColumn("EX").RightAligned())
            .AddColumn(new TableColumn("EW").RightAligned())
            .AddColumn(new TableColumn("CR(PE)").RightAligned())
            .AddColumn(new TableColumn("CR(PEW)").RightAligned())
            .AddColumn(new TableColumn("CR").RightAligned())
            .AddColumn(new TableColumn("EN").RightAligned())
            .AddColumn(new TableColumn("VU").RightAligned())
            .AddColumn(new TableColumn("NT").RightAligned())
            .AddColumn(new TableColumn("LC").RightAligned())
            .AddColumn(new TableColumn("DD").RightAligned());

        var allResults = new List<ChartGroupResult>();

        foreach (var (groupId, chartDef) in groupsToGenerate) {
            // Resolve taxonomic filters
            List<TaxonFilterDefinition>? filters = null;
            if (chartDef.TaxaGroup is not null) {
                if (!taxaGroups.TryGetValue(chartDef.TaxaGroup, out var taxaGroup)) {
                    AnsiConsole.MarkupLine($"[yellow]Warning: Unknown taxa_group '{chartDef.TaxaGroup}' in chart group '{groupId}', skipping.[/]");
                    continue;
                }
                filters = taxaGroup.Filters;
            }

            var result = builder.BuildCounts(
                groupId,
                chartDef.ChartName ?? groupId,
                chartDef.Comprehensive,
                chartDef.TemplateName,
                chartDef.Caption,
                filters);

            ChartOutputWriter.WriteGroupFiles(result, outputDir);
            allResults.Add(result);

            // Add row to summary table
            var countByCode = result.Counts.ToDictionary(c => c.Code, c => c.Count);
            table.AddRow(
                Markup.Escape(result.ChartName),
                result.TotalAssessed.ToString("N0"),
                FormatCount(countByCode, "EX"),
                FormatCount(countByCode, "EW"),
                FormatCount(countByCode, "CR(PE)"),
                FormatCount(countByCode, "CR(PEW)"),
                FormatCount(countByCode, "CR"),
                FormatCount(countByCode, "EN"),
                FormatCount(countByCode, "VU"),
                FormatCount(countByCode, "NT"),
                FormatCount(countByCode, "LC"),
                FormatCount(countByCode, "DD")
            );

            var baseName = ChartOutputWriter.BaseFileName(result);
            AnsiConsole.MarkupLine($"  [green]✓[/] {Markup.Escape(baseName)} ([cyan]{result.TotalAssessed:N0}[/] species)");
        }

        // Write summary file
        if (allResults.Count > 0) {
            ChartOutputWriter.WriteSummary(allResults, outputDir);
            AnsiConsole.MarkupLine($"  [green]✓[/] summary.txt");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.Write(table);

        return 0;
    }

    private static string FormatCount(Dictionary<string, int> counts, string code) =>
        counts.TryGetValue(code, out var c) ? c.ToString("N0") : "0";

    private static List<KeyValuePair<string, ChartGroupDefinition>> FilterGroups(
        Dictionary<string, ChartGroupDefinition> allGroups,
        string[]? requestedIds) {
        if (requestedIds is null or { Length: 0 }) {
            return allGroups.ToList();
        }

        var requested = new HashSet<string>(requestedIds, StringComparer.OrdinalIgnoreCase);
        return allGroups.Where(kv => requested.Contains(kv.Key)).ToList();
    }

    // ==================== Config loading ====================

    private static Dictionary<string, ChartGroupDefinition> LoadChartGroups(string path) {
        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        using var reader = File.OpenText(path);
        var file = deserializer.Deserialize<ChartGroupsFile>(reader);
        return file?.Groups ?? new();
    }

    private static Dictionary<string, TaxaGroupDefinition> LoadTaxaGroups(string path) {
        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        using var reader = File.OpenText(path);
        var file = deserializer.Deserialize<TaxaGroupsFile>(reader);
        return file?.Groups ?? new();
    }

    // ==================== Path resolution ====================

    private static string ResolveChartConfigPath(PathsService paths, string? explicitPath) {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return Path.GetFullPath(explicitPath);

        // Default: look for chart-groups.yml next to the executable
        var candidates = new[] {
            Path.Combine(paths.BaseDirectory, "rules", "chart-groups.yml"),
            Path.Combine(AppContext.BaseDirectory, "rules", "chart-groups.yml"),
        };
        foreach (var candidate in candidates) {
            if (File.Exists(candidate)) return candidate;
        }

        throw new FileNotFoundException("chart-groups.yml not found. Pass --chart-config explicitly.");
    }

    private static string ResolveTaxaConfigPath(PathsService paths, string? explicitPath) {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return Path.GetFullPath(explicitPath);

        var candidates = new[] {
            Path.Combine(paths.BaseDirectory, "rules", "taxa-groups.yml"),
            Path.Combine(AppContext.BaseDirectory, "rules", "taxa-groups.yml"),
        };
        foreach (var candidate in candidates) {
            if (File.Exists(candidate)) return candidate;
        }

        throw new FileNotFoundException("taxa-groups.yml not found. Pass --taxa-config explicitly.");
    }

    private static string ResolveOutputDir(PathsService paths, string? explicitDir) {
        if (!string.IsNullOrWhiteSpace(explicitDir)) {
            Directory.CreateDirectory(explicitDir);
            return Path.GetFullPath(explicitDir);
        }

        var configured = paths.GetWikipediaOutputDirectory();
        if (!string.IsNullOrWhiteSpace(configured)) {
            var chartsDir = Path.Combine(configured, "charts");
            Directory.CreateDirectory(chartsDir);
            return chartsDir;
        }

        return ReportPathResolver.ResolveDirectory(paths, null, null);
    }
}

// ==================== YAML model ====================

internal sealed class ChartGroupsFile {
    public Dictionary<string, ChartGroupDefinition> Groups { get; init; } = new();
}

internal sealed class ChartGroupDefinition {
    /// <summary>
    /// Reference to a taxa group in taxa-groups.yml.
    /// Null means no filter (all species).
    /// </summary>
    public string? TaxaGroup { get; init; }

    /// <summary>
    /// Name used in output filenames and chart titles (e.g. "mammals").
    /// </summary>
    public string? ChartName { get; init; }

    /// <summary>
    /// Whether IUCN considers this group comprehensively assessed.
    /// </summary>
    public bool Comprehensive { get; init; }

    /// <summary>
    /// Wikipedia template name this chart replaces (e.g. "IUCN mammal chart").
    /// </summary>
    public string? TemplateName { get; init; }

    /// <summary>
    /// Optional custom caption override.
    /// </summary>
    public string? Caption { get; init; }
}
