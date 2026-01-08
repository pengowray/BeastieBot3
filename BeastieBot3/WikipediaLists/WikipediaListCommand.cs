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
        using var commonNames = new CommonNameProvider(paths.GetWikidataCachePath(), paths.GetIucnApiCachePath());
        var templates = new WikipediaTemplateRenderer(templatesDir);
        var rules = new Legacy.LegacyTaxaRuleList(rulesPath);
        var generator = new WikipediaListGenerator(query, templates, rules, commonNames);

        foreach (var definition in definitions) {
            AnsiConsole.MarkupLine($"[grey]Generating[/] [white]{definition.Title}[/]...");
            var result = generator.Generate(definition, config.Defaults, outputDir, settings.Limit);
            AnsiConsole.MarkupLine($"  [green]saved[/] {result.OutputPath} ([cyan]{result.TotalEntries}[/] taxa, dataset {result.DatasetVersion}).");
        }

        return 0;
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

        return paths.GetReportOutputDirectory()
            ?? Path.Combine(paths.BaseDirectory, "output", "wikipedia");
    }

    private static IReadOnlyList<WikipediaListDefinition> FilterDefinitions(IReadOnlyList<WikipediaListDefinition> definitions, string[]? ids) {
        if (ids is null || ids.Length == 0) {
            return definitions;
        }

        var wanted = new HashSet<string>(ids.Select(id => id.Trim()), StringComparer.OrdinalIgnoreCase);
        return definitions.Where(def => wanted.Contains(def.Id)).ToList();
    }
}
