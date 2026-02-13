using System.IO;
using System.Linq;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaShowListsCommand : Command<WikipediaShowListsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--config <FILE>")]
        [System.ComponentModel.Description("Override config file path (default: rules/wikipedia-lists.yml).")]
        public string? ConfigPath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        var paths = new Configuration.PathsService(settings.IniFile, settings.SettingsDir);
        var configPath = ResolveConfigPath(paths, settings.ConfigPath);

        if (!File.Exists(configPath)) {
            AnsiConsole.MarkupLine($"[red]Config file not found:[/] {configPath}");
            return 1;
        }

        var loader = new WikipediaListDefinitionLoader();
        var config = loader.Load(configPath);

        if (config.Lists.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No lists defined in the configuration.[/]");
            return 0;
        }

        var table = new Table();
        table.AddColumn("ID");
        table.AddColumn("Title");

        foreach (var list in config.Lists.OrderBy(l => l.Id)) {
            table.AddRow(
                Markup.Escape(list.Id),
                Markup.Escape(list.Title));
        }

        AnsiConsole.MarkupLine($"[grey]Loaded from:[/] {configPath}");
        AnsiConsole.MarkupLine($"[grey]Total lists:[/] {config.Lists.Count}");
        AnsiConsole.WriteLine();
        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]To generate a specific list:[/]  wikipedia generate-lists --list <ID>");
        AnsiConsole.MarkupLine("[grey]To generate all lists:[/]       wikipedia generate-lists");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[grey]Edit list definitions in:[/]    {Path.GetFileName(configPath)}");

        var configDir = Path.GetDirectoryName(configPath) ?? ".";
        var taxaGroupsPath = Path.Combine(configDir, "taxa-groups.yml");
        var presetsPath = Path.Combine(configDir, "list-presets.yml");
        if (File.Exists(taxaGroupsPath)) {
            AnsiConsole.MarkupLine($"[grey]Edit taxa groups in:[/]        taxa-groups.yml");
        }
        if (File.Exists(presetsPath)) {
            AnsiConsole.MarkupLine($"[grey]Edit presets in:[/]            list-presets.yml");
        }

        return 0;
    }

    private static string ResolveConfigPath(Configuration.PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }

        return Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
    }
}
