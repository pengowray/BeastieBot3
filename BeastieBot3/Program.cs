using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;
using System.Threading;

namespace BeastieBot3 {
    // Common CLI settings shared by all commands
    public sealed class CommonSettings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }
    }

    // Command: show-paths => dump all paths from the INI
    public sealed class ShowPathsCommand : Command<CommonSettings> {
        public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken) {
            var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
            var iniFile = settings.IniFile ?? "paths.ini";
            var reader = new IniPathReader(iniFile, baseDir);

            AnsiConsole.MarkupLine($"[grey]Reading paths from:[/] {reader.SourceFilePath}");
            var all = reader.GetAll();
            if (all.Count ==0) {
                AnsiConsole.MarkupLine("[yellow]No values found.[/]");
                return 0;
            }

            var table = new Table().AddColumns("Key", "Value");
            foreach (var kv in all) table.AddRow(kv.Key, kv.Value);
            AnsiConsole.Write(table);
            return 0;
        }
    }

    // Command: col check => detect COL dataset inside container
    public sealed class CheckColCommand : Command<CommonSettings> {
        public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken) {
            // Detect container path for COL dataset
            var colPath = "/app/datasets/Catalogue_of_Life_2025-10-10_XR";
            if (Directory.Exists(colPath)) {
                AnsiConsole.MarkupLine($"[green]COL dataset mounted at:[/] {colPath}");
            } else {
                AnsiConsole.MarkupLine("[yellow]COL dataset not mounted. Configure docker-compose volume mapping.[/]");
            }

            // Also show which settings directory is in use (for awareness)
            var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
            var iniFile = settings.IniFile ?? "paths.ini";
            var reader = new IniPathReader(iniFile, baseDir);
            AnsiConsole.MarkupLine($"[grey]Using settings from:[/] {reader.SourceFilePath}");
            return 0;
        }
    }

    internal class Program {
        static int Main(string[] args) {
            var app = new CommandApp();
            app.Configure(config => {
                config.SetApplicationName("beastiebot3");
                config.ValidateExamples();

                // show-paths (list settings)
                config.AddCommand<ShowPathsCommand>("show-paths")
                    .WithDescription("Show all key/value pairs from the settings INI file.")
                    .WithExample(new[] { "show-paths" })
                    .WithExample(new[] { "show-paths", "--settings-dir", "/config" });

                // col check
                config.AddBranch("col", col => {
                    col.SetDescription("Catalogue of Life related commands");
                    col.AddCommand<CheckColCommand>("check")
                        .WithDescription("Detect the mounted COL dataset inside the container.")
                        .WithExample(new[] { "col", "check" });
                });
            });

            return app.Run(args);
        }
    }
}
