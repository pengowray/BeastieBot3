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
                    col.AddCommand<ColImportCommand>("import")
                        .WithDescription("Import Catalogue of Life ColDP zip archives into individual SQLite databases.")
                        .WithExample(new[] { "col", "import" })
                        .WithExample(new[] { "col", "import", "--force" });
                    col.AddCommand<ColSubgenusHomonymReportCommand>("report-subgenus-homonyms")
                        .WithDescription("Report subgenus entries whose names collide with genus names in the COL database.")
                        .WithExample(new[] { "col", "report-subgenus-homonyms" });
                });

                config.AddBranch("iucn", iucn => {
                    iucn.SetDescription("IUCN Red List dataset commands");
                    iucn.AddCommand<IucnImportCommand>("import")
                        .WithDescription("Import IUCN CSV data from zip archives into the SQLite datastore.")
                        .WithExample(new[] { "iucn", "import" })
                        .WithExample(new[] { "iucn", "import", "--force" });
                });
            });

            return app.Run(args);
        }
    }
}
