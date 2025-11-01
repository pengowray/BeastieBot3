using System;
using System.IO;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Threading;

namespace BeastieBot3 {
    public sealed class CheckColCommand : Command<CommonSettings> {
        public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken) {
            var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
            var iniFile = settings.IniFile ?? "paths.ini";
            var paths = new PathsService(iniFile, baseDir);

            var colPath = paths.GetColDir() ?? "/app/datasets/Catalogue_of_Life_2025-10-10_XR";
            if (Directory.Exists(colPath)) {
                AnsiConsole.MarkupLine($"[green]COL dataset mounted at:[/] {colPath}");
            } else {
                AnsiConsole.MarkupLine("[yellow]COL dataset not mounted. Configure docker-compose volume mapping.[/]");
            }

            AnsiConsole.MarkupLine($"[grey]Using settings from:[/] {paths.SourceFilePath}");
            return 0;
        }
    }
}
