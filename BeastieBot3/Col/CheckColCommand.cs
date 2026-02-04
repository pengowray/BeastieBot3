using System;
using System.IO;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Threading;

// Diagnostic command to verify the COL dataset directory is accessible. Primarily
// useful in Docker where COL data is volume-mounted (configured in docker-compose.yml).
// Checks the path from [Dirs] col_dir in paths.ini, falling back to default mount point.

namespace BeastieBot3.Col;

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
