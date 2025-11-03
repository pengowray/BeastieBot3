using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class ColImportCommand : Command<ColImportCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--force")]
        [Description("Re-import zip files even if the database already exists; existing files will be replaced.")]
        public bool Force { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var colDir = paths.GetColDir();
        if (string.IsNullOrWhiteSpace(colDir) || !Directory.Exists(colDir)) {
            AnsiConsole.MarkupLine("[red]COL directory not found. Configure [bold]Datasets:COL_dir[/] in paths.ini.[/]");
            return -1;
        }

        var datastoreDir = paths.GetDatastoreDir();
        if (string.IsNullOrWhiteSpace(datastoreDir)) {
            datastoreDir = Path.Combine(baseDir, "datastore");
            Directory.CreateDirectory(datastoreDir);
            AnsiConsole.MarkupLine($"[grey]Using default datastore directory:[/] {datastoreDir}");
        } else {
            Directory.CreateDirectory(datastoreDir);
        }

        var zipFiles = Directory.EnumerateFiles(colDir, "*.zip", SearchOption.AllDirectories)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (zipFiles.Count == 0) {
            AnsiConsole.MarkupLine($"[yellow]No ColDP zip files found under:[/] {colDir}");
            return 0;
        }

        AnsiConsole.MarkupLine($"[grey]Preparing to import {zipFiles.Count} ColDP zip file(s).[/]");
        var anyFailures = false;

        foreach (var zipFile in zipFiles) {
            cancellationToken.ThrowIfCancellationRequested();
            try {
                var importer = new ColImporter(AnsiConsole.Console, zipFile, colDir, datastoreDir!, settings.Force);
                importer.Process(cancellationToken);
            } catch (OperationCanceledException) {
                throw;
            } catch (Exception ex) {
                anyFailures = true;
                AnsiConsole.MarkupLine($"[red]Failed to import[/] {zipFile}: {ex.Message}");
            }
        }

        if (anyFailures) {
            AnsiConsole.MarkupLine("[red]One or more ColDP zip files failed. Review the errors above.[/]");
            return -2;
        }

        AnsiConsole.MarkupLine("[green]ColDP import complete.[/]");
        return 0;
    }
}
