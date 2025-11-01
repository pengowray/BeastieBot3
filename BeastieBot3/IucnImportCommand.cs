using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.ComponentModel;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class IucnImportCommand : Command<IucnImportCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--force")]
        [Description("Re-import zip files even if already imported; existing rows for that zip will be replaced.")]
        public bool Force { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var cvsDir = paths.GetIucnCvsDir();
        if (string.IsNullOrWhiteSpace(cvsDir) || !Directory.Exists(cvsDir)) {
            AnsiConsole.MarkupLine("[red]IUCN CVS directory not found. Configure [bold]Datasets:IUCN_CVS_dir[/] in paths.ini.[/]");
            return -1;
        }

        var databasePath = paths.GetMainDatabasePath();
        if (string.IsNullOrWhiteSpace(databasePath)) {
            var datastore = paths.GetDatastoreDir();
            if (!string.IsNullOrWhiteSpace(datastore)) {
                Directory.CreateDirectory(datastore);
                databasePath = Path.Combine(datastore, "beastiebot.db");
            } else {
                databasePath = Path.Combine(baseDir, "beastiebot.db");
            }
            AnsiConsole.MarkupLine($"[yellow]No [bold]Datastore:MainDB[/] configured; defaulting to:[/] {databasePath}");
        }

        var fullDbPath = Path.GetFullPath(databasePath);
        var dbDirectory = Path.GetDirectoryName(fullDbPath);
        if (!string.IsNullOrWhiteSpace(dbDirectory)) {
            Directory.CreateDirectory(dbDirectory);
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = fullDbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Default
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using (var pragmaCmd = connection.CreateCommand()) {
            pragmaCmd.CommandText = "PRAGMA foreign_keys = ON;";
            pragmaCmd.ExecuteNonQuery();
        }

        var zipFiles = Directory.EnumerateFiles(cvsDir, "*.zip", SearchOption.AllDirectories)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (zipFiles.Count == 0) {
            AnsiConsole.MarkupLine($"[yellow]No zip files found under:[/] {cvsDir}");
            return 0;
        }

        AnsiConsole.MarkupLine($"[grey]Importing into database:[/] {fullDbPath}");

        var importer = new IucnImporter(AnsiConsole.Console, connection, cvsDir, settings.Force);
        var anyFailures = false;

        foreach (var zipPath in zipFiles) {
            cancellationToken.ThrowIfCancellationRequested();
            try {
                importer.ProcessZip(zipPath, cancellationToken);
            } catch (OperationCanceledException) {
                throw;
            } catch (Exception ex) {
                anyFailures = true;
                AnsiConsole.MarkupLine($"[red]Failed to import[/] {zipPath}: {ex.Message}");
            }
        }

        if (anyFailures) {
            AnsiConsole.MarkupLine("[red]One or more zip files failed to import. Review the logs above.[/]");
            return -2;
        }

        AnsiConsole.MarkupLine("[green]Import complete.[/]");
        return 0;
    }
}
