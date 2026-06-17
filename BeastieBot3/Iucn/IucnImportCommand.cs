using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.ComponentModel;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// CLI entry point for IUCN CSV import. Orchestrates IucnImporter to read
// CSV exports (taxonomy, assessments) into Datastore:IUCN_sqlite_from_cvs.
// Looks for CSV files in Dirs:iucn_csv_folder. First step in IUCN data pipeline.
// Run via: iucn import

namespace BeastieBot3.Iucn;

[CommandInfo("iucn import", CommandKind.Destructive,
    "Import IUCN CSV data from zip archives into the SQLite datastore.",
    Reason = "Rewrites IUCN SQLite tables from the CSV release; --force drops existing data.",
    Rerun = RerunEffect.FreshDataset,
    RerunNote = "A new IUCN release belongs in a fresh database file (IUCN_<version>.sqlite). Importing a different release into an existing DB accumulates rows and double-counts; re-importing the same zip is skipped unless --force.",
    Examples = new[] { "iucn import", "iucn import --force" })]
public sealed class IucnImportCommand : Command<IucnImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--force")]
        [Description("Re-import zip files even if already imported; existing rows for that zip will be replaced.")]
        public bool Force { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var paths = settings.CreatePaths();

        var cvsDir = paths.GetIucnCvsDir();
        if (string.IsNullOrWhiteSpace(cvsDir) || !Directory.Exists(cvsDir)) {
            AnsiConsole.MarkupLine("[red]IUCN CVS directory not found. Configure [bold]Datasets:IUCN_CVS_dir[/] in paths.ini.[/]");
            return -1;
        }

        var redlistVersionHint = IucnImporter.ExtractRedlistVersionFromPath(cvsDir);

        var databasePath = paths.GetIucnDatabasePath();

        if (string.IsNullOrWhiteSpace(databasePath)) {
            var datastore = paths.GetDatastoreDir();
            var targetDir = !string.IsNullOrWhiteSpace(datastore) ? datastore! : baseDir;
            Directory.CreateDirectory(targetDir);

            var fileStem = string.Equals(redlistVersionHint, "unknown", StringComparison.OrdinalIgnoreCase)
                ? "IUCN"
                : $"IUCN_{redlistVersionHint}";

            databasePath = Path.Combine(targetDir, fileStem + ".sqlite");
            AnsiConsole.MarkupLine($"[grey]Using default IUCN database path:[/] {databasePath}");
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

        var importer = new IucnImporter(AnsiConsole.Console, connection, cvsDir, settings.Force, redlistVersionHint);
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

        if (anyFailures)
        {
            AnsiConsole.MarkupLine("[red]One or more zip files failed to import. Review the logs above.[/]");
            return -2;
        }

        /*
        // Vacuum the database to optimize it
        // (takes a long time and doesn't make much difference)
        AnsiConsole.MarkupLine("[grey]Running VACUUM...[/]");
        using (var vacuum = connection.CreateCommand())
        {
            vacuum.CommandText = "VACUUM;";
            vacuum.ExecuteNonQuery();
        }
        AnsiConsole.MarkupLine("[grey]VACUUM completed.[/]");
        */

        AnsiConsole.MarkupLine("[green]Import complete.[/]");
        return 0;
    }
}
