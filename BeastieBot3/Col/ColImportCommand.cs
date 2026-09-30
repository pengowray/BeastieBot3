using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// CLI entry point for COL import. Locates ColDP ZIP files in the configured directory
// (from paths.ini), creates output SQLite databases named after each archive, and
// delegates to ColImporter. Supports --force to re-import existing databases.
// Output: one col_coldp_<label>.sqlite per archive in [Datastore] datastore_dir.

namespace BeastieBot3.Col;

[CommandInfo("col import", CommandKind.Destructive,
    "Import Catalogue of Life ColDP zip archives into individual SQLite databases.",
    Reason = "With --force, col import deletes and rebuilds the database for every ColDP zip in Datasets:COL_dir, and each rebuild takes tens of minutes. Without --force, it skips a complete database, and deletes and rebuilds an incomplete one.",
    Rerun = RerunEffect.FreshDataset,
    RerunNote = "For a new CoL release, set Datasets:COL_dir to the folder with its ColDP zip and run again. Each release gets its own col_coldp_<label>.sqlite, so a new release imports without --force. After the import, set Datastore:COL_sqlite to the new col_coldp_<label>.sqlite file; until you do, every command still reads the previous release. Then restart serve, which reads paths.ini only when it starts.",
    Examples = new[] { "col import", "col import --force" })]
public sealed class ColImportCommand : Command<ColImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--force")]
        [Description("Re-import each ColDP zip even if its database is complete. The existing database file is replaced.")]
        public bool Force { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var paths = settings.CreatePaths();

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
        // The import never changes paths.ini, so a new release is not used until COL_sqlite names it.
        var colSqlite = paths.GetColSqlitePath();
        AnsiConsole.MarkupLineInterpolated($"[grey]CoL database read by other commands (Datastore:COL_sqlite):[/] {(string.IsNullOrWhiteSpace(colSqlite) ? "not set" : colSqlite)}");
        AnsiConsole.MarkupLine("[grey]To use a newly imported release, set Datastore:COL_sqlite in paths.ini to its col_coldp_<label>.sqlite file, then restart serve if it is running.[/]");
        return 0;
    }
}
