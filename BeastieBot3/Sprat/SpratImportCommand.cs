using System;
using System.ComponentModel;
using System.IO;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// CLI entry point for the SPRAT import. Reads the single EPBC report CSV configured in paths.ini
// ([Datasets] SPRAT_csv) and writes a self-contained sprat.sqlite ([Datastore] SPRAT_sqlite),
// rebuilding from scratch (a complete existing DB is skipped unless --force). Delegates the CSV
// load to SpratImporter. Registers automatically via [CommandInfo]; no Program.cs wiring.

namespace BeastieBot3.Sprat;

[CommandInfo("sprat import", CommandKind.Destructive,
    "Import the Australian SPRAT (EPBC) species report CSV into its own SQLite datastore.",
    Reason = "Rebuilds the SPRAT SQLite table from the report CSV; --force replaces an existing database.",
    Rerun = RerunEffect.FreshDataset,
    RerunNote = "The SPRAT report CSV imports into its own sprat.sqlite. A complete existing DB is skipped unless --force. After downloading a fresh SPRAT report, point Datasets:SPRAT_csv at it and re-run.",
    Examples = new[] { "sprat import", "sprat import --force" })]
public sealed class SpratImportCommand : Command<SpratImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--csv <PATH>")]
        [Description("Override the SPRAT report CSV path (default: Datasets:SPRAT_csv).")]
        public string? CsvPath { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Override the output SQLite path (default: Datastore:SPRAT_sqlite).")]
        public string? DatabasePath { get; init; }

        [CommandOption("--force")]
        [Description("Re-import even if a completed database already exists; the existing file is replaced.")]
        public bool Force { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();

        var csvPath = !string.IsNullOrWhiteSpace(settings.CsvPath) ? settings.CsvPath : paths.GetSpratCsvPath();
        if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath)) {
            AnsiConsole.MarkupLine("[red]SPRAT report CSV not found. Set [bold]Datasets:SPRAT_csv[/] in paths.ini or pass [bold]--csv[/].[/]");
            return -1;
        }

        var dbPath = paths.ResolveSpratDatabasePath(settings.DatabasePath);

        if (File.Exists(dbPath)) {
            if (!settings.Force && SpratImporter.IsImportComplete(dbPath)) {
                AnsiConsole.MarkupLine($"[yellow]SPRAT database already imported; skipping (use --force to rebuild):[/] {dbPath}");
                return 0;
            }
            AnsiConsole.MarkupLine(settings.Force
                ? $"[grey]Removing existing SPRAT database:[/] {dbPath}"
                : $"[yellow]Existing SPRAT database is incomplete; rebuilding:[/] {dbPath}");
            DeleteDatabaseFiles(dbPath);
        }

        var version = Path.GetFileNameWithoutExtension(csvPath);

        try {
            using var store = SpratStore.Open(dbPath);
            var importer = new SpratImporter(AnsiConsole.Console, store.Connection, csvPath, version);
            importer.Run(cancellationToken);
        } catch (OperationCanceledException) {
            throw;
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]SPRAT import failed:[/] {ex.Message}");
            return -2;
        }

        AnsiConsole.MarkupLine("[green]SPRAT import complete.[/]");
        return 0;
    }

    // The store opens in WAL mode, so a prior import leaves -wal/-shm sidecars next to the DB.
    private static void DeleteDatabaseFiles(string dbPath) {
        foreach (var suffix in new[] { "", "-wal", "-shm" }) {
            var path = dbPath + suffix;
            if (File.Exists(path)) {
                File.Delete(path);
            }
        }
    }
}
