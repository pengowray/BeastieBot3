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
    RerunNote = "A new IUCN release belongs in a fresh database file (IUCN_<version>.sqlite). When the configured database already holds a different release, the import creates that new file itself and tells you which paths.ini line to change; it never overwrites the previous release unless you pass --force --replace-release. Re-importing the same zip is skipped unless --force.",
    Examples = new[] { "iucn import", "iucn import --force" })]
public sealed class IucnImportCommand : Command<IucnImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--force")]
        [Description("Re-import zip files even if already imported; existing rows for that zip will be replaced.")]
        public bool Force { get; init; }

        [CommandOption("--replace-release")]
        [Description("With --force, allow wiping a database that holds a different release instead of importing into a new file.")]
        public bool ReplaceRelease { get; init; }
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

        var zipFiles = Directory.EnumerateFiles(cvsDir, "*.zip", SearchOption.AllDirectories)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (zipFiles.Count == 0) {
            AnsiConsole.MarkupLine($"[yellow]No zip files found under:[/] {cvsDir}");
            return 0;
        }

        // The importer reads each zip's release from its path below the CSV folder, and IUCN's
        // random zip filenames can contain digit pairs that read as a version. Catch that here,
        // before any database is opened, instead of filing rows under a release nobody meant.
        if (!string.Equals(redlistVersionHint, "unknown", StringComparison.OrdinalIgnoreCase)) {
            var misread = zipFiles
                .Select(z => new {
                    Name = Path.GetRelativePath(cvsDir, z),
                    Version = IucnImporter.ExtractRedlistVersionFromPath(Path.GetRelativePath(cvsDir, z)),
                })
                .Where(z => !string.Equals(z.Version, "unknown", StringComparison.OrdinalIgnoreCase)
                            && !string.Equals(z.Version, redlistVersionHint, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (misread.Count > 0) {
                AnsiConsole.MarkupLine($"[red]Stopping: the release read from these files is not {Markup.Escape(redlistVersionHint)}, the release the CSV folder is named for.[/]");
                foreach (var z in misread) {
                    AnsiConsole.MarkupLine($"  reads as [bold]{Markup.Escape(z.Version)}[/]  {Markup.Escape(z.Name)}");
                }
                AnsiConsole.MarkupLine($"Put each download in its own subfolder whose name starts with the release, for example \"{Markup.Escape(redlistVersionHint)} non-passerines\".");
                return -1;
            }
        }

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

        // One release per database file. If the configured file already holds a different release,
        // send this release to its own file rather than refusing (or, under --force, destroying the
        // previous release). paths.ini is never edited here, so say plainly that it still points at
        // the old file and everything else keeps reading that until the user changes it.
        var existingRelease = IucnImporter.FindReleaseConflict(ReadCompletedReleases(fullDbPath), redlistVersionHint);
        if (existingRelease is not null) {
            var knownVersion = !string.Equals(redlistVersionHint, "unknown", StringComparison.OrdinalIgnoreCase);

            if (settings.ReplaceRelease) {
                if (!settings.Force) {
                    AnsiConsole.MarkupLine("[red]--replace-release only works together with --force.[/]");
                    AnsiConsole.MarkupLine($"Add --force to wipe [bold]{Markup.Escape(fullDbPath)}[/] and rebuild it as release [bold]{Markup.Escape(redlistVersionHint)}[/].");
                    return -1;
                }
                AnsiConsole.MarkupLine($"[yellow]Wiping[/] {Markup.Escape(fullDbPath)} [yellow]and rebuilding it as release[/] [bold]{Markup.Escape(redlistVersionHint)}[/] (was {Markup.Escape(existingRelease)}).");
            } else if (knownVersion) {
                var switchedDir = Path.GetDirectoryName(fullDbPath);
                var switchedPath = Path.Combine(
                    string.IsNullOrWhiteSpace(switchedDir) ? baseDir : switchedDir,
                    $"IUCN_{redlistVersionHint}.sqlite");

                AnsiConsole.MarkupLine($"[yellow]Release {Markup.Escape(redlistVersionHint)} is new.[/] {Markup.Escape(fullDbPath)} holds release {Markup.Escape(existingRelease)}, so it is left untouched.");
                AnsiConsole.MarkupLine($"[green]Importing into a new file instead:[/] {Markup.Escape(switchedPath)}");
                AnsiConsole.MarkupLine("[yellow]Update paths.ini before running anything else:[/]");
                AnsiConsole.MarkupLine($"  [bold][[Datastore]] IUCN_sqlite_from_cvs={Markup.Escape(switchedPath)}[/]");
                AnsiConsole.MarkupLine($"Until you do, every other command and the web pages still read the {Markup.Escape(existingRelease)} database. Restart serve after editing.");
                AnsiConsole.MarkupLine($"[grey]To wipe and reuse the old file instead, run: iucn import --force --replace-release[/]");

                fullDbPath = switchedPath;
            } else if (settings.Force) {
                AnsiConsole.MarkupLine($"[red]Refusing to wipe[/] {Markup.Escape(fullDbPath)}[red]: it holds release[/] [bold]{Markup.Escape(existingRelease)}[/][red], and the release of the incoming zips could not be read from the folder name.[/]");
                AnsiConsole.MarkupLine($"Name the CSV folder for the release (for example IUCN_CVS_2026-1) so a new database can be created, or add --replace-release to wipe this one anyway.");
                return -1;
            }
        }

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

    // Which release(s) a database file already holds, read before anything is opened for writing so
    // the command can pick a different target file. Mirrors IucnImporter's own completed-imports
    // query; a file with no import_metadata table yet counts as empty.
    private static IReadOnlyList<string> ReadCompletedReleases(string databasePath) {
        if (!File.Exists(databasePath)) {
            return Array.Empty<string>();
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ToString();

        try {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT redlist_version FROM import_metadata WHERE ended_at IS NOT NULL;";
            using var reader = cmd.ExecuteReader();
            var versions = new List<string>();
            while (reader.Read()) {
                if (!reader.IsDBNull(0)) {
                    versions.Add(reader.GetString(0));
                }
            }
            return versions;
        } catch (SqliteException) {
            return Array.Empty<string>();
        }
    }
}
