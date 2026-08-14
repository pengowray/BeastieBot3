using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

// What the CSV route looks like on disk right now: which release is sitting in the input
// folder, whether its zips have been imported, and which database file holds them.
//
// Read-only, offline, and cheap enough for the web workflow page to poll: it answers
// "is THIS release in, and is it the one everything reads?", which "when did `iucn import`
// last run" cannot. A run three minutes ago says nothing about the release downloaded
// since, and a database that holds the previous release looks perfectly healthy.

namespace BeastieBot3.Iucn;

// One database file's completed imports. Half-written imports (ended_at NULL) are ignored,
// so a crashed or in-flight run never reads as done.
public sealed record IucnDatabaseState {
    public required string Path { get; init; }
    public required bool Exists { get; init; }
    public IReadOnlyList<string> Releases { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> ImportedZips { get; init; } = Array.Empty<string>();

    public string FileName => System.IO.Path.GetFileName(Path);

    public bool Holds(string? release) =>
        !string.IsNullOrWhiteSpace(release)
        && Releases.Any(r => string.Equals(r, release, StringComparison.OrdinalIgnoreCase));

    // The release this file holds, for reporting what is in the way. One release per file is
    // enforced on import, so the first is the only one in practice.
    public string? HeldRelease => Releases.Count > 0 ? Releases[0] : null;
}

public sealed record IucnReleaseState {
    public string? InputDir { get; init; }
    public bool InputDirExists { get; init; }

    // The release the input folder reads as; null when nothing in its name looks like a version.
    public string? InputRelease { get; init; }

    // Zip paths relative to InputDir, forward-slashed to match import_metadata.filename.
    public IReadOnlyList<string> Zips { get; init; } = Array.Empty<string>();

    // Zips whose own path reads as some other release — the trap the import stops on.
    public IReadOnlyList<string> MisreadZips { get; init; } = Array.Empty<string>();
    public string? MisreadRelease { get; init; }

    // Datastore:IUCN_sqlite_from_cvs — the file every other command and page reads.
    public IucnDatabaseState? ConfiguredDb { get; init; }

    // IUCN_<InputRelease>.sqlite beside it, when that is a different file that exists. This is
    // where `iucn import` diverts a new release rather than overwrite the configured one.
    public IucnDatabaseState? ReleaseDb { get; init; }

    // How many of the input folder's zips a given database has finished importing.
    public int ImportedZipCount(IucnDatabaseState? db) =>
        db is null ? 0 : Zips.Count(z => db.ImportedZips.Contains(z, StringComparer.OrdinalIgnoreCase));

    // The database holding the input release, preferring the configured one. Null = not imported.
    public IucnDatabaseState? HoldingDb =>
        ConfiguredDb is not null && ConfiguredDb.Holds(InputRelease) ? ConfiguredDb
        : ReleaseDb is not null && ReleaseDb.Holds(InputRelease) ? ReleaseDb
        : null;
}

public static class IucnReleaseStateReader {
    public static IucnReleaseState Read(PathsService paths) {
        string? inputDir = null;
        try { inputDir = paths.GetIucnCvsDir(); } catch { /* unset or unreadable — reported as no folder */ }

        var full = string.IsNullOrWhiteSpace(inputDir) ? null : Path.GetFullPath(inputDir);
        var exists = full is not null && Directory.Exists(full);

        var release = full is null ? null : Normalise(IucnImporter.ExtractRedlistVersionFromPath(full));

        var zips = new List<string>();
        if (exists) {
            try {
                zips = Directory.EnumerateFiles(full!, "*.zip", SearchOption.AllDirectories)
                    .Select(z => Path.GetRelativePath(full!, z).Replace('\\', '/'))
                    .OrderBy(z => z, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            } catch { /* unreadable folder — reported as no zips */ }
        }

        // Same check the import makes before opening anything: a zip whose path reads as another
        // release stops the run, because IUCN's random zip filenames contain digit pairs like
        // 1373-414 that the version regex matches.
        var misread = new List<string>();
        string? misreadRelease = null;
        if (release is not null) {
            foreach (var zip in zips) {
                var perZip = Normalise(IucnImporter.ExtractRedlistVersionFromPath(zip));
                if (perZip is null || string.Equals(perZip, release, StringComparison.OrdinalIgnoreCase)) continue;
                misread.Add(zip);
                misreadRelease ??= perZip;
            }
        }

        var configuredPath = ResolveConfiguredDbPath(paths, release);
        var configured = configuredPath is null ? null : ReadDatabase(configuredPath);
        var releaseDb = ResolveReleaseDbPath(configuredPath, release) is { } siblingPath
            ? ReadDatabase(siblingPath)
            : null;

        return new IucnReleaseState {
            InputDir = full,
            InputDirExists = exists,
            InputRelease = release,
            Zips = zips,
            MisreadZips = misread,
            MisreadRelease = misreadRelease,
            ConfiguredDb = configured,
            ReleaseDb = releaseDb is { Exists: true } ? releaseDb : null,
        };
    }

    // The database `iucn import` writes to: the configured path, or the release-named default
    // under the datastore folder when none is set. Mirrors IucnImportCommand's own resolution.
    internal static string? ResolveConfiguredDbPath(PathsService paths, string? release) {
        try {
            var configured = paths.GetIucnDatabasePath();
            if (!string.IsNullOrWhiteSpace(configured)) return Path.GetFullPath(configured);

            var datastore = paths.GetDatastoreDir();
            if (string.IsNullOrWhiteSpace(datastore)) return null;
            return Path.GetFullPath(Path.Combine(datastore, ReleaseFileStem(release) + ".sqlite"));
        } catch {
            return null;
        }
    }

    // The file a new release is diverted to, beside the configured one. Null when there is no
    // release to name it after, or when it would be the configured file itself.
    internal static string? ResolveReleaseDbPath(string? configuredPath, string? release) {
        if (configuredPath is null || release is null) return null;
        var dir = Path.GetDirectoryName(configuredPath);
        if (string.IsNullOrEmpty(dir)) return null;
        var sibling = Path.Combine(dir, ReleaseFileStem(release) + ".sqlite");
        return string.Equals(sibling, configuredPath, StringComparison.OrdinalIgnoreCase) ? null : sibling;
    }

    internal static string ReleaseFileStem(string? release) =>
        release is null ? "IUCN" : "IUCN_" + release;

    // "unknown" is the importer's sentinel for "no version in this path"; null reads better here.
    private static string? Normalise(string version) =>
        string.IsNullOrWhiteSpace(version) || string.Equals(version, "unknown", StringComparison.OrdinalIgnoreCase)
            ? null
            : version;

    // Completed imports only, in one pass. A missing import_metadata table (fresh or unrelated
    // file) reads as empty rather than throwing.
    internal static IucnDatabaseState ReadDatabase(string databasePath) {
        var state = new IucnDatabaseState { Path = databasePath, Exists = File.Exists(databasePath) };
        if (!state.Exists) return state;

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ToString();

        var releases = new List<string>();
        var zips = new List<string>();
        try {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT filename, redlist_version FROM import_metadata WHERE ended_at IS NOT NULL;";
            cmd.CommandTimeout = 5;
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) {
                if (!reader.IsDBNull(0)) zips.Add(reader.GetString(0).Replace('\\', '/'));
                if (!reader.IsDBNull(1)) {
                    var version = reader.GetString(1);
                    if (!releases.Contains(version, StringComparer.OrdinalIgnoreCase)) releases.Add(version);
                }
            }
        } catch (SqliteException) {
            return state;
        }

        return state with { Releases = releases, ImportedZips = zips };
    }
}
