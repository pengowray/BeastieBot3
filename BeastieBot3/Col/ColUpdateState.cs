using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

// Where a Catalogue of Life update has got to: which release is in the input folder, which one the
// configured database holds, whether config points at it, and whether the things built from CoL
// have been rebuilt since.
//
// CoL has no version check anywhere else — a database from the previous release looks perfectly
// healthy — and its consumers degrade quietly rather than failing, so their output simply stays
// frozen on the old release until someone re-runs them. Nothing surfaced that, which is why the
// flow's own notes had to warn about it in prose.
//
// Read-only, offline, and cheap enough to poll: the multi-GB archives are read for their metadata
// once and cached by (path, size, mtime).

namespace BeastieBot3.Col;

public sealed record ColLoadedRelease {
    public required string Path { get; init; }
    public required bool Exists { get; init; }
    public bool Complete { get; init; }              // has a finished import
    public string? Label { get; init; }              // the alias, e.g. "COL26.5 XR"
    public string? Version { get; init; }
    public string? Issued { get; init; }
    public DateTime? ImportedAt { get; init; }

    public string FileName => System.IO.Path.GetFileName(Path);
}

public sealed record ColInputRelease {
    public required string Dir { get; init; }
    public required int ArchiveCount { get; init; }
    public string? Label { get; init; }
    public string? Version { get; init; }
    public string? Issued { get; init; }
}

// A previous release's database left on disk. Each is multi-GB, so it is worth saying so.
public sealed record ColLeftover {
    public required string Path { get; init; }
    public required long Bytes { get; init; }
    public string FileName => System.IO.Path.GetFileName(Path);
}

public sealed record ColUpdateState {
    public ColLoadedRelease? Loaded { get; init; }
    public ColInputRelease? Input { get; init; }

    // True when the input archive has not been read yet, so "is there a newer release?" is
    // unanswered rather than answered "no". Reading a multi-GB archive for the first time takes
    // around twenty seconds, which the workflow page cannot wait for; it is warmed in the
    // background and the next poll has it.
    public bool InputPending { get; init; }

    // "fresh" | "update-available" | "not-imported" | "incomplete" | "no-input" | "unknown"
    public required string Status { get; init; }
    public bool? Fresh { get; init; }
    public required string Message { get; init; }

    // Datasets:COL_dir and Datastore:COL_sqlite can drift apart, and nothing else notices.
    public bool ConfigDisagrees { get; init; }

    public IReadOnlyList<ColLeftover> Leftovers { get; init; } = Array.Empty<ColLeftover>();
    public long LeftoverBytes => Leftovers.Sum(l => l.Bytes);

    // When the release now being read finished importing. Anything built from CoL before this
    // reflects a different release.
    //
    // Deliberately not "when paths.ini last changed", which would seem to be the moment this
    // release became the one being read: paths.ini also changes for reasons that have nothing to
    // do with CoL, and every such edit would mark all of these outputs stale forever. A release
    // imported but not yet repointed is the repoint step's business, not this one's.
    public DateTime? CurrentSince { get; init; }

    public bool IsStale(DateTime? artifactModified) =>
        CurrentSince is { } since && (artifactModified is null || artifactModified.Value < since);
}

public static class ColUpdateStateReader {
    private static readonly ConcurrentDictionary<string, ColDatasetInfo?> ZipCache = new();
    private static readonly ConcurrentDictionary<string, byte> Warming = new();

    public static void ClearZipCache() {
        ZipCache.Clear();
        Warming.Clear();
    }

    /// <param name="readArchives">
    /// True to read the input archives now, blocking for as long as that takes (about twenty
    /// seconds the first time, then cached). False for callers that must return promptly — the
    /// workflow page polls every few seconds — which read only what is already cached and warm
    /// the rest in the background.
    /// </param>
    public static ColUpdateState Read(PathsService paths, bool readArchives = true) {
        var loaded = ReadLoaded(paths);
        var (input, pending) = ReadNewestInput(paths, readArchives);
        var (status, fresh, message) = Evaluate(loaded, input, pending);

        return new ColUpdateState {
            Loaded = loaded,
            Input = input,
            InputPending = pending,
            Status = status,
            Fresh = fresh,
            Message = message,
            ConfigDisagrees = !pending && ConfigDisagrees(loaded, input),
            Leftovers = FindLeftovers(loaded),
            CurrentSince = CurrentSince(loaded),
        };
    }

    // The importer names the file after the alias inside the zip, so a database whose name doesn't
    // match the folder it was built from is the usual sign that only one of the two config keys was
    // changed. Compared loosely — the alias is turned into a filename by replacing spaces.
    private static bool ConfigDisagrees(ColLoadedRelease? loaded, ColInputRelease? input) {
        if (loaded is not { Exists: true, Label: { Length: > 0 } loadedLabel }) return false;
        if (input?.Label is not { Length: > 0 } inputLabel) return false;
        return !string.Equals(Slug(loadedLabel), Slug(inputLabel), StringComparison.OrdinalIgnoreCase);
    }

    private static string Slug(string label) => label.Replace(' ', '_').Replace('.', '_');

    private static DateTime? CurrentSince(ColLoadedRelease? loaded) =>
        loaded is { Exists: true } ? loaded.ImportedAt : null;

    // The previous release's database and its enrich-cache sidecar sit beside the current one and
    // are never read again.
    private static IReadOnlyList<ColLeftover> FindLeftovers(ColLoadedRelease? loaded) {
        if (loaded is not { Exists: true }) return Array.Empty<ColLeftover>();
        var dir = Path.GetDirectoryName(loaded.Path);
        if (string.IsNullOrEmpty(dir)) return Array.Empty<ColLeftover>();

        try {
            var current = Path.GetFileName(loaded.Path);
            return new DirectoryInfo(dir)
                .EnumerateFiles("col_coldp_*.sqlite*", SearchOption.TopDirectoryOnly)
                .Where(f => !f.Name.StartsWith(current, StringComparison.OrdinalIgnoreCase))
                .OrderByDescending(f => f.Length)
                .Select(f => new ColLeftover { Path = f.FullName, Bytes = f.Length })
                .ToList();
        } catch {
            return Array.Empty<ColLeftover>();
        }
    }

    internal static ColLoadedRelease? ReadLoaded(PathsService paths) {
        string? dbPath;
        try { dbPath = paths.GetColSqlitePath(); } catch { return null; }
        if (string.IsNullOrWhiteSpace(dbPath)) return null;

        var full = Path.GetFullPath(dbPath);
        if (!File.Exists(full)) return new ColLoadedRelease { Path = full, Exists = false };

        try {
            var csb = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();

            string? alias = null, version = null, issued = null;
            try {
                using var meta = conn.CreateCommand();
                meta.CommandText = "SELECT alias, version, issued FROM dataset_metadata ORDER BY rowid DESC LIMIT 1";
                using var reader = meta.ExecuteReader();
                if (reader.Read()) {
                    alias = reader.IsDBNull(0) ? null : reader.GetString(0);
                    version = reader.IsDBNull(1) ? null : reader.GetString(1);
                    issued = reader.IsDBNull(2) ? null : reader.GetString(2);
                }
            } catch { /* dataset_metadata missing — leave nulls */ }

            var complete = false;
            DateTime? importedAt = null;
            try {
                using var imp = conn.CreateCommand();
                imp.CommandText = "SELECT MAX(ended_at) FROM import_metadata WHERE ended_at IS NOT NULL";
                if (imp.ExecuteScalar() is string ended) {
                    complete = true;
                    importedAt = ParseUtc(ended);
                }
            } catch { /* import_metadata missing — treat as incomplete */ }

            importedAt ??= File.GetLastWriteTimeUtc(full);

            return new ColLoadedRelease {
                Path = full,
                Exists = true,
                Complete = complete,
                Label = !string.IsNullOrWhiteSpace(alias) ? alias : (!string.IsNullOrWhiteSpace(version) ? version : null),
                Version = version,
                Issued = issued,
                ImportedAt = importedAt,
            };
        } catch {
            // File exists but won't open/query (corrupt / mid-write) — exists but incomplete.
            return new ColLoadedRelease { Path = full, Exists = true, Complete = false };
        }
    }

    internal static (ColInputRelease? Input, bool Pending) ReadNewestInput(PathsService paths, bool readArchives = true) {
        string? dir;
        try { dir = paths.GetColDir(); } catch { return (null, false); }
        if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) return (null, false);

        string[] zips;
        try {
            zips = Directory.EnumerateFiles(dir, "*.zip", SearchOption.AllDirectories).ToArray();
        } catch {
            return (null, false);
        }
        if (zips.Length == 0) return (null, false);

        ColDatasetInfo? newest = null;
        var pending = false;
        foreach (var zip in zips) {
            var key = CacheKey(zip);
            ColDatasetInfo? info;
            if (readArchives) {
                info = ReadZipCached(zip, key);
            } else if (key is not null && ZipCache.TryGetValue(key, out var cached)) {
                info = cached;
            } else {
                pending = true;
                WarmInBackground(zip, key);
                continue;
            }
            if (info is null) continue;
            if (newest is null || CompareIssued(info.Issued, newest.Issued) > 0) newest = info;
        }

        var input = newest is null ? null : new ColInputRelease {
            Dir = Path.GetFullPath(dir),
            ArchiveCount = zips.Length,
            Label = newest.DisplayLabel,
            Version = newest.Version,
            Issued = newest.Issued,
        };
        return (input, pending);
    }

    // Fire-and-forget so the next poll has the answer. Deduped, because the page asks repeatedly
    // while the first read is still running.
    private static void WarmInBackground(string zipPath, string? key) {
        if (key is null || !Warming.TryAdd(key, 0)) return;
        System.Threading.Tasks.Task.Run(() => {
            try { ReadZipCached(zipPath, key); } catch { /* the next poll tries again */ }
            finally { Warming.TryRemove(key, out _); }
        });
    }

    private static string? CacheKey(string zipPath) {
        try {
            var fi = new FileInfo(zipPath);
            return $"{fi.FullName}|{fi.Length}|{fi.LastWriteTimeUtc.Ticks}";
        } catch {
            return null;
        }
    }

    private static ColDatasetInfo? ReadZipCached(string zipPath, string? key) =>
        key is null
            ? ColDatasetInfo.ReadFromZip(zipPath)
            : ZipCache.GetOrAdd(key, _ => ColDatasetInfo.ReadFromZip(zipPath));

    internal static (string status, bool? fresh, string message) Evaluate(
        ColLoadedRelease? loaded, ColInputRelease? input, bool inputPending = false) {
        if (loaded is not { Exists: true }) {
            var hint = input is not null
                ? $"The input folder holds {Describe(input.Label, input.Issued)} — run `col import`, then repoint Datastore:COL_sqlite."
                : "Set Datastore:COL_sqlite (and import a ColDP archive with `col import`).";
            return ("not-imported", false, "No imported Catalogue of Life database found. " + hint);
        }
        if (!loaded.Complete) {
            return ("incomplete", false, "The imported database has no completed import (it may be corrupt or half-written) — re-run `col import --force`.");
        }
        if (inputPending) {
            return ("input-pending", null, "Still reading the archive in the input folder, so a newer release can't be reported yet.");
        }
        if (input is null) {
            return ("no-input", null, "Loaded release shown; the input folder (Datasets:COL_dir) is missing or has no ColDP zip, so a newer release can't be detected.");
        }

        if (!string.IsNullOrWhiteSpace(loaded.Version) && !string.IsNullOrWhiteSpace(input.Version) &&
            string.Equals(loaded.Version, input.Version, StringComparison.OrdinalIgnoreCase)) {
            return ("fresh", true, "The imported database matches the newest release in the input folder.");
        }

        var cmp = CompareIssued(input.Issued, loaded.Issued);
        if (cmp > 0 || (cmp == 0 && !(HasText(loaded.Version) && HasText(input.Version)))) {
            return ("update-available", false,
                $"The input folder has a newer release ({Describe(input.Label, input.Issued)}) than the loaded database ({Describe(loaded.Label, loaded.Issued)}). Re-run `col import`, then repoint Datastore:COL_sqlite to the new file and restart serve.");
        }

        return ("fresh", true, "The imported database is current relative to the input folder.");
    }

    private static bool HasText(string? s) => !string.IsNullOrWhiteSpace(s);

    // Compares two ColDP `issued` strings (ISO dates like 2026-05-15). Returns >0 if a is newer
    // than b. Parses as dates when possible, else ordinal string compare; a blank sorts oldest.
    internal static int CompareIssued(string? a, string? b) {
        var hasA = DateTimeOffset.TryParse(a, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var da);
        var hasB = DateTimeOffset.TryParse(b, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var db);
        if (hasA && hasB) return da.CompareTo(db);
        return string.Compare(a ?? "", b ?? "", StringComparison.Ordinal);
    }

    internal static string Describe(string? label, string? issued) {
        var name = string.IsNullOrWhiteSpace(label) ? "unknown release" : label!;
        return string.IsNullOrWhiteSpace(issued) ? name : $"{name}, issued {issued}";
    }

    private static DateTime? ParseUtc(string? text) =>
        DateTime.TryParse(text, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed
            : null;
}
