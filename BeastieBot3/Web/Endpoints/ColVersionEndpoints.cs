using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using BeastieBot3.Col;
using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Web.Endpoints;

// "Is my imported Catalogue of Life release out of date?" endpoint — the CoL
// counterpart to /api/iucn-version, but fully OFFLINE. It compares two local
// facts, so it catches both ways a CoL update goes wrong:
//   loaded — the release inside Datastore:COL_sqlite (dataset_metadata + a
//            completed import_metadata row)
//   input  — the newest ColDP archive sitting in Datasets:COL_dir (read from
//            each zip's metadata.yaml via ColDatasetInfo)
// If the input folder holds a newer release than the loaded DB, you either
// haven't re-run `col import` or haven't repointed COL_sqlite — either way the
// badge goes amber. The zip read is cached by (path,size,mtime) so the Data
// sources poll never re-inflates an unchanged multi-GB archive.

public static class ColVersionEndpoints {
    private static readonly ConcurrentDictionary<string, ColDatasetInfo?> ZipCache = new();

    public static void MapColVersionEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/col-version", (HttpContext ctx, PathsService paths) => {
            var forceRefresh = ctx.Request.Query.TryGetValue("refresh", out var r) && r == "1";
            if (forceRefresh) ZipCache.Clear();

            var loaded = ReadLoaded(paths);
            var input = ReadNewestInput(paths);

            var (status, fresh, message) = Evaluate(loaded, input);

            return Results.Json(new {
                loaded = loaded.Label,
                loadedVersion = loaded.Version,
                loadedIssued = loaded.Issued,
                loadedComplete = loaded.Complete,
                loadedExists = loaded.Exists,
                loadedPath = loaded.Path,
                input = input?.Info.DisplayLabel,
                inputVersion = input?.Info.Version,
                inputIssued = input?.Info.Issued,
                inputDir = input?.Dir,
                archiveCount = input?.ArchiveCount ?? 0,
                fresh,
                status,
                message,
            });
        });
    }

    private sealed record Loaded(bool Exists, bool Complete, string? Label, string? Version, string? Issued, string? Path);
    private sealed record Input(ColDatasetInfo Info, string Dir, int ArchiveCount);

    private static Loaded ReadLoaded(PathsService paths) {
        string? dbPath;
        try { dbPath = paths.GetColSqlitePath(); } catch { return new Loaded(false, false, null, null, null, null); }
        if (string.IsNullOrWhiteSpace(dbPath)) return new Loaded(false, false, null, null, null, null);
        var full = Path.GetFullPath(dbPath);
        if (!File.Exists(full)) return new Loaded(false, false, null, null, null, full);

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
            try {
                using var imp = conn.CreateCommand();
                imp.CommandText = "SELECT COUNT(*) FROM import_metadata WHERE ended_at IS NOT NULL";
                complete = Convert.ToInt64(imp.ExecuteScalar() ?? 0L) > 0;
            } catch { /* import_metadata missing — treat as incomplete */ }

            var label = !string.IsNullOrWhiteSpace(alias) ? alias
                      : !string.IsNullOrWhiteSpace(version) ? version : null;
            return new Loaded(true, complete, label, version, issued, full);
        } catch {
            // File exists but won't open/query (corrupt / mid-write) — present as exists-but-incomplete.
            return new Loaded(true, false, null, null, null, full);
        }
    }

    private static Input? ReadNewestInput(PathsService paths) {
        string? dir;
        try { dir = paths.GetColDir(); } catch { return null; }
        if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) return null;

        string[] zips;
        try {
            zips = Directory.EnumerateFiles(dir, "*.zip", SearchOption.AllDirectories).ToArray();
        } catch {
            return null;
        }
        if (zips.Length == 0) return null;

        ColDatasetInfo? newest = null;
        foreach (var zip in zips) {
            var info = ReadZipCached(zip);
            if (info is null) continue;
            if (newest is null || CompareIssued(info.Issued, newest.Issued) > 0) {
                newest = info;
            }
        }

        return newest is null ? null : new Input(newest, Path.GetFullPath(dir), zips.Length);
    }

    private static ColDatasetInfo? ReadZipCached(string zipPath) {
        string key;
        try {
            var fi = new FileInfo(zipPath);
            key = $"{fi.FullName}|{fi.Length}|{fi.LastWriteTimeUtc.Ticks}";
        } catch {
            return ColDatasetInfo.ReadFromZip(zipPath);
        }
        return ZipCache.GetOrAdd(key, _ => ColDatasetInfo.ReadFromZip(zipPath));
    }

    // status values: "fresh" | "update-available" | "not-imported" | "incomplete" | "no-input" | "unknown"
    private static (string status, bool? fresh, string message) Evaluate(Loaded loaded, Input? input) {
        if (!loaded.Exists) {
            var hint = input is not null
                ? $"The input folder holds {Describe(input.Info)} — run `col import`, then repoint Datastore:COL_sqlite."
                : "Set Datastore:COL_sqlite (and import a ColDP archive with `col import`).";
            return ("not-imported", false, "No imported Catalogue of Life database found. " + hint);
        }
        if (!loaded.Complete) {
            return ("incomplete", false, "The imported database has no completed import (it may be corrupt or half-written) — re-run `col import --force`.");
        }
        if (input is null) {
            return ("no-input", null, "Loaded release shown; the input folder (Datasets:COL_dir) is missing or has no ColDP zip, so a newer release can't be detected.");
        }

        var loadedVer = loaded.Version;
        var inputVer = input.Info.Version;

        if (!string.IsNullOrWhiteSpace(loadedVer) && !string.IsNullOrWhiteSpace(inputVer) &&
            string.Equals(loadedVer, inputVer, StringComparison.OrdinalIgnoreCase)) {
            return ("fresh", true, "The imported database matches the newest release in the input folder.");
        }

        var cmp = CompareIssued(input.Info.Issued, loaded.Issued);
        if (cmp > 0 || (cmp == 0 && !VersionsKnown(loadedVer, inputVer))) {
            // Input folder is newer than the loaded DB (or versions differ and dates can't disprove it).
            return ("update-available", false,
                $"The input folder has a newer release ({Describe(input.Info)}) than the loaded database ({DescribeLoaded(loaded)}). Re-run `col import`, then repoint Datastore:COL_sqlite to the new file and restart serve.");
        }

        // Loaded release is the same date or newer than anything in the folder.
        return ("fresh", true, "The imported database is current relative to the input folder.");
    }

    private static bool VersionsKnown(string? a, string? b) =>
        !string.IsNullOrWhiteSpace(a) && !string.IsNullOrWhiteSpace(b);

    // Compares two ColDP `issued` strings (ISO dates like 2026-05-15). Returns >0
    // if a is newer than b. Parses as dates when possible, else ordinal string
    // compare; a null/blank value sorts oldest.
    private static int CompareIssued(string? a, string? b) {
        var hasA = DateTimeOffset.TryParse(a, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var da);
        var hasB = DateTimeOffset.TryParse(b, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var db);
        if (hasA && hasB) return da.CompareTo(db);
        return string.Compare(a ?? "", b ?? "", StringComparison.Ordinal);
    }

    private static string Describe(ColDatasetInfo info) {
        var label = info.DisplayLabel ?? "unknown release";
        return !string.IsNullOrWhiteSpace(info.Issued) ? $"{label}, issued {info.Issued}" : label;
    }

    private static string DescribeLoaded(Loaded loaded) {
        var label = loaded.Label ?? "unknown release";
        return !string.IsNullOrWhiteSpace(loaded.Issued) ? $"{label}, issued {loaded.Issued}" : label;
    }
}
