using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Configuration;

// Shared state handed to every report producer: path resolution, the release being audited, the
// commentary file, an optional row limit (for fast --limit test runs), cancellation, and a small
// cache of read-only SQLite connections so producers that read the same database reuse one handle.

namespace BeastieBot3.Audit;

internal sealed class AuditContext : IDisposable {
    public PathsService Paths { get; }
    public int? Limit { get; }
    public string Release { get; }
    public int? ReleaseYear { get; }
    public AuditCommentary Commentary { get; }
    public CancellationToken Ct { get; }

    private readonly Dictionary<string, SqliteConnection> _connections = new(StringComparer.OrdinalIgnoreCase);

    public AuditContext(PathsService paths, int? limit, string release, int? releaseYear,
        AuditCommentary commentary, CancellationToken ct) {
        Paths = paths;
        Limit = limit;
        Release = release;
        ReleaseYear = releaseYear;
        Commentary = commentary;
        Ct = ct;
    }

    public static bool Exists(string? path) => !string.IsNullOrWhiteSpace(path) && File.Exists(path);

    // Opens (or returns a cached) read-only connection for an already-resolved path.
    public SqliteConnection OpenReadOnly(string path) {
        var full = Path.GetFullPath(path);
        if (_connections.TryGetValue(full, out var existing)) {
            return existing;
        }
        var cs = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly }.ConnectionString;
        var connection = new SqliteConnection(cs);
        connection.Open();
        _connections[full] = connection;
        return connection;
    }

    public bool TryOpenReadOnly(string? path, out SqliteConnection? connection) {
        connection = null;
        if (!Exists(path)) {
            return false;
        }
        connection = OpenReadOnly(path!);
        return true;
    }

    private string? SafeResolve(Func<string> resolver) {
        try { return resolver(); } catch { return null; }
    }

    // Typed read-only opens. Each returns null when the database file is absent or unconfigured,
    // so a producer can render an "unavailable" note instead of crashing.
    public SqliteConnection? IucnCsvOrNull() {
        var p = SafeResolve(() => Paths.ResolveIucnDatabasePath(null));
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    public SqliteConnection? IucnApiCacheOrNull() {
        var p = SafeResolve(() => Paths.ResolveIucnApiCachePath(null));
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    public SqliteConnection? IucnApiProjectedOrNull() {
        var p = SafeResolve(() => Paths.ResolveIucnApiProjectedPath(null));
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    // The Wikidata and Wikipedia caches. Neither is part of the Red List, so a report that reads
    // them must work without them; both are absent on a machine that has only ever imported IUCN.
    public SqliteConnection? WikidataCacheOrNull() {
        var p = SafeResolve(() => Paths.GetWikidataCachePath() ?? throw new InvalidOperationException());
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    public SqliteConnection? WikipediaCacheOrNull() {
        var p = SafeResolve(() => Paths.GetWikipediaCachePath() ?? throw new InvalidOperationException());
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    public SqliteConnection? ColOrNull() {
        var p = SafeResolve(() => Paths.GetColSqlitePath() ?? throw new InvalidOperationException());
        return Exists(p) ? OpenReadOnly(p!) : null;
    }

    private string? _colRelease;
    private bool _colReleaseRead;

    /// Human-readable Catalogue of Life release, e.g. "COL26.7 XR (2026-07-24)", read from the
    /// ColDP dataset metadata. Null when the CoL database (or its metadata) is unavailable.
    public string? ColReleaseLabel() {
        if (_colReleaseRead) {
            return _colRelease;
        }
        _colReleaseRead = true;
        try {
            var col = ColOrNull();
            if (col is null || !ObjectExists(col, "dataset_metadata")) {
                return null;
            }
            using var cmd = col.CreateCommand();
            cmd.CommandText = "SELECT alias, version, issued FROM dataset_metadata ORDER BY rowid DESC LIMIT 1";
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) {
                var alias = reader.IsDBNull(0) ? null : reader.GetString(0);
                var version = reader.IsDBNull(1) ? null : reader.GetString(1);
                var issued = reader.IsDBNull(2) ? null : reader.GetString(2);
                var label = !string.IsNullOrWhiteSpace(alias) ? alias : version;
                if (!string.IsNullOrWhiteSpace(label)) {
                    _colRelease = string.IsNullOrWhiteSpace(issued) ? label!.Trim() : $"{label!.Trim()} ({issued.Trim()})";
                }
            }
        } catch { /* leave null */ }
        return _colRelease;
    }

    public static bool ObjectExists(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE name = @name LIMIT 1";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    public void Dispose() {
        foreach (var c in _connections.Values) {
            try { c.Dispose(); } catch { /* best effort */ }
        }
        _connections.Clear();
    }
}
