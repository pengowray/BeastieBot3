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

    public SqliteConnection? ColOrNull() {
        var p = SafeResolve(() => Paths.GetColSqlitePath() ?? throw new InvalidOperationException());
        return Exists(p) ? OpenReadOnly(p!) : null;
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
