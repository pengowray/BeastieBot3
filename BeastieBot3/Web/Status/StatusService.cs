using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Web.Status;

// Collects a status snapshot for every data source in DataSourceCatalogue.
//
// SQLite databases are opened read-only with a short busy timeout, so dashboard
// refreshes cannot contend with a running import (which uses WAL anyway).
// Each metric query is wrapped in try/catch: missing tables in a freshly-
// cloned environment are reported as null, not as fatal errors.
//
// Collect() reuses its last result for a few seconds. /api/status and every
// /api/flows/{id} call it, and the dashboard requests all of them at once every
// 10 seconds, so without the reuse one dashboard refresh ran every metric query
// once per flow. The lock makes those parallel requests share one result
// instead of each running the queries. The slowest metric ("assessments to
// download") takes about 0.15s on a full API cache.

public sealed class StatusService {
    internal static readonly TimeSpan ReuseFor = TimeSpan.FromSeconds(5);

    private readonly PathsService _paths;
    private readonly TimeProvider _clock;
    private readonly object _gate = new();
    private (DateTimeOffset At, IReadOnlyList<DataSourceStatus> Value)? _last;

    public StatusService(PathsService paths) : this(paths, TimeProvider.System) { }

    // Internal so tests can control the clock. DI only looks at public constructors.
    internal StatusService(PathsService paths, TimeProvider clock) {
        _paths = paths;
        _clock = clock;
    }

    public IReadOnlyList<DataSourceStatus> Collect() {
        lock (_gate) {
            var now = _clock.GetUtcNow();
            if (_last is { } last && now - last.At < ReuseFor && now >= last.At) return last.Value;
            var value = DataSourceCatalogue.All
                .Select(Snapshot)
                .ToList()
                .AsReadOnly();
            _last = (_clock.GetUtcNow(), value);
            return value;
        }
    }

    private DataSourceStatus Snapshot(DataSourceDescriptor d) {
        string? path;
        try {
            path = d.ResolvePath(_paths);
        } catch (Exception ex) {
            return new DataSourceStatus {
                Id = d.Id, Name = d.Name, Kind = d.Kind, Description = d.Description,
                Path = null, Exists = false, Error = ex.Message,
            };
        }

        if (string.IsNullOrWhiteSpace(path)) {
            return new DataSourceStatus {
                Id = d.Id, Name = d.Name, Kind = d.Kind, Description = d.Description,
                Path = null, Exists = false,
                Error = "Not configured in paths.ini.",
            };
        }

        var resolved = Path.GetFullPath(path);
        return d.Kind switch {
            "sqlite"    => SnapshotSqlite(d, resolved),
            "directory" => SnapshotDirectory(d, resolved),
            _           => new DataSourceStatus {
                Id = d.Id, Name = d.Name, Kind = d.Kind, Description = d.Description, Path = resolved,
                Exists = false, Error = $"Unknown kind '{d.Kind}'.",
            },
        };
    }

    private static DataSourceStatus SnapshotSqlite(DataSourceDescriptor d, string path) {
        var status = new DataSourceStatus {
            Id = d.Id, Name = d.Name, Kind = d.Kind, Description = d.Description,
            Path = path, Exists = File.Exists(path),
        };
        if (!status.Exists) return status;

        var info = new FileInfo(path);
        status = status with { SizeBytes = info.Length, LastModified = info.LastWriteTimeUtc };

        var metrics = new List<MetricResult>();
        try {
            var csb = new SqliteConnectionStringBuilder {
                DataSource = path,
                Mode = SqliteOpenMode.ReadOnly,
                Cache = SqliteCacheMode.Shared,
            };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            // Allow a generous timeout for COUNT(*) on large tables.
            using (var pragma = conn.CreateCommand()) {
                pragma.CommandText = "PRAGMA busy_timeout = 5000;";
                pragma.ExecuteNonQuery();
            }

            foreach (var m in d.Metrics) {
                metrics.Add(RunMetric(conn, m));
            }
        } catch (Exception ex) {
            return status with { Error = ex.Message, Metrics = metrics };
        }

        return status with { Metrics = metrics };
    }

    // Internal so tests can run a catalogue metric over an in-memory store.
    internal static MetricResult RunMetric(SqliteConnection conn, MetricSpec spec) {
        try {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = spec.Sql;
            cmd.CommandTimeout = 15;
            var raw = cmd.ExecuteScalar();
            var value = raw is null || raw is DBNull ? (long?)null : Convert.ToInt64(raw);
            return new MetricResult { Label = spec.Label, Value = value };
        } catch (SqliteException ex) when (spec.TolerateMissing && IsMissingTable(ex)) {
            return new MetricResult { Label = spec.Label, Value = null, Note = "none in this file yet" };
        } catch (Exception ex) {
            return new MetricResult { Label = spec.Label, Value = null, Error = ex.Message };
        }
    }

    private static bool IsMissingTable(SqliteException ex) =>
        ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase);

    private static DataSourceStatus SnapshotDirectory(DataSourceDescriptor d, string path) {
        var status = new DataSourceStatus {
            Id = d.Id, Name = d.Name, Kind = d.Kind, Description = d.Description,
            Path = path, Exists = Directory.Exists(path),
        };
        if (!status.Exists) return status;

        try {
            // Recursive: input folders frequently contain nested release subdirectories
            // (e.g. IUCN_CVS_2025-2/<release-name>/*.csv). A top-level-only scan would
            // miss these and report a misleading "0 files".
            var files = new DirectoryInfo(path).EnumerateFiles("*", SearchOption.AllDirectories).ToList();
            long total = files.Sum(f => f.Length);
            DateTime? newest = files.Count == 0 ? null : files.Max(f => f.LastWriteTimeUtc);
            return status with {
                SizeBytes = total,
                LastModified = newest,
                Metrics = new[] {
                    new MetricResult { Label = "files", Value = files.Count },
                },
            };
        } catch (Exception ex) {
            return status with { Error = ex.Message };
        }
    }
}

public sealed record DataSourceStatus {
    public required string Id { get; init; }
    public required string Name { get; init; }
    public required string Kind { get; init; }
    public string? Description { get; init; }
    public string? Path { get; init; }
    public bool Exists { get; init; }
    public long? SizeBytes { get; init; }
    public DateTime? LastModified { get; init; }
    public string? Error { get; init; }
    public IReadOnlyList<MetricResult> Metrics { get; init; } = Array.Empty<MetricResult>();
}

public sealed record MetricResult {
    public required string Label { get; init; }
    public long? Value { get; init; }
    public string? Note { get; init; }
    public string? Error { get; init; }
}
