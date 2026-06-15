using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Web.Status;

// Collects a status snapshot for every data source in DataSourceCatalogue.
//
// SQLite databases are opened read-only with a short busy timeout, so dashboard
// refreshes cannot contend with a running import (which uses WAL anyway).
// Each metric query is wrapped in try/catch: missing tables in a freshly-
// cloned environment are reported as null, not as fatal errors.

public sealed class StatusService {
    private readonly PathsService _paths;
    public StatusService(PathsService paths) { _paths = paths; }

    public IReadOnlyList<DataSourceStatus> Collect() {
        return DataSourceCatalogue.All
            .Select(Snapshot)
            .ToList();
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
                Id = d.Id, Name = d.Name, Kind = d.Kind, Path = resolved,
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

    private static MetricResult RunMetric(SqliteConnection conn, MetricSpec spec) {
        try {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = spec.Sql;
            cmd.CommandTimeout = 15;
            var raw = cmd.ExecuteScalar();
            var value = raw is null || raw is DBNull ? (long?)null : Convert.ToInt64(raw);
            return new MetricResult { Label = spec.Label, Value = value };
        } catch (SqliteException ex) when (spec.TolerateMissing && IsMissingTable(ex)) {
            return new MetricResult { Label = spec.Label, Value = null, Note = "table not yet created" };
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
