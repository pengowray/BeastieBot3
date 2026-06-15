using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.WikipediaLists;

// Computes a comparable statistics set for any CSV-shaped IUCN relational DB
// (the CSV main DB *and* the API projection share view_assessments_html_taxonomy_html,
// so one query set works on both). Backs the Data-sources "CSV vs API" compare card.
//
// Results are cached in-process keyed by (path, file-mtime, file-size) so the 10s
// dashboard poller never re-runs the GROUP BY scan until the underlying DB changes
// — the "freely-updated-as-needed" statistics cache, without a second DB file.

namespace BeastieBot3.Web.Status;

public sealed record DatasetCategoryCount(string Category, long Count);

public sealed record DatasetStats {
    public required bool Exists { get; init; }
    public string? Path { get; init; }
    public string? Version { get; init; }
    public DateTime? LastModified { get; init; }
    public long? SizeBytes { get; init; }
    public long? TotalAssessments { get; init; }
    public long? DistinctTaxa { get; init; }
    public long? GlobalSpecies { get; init; }
    public IReadOnlyList<DatasetCategoryCount> ByCategory { get; init; } = Array.Empty<DatasetCategoryCount>();
    public string? Error { get; init; }

    /// <summary>True for an API projection built while some taxa's latest assessment JSON
    /// wasn't downloaded (so those taxa are missing). Null for the CSV DB / pre-coverage projections.</summary>
    public bool? IsPartial { get; init; }
    /// <summary>Count of taxa whose latest assessment wasn't downloaded when the projection was built.</summary>
    public long? LatestNotDownloaded { get; init; }
}

public static class DatasetStatsService {
    private static readonly ConcurrentDictionary<string, (string Key, DatasetStats Stats)> _cache = new();

    /// <summary>Compute (or return cached) stats for the given IUCN relational DB path.</summary>
    public static DatasetStats Compute(string? path) {
        if (string.IsNullOrWhiteSpace(path)) {
            return new DatasetStats { Exists = false, Error = "Not configured." };
        }
        var full = System.IO.Path.GetFullPath(path);
        if (!File.Exists(full)) {
            return new DatasetStats { Exists = false, Path = full };
        }

        var info = new FileInfo(full);
        var key = $"{info.LastWriteTimeUtc.Ticks}:{info.Length}";
        if (_cache.TryGetValue(full, out var cached) && cached.Key == key) {
            return cached.Stats;
        }

        var stats = ComputeUncached(full, info);
        _cache[full] = (key, stats);
        return stats;
    }

    private static DatasetStats ComputeUncached(string path, FileInfo info) {
        try {
            var csb = new SqliteConnectionStringBuilder {
                DataSource = path,
                Mode = SqliteOpenMode.ReadOnly,
                Cache = SqliteCacheMode.Shared,
            };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            using (var pragma = conn.CreateCommand()) {
                pragma.CommandText = "PRAGMA busy_timeout = 5000;";
                pragma.ExecuteNonQuery();
            }

            // Without the view this isn't an IUCN relational DB — bail cleanly.
            if (!HasView(conn, "view_assessments_html_taxonomy_html")) {
                return new DatasetStats { Exists = true, Path = path, LastModified = info.LastWriteTimeUtc, SizeBytes = info.Length,
                    Error = "No view_assessments_html_taxonomy_html (not an IUCN relational dataset)." };
            }

            var version = ScalarString(conn, "SELECT DISTINCT redlist_version FROM import_metadata LIMIT 1");
            var (isPartial, latestNotDownloaded) = ReadCoverage(conn);
            var total = ScalarLong(conn, "SELECT COUNT(*) FROM view_assessments_html_taxonomy_html");
            var distinct = ScalarLong(conn, "SELECT COUNT(DISTINCT taxonId) FROM view_assessments_html_taxonomy_html");

            var byCategory = new List<DatasetCategoryCount>();
            long globalSpecies = 0;
            using (var cmd = conn.CreateCommand()) {
                cmd.CommandText =
                    "SELECT v.redlistCategory, COUNT(*) FROM view_assessments_html_taxonomy_html v " +
                    $"WHERE {TaxonFilterSql.GlobalSpeciesPredicate()} GROUP BY v.redlistCategory";
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    var cat = reader.IsDBNull(0) ? "(none)" : reader.GetString(0);
                    var n = reader.GetInt64(1);
                    byCategory.Add(new DatasetCategoryCount(cat, n));
                    globalSpecies += n;
                }
            }
            byCategory = byCategory.OrderBy(c => CategoryRank(c.Category)).ThenBy(c => c.Category).ToList();

            return new DatasetStats {
                Exists = true,
                Path = path,
                Version = version,
                LastModified = info.LastWriteTimeUtc,
                SizeBytes = info.Length,
                TotalAssessments = total,
                DistinctTaxa = distinct,
                GlobalSpecies = globalSpecies,
                ByCategory = byCategory,
                IsPartial = isPartial,
                LatestNotDownloaded = latestNotDownloaded,
            };
        }
        catch (Exception ex) {
            return new DatasetStats { Exists = true, Path = path, LastModified = info.LastWriteTimeUtc, SizeBytes = info.Length, Error = ex.Message };
        }
    }

    // Canonical IUCN bar order so the compare card lists categories consistently.
    private static int CategoryRank(string category) {
        var order = new[] {
            "Extinct", "Extinct in the Wild", "Critically Endangered", "Endangered",
            "Vulnerable", "Near Threatened",
            "Lower Risk/conservation dependent", "Lower Risk/near threatened", "Lower Risk/least concern",
            "Least Concern", "Data Deficient", "Not Applicable", "Regionally Extinct",
        };
        var idx = Array.IndexOf(order, category);
        return idx < 0 ? order.Length : idx;
    }

    private static bool HasView(SqliteConnection conn, string viewName) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name=@n";
        cmd.Parameters.AddWithValue("@n", viewName);
        return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L) > 0;
    }

    private static long ScalarLong(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        try { return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L); } catch { return 0L; }
    }

    private static string? ScalarString(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        try { return cmd.ExecuteScalar() as string; } catch { return null; }
    }

    // Best-effort: only the API projection's import_metadata carries coverage columns. The CSV DB
    // (and pre-coverage projections) lack them, so the query throws and we report "unknown" (null).
    private static (bool? IsPartial, long? LatestNotDownloaded) ReadCoverage(SqliteConnection conn) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT is_partial, latest_not_downloaded FROM import_metadata ORDER BY id DESC LIMIT 1";
        try {
            using var reader = cmd.ExecuteReader();
            if (!reader.Read()) return (null, null);
            bool? partial = reader.IsDBNull(0) ? null : reader.GetInt64(0) != 0;
            long? missing = reader.IsDBNull(1) ? null : reader.GetInt64(1);
            return (partial, missing);
        } catch (SqliteException) {
            return (null, null);
        }
    }
}
