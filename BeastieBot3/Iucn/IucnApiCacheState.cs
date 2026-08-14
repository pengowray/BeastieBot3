using System;
using System.IO;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

// What the API route looks like on disk right now: how much is cached, how old it is, whether a
// refresh is in progress and how far it has got, and whether the projection everything reads was
// built from the current cache.
//
// Read-only and offline. Same reason as the CSV side: "when did this command last run" cannot say
// whether the release you are importing today is actually in, and for a download that takes tens of
// hours the question people ask between runs is "how far in am I".

namespace BeastieBot3.Iucn;

public sealed record IucnProjectionState {
    public required string Path { get; init; }
    public required bool Exists { get; init; }
    public string? RedlistVersion { get; init; }
    public DateTime? BuiltAt { get; init; }
    public bool IsPartial { get; init; }
    public long LatestNotDownloaded { get; init; }
    public long ProjectedTaxa { get; init; }

    public string FileName => System.IO.Path.GetFileName(Path);
}

public sealed record IucnApiCacheState {
    public string? CachePath { get; init; }
    public bool CacheExists { get; init; }
    public long TaxaCached { get; init; }
    public long AssessmentsCached { get; init; }
    public long BacklogOutstanding { get; init; }
    public DateTime? OldestTaxaDownloadedAt { get; init; }
    public long TombstonedTaxa { get; init; }
    public long ServerErrorAssessments { get; init; }
    public IucnRefreshSession? ActiveSession { get; init; }
    public long RefreshTaxaRemaining { get; init; }
    public long RefreshAssessmentsRemaining { get; init; }
    public IucnProjectionState? Projection { get; init; }

    public IucnRefreshProgress? RefreshProgress => ActiveSession is null ? null : new IucnRefreshProgress {
        Session = ActiveSession,
        TaxaRemaining = RefreshTaxaRemaining,
        AssessmentsRemaining = RefreshAssessmentsRemaining,
    };
}

public static class IucnApiCacheStateReader {
    public static IucnApiCacheState Read(PathsService paths) {
        string? cachePath = null;
        try { cachePath = paths.ResolveIucnApiCachePath(null); } catch { /* unset — reported as no cache */ }

        var state = new IucnApiCacheState {
            CachePath = cachePath,
            CacheExists = cachePath is not null && File.Exists(cachePath),
            Projection = ReadProjection(paths),
        };
        if (!state.CacheExists) return state;

        try {
            using var store = IucnApiCacheStore.OpenReadOnly(cachePath!);
            if (store is null) return state;

            var session = store.GetActiveRefreshSession();
            return state with {
                TaxaCached = store.CountTaxa(),
                AssessmentsCached = store.CountAssessments(),
                BacklogOutstanding = store.CountBacklogOutstanding(),
                OldestTaxaDownloadedAt = store.GetOldestTaxaDownloadedAt(),
                TombstonedTaxa = store.GetTombstonedEntityIds("taxa_sis").Count,
                ServerErrorAssessments = store.GetServerErrorEntityIds("assessment").Count,
                ActiveSession = session,
                RefreshTaxaRemaining = session is null ? 0 : store.CountTaxaDownloadedBefore(session.CutoffUtc),
                RefreshAssessmentsRemaining = session is null ? 0 : store.CountAssessmentsDownloadedBefore(session.CutoffUtc),
            };
        } catch {
            return state;
        }
    }

    // The projection records its own coverage, so whether it is current is a local read.
    private static IucnProjectionState? ReadProjection(PathsService paths) {
        string? path;
        try { path = paths.ResolveIucnApiProjectedPath(null); } catch { return null; }
        if (string.IsNullOrWhiteSpace(path)) return null;

        var full = Path.GetFullPath(path);
        var state = new IucnProjectionState { Path = full, Exists = File.Exists(full) };
        if (!state.Exists) return state;

        try {
            var csb = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"SELECT redlist_version, ended_at, is_partial, latest_not_downloaded, projected_taxa
FROM import_metadata WHERE ended_at IS NOT NULL ORDER BY rowid DESC LIMIT 1";
            cmd.CommandTimeout = 5;
            using var reader = cmd.ExecuteReader();
            if (!reader.Read()) return state;

            return state with {
                RedlistVersion = reader.IsDBNull(0) ? null : reader.GetString(0),
                BuiltAt = reader.IsDBNull(1) ? null : IucnApiCacheStore.ParseStoredUtc(reader.GetString(1)),
                IsPartial = !reader.IsDBNull(2) && reader.GetInt64(2) != 0,
                LatestNotDownloaded = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                ProjectedTaxa = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
            };
        } catch {
            return state;
        }
    }
}
