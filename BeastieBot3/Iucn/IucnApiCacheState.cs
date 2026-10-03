using System;
using System.IO;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
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
    // True when the file holds a finished build. Every build empties the projection before it
    // writes, so a build that was stopped part way leaves it empty: that reads as not built, with
    // UnfinishedBuildStartedAt set, never as a complete projection of 0 taxa.
    public required bool Exists { get; init; }
    public DateTime? UnfinishedBuildStartedAt { get; init; }
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
    // Queued assessments not downloaded yet that a normal run still asks the API for. Includes
    // ServerErrorAssessments; excludes BacklogNotFound.
    public long BacklogOutstanding { get; init; }
    // Queued assessments the API answered 404/410 for. Only a tombstone re-check asks for them again.
    public long BacklogNotFound { get; init; }
    public DateTime? OldestTaxaDownloadedAt { get; init; }
    public long TombstonedTaxa { get; init; }
    // The part of BacklogOutstanding whose last attempt got a server error (HTTP 5xx).
    public long ServerErrorAssessments { get; init; }
    public IucnRefreshSession? ActiveSession { get; init; }
    public long RefreshTaxaRemaining { get; init; }
    public long RefreshAssessmentsRemaining { get; init; }
    // Rows older than the refresh cutoff that the API has since answered 404/410 for. The two
    // remaining counts leave them out.
    public long RefreshTaxaNotFound { get; init; }
    public long RefreshAssessmentsNotFound { get; init; }
    public IucnProjectionState? Projection { get; init; }

    // What a run can still download: the outstanding queue less the server errors, which the
    // workflow steps do not count as work left.
    public long AssessmentsToDownload => Math.Max(0, BacklogOutstanding - ServerErrorAssessments);

    public IucnRefreshProgress? RefreshProgress => ActiveSession is null ? null : new IucnRefreshProgress {
        Session = ActiveSession,
        TaxaRemaining = RefreshTaxaRemaining,
        AssessmentsRemaining = RefreshAssessmentsRemaining,
        TaxaNotFound = RefreshTaxaNotFound,
        AssessmentsNotFound = RefreshAssessmentsNotFound,
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
            var backlog = store.CountAssessmentBacklog();
            var refreshTaxa = session is null ? default : store.CountTaxaBefore(session.CutoffUtc);
            var refreshAssessments = session is null ? default : store.CountAssessmentsBefore(session.CutoffUtc);
            return state with {
                TaxaCached = store.CountTaxa(),
                AssessmentsCached = store.CountAssessments(),
                BacklogOutstanding = backlog.Outstanding,
                BacklogNotFound = backlog.NotFound,
                OldestTaxaDownloadedAt = store.GetOldestTaxaDownloadedAt(),
                TombstonedTaxa = store.GetTombstonedEntityIds("taxa_sis").Count,
                ServerErrorAssessments = backlog.ServerErrors,
                ActiveSession = session,
                RefreshTaxaRemaining = refreshTaxa.Remaining,
                RefreshAssessmentsRemaining = refreshAssessments.Remaining,
                RefreshTaxaNotFound = refreshTaxa.NotFound,
                RefreshAssessmentsNotFound = refreshAssessments.NotFound,
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

        return ReadProjectionFile(Path.GetFullPath(path));
    }

    // The newest build recorded in the file. project-view deletes the data and the build records
    // before it starts, so the newest record says what the file holds: a record with no end time
    // is a build that stopped part way (or is running now), and the tables are empty.
    internal static IucnProjectionState ReadProjectionFile(string full) {
        var state = new IucnProjectionState { Path = full, Exists = File.Exists(full) };
        if (!state.Exists) return state;

        try {
            var csb = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly, Pooling = false };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"SELECT redlist_version, ended_at, is_partial, latest_not_downloaded, projected_taxa, started_at
FROM import_metadata ORDER BY rowid DESC LIMIT 1";
            cmd.CommandTimeout = 5;
            using var reader = cmd.ExecuteReader();
            if (!reader.Read()) return state with { Exists = false };

            if (reader.IsDBNull(1)) {
                return state with {
                    Exists = false,
                    UnfinishedBuildStartedAt = reader.IsDBNull(5) ? null : StoredUtc.Parse(reader.GetString(5)),
                };
            }

            return state with {
                RedlistVersion = reader.IsDBNull(0) ? null : reader.GetString(0),
                BuiltAt = StoredUtc.Parse(reader.GetString(1)),
                IsPartial = !reader.IsDBNull(2) && reader.GetInt64(2) != 0,
                LatestNotDownloaded = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                ProjectedTaxa = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
            };
        } catch {
            return state;
        }
    }
}
