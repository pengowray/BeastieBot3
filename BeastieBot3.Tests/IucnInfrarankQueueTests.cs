using System;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;
using Candidate = BeastieBot3.Iucn.IucnApiCacheInfraranksCommand.InfrarankCandidate;

namespace BeastieBot3.Tests;

// Pins how iucn api cache-infraranks sorts each subspecies/variety candidate: download it, count
// it as already cached, or skip it because the API answered 404 on an earlier run. The last two
// used to be one "already cached" bucket, so a run whose only leftovers were 404s reported every
// candidate as cached.
public class IucnInfrarankQueueTests {
    private static readonly DateTime Downloaded = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    private static IucnApiCacheStore NewStore(SqliteConnection conn) {
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        // 100: cached on 2026-06-01. 200: tombstoned as 404. 300: never seen.
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        store.UpsertTaxa(100, importId, "{\"taxon\":{}}", Downloaded);
        store.RecordFailedRequest("taxa_sis", 200, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        return store;
    }

    [Fact]
    public void NoCutoff_SplitsDownloadCachedAndNotFound() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);

        Assert.Equal(Candidate.AlreadyCached, IucnApiCacheInfraranksCommand.Classify(store, 100, refreshThreshold: null));
        Assert.Equal(Candidate.NotFoundEarlier, IucnApiCacheInfraranksCommand.Classify(store, 200, refreshThreshold: null));
        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 300, refreshThreshold: null));
    }

    [Fact]
    public void RefreshCutoff_RedownloadsOlderCopies_ButStillSkips404s() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);
        var after = Downloaded.AddDays(1);
        var before = Downloaded.AddDays(-1);

        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 100, refreshThreshold: after));
        Assert.Equal(Candidate.AlreadyCached, IucnApiCacheInfraranksCommand.Classify(store, 100, refreshThreshold: before));
        Assert.Equal(Candidate.NotFoundEarlier, IucnApiCacheInfraranksCommand.Classify(store, 200, refreshThreshold: after));
    }

    [Fact]
    public void BuildQueue_QueuesOnlyDownloads_AndCountsTheRest() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);

        var result = IucnApiCacheInfraranksCommand.BuildQueue(
            new long[] { 100, 200, 300 },
            id => IucnApiCacheInfraranksCommand.Classify(store, id, refreshThreshold: null),
            force: false);

        Assert.Equal(new long[] { 300 }, result.Queue);
        Assert.Equal(1, result.AlreadyCached);
        Assert.Equal(1, result.NotFoundEarlier);
    }

    // --force queues every candidate, including the 404s, but the counts still say which were
    // cached and which were 404s. They used to read 0 and 0 under --force.
    [Fact]
    public void BuildQueue_Force_QueuesEveryCandidate_AndKeepsTheCounts() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);

        var result = IucnApiCacheInfraranksCommand.BuildQueue(
            new long[] { 100, 200, 300 },
            id => IucnApiCacheInfraranksCommand.Classify(store, id, refreshThreshold: null),
            force: true);

        Assert.Equal(new long[] { 100, 200, 300 }, result.Queue);
        Assert.Equal(1, result.AlreadyCached);
        Assert.Equal(1, result.NotFoundEarlier);
    }
}
