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

        Assert.Equal(Candidate.AlreadyCached, IucnApiCacheInfraranksCommand.Classify(store, 100, force: false, refreshThreshold: null));
        Assert.Equal(Candidate.NotFoundEarlier, IucnApiCacheInfraranksCommand.Classify(store, 200, force: false, refreshThreshold: null));
        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 300, force: false, refreshThreshold: null));
    }

    [Fact]
    public void RefreshCutoff_RedownloadsOlderCopies_ButStillSkips404s() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);
        var after = Downloaded.AddDays(1);
        var before = Downloaded.AddDays(-1);

        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 100, force: false, refreshThreshold: after));
        Assert.Equal(Candidate.AlreadyCached, IucnApiCacheInfraranksCommand.Classify(store, 100, force: false, refreshThreshold: before));
        Assert.Equal(Candidate.NotFoundEarlier, IucnApiCacheInfraranksCommand.Classify(store, 200, force: false, refreshThreshold: after));
    }

    [Fact]
    public void Force_DownloadsEveryCandidate_Including404s() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        var store = NewStore(conn);

        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 100, force: true, refreshThreshold: null));
        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 200, force: true, refreshThreshold: null));
        Assert.Equal(Candidate.Download, IucnApiCacheInfraranksCommand.Classify(store, 300, force: true, refreshThreshold: null));
    }
}
