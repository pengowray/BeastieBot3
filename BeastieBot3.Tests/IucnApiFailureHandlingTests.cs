using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the 404 "no standalone record" handling: a 404 is tombstoned as a permanent failure so the
// download gates (ShouldDownload / ShouldDownloadInfrarank) skip it and it isn't re-probed every
// run, while a transient failure stays retryable.
public class IucnApiFailureHandlingTests {
    [Fact]
    public void Permanent404_IsTombstoned_TransientStaysRetryable() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);

        // Transient server error, already due for retry (negative delay puts next_attempt in the past).
        store.RecordFailedRequest("taxa_sis", 111, "server blew up", 500, System.TimeSpan.FromMinutes(-1));
        // A 404 tombstoned with the permanent delay.
        store.RecordFailedRequest("taxa_sis", 222, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        // HasPermanentFailure keys off the 404/410 status, not the delay.
        Assert.False(store.HasPermanentFailure("taxa_sis", 111));
        Assert.True(store.HasPermanentFailure("taxa_sis", 222));

        // The retry queue surfaces the due transient one but not the far-future 404 tombstone.
        var retryable = store.GetFailedEntityIds("taxa_sis");
        Assert.Contains(111L, retryable);
        Assert.DoesNotContain(222L, retryable);
    }

    [Fact]
    public void HasPermanentFailure_IsScopedByEndpoint() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);

        store.RecordFailedRequest("assessment", 999, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        Assert.True(store.HasPermanentFailure("assessment", 999));
        Assert.False(store.HasPermanentFailure("taxa_sis", 999)); // different endpoint, same id
    }
}
