using System;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins what a re-import counts as still to re-download. A cached row whose re-download got a
// 404/410 keeps its old downloaded_at, and no normal run asks for it again, so counting it kept
// re-import 2026-1 open at 99% with 3 assessments that could never be re-downloaded.
public class IucnRefreshRemainingCountTests {
    private static readonly DateTime Old = new(2025, 11, 16, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Cutoff = new(2026, 8, 14, 0, 0, 0, DateTimeKind.Utc);

    // One species (SIS 100) and one subspecies with its own taxa row (SIS 200), both downloaded
    // before the cutoff, plus assessments 900-903 downloaded before the cutoff.
    private static IucnApiCacheStore Seed(SqliteConnection conn) {
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        store.WriteTaxonAtomic(100, importId, "{\"taxon\":100}", Old,
            new[] { new TaxaLookupRow(100, 100, "species"), new TaxaLookupRow(200, 100, "infrarank") },
            Array.Empty<IucnAssessmentHeader>());
        store.WriteTaxonAtomic(200, importId, "{\"taxon\":200}", Old,
            new[] { new TaxaLookupRow(200, 200, "infrarank") },
            Array.Empty<IucnAssessmentHeader>());
        foreach (var id in new long[] { 900, 901, 902, 903 }) {
            store.UpsertAssessment(id, 100, importId, $"{{\"assessment\":{id}}}", Old);
        }
        return store;
    }

    [Fact]
    public void AssessmentTombstones_AreLeftOut_ServerErrorsAndOtherEndpointsAreNot() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Assert.Equal(4L, store.CountAssessmentsDownloadedBefore(Cutoff));

        store.RecordFailedRequest("assessment", 900, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        store.RecordFailedRequest("assessment", 901, "Gone", 410, IucnApiCacheStore.PermanentRetryDelay);
        // A normal run retries a server error once its back-off ends, so it is still work left.
        store.RecordFailedRequest("assessment", 902, "Internal Server Error", 500);
        // A 404 on another endpoint with the same id is not a tombstone for assessment 903.
        store.RecordFailedRequest("taxa_sis", 903, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        Assert.Equal(2L, store.CountAssessmentsDownloadedBefore(Cutoff));   // 902 and 903
        Assert.Equal(new RefreshRemainingCounts(2, 2), store.CountAssessmentsBefore(Cutoff));   // 900 and 901 not found
    }

    // The count is "old rows" minus "old rows with a tombstone". A row downloaded after the cutoff
    // that also has a tombstone (a --force re-request that got a 404 does this) is in neither half,
    // so it must not be subtracted: the tombstone half filters on downloaded_at too.
    [Fact]
    public void TombstoneOnARowDownloadedAfterTheCutoff_IsNotSubtracted() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        var importId = store.BeginImport("/api/v4/taxa/sis/300");
        var fresh = Cutoff.AddDays(1);
        store.WriteTaxonAtomic(300, importId, "{\"taxon\":300}", fresh,
            new[] { new TaxaLookupRow(300, 300, "species") }, Array.Empty<IucnAssessmentHeader>());
        store.UpsertAssessment(904, 300, importId, "{\"assessment\":904}", fresh);

        store.RecordFailedRequest("taxa_sis", 300, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        store.RecordFailedRequest("assessment", 904, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        Assert.Equal(new RefreshRemainingCounts(2, 0), store.CountTaxaBefore(Cutoff));
        Assert.Equal(new RefreshRemainingCounts(4, 0), store.CountAssessmentsBefore(Cutoff));

        // A tombstone for an id with no cached row at all is in neither half either.
        store.RecordFailedRequest("taxa_sis", 999, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        store.RecordFailedRequest("assessment", 999, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        Assert.Equal(new RefreshRemainingCounts(2, 0), store.CountTaxaBefore(Cutoff));
        Assert.Equal(new RefreshRemainingCounts(4, 0), store.CountAssessmentsBefore(Cutoff));
    }

    // Taxa are matched on root_sis_id, the id cache-taxa and cache-infraranks request. A 404 on a
    // subspecies' SIS id leaves out the subspecies' own row and not its species' row, which
    // taxa_lookup also maps that id to.
    [Fact]
    public void TaxonTombstone_LeavesOutOnlyTheRowWithThatRootSisId() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Assert.Equal(2L, store.CountTaxaDownloadedBefore(Cutoff));

        store.RecordFailedRequest("taxa_sis", 200, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        Assert.Equal(1L, store.CountTaxaDownloadedBefore(Cutoff));   // species 100
        Assert.Equal(new RefreshRemainingCounts(1, 1), store.CountTaxaBefore(Cutoff));

        store.RecordFailedRequest("assessment", 100, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        Assert.Equal(1L, store.CountTaxaDownloadedBefore(Cutoff));
    }

    // The session's starting counts use the same definition, so a re-import whose only old rows
    // are tombstoned reaches 100% and finishes once its phases have run.
    [Fact]
    public void Session_WithOnlyTombstonedRowsLeft_Finishes() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        store.RecordFailedRequest("assessment", 900, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        var session = store.StartRefreshSession(Cutoff, "2026-1", includeTombstones: true, includeDiscovery: false);
        Assert.Equal(2L, session.StartTaxaRemaining);
        Assert.Equal(3L, session.StartAssessmentsRemaining);

        // The re-download: both taxa and two assessments come back, and the last one is a 404.
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        var fresh = Cutoff.AddDays(1);
        store.WriteTaxonAtomic(100, importId, "{\"taxon\":100}", fresh,
            new[] { new TaxaLookupRow(100, 100, "species") }, Array.Empty<IucnAssessmentHeader>());
        store.WriteTaxonAtomic(200, importId, "{\"taxon\":200}", fresh,
            new[] { new TaxaLookupRow(200, 200, "infrarank") }, Array.Empty<IucnAssessmentHeader>());
        store.UpsertAssessment(901, 100, importId, "{\"assessment\":901}", fresh);
        store.UpsertAssessment(902, 100, importId, "{\"assessment\":902}", fresh);
        store.RecordFailedRequest("assessment", 903, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        var progress = IucnApiRefreshStartCommand.ReadProgress(store, store.GetActiveRefreshSession()!);
        Assert.Equal(0L, progress.TaxaRemaining);
        Assert.Equal(0L, progress.AssessmentsRemaining);
        Assert.Equal(100, progress.PercentDone);
        Assert.Equal(0L, progress.TaxaNotFound);
        Assert.Equal(2L, progress.AssessmentsNotFound);   // 900 and 903
        Assert.Equal(" except 2 assessments not found on the API (404)", progress.NotFoundClause);
        Assert.False(progress.IsFinished);   // the tombstone pass has not run yet

        store.MarkRefreshPhaseDone(session.Id, "tombstones_done_at");
        progress = IucnApiRefreshStartCommand.ReadProgress(store, store.GetActiveRefreshSession()!);
        Assert.True(progress.IsFinished);
    }
}
