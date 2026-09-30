using System;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the queued-assessment counts behind the API workflow lights. A 404/410 tombstone is never
// asked for again by a normal cache-assessments run, so it must not sit in the outstanding count,
// or the "Build or update the API dataset" step can never finish.
public class IucnAssessmentBacklogCountTests {
    private static (IucnApiCacheStore Store, long ImportId, long TaxaId) Seed(SqliteConnection conn, params long[] assessmentIds) {
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        var headers = new IucnAssessmentHeader[assessmentIds.Length];
        for (var i = 0; i < assessmentIds.Length; i++) {
            headers[i] = new IucnAssessmentHeader(assessmentIds[i], 100, Latest: i == 0, YearPublished: 2024 - i);
        }
        var taxaId = store.WriteTaxonAtomic(
            rootSisId: 100,
            importId: importId,
            json: "{\"taxon\":1}",
            downloadedAt: DateTime.UtcNow,
            mappings: new[] { new TaxaLookupRow(100, 100, "species") },
            assessments: headers);
        return (store, importId, taxaId);
    }

    [Fact]
    public void Tombstones_AreCountedApartFromOutstanding_ServerErrorsStayInside() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var (store, importId, _) = Seed(conn, 900, 901, 902, 903, 904);

        Assert.Equal(new AssessmentBacklogCounts(5, 0, 0), store.CountAssessmentBacklog());

        store.UpsertAssessment(900, 100, importId, "{\"assessment\":900}", DateTime.UtcNow);
        store.RecordFailedRequest("assessment", 901, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        store.RecordFailedRequest("assessment", 902, "Gone", 410);
        store.RecordFailedRequest("assessment", 903, "Internal Server Error", 500, TimeSpan.FromMinutes(-1));
        // A 404 on another endpoint with the same id does not make assessment 904 a tombstone.
        store.RecordFailedRequest("taxa_sis", 904, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        var counts = store.CountAssessmentBacklog();
        Assert.Equal(2L, counts.Outstanding);   // 903 (server error) and 904
        Assert.Equal(2L, counts.NotFound);      // 901 and 902
        Assert.Equal(1L, counts.ServerErrors);  // 903
    }

    // A tombstone that a re-check later downloads leaves both counts.
    [Fact]
    public void DownloadedTombstone_IsNoLongerCounted() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var (store, importId, _) = Seed(conn, 900, 901);

        store.RecordFailedRequest("assessment", 901, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        Assert.Equal(new AssessmentBacklogCounts(1, 1, 0), store.CountAssessmentBacklog());

        store.UpsertAssessment(901, 100, importId, "{\"assessment\":901}", DateTime.UtcNow);
        Assert.Equal(new AssessmentBacklogCounts(1, 0, 0), store.CountAssessmentBacklog());
    }

    // Re-downloading a taxon replaces its backlog rows, but failed_requests keeps the old ids. A
    // failure for an assessment no longer queued is not part of the backlog, and subtracting it
    // from the outstanding count would hide real work.
    [Fact]
    public void FailuresForAssessmentsNoLongerQueued_AreNotCounted() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var (store, _, taxaId) = Seed(conn, 900, 901, 902);

        store.RecordFailedRequest("assessment", 901, "Internal Server Error", 500);
        store.RecordFailedRequest("assessment", 902, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        Assert.Equal(new AssessmentBacklogCounts(2, 1, 1), store.CountAssessmentBacklog());

        store.ReplaceAssessmentBacklog(taxaId, 100, new[] {
            new IucnAssessmentHeader(900, 100, Latest: true, YearPublished: 2024),
            new IucnAssessmentHeader(905, 100, Latest: false, YearPublished: 2018),
        });

        Assert.Equal(new AssessmentBacklogCounts(2, 0, 0), store.CountAssessmentBacklog());
    }
}
