using System;
using System.Collections.Generic;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins what cache-taxa asks the API for during a refresh. The refresh's remaining count includes
// every taxa row older than the cutoff, so the queue has to reach every one of them, or the
// refresh stays open for good: re-import 2026-1 sat on 5 species rows that were no longer in the
// CSV and that no cached species listed. The tombstone re-check has to ask about subspecies and
// varieties the API answered 404 for, even though taxa_lookup maps their ids to a species row the
// refresh has just downloaded again.
public class IucnApiCacheTaxaQueueTests {
    private static readonly DateTime Cutoff = new(2026, 8, 14, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Old = Cutoff.AddDays(-100);
    private static readonly DateTime Fresh = Cutoff.AddDays(1);

    // Species 100, downloaded again after the cutoff, lists subspecies 200 and 201.
    // Subspecies 200 has its own row, downloaded before the cutoff.
    // Subspecies 201 has no row of its own: the API answered 404 for it.
    // Species 300 has its own row, downloaded before the cutoff; it is not in the CSV and no
    // cached species lists it.
    private static IucnApiCacheStore Seed(SqliteConnection conn) {
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        store.WriteTaxonAtomic(100, importId, "{\"taxon\":100}", Fresh,
            new[] {
                new TaxaLookupRow(100, 100, "species"),
                new TaxaLookupRow(200, 100, "infrarank"),
                new TaxaLookupRow(201, 100, "infrarank"),
            },
            Array.Empty<IucnAssessmentHeader>());
        store.WriteTaxonAtomic(200, importId, "{\"taxon\":200}", Old,
            new[] { new TaxaLookupRow(200, 200, "infrarank") }, Array.Empty<IucnAssessmentHeader>());
        store.WriteTaxonAtomic(300, importId, "{\"taxon\":300}", Old,
            new[] { new TaxaLookupRow(300, 300, "species") }, Array.Empty<IucnAssessmentHeader>());
        return store;
    }

    private static void Tombstone(SqliteConnection conn, IucnApiCacheStore store, long sisId, DateTime? lastAttemptAt) {
        store.RecordFailedRequest("taxa_sis", sisId, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        using var command = conn.CreateCommand();
        command.CommandText = "UPDATE failed_requests SET last_attempt_at=@at WHERE endpoint='taxa_sis' AND entity_id=@id";
        command.Parameters.AddWithValue("@at", lastAttemptAt is { } at ? at.ToString("O") : DBNull.Value);
        command.Parameters.AddWithValue("@id", sisId.ToString());
        command.ExecuteNonQuery();
    }

    private static readonly long[] CsvSpecies = { 100 };

    // ---- the queue ----

    [Fact]
    public void DuringARefresh_EveryOldRowIsQueued_AfterTheCsvSpecies() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var queue = IucnApiCacheTaxaCommand.BuildSisQueue(store, CsvSpecies, Cutoff, new IucnApiCacheTaxaSettings());

        Assert.Equal(new long[] { 100, 200, 300 }, queue);
    }

    [Fact]
    public void WithoutACutoff_OnlyTheCsvSpeciesAreQueued() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var queue = IucnApiCacheTaxaCommand.BuildSisQueue(store, CsvSpecies, null, new IucnApiCacheTaxaSettings());

        Assert.Equal(new long[] { 100 }, queue);
    }

    [Fact]
    public void Limit_TakesTheCsvSpeciesFirst() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var queue = IucnApiCacheTaxaCommand.BuildSisQueue(store, CsvSpecies, Cutoff, new IucnApiCacheTaxaSettings { Limit = 2 });

        Assert.Equal(new long[] { 100, 200 }, queue);
    }

    // --failed-only retries failures and nothing else, so it neither reads the CSV nor adds old rows.
    [Fact]
    public void FailedOnly_QueuesNeitherTheCsvNorTheOldRows() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        store.RecordFailedRequest("taxa_sis", 500, "Internal Server Error", 500, TimeSpan.FromMinutes(-1));

        var queue = IucnApiCacheTaxaCommand.BuildSisQueue(store, CsvThatMustNotBeRead(), Cutoff, new IucnApiCacheTaxaSettings { FailedOnly = true });

        Assert.Equal(new long[] { 500 }, queue);
    }

    private static IEnumerable<long> CsvThatMustNotBeRead() {
        throw new InvalidOperationException("--failed-only read the CSV species list.");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

    // ---- the download decision ----

    // taxa_lookup maps 200 to species 100's row as well as to its own. The species' new row must
    // not hide the subspecies' old one.
    [Fact]
    public void OldSubspeciesRow_IsDownloadedAgain_ThoughItsSpeciesIsNew() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 200, Cutoff));
        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 300, Cutoff));
        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 100, Cutoff));
    }

    [Fact]
    public void WithoutACutoff_CachedRowsAreNotDownloadedAgain() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 100, null));
        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 200, null));
        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 300, null));
        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 400, null));   // never downloaded
    }

    // ---- the ids due before the progress bar starts ----

    // The progress total counts only the ids SplitDue keeps. It used to be the whole queue, almost
    // all skipped at once, and a 44-minute run showed "~73:10:06 left".
    [Fact]
    public void SplitDue_KeepsQueueOrder_AndCountsEachSkipReason() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Old);

        var split = IucnApiCacheTaxaCommand.SplitDue(store, new List<long> { 400, 100, 200, 201, 300 }, Cutoff, new IucnApiCacheTaxaSettings());

        Assert.Equal(new long[] { 400, 200, 300 }, split.Due);
        Assert.Equal(1, split.UpToDate);          // 100, downloaded after the cutoff
        Assert.Equal(1, split.NotFoundEarlier);   // 201, tombstoned and not re-checked
    }

    [Fact]
    public void SplitDue_WithForce_KeepsEveryId() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Old);

        var ids = new List<long> { 100, 201 };
        var split = IucnApiCacheTaxaCommand.SplitDue(store, ids, null, new IucnApiCacheTaxaSettings { Force = true });

        Assert.Equal(ids, split.Due);
        Assert.Equal(0, split.UpToDate);
        Assert.Equal(0, split.NotFoundEarlier);
    }

    [Fact]
    public void QueueSummary_NamesTheCutoff_AndLeavesOutZeroCounts() {
        Assert.Equal(
            "Taxa in the queue: 186,627. To download: 1,522. Downloaded after 2026-08-14 00:00 UTC: 185,104. Not found (HTTP 404) on an earlier run: 1.",
            IucnDownloadQueueSummary.Describe("Taxa", 186_627, 1_522, 185_104, 1, Cutoff));
        Assert.Equal(
            "Assessments in the queue: 10. To download: 0. Already cached: 10.",
            IucnDownloadQueueSummary.Describe("Assessments", 10, 0, 10, 0, null));
        // The --retry-tombstones re-check with a cutoff skips only the 404s recorded after it.
        Assert.Equal(
            "Taxa in the queue: 3. To download: 1. Not found (HTTP 404) after 2026-08-14 00:00 UTC: 2.",
            IucnDownloadQueueSummary.Describe("Taxa", 3, 1, 0, 2, Cutoff, notFoundAfterCutoff: true));
    }

    // ---- the tombstone re-check ----

    [Fact]
    public void Tombstone_IsSkippedOutsideTheReCheck() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Old);

        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 201, Cutoff));
        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 201, null));
    }

    // The bug this pins: the re-check read the species row through taxa_lookup, found it newer
    // than the cutoff, and skipped the subspecies, so 1,522 of 1,576 tombstones on the live cache
    // were never asked about again.
    [Fact]
    public void ReCheck_AsksAgainWhenThe404PredatesTheCutoff() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Old);

        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 201, Cutoff, retryTombstones: true));
    }

    // Asked about since the cutoff: the 404 is the new release's answer, so an interrupted re-check
    // carries on instead of starting again.
    [Fact]
    public void ReCheck_SkipsA404RecordedSinceTheCutoff() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Fresh);

        Assert.False(IucnApiCacheTaxaCommand.ShouldDownload(store, 201, Cutoff, retryTombstones: true));
    }

    // With no cutoff, --retry-tombstones asks about every tombstone, as its description says.
    [Fact]
    public void ReCheck_WithoutACutoff_AsksAboutEveryTombstone() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(conn, store, 201, Fresh);
        Tombstone(conn, store, 202, null);

        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 201, null, retryTombstones: true));
        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 202, null, retryTombstones: true));
        Assert.True(IucnApiCacheTaxaCommand.ShouldDownload(store, 202, Cutoff, retryTombstones: true));   // time not recorded
    }
}
