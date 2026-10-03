using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the two queues cache-assessments gained for gaps in the API cache, and the refresh rule
// they bring with them.
//
// --csv-missing: about 300 subpopulation assessments in the 2026-1 CSV export were never
// downloaded, because no cached taxon record lists them (a subpopulation's id maps to its
// species' row, whose assessments[] leaves it out), so they never reached the backlog.
//
// --stale-latest: three payloads downloaded in November 2025 still said latest=true after their
// taxon records, downloaded in August 2026, said latest=false.
//
// The refresh rule: a payload --csv-missing downloads has no backlog row, but a later refresh
// counts it like any other payload older than its cutoff. The default queue has to reach it, or
// the refresh never closes.
public class IucnApiCacheAssessmentQueueTests {
    private static readonly DateTime Cutoff = new(2026, 8, 14, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Old = Cutoff.AddDays(-100);
    private static readonly DateTime Fresh = Cutoff.AddDays(1);

    // Taxon 100, downloaded after the cutoff, lists:
    //   900 current, payload says current (agrees), downloaded after the cutoff
    //   901 not current, payload says current, payload downloaded before the taxon record (stale)
    //   902 not current, payload has no latest field (nothing to compare)
    //   903 not current, payload says not current (agrees)
    // Taxon 200, downloaded before the cutoff, lists:
    //   910 current, payload says not current, payload downloaded after the taxon record
    // Payloads no taxon record lists (from --csv-missing):
    //   950 downloaded before the cutoff, 951 downloaded after it
    private static IucnApiCacheStore Seed(SqliteConnection conn) {
        var store = IucnApiCacheStore.OpenFromConnection(conn);
        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        store.WriteTaxonAtomic(100, importId, "{\"taxon\":100}", Fresh,
            new[] { new TaxaLookupRow(100, 100, "species"), new TaxaLookupRow(300, 100, "subpopulation") },
            new[] {
                new IucnAssessmentHeader(900, 100, Latest: true, YearPublished: 2024),
                new IucnAssessmentHeader(901, 100, Latest: false, YearPublished: 2019),
                new IucnAssessmentHeader(902, 100, Latest: false, YearPublished: 2010),
                new IucnAssessmentHeader(903, 100, Latest: false, YearPublished: 2008),
            });
        store.WriteTaxonAtomic(200, importId, "{\"taxon\":200}", Old,
            new[] { new TaxaLookupRow(200, 200, "species") },
            new[] { new IucnAssessmentHeader(910, 200, Latest: true, YearPublished: 2016) });

        store.UpsertAssessment(900, 100, importId, "{\"assessment_id\":900,\"latest\":true}", Fresh);
        store.UpsertAssessment(901, 100, importId, "{\"assessment_id\":901,\"latest\":true}", Old);
        store.UpsertAssessment(902, 100, importId, "{\"assessment_id\":902}", Old.AddDays(1));
        store.UpsertAssessment(903, 100, importId, "{\"assessment_id\":903,\"latest\":false}", Fresh);
        store.UpsertAssessment(910, 200, importId, "{\"assessment_id\":910,\"latest\":false}", Fresh);
        store.UpsertAssessment(950, 300, importId, "{\"assessment_id\":950,\"latest\":true}", Old);
        store.UpsertAssessment(951, 300, importId, "{\"assessment_id\":951,\"latest\":true}", Fresh);
        return store;
    }

    private static void Tombstone(IucnApiCacheStore store, long assessmentId) =>
        store.RecordFailedRequest("assessment", assessmentId, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

    private static long[] Ids(IEnumerable<AssessmentQueueRow> rows) => rows.Select(r => r.AssessmentId).ToArray();

    // ---- --csv-missing ----

    // CSV ids: 900, 950 and 951 are cached; 960 to 963 are not. 961 got a 404, 962 failed with a
    // server error and is due for another try, 963 failed a moment ago and is backing off.
    private static readonly long[] CsvIds = { 900, 950, 951, 960, 961, 962, 963 };

    private static void SeedFailures(IucnApiCacheStore store) {
        Tombstone(store, 961);
        store.RecordFailedRequest("assessment", 962, "Internal Server Error", 500, TimeSpan.FromMinutes(-1));
        store.RecordFailedRequest("assessment", 963, "Internal Server Error", 500);
    }

    [Fact]
    public void CsvMissing_QueuesCsvIdsWithNoPayload_FailureDueLast() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        SeedFailures(store);

        var q = IucnApiCacheAssessmentsCommand.BuildCsvMissingQueue(store, CsvIds, new IucnApiCacheAssessmentsSettings { CsvMissing = true });

        Assert.Equal(new long[] { 960, 962 }, Ids(q.Rows));
        Assert.Equal(7, q.CsvAssessments);
        Assert.Equal(4, q.NotCached);
        Assert.Equal(new IucnApiCacheAssessmentsCommand.QueueLeftOut(NotFound: 1, WaitingToRetry: 1, Historical: 0), q.LeftOut);
        Assert.All(q.Rows, r => Assert.True(r.Latest));
        Assert.All(q.Rows, r => Assert.Null(r.DownloadedAt));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void CsvMissing_RetryTombstonesOrForce_AsksForLeftOutIdsAgain(bool retryTombstones, bool force) {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        SeedFailures(store);

        var q = IucnApiCacheAssessmentsCommand.BuildCsvMissingQueue(store, CsvIds,
            new IucnApiCacheAssessmentsSettings { CsvMissing = true, RetryTombstones = retryTombstones, Force = force });

        // --force does not re-download the CSV assessments already cached.
        Assert.Equal(new long[] { 960, 961, 963, 962 }, Ids(q.Rows));
        Assert.Equal(default, q.LeftOut);
    }

    [Fact]
    public void CsvMissing_Limit_TakesTheFirstIds() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var q = IucnApiCacheAssessmentsCommand.BuildCsvMissingQueue(store, CsvIds,
            new IucnApiCacheAssessmentsSettings { CsvMissing = true, Limit = 2 });

        Assert.Equal(new long[] { 960, 961 }, Ids(q.Rows));
        Assert.Equal(4, q.NotCached);
    }

    [Fact]
    public void CsvMissing_EverythingCached_QueuesNothing() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var q = IucnApiCacheAssessmentsCommand.BuildCsvMissingQueue(store, new long[] { 900, 950 },
            new IucnApiCacheAssessmentsSettings { CsvMissing = true });

        Assert.Empty(q.Rows);
        Assert.Equal(0, q.NotCached);
    }

    // The count on cache-all's plan uses the same definition as the queue.
    [Fact]
    public void CacheAllPlanCount_MatchesTheQueue_AndCountsThe404sApart() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        SeedFailures(store);

        var count = IucnApiCacheFullCommand.CountCsvMissing(store, CsvIds);

        Assert.Equal(new IucnApiCacheFullCommand.CsvMissingCount(NotCached: 4, NotFound: 1), count);
    }

    // ---- the default queue and a refresh ----

    // The invariant behind refresh sessions: every payload CountAssessmentsBefore counts is one the
    // queue asks for. 950 has no backlog row, so without the unlisted-payload step it was counted
    // and never requested, and the refresh could not close.
    [Fact]
    public void DuringARefresh_TheQueueReachesEveryPayloadTheRefreshCounts() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var counted = OldPayloadIds(conn);
        Assert.Equal(new long[] { 901, 902, 950 }, counted);
        Assert.Equal(3, store.CountAssessmentsDownloadedBefore(Cutoff));

        var queue = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, Cutoff, new IucnApiCacheAssessmentsSettings());
        var due = queue.Where(r => IucnApiCacheAssessmentsCommand.ShouldDownload(r.DownloadedAt, Cutoff)).Select(r => r.AssessmentId).OrderBy(id => id);

        Assert.Equal(counted, due);
        Assert.Contains(queue, r => r.AssessmentId == 950 && r.Latest);   // the payload's own flag
        Assert.DoesNotContain(queue, r => r.AssessmentId == 951);       // downloaded after the cutoff
    }

    // Once the API answers 404 for the unlisted payload, the count leaves it out and so does the
    // queue: the two stay in step and the refresh can close.
    [Fact]
    public void DuringARefresh_ATombstonedUnlistedPayload_LeavesBothTheCountAndTheQueue() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(store, 950);

        Assert.Equal(new RefreshRemainingCounts(2, 1), store.CountAssessmentsBefore(Cutoff));
        var queue = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, Cutoff, new IucnApiCacheAssessmentsSettings());
        Assert.DoesNotContain(queue, r => r.AssessmentId == 950);
    }

    [Fact]
    public void WithoutACutoff_UnlistedPayloadsAreNotQueued() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var queue = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, null, new IucnApiCacheAssessmentsSettings());

        Assert.Equal(new long[] { 900, 910, 901, 902, 903 }, Ids(queue));
    }

    [Fact]
    public void DuringARefresh_LatestOnly_GoesByTheUnlistedPayloadsOwnFlag() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        var importId = store.BeginImport("/api/v4/assessment/952");
        store.UpsertAssessment(952, 300, importId, "{\"assessment_id\":952,\"latest\":false}", Old);

        var queue = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, Cutoff, new IucnApiCacheAssessmentsSettings { LatestOnly = true });

        Assert.Contains(queue, r => r.AssessmentId == 950);
        Assert.DoesNotContain(queue, r => r.AssessmentId == 952);
    }

    // The tombstone re-check also asks about ids no taxon record lists: a CSV assessment that got a
    // 404 from --csv-missing has no backlog row and no payload, so only this step reaches it.
    [Fact]
    public void RetryTombstones_AlsoAsksAboutTombstonedIdsNoTaxonRecordLists() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(store, 961);
        Tombstone(store, 901);

        var plain = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, Cutoff, new IucnApiCacheAssessmentsSettings());
        Assert.DoesNotContain(plain, r => r.AssessmentId is 961 or 901);

        var recheck = IucnApiCacheAssessmentsCommand.BuildAssessmentQueue(store, Cutoff, new IucnApiCacheAssessmentsSettings { RetryTombstones = true });
        var row961 = Assert.Single(recheck, r => r.AssessmentId == 961);
        Assert.Null(row961.DownloadedAt);
        Assert.True(IucnApiCacheAssessmentsCommand.ShouldDownload(row961.DownloadedAt, Cutoff));
        Assert.Single(recheck, r => r.AssessmentId == 901);   // once, from the backlog
    }

    // ---- --stale-latest ----

    [Fact]
    public void StaleLatest_QueuesPayloadsOlderThanTheirTaxonRecord_ReportsTheOthers() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var q = IucnApiCacheAssessmentsCommand.BuildStaleLatestQueue(store, new IucnApiCacheAssessmentsSettings { StaleLatest = true });

        Assert.Equal(new long[] { 901 }, Ids(q.Rows));
        Assert.Equal(2, q.Disagreeing);               // 901 and 910; 902 has no flag, 903 agrees
        Assert.Equal(1, q.OlderThanTaxonRecord);
        var newer = Assert.Single(q.NewerThanTaxonRecord);
        Assert.Equal(910, newer.Row.AssessmentId);
        Assert.Equal(200, newer.TaxonRootSisId);
        Assert.True(newer.Row.Latest);                // the taxon record's header
        Assert.False(newer.PayloadLatest);
        Assert.Equal(Old, newer.TaxonDownloadedAt);
    }

    [Fact]
    public void StaleLatest_ATombstonedPayload_IsLeftOutUnlessRetried() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        Tombstone(store, 901);

        var q = IucnApiCacheAssessmentsCommand.BuildStaleLatestQueue(store, new IucnApiCacheAssessmentsSettings { StaleLatest = true });
        Assert.Empty(q.Rows);
        Assert.Equal(1, q.LeftOut.NotFound);

        var retried = IucnApiCacheAssessmentsCommand.BuildStaleLatestQueue(store, new IucnApiCacheAssessmentsSettings { StaleLatest = true, RetryTombstones = true });
        Assert.Equal(new long[] { 901 }, Ids(retried.Rows));
    }

    // --latest-only goes by the taxon record's header, which says 901 is historical.
    [Fact]
    public void StaleLatest_LatestOnly_GoesByTheTaxonRecord() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);

        var q = IucnApiCacheAssessmentsCommand.BuildStaleLatestQueue(store, new IucnApiCacheAssessmentsSettings { StaleLatest = true, LatestOnly = true });

        Assert.Empty(q.Rows);
        Assert.Equal(1, q.LeftOut.Historical);
    }

    // If the API itself keeps answering latest=true for 901, the new payload is newer than the
    // taxon record, so the next run reports it rather than downloading it again for ever.
    [Fact]
    public void StaleLatest_ADownloadedPayloadThatStillDisagrees_IsNotQueuedAgain() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        var importId = store.BeginImport("/api/v4/assessment/901");
        store.UpsertAssessment(901, 100, importId, "{\"assessment_id\":901,\"latest\":true}", Fresh.AddHours(1));

        var q = IucnApiCacheAssessmentsCommand.BuildStaleLatestQueue(store, new IucnApiCacheAssessmentsSettings { StaleLatest = true });

        Assert.Empty(q.Rows);
        Assert.Contains(q.NewerThanTaxonRecord, r => r.Row.AssessmentId == 901);
    }

    // One payload that is not JSON must not stop the scan over the others.
    [Fact]
    public void MalformedPayload_IsSkippedByBothQueries() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = Seed(conn);
        var importId = store.BeginImport("/api/v4/assessment/903");
        store.UpsertAssessment(903, 100, importId, "<html>Bad gateway</html>", Old);
        store.UpsertAssessment(953, 300, importId, "not json", Old);

        var stale = store.GetAssessmentsWithDisagreeingLatestFlag();
        Assert.Equal(new long[] { 901, 910 }, stale.Select(r => r.Row.AssessmentId).OrderBy(id => id));

        var unlisted = store.GetAssessmentsNoTaxonRecordLists();
        Assert.Equal(new long[] { 950, 951, 953 }, Ids(unlisted));
        Assert.False(unlisted.Single(r => r.AssessmentId == 953).Latest);
    }

    [Fact]
    public void IsOlderThanTaxonRecord_TreatsAMissingTimeAsOlder() {
        var row = new AssessmentQueueRow(1, 1, 1, false, null, Old);
        Assert.True(IucnApiCacheAssessmentsCommand.IsOlderThanTaxonRecord(new StaleLatestRow(row, true, Fresh, 1)));
        Assert.False(IucnApiCacheAssessmentsCommand.IsOlderThanTaxonRecord(new StaleLatestRow(row, true, Old, 1)));
        Assert.True(IucnApiCacheAssessmentsCommand.IsOlderThanTaxonRecord(new StaleLatestRow(row, true, null, 1)));
        Assert.True(IucnApiCacheAssessmentsCommand.IsOlderThanTaxonRecord(new StaleLatestRow(row with { DownloadedAt = null }, true, Old, 1)));
    }

    [Theory]
    [InlineData("2026-06-15T22:37:45.4879892Z", "2026-06-15T22:37:46Z")]
    [InlineData("2026-06-15T22:37:45.0000000Z", "2026-06-15T22:37:46Z")]
    [InlineData("2026-12-31T23:59:59.9000000Z", "2027-01-01T00:00:00Z")]
    public void CutoffJustAfter_IsTheNextWholeSecond(string stored, string expected) {
        var utc = Infrastructure.StoredUtc.Parse(stored)!.Value;
        Assert.Equal(expected, IucnApiCacheAssessmentsCommand.CutoffJustAfter(utc));
        Assert.True(IucnRefreshMath.TryParseCutoffUtc(expected, out var parsed));
        Assert.True(parsed > utc);
    }

    // ---- options ----

    [Fact]
    public void QueueOptions_OneQueuePerRun() {
        Assert.Null(IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings()));
        Assert.Null(IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings { CsvMissing = true, RetryTombstones = true, Limit = 5 }));
        Assert.Null(IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings { FailedOnly = true, RefreshBefore = "2026-06-16" }));

        var both = IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings { FailedOnly = true, StaleLatest = true });
        Assert.Equal("Use only one of these options in a run: --failed-only, --stale-latest.", both);

        var cutoff = IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings { CsvMissing = true, RefreshBefore = "2026-06-16" });
        Assert.Equal("--refresh-before cannot be used with --csv-missing.", cutoff);

        var age = IucnApiCacheAssessmentsCommand.QueueOptionConflict(new IucnApiCacheAssessmentsSettings { StaleLatest = true, MaxAgeHours = 24 });
        Assert.Equal("--max-age-hours cannot be used with --stale-latest.", age);
    }

    // ---- reading the CSV export ----

    [Theory]
    [InlineData("assessments_html")]
    [InlineData("assessments")]
    public void ReadAssessmentIds_ListsEveryCsvAssessmentInOrder(string table) {
        var dir = Directory.CreateTempSubdirectory("beastiebot-csv-ids-");
        try {
            var path = Path.Combine(dir.FullName, "iucn.sqlite");
            using (var conn = new SqliteConnection($"Data Source={path};Pooling=False")) {
                conn.Open();
                using var command = conn.CreateCommand();
                command.CommandText = $@"CREATE TABLE {table} (assessmentId INTEGER NOT NULL, taxonId INTEGER NOT NULL);
INSERT INTO {table} VALUES (497802, 10273), (22823, 22823), (169801408, 169666550);";
                command.ExecuteNonQuery();
            }

            var ids = new IucnSisIdProvider(path).ReadAssessmentIds(default).ToArray();

            Assert.Equal(new long[] { 22823, 497802, 169801408 }, ids);
        } finally {
            dir.Delete(recursive: true);
        }
    }

    // Payload ids downloaded before the cutoff, as CountAssessmentsBefore counts them.
    private static long[] OldPayloadIds(SqliteConnection conn) {
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT assessment_id FROM assessments WHERE downloaded_at < @cutoff ORDER BY assessment_id";
        command.Parameters.AddWithValue("@cutoff", Cutoff.ToString("O"));
        var ids = new List<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) ids.Add(reader.GetInt64(0));
        return ids.ToArray();
    }
}
