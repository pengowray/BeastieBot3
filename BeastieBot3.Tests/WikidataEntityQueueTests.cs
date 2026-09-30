using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Infrastructure;
using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests;

// The download queue behind `wikidata cache-entities` (and `wikidata cache-all`, which passes
// --download-force through as --force).
//
// --force used to change nothing: the queue only ever held never-downloaded items plus, with
// --max-age-hours, cached copies older than the cutoff, so an item already cached was never
// selected. It now sets the cutoff to the start of the run.
//
// Every batch is a fresh "first N rows" query, so an item tried in this run has to stop matching.
// A successful download does (json_downloaded = 1, new downloaded_at); a failed one did not, so it
// came straight back in the next batch, and --failed-only (newest attempt first) picked the item
// that had just failed again. The queue now leaves out anything attempted since the run started.
//
// Cached copies used to be queued in Q-number order. Every cached copy is older than a --force
// run, so each --force run started again from the lowest Q-number: "--force --limit N" re-downloaded
// the same N items every time. They are now queued least recently downloaded first.
public class WikidataEntityQueueTests {
    private static SqliteConnection OpenMemory() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    private static void AddEntity(SqliteConnection connection, long numericId, DateTime? downloadedAt,
                                  DateTime? lastAttemptAt = null, string? lastError = null) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at,
                                          json_downloaded, downloaded_at, attempt_count, last_error, last_attempt_at)
            VALUES (@id, @qid, @seen, @seen, @json, @dl, @attempts, @error, @attempted)
            """;
        cmd.Parameters.AddWithValue("@id", numericId);
        cmd.Parameters.AddWithValue("@qid", $"Q{numericId}");
        cmd.Parameters.AddWithValue("@seen", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).ToString("O"));
        cmd.Parameters.AddWithValue("@json", downloadedAt.HasValue ? 1 : 0);
        cmd.Parameters.AddWithValue("@dl", downloadedAt?.ToString("O") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@attempts", lastError is null ? 0 : 1);
        cmd.Parameters.AddWithValue("@error", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@attempted", lastAttemptAt?.ToString("O") ?? (object)DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static WikidataEntityRecord EmptyRecord(long numericId) => new(
        numericId, $"Q{numericId}", null, null, false, false,
        Array.Empty<string>(), Array.Empty<string>(), Array.Empty<WikidataP141Statement>(),
        Array.Empty<WikidataMonolingualText>(), Array.Empty<WikidataMonolingualText>(),
        null, Array.Empty<long>());

    private static IAnsiConsole QuietConsole() => ConsoleSize.EnsureUsable(AnsiConsole.Create(new AnsiConsoleSettings {
        Ansi = AnsiSupport.No,
        ColorSystem = ColorSystemSupport.NoColors,
        Interactive = InteractionSupport.No,
        Out = new AnsiConsoleOutput(new StringWriter()),
    }));

    private static DateTime? DownloadedAt(SqliteConnection connection, long numericId) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT downloaded_at FROM wikidata_entities WHERE entity_numeric_id = @id";
        cmd.Parameters.AddWithValue("@id", numericId);
        return StoredUtc.Parse(cmd.ExecuteScalar() as string);
    }

    // Runs the command's plan and download loop with a fake download that writes the same rows the
    // real one does, and returns the Q-numbers in the order they were tried.
    private static async Task<(List<long> Tried, (int downloaded, int skipped, int failed, int completed) Result)> RunLoopAsync(
        WikidataCacheStore store, WikidataCacheItemsSettings settings, DateTime runStart, int batchSize,
        ISet<long> failing) {
        var (cutoff, total) = WikidataCacheItemsCommand.PlanQueue(store, settings, runStart);

        var tried = new List<long>();
        var result = await WikidataCacheItemsCommand.DownloadEntitiesAsync(
            total, batchSize, settings, cutoff, runStart, store,
            item => {
                tried.Add(item.NumericId);
                if (failing.Contains(item.NumericId)) {
                    store.RecordFailure(item.NumericId, "HTTP 500");
                    return Task.FromResult(false);
                }
                store.RecordSuccess(EmptyRecord(item.NumericId), store.BeginImport($"test {item.EntityId}"), "{}", DateTime.UtcNow);
                return Task.FromResult(true);
            },
            QuietConsole(), CancellationToken.None);
        return (tried, result);
    }

    [Fact]
    public void RefreshCutoff_Force_IsRunStart_AndOverridesMaxAge() {
        var runStart = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal(runStart, WikidataCacheItemsCommand.RefreshCutoff(force: true, maxAgeHours: null, runStart));
        Assert.Equal(runStart, WikidataCacheItemsCommand.RefreshCutoff(force: true, maxAgeHours: 24, runStart));
    }

    [Fact]
    public void RefreshCutoff_MaxAge_IsThatManyHoursBeforeRunStart() {
        var runStart = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal(runStart.AddHours(-24), WikidataCacheItemsCommand.RefreshCutoff(force: false, maxAgeHours: 24, runStart));
        Assert.Null(WikidataCacheItemsCommand.RefreshCutoff(force: false, maxAgeHours: null, runStart));
        Assert.Null(WikidataCacheItemsCommand.RefreshCutoff(force: false, maxAgeHours: 0, runStart));
    }

    [Fact]
    public void ForceCutoff_QueuesCachedEntities_AfterNeverDownloadedOnes() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        AddEntity(connection, 5, downloadedAt: null);
        AddEntity(connection, 1, downloadedAt: runStart.AddDays(-1), lastAttemptAt: runStart.AddDays(-1));
        AddEntity(connection, 2, downloadedAt: runStart.AddMinutes(-1), lastAttemptAt: runStart.AddMinutes(-1));

        // No cutoff (what --force used to give): only the never-downloaded item.
        Assert.Equal(1, store.CountPendingEntities(null, attemptedBefore: runStart));

        var cutoff = WikidataCacheItemsCommand.RefreshCutoff(force: true, maxAgeHours: null, runStart);
        Assert.Equal(3, store.CountPendingEntities(cutoff, attemptedBefore: runStart));
        Assert.Equal(new long[] { 5, 1, 2 },
            store.GetPendingEntities(10, cutoff, attemptedBefore: runStart).Select(i => i.NumericId));

        // --force --refresh-only: every cached item, and nothing never downloaded.
        Assert.Equal(2, store.CountPendingEntities(cutoff, refreshOnly: true, attemptedBefore: runStart));
        Assert.Equal(new long[] { 1, 2 },
            store.GetPendingEntities(10, cutoff, refreshOnly: true, attemptedBefore: runStart).Select(i => i.NumericId));
    }

    [Fact]
    public async Task ForceRun_RedownloadsCachedEntities_AndTriesEachEntityOnce() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = DateTime.UtcNow;   // the fake download stamps real times, as the real one does
        var yesterday = runStart.AddDays(-1);
        AddEntity(connection, 1, downloadedAt: null);
        AddEntity(connection, 2, downloadedAt: null);
        AddEntity(connection, 3, downloadedAt: yesterday, lastAttemptAt: yesterday);
        AddEntity(connection, 4, downloadedAt: yesterday, lastAttemptAt: yesterday);
        AddEntity(connection, 5, downloadedAt: yesterday, lastAttemptAt: yesterday);

        // Q1 fails. Before the fix it was first in every batch, so the run tried it again and again.
        var (tried, result) = await RunLoopAsync(store, new WikidataCacheItemsSettings { Force = true },
            runStart, batchSize: 2, failing: new HashSet<long> { 1 });

        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, tried);
        Assert.Equal((4, 0, 1, 5), result);
        foreach (var id in new long[] { 3, 4, 5 }) {
            Assert.True(DownloadedAt(connection, id) >= runStart, $"Q{id} was not re-downloaded");
        }

        var cutoff = WikidataCacheItemsCommand.RefreshCutoff(force: true, maxAgeHours: null, runStart);
        Assert.Equal(0, store.CountPendingEntities(cutoff, attemptedBefore: runStart));
    }

    [Fact]
    public async Task PendingRun_WithManyFailuresAtTheFront_ReachesTheRestOfTheQueue() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = DateTime.UtcNow;
        for (var id = 1; id <= 6; id++) {
            AddEntity(connection, id, downloadedAt: null);
        }

        // A whole batch of failures: before the fix the next batch was the same two items.
        var (tried, result) = await RunLoopAsync(store, new WikidataCacheItemsSettings(),
            runStart, batchSize: 2, failing: new HashSet<long> { 1, 2 });

        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6 }, tried);
        Assert.Equal((4, 0, 2, 6), result);
    }

    [Fact]
    public async Task FailedOnlyRun_TriesEachFailureOnce_OldestAttemptFirst() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = DateTime.UtcNow;
        AddEntity(connection, 7, downloadedAt: null, lastAttemptAt: runStart.AddHours(-2), lastError: "HTTP 500");
        AddEntity(connection, 8, downloadedAt: null, lastAttemptAt: runStart.AddDays(-1), lastError: "HTTP 500");
        AddEntity(connection, 9, downloadedAt: null);

        var (tried, result) = await RunLoopAsync(store, new WikidataCacheItemsSettings { FailedOnly = true },
            runStart, batchSize: 1, failing: new HashSet<long> { 7, 8 });

        Assert.Equal(new long[] { 8, 7 }, tried);
        Assert.Equal((0, 0, 2, 2), result);
        Assert.Empty(store.GetFailedEntities(10, runStart));
        Assert.Equal(2, store.CountFailedEntities());
    }

    [Fact]
    public void PlanQueue_TakesCutoffAndTotalFromTheSettings() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        AddEntity(connection, 1, downloadedAt: null);
        AddEntity(connection, 2, downloadedAt: null, lastAttemptAt: runStart.AddDays(-1), lastError: "HTTP 500");
        AddEntity(connection, 3, downloadedAt: runStart.AddDays(-3), lastAttemptAt: runStart.AddDays(-3));
        AddEntity(connection, 4, downloadedAt: runStart.AddMinutes(-10), lastAttemptAt: runStart.AddMinutes(-10));

        (DateTime?, int) Plan(WikidataCacheItemsSettings settings) =>
            WikidataCacheItemsCommand.PlanQueue(store, settings, runStart);

        Assert.Equal((null, 2), Plan(new WikidataCacheItemsSettings()));
        Assert.Equal((runStart, 4), Plan(new WikidataCacheItemsSettings { Force = true }));
        Assert.Equal((runStart, 2), Plan(new WikidataCacheItemsSettings { Force = true, RefreshOnly = true }));
        Assert.Equal((runStart, 4), Plan(new WikidataCacheItemsSettings { Force = true, MaxAgeHours = 24 }));
        Assert.Equal((runStart.AddHours(-24), 3), Plan(new WikidataCacheItemsSettings { MaxAgeHours = 24 }));
        Assert.Equal((runStart.AddHours(-24), 1), Plan(new WikidataCacheItemsSettings { MaxAgeHours = 24, RefreshOnly = true }));
        Assert.Equal((null, 1), Plan(new WikidataCacheItemsSettings { FailedOnly = true }));
        Assert.Equal((runStart, 3), Plan(new WikidataCacheItemsSettings { Force = true, Limit = 3 }));
        Assert.Equal((runStart, 4), Plan(new WikidataCacheItemsSettings { Force = true, Limit = 0 }));

        // --refresh-only with no cutoff: RunAsync reports this as an error.
        Assert.Equal((null, 0), Plan(new WikidataCacheItemsSettings { RefreshOnly = true }));
    }

    [Fact]
    public void PendingQueue_CachedCopies_LeastRecentlyDownloadedFirst() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var runStart = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        AddEntity(connection, 1, downloadedAt: runStart.AddDays(-3), lastAttemptAt: runStart.AddDays(-3));
        AddEntity(connection, 2, downloadedAt: runStart.AddDays(-1), lastAttemptAt: runStart.AddDays(-1));
        AddEntity(connection, 3, downloadedAt: runStart.AddDays(-2), lastAttemptAt: runStart.AddDays(-2));
        AddEntity(connection, 4, downloadedAt: null);

        var cutoff = WikidataCacheItemsCommand.RefreshCutoff(force: true, maxAgeHours: null, runStart);
        Assert.Equal(new long[] { 4, 1, 3, 2 },
            store.GetPendingEntities(10, cutoff, attemptedBefore: runStart).Select(i => i.NumericId));
        Assert.Equal(new long[] { 1, 3, 2 },
            store.GetPendingEntities(10, cutoff, refreshOnly: true, attemptedBefore: runStart).Select(i => i.NumericId));
    }

    [Fact]
    public async Task ForceRunsWithLimit_EachRunContinuesWhereTheLastOneStopped() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        var now = DateTime.UtcNow;
        AddEntity(connection, 1, downloadedAt: now.AddDays(-1), lastAttemptAt: now.AddDays(-1));
        AddEntity(connection, 2, downloadedAt: now.AddDays(-10), lastAttemptAt: now.AddDays(-10));
        AddEntity(connection, 3, downloadedAt: now.AddDays(-5), lastAttemptAt: now.AddDays(-5));
        AddEntity(connection, 4, downloadedAt: now.AddDays(-2), lastAttemptAt: now.AddDays(-2));
        AddEntity(connection, 5, downloadedAt: now.AddDays(-20), lastAttemptAt: now.AddDays(-20));

        var settings = new WikidataCacheItemsSettings { Force = true, Limit = 2 };
        var noFailures = new HashSet<long>();

        // Each run takes its own start time, after the previous run's downloads. Only the items
        // each run tries first are checked: a copy re-downloaded in the same tick as the next run
        // starts is left out of that run rather than sorted last, and either is correct.
        var (run1, _) = await RunLoopAsync(store, settings, DateTime.UtcNow, batchSize: 25, noFailures);
        var (run2, _) = await RunLoopAsync(store, settings, DateTime.UtcNow, batchSize: 25, noFailures);
        var (run3, _) = await RunLoopAsync(store, settings, DateTime.UtcNow, batchSize: 25, noFailures);

        Assert.Equal(new long[] { 5, 2 }, run1);
        Assert.Equal(new long[] { 3, 4 }, run2);
        Assert.Equal(1, run3[0]);
    }

    [Fact]
    public void Schema_HasTheIndexForTheQueueOrder() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_wikidata_entities_downloaded_at'";
        Assert.Equal(1L, cmd.ExecuteScalar());
    }
}
