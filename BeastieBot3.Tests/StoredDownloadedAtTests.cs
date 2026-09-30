using System;
using BeastieBot3.Infrastructure;
using BeastieBot3.Wikidata;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// downloaded_at is stored as a UTC "O" string. Plain DateTime.TryParse turns the trailing Z into
// local time, and DateTime comparison ignores Kind, so at UTC+10 a cached copy downloaded up to ten
// hours before the refresh cutoff looked newer than it. The SQL still selected it (string compare),
// but WikidataCacheItemsCommand.ShouldDownload then skipped it, so it was never refreshed. The Kind
// check fails on any machine, including one on UTC; the "< threshold" check only fails east of UTC.
public class StoredDownloadedAtTests {
    private static readonly DateTime Threshold = new(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Downloaded = Threshold.AddHours(-1);

    private static SqliteConnection OpenMemory() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    private static void AddEntity(SqliteConnection connection, long numericId, bool jsonDownloaded, bool failed) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at,
                                          json_downloaded, downloaded_at, attempt_count, last_error)
            VALUES (@id, @qid, @dl, @dl, @json, @dl, @attempts, @error)
            """;
        cmd.Parameters.AddWithValue("@id", numericId);
        cmd.Parameters.AddWithValue("@qid", $"Q{numericId}");
        cmd.Parameters.AddWithValue("@dl", Downloaded.ToString("O"));
        cmd.Parameters.AddWithValue("@json", jsonDownloaded ? 1 : 0);
        cmd.Parameters.AddWithValue("@attempts", failed ? 1 : 0);
        cmd.Parameters.AddWithValue("@error", failed ? "HTTP 500" : (object)DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static void AssertUtc(DateTime? value) {
        Assert.NotNull(value);
        Assert.Equal(DateTimeKind.Utc, value!.Value.Kind);
        Assert.Equal(Downloaded, value.Value);
        Assert.True(value.Value < Threshold);
    }

    [Fact]
    public void WikidataPendingEntities_ReadDownloadedAtAsUtc() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        AddEntity(connection, 42, jsonDownloaded: true, failed: false);

        var item = Assert.Single(store.GetPendingEntities(10, Threshold, refreshOnly: true));
        AssertUtc(item.DownloadedAt);
    }

    [Fact]
    public void WikidataFailedEntities_ReadDownloadedAtAsUtc() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        AddEntity(connection, 43, jsonDownloaded: false, failed: true);

        var item = Assert.Single(store.GetFailedEntities(10));
        AssertUtc(item.DownloadedAt);
    }

    [Fact]
    public void WikipediaPendingPages_ReadDownloadedAtAsUtc() {
        using var connection = OpenMemory();
        using var store = WikipediaCacheStore.OpenFromConnection(connection);
        using (var cmd = connection.CreateCommand()) {
            cmd.CommandText = """
                INSERT INTO wiki_pages(page_title, normalized_title, discovered_at, last_seen_at,
                                       download_status, downloaded_at)
                VALUES ('Kakapo', 'Kakapo', @dl, @dl, @status, @dl)
                """;
            cmd.Parameters.AddWithValue("@dl", Downloaded.ToString("O"));
            cmd.Parameters.AddWithValue("@status", WikiPageDownloadStatus.Cached);
            cmd.ExecuteNonQuery();
        }

        var scope = new WikipediaCacheStore.WikiFetchScope { RefreshOnly = true, RefreshThreshold = Threshold };
        var page = Assert.Single(store.GetPendingPages(10, scope));
        AssertUtc(page.DownloadedAt);
    }

    // The shared parser every reader above should use. Moved here from the removed common-name
    // probe tests, which were its only direct test.
    [Fact]
    public void StoredUtc_ParsesAStoredStampAsUtc() {
        var parsed = StoredUtc.Parse("2026-08-20T10:00:00.0000000Z");
        Assert.Equal(DateTimeKind.Utc, parsed!.Value.Kind);
        Assert.Equal(new DateTime(2026, 8, 20, 10, 0, 0, DateTimeKind.Utc), parsed.Value);
    }
}
