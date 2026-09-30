using System.Net;
using BeastieBot3.Configuration;
using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// WikidataEntityDownloader's catch-all used to catch OperationCanceledException too, so Ctrl+C or
// Cancel on a web UI job recorded the item being downloaded as a failed download: attempt_count
// went up, last_error was set, and the item moved to the --failed-only queue. A cancellation now
// leaves the item as it was and stops the run; a real failure is still recorded.
public class WikidataEntityDownloaderCancelTests {
    private const long NumericId = 42;

    private static WikidataConfiguration Config() => new(
        ApiEndpoint: new Uri("https://api.example.test/w/api.php"),
        SparqlEndpoint: new Uri("https://sparql.example.test/sparql"),
        UserAgent: "BeastieBot3-tests/1.0",
        Timeout: TimeSpan.FromSeconds(30),
        RequestDelay: TimeSpan.FromMilliseconds(1),
        SparqlDelay: TimeSpan.FromMilliseconds(1),
        SparqlBatchSize: 50);

    private static SqliteConnection OpenMemory() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    private static void QueueItem(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at,
                                          json_downloaded, attempt_count)
            VALUES (@id, @qid, @seen, @seen, 0, 0)
            """;
        cmd.Parameters.AddWithValue("@id", NumericId);
        cmd.Parameters.AddWithValue("@qid", $"Q{NumericId}");
        cmd.Parameters.AddWithValue("@seen", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).ToString("O"));
        cmd.ExecuteNonQuery();
    }

    private static (long Attempts, string? LastError) ItemState(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT attempt_count, last_error FROM wikidata_entities WHERE entity_numeric_id = @id";
        cmd.Parameters.AddWithValue("@id", NumericId);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        return (reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private static WikidataEntityWorkItem Item() => new(NumericId, $"Q{NumericId}", null, 0);

    [Fact]
    public async Task CancelDuringADownload_StopsWithoutRecordingAFailure() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        QueueItem(connection);

        // Cancelled from inside the handler, so the request is in flight when the token fires,
        // as it is when someone presses Ctrl+C during a download.
        using var cts = new CancellationTokenSource();
        var handler = new FakeHandler(HttpStatusCode.OK) { OnCall = cts.Cancel };
        using var client = new WikidataApiClient(Config(), handler, handler);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WikidataEntityDownloader.DownloadSingleAsync(client, store, Item(), cts.Token));

        Assert.Equal(1, handler.Calls);
        Assert.Equal((0L, (string?)null), ItemState(connection));
        Assert.Equal(0, store.CountFailedEntities());
    }

    [Fact]
    public async Task AFailedDownload_IsStillRecorded() {
        using var connection = OpenMemory();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        QueueItem(connection);

        // A 400 is not retried, so this is one request and one failure.
        var handler = new FakeHandler(HttpStatusCode.BadRequest);
        using var client = new WikidataApiClient(Config(), handler, handler);

        var ok = await WikidataEntityDownloader.DownloadSingleAsync(client, store, Item(), CancellationToken.None);

        Assert.False(ok);
        var (attempts, lastError) = ItemState(connection);
        Assert.Equal(1, attempts);
        Assert.NotNull(lastError);
    }

    private sealed class FakeHandler : HttpMessageHandler {
        private readonly HttpStatusCode _status;
        public int Calls { get; private set; }

        // Runs after the call is counted, so a test can cancel mid-request.
        public Action? OnCall { get; init; }

        public FakeHandler(HttpStatusCode status) => _status = status;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            OnCall?.Invoke();
            if (cancellationToken.IsCancellationRequested) {
                return Task.FromException<HttpResponseMessage>(new OperationCanceledException(cancellationToken));
            }

            return Task.FromResult(new HttpResponseMessage(_status) {
                Content = new StringContent("{}")
            });
        }
    }
}
