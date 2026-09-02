using System.Net;
using BeastieBot3.Configuration;
using BeastieBot3.Wikidata;

namespace BeastieBot3.Tests;

// Pins the one that bit a 2-hour `wikidata backfill-iucn` run: an HttpClient timeout arrives
// as TaskCanceledException with the caller's token untouched, and the retry loop used to
// rethrow it. Nothing above catches it, Program.cs reads any OperationCanceledException as
// "cancelled" and exits -2 without printing anything, so the whole run died silently. A
// timeout is a transient failure and must be retried like a 503.
public class WikidataApiClientRetryTests {
    private static WikidataConfiguration Config() => new(
        ApiEndpoint: new Uri("https://api.example.test/w/api.php"),
        SparqlEndpoint: new Uri("https://sparql.example.test/sparql"),
        UserAgent: "BeastieBot3-tests/1.0",
        Timeout: TimeSpan.FromSeconds(30),
        RequestDelay: TimeSpan.FromMilliseconds(1),
        SparqlDelay: TimeSpan.FromMilliseconds(1),
        SparqlBatchSize: 50);

    private static WikidataApiClient ClientWith(ScriptedHandler handler) =>
        new(Config(), handler, handler);

    [Fact]
    public async Task RequestTimeout_Then_Succeeds_IsRetried() {
        var handler = new ScriptedHandler(Step.Timeout, Step.Ok);
        using var client = ClientWith(handler);

        var results = await client.SearchTaxaByP225Async("Panthera leo", CancellationToken.None);

        Assert.Empty(results);       // the OK body is an empty result set
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task RequestTimeout_Forever_ThrowsApiException_NotCancellation() {
        var handler = ScriptedHandler.Always(Step.Timeout);
        using var client = ClientWith(handler);

        // The point of the fix: a run out of retries fails with something the CLI can report,
        // not an OperationCanceledException that reads as "the user cancelled".
        var ex = await Assert.ThrowsAsync<WikidataApiException>(
            () => client.SearchTaxaByLabelAsync("Panthera leo", CancellationToken.None));

        Assert.Contains("timed out", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(5, handler.Calls);
    }

    [Fact]
    public async Task RealCancellation_StillAborts() {
        // Cancelled from inside the handler, so the request is genuinely in flight when the
        // token fires. Cancelling up front would be caught by the loop's top-of-iteration
        // check and never reach the catch filter this is here to pin.
        var handler = ScriptedHandler.Always(Step.Timeout);
        using var cts = new CancellationTokenSource();
        handler.OnCall = cts.Cancel;
        using var client = ClientWith(handler);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.SearchTaxaByP225Async("Panthera leo", cts.Token));

        Assert.Equal(1, handler.Calls);   // aborted, not retried
    }

    [Fact]
    public async Task TransientServerError_Then_Succeeds() {
        var handler = new ScriptedHandler(Step.ServiceUnavailable, Step.Ok);
        using var client = ClientWith(handler);

        await client.SearchTaxaByP225Async("Panthera leo", CancellationToken.None);

        Assert.Equal(2, handler.Calls);
    }

    private enum Step { Ok, Timeout, ServiceUnavailable }

    // Plays a scripted sequence of outcomes; the last one repeats once the script runs out.
    private sealed class ScriptedHandler : HttpMessageHandler {
        private const string EmptyResults = """{"results":{"bindings":[]}}""";

        private readonly Step[] _sequence;
        private int _index;
        public int Calls { get; private set; }

        // Runs after the call is counted, so a test can cancel mid-request.
        public Action? OnCall { get; set; }

        public ScriptedHandler(params Step[] sequence) => _sequence = sequence;

        public static ScriptedHandler Always(Step step) => new(step);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            OnCall?.Invoke();
            var step = _index < _sequence.Length ? _sequence[_index] : _sequence[^1];
            _index++;
            if (cancellationToken.IsCancellationRequested) {
                return Task.FromException<HttpResponseMessage>(new OperationCanceledException(cancellationToken));
            }

            return step switch {
                // Exactly how HttpClient surfaces its own Timeout: a cancellation the caller
                // never asked for, wrapping a TimeoutException.
                Step.Timeout => Task.FromException<HttpResponseMessage>(
                    new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout of 30 seconds elapsing.", new TimeoutException())),
                Step.ServiceUnavailable => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) {
                    Content = new StringContent("busy")
                }),
                _ => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) {
                    Content = new StringContent(EmptyResults)
                })
            };
        }
    }
}
