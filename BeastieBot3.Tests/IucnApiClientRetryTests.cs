using System.Net;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;

namespace BeastieBot3.Tests;

// Pins the IUCN API client's retry/backoff contract — in particular that a 429 (rate limit)
// is waited out and retried on its own budget rather than failing fast, which is what bit a
// long cache-infraranks run. Uses an injected fake handler so no real HTTP happens; waits are
// configured to a few ms so the tests stay fast.
public class IucnApiClientRetryTests {
    private static IucnApiConfiguration Config(int maxRateLimitRetries = 10) => new(
        BaseUri: new Uri("https://example.test"),
        Token: "test-token",
        Timeout: TimeSpan.FromSeconds(30),
        MaxConcurrency: 1,
        InitialDelay: TimeSpan.FromMilliseconds(5),
        MaxDelay: TimeSpan.FromMilliseconds(20),
        RateLimitWait: TimeSpan.FromMilliseconds(10),
        MaxRateLimitRetries: maxRateLimitRetries);

    [Fact]
    public async Task RateLimited_Then_Succeeds_RetriesPastThe429() {
        var handler = new ScriptedHandler(
            (HttpStatusCode)429,
            (HttpStatusCode)429,
            HttpStatusCode.OK);
        using var client = new IucnApiClient(Config(), handler);

        var response = await client.GetTaxaSisAsync(123, CancellationToken.None);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(3, handler.Calls); // two 429s waited out, third succeeds
    }

    [Fact]
    public async Task RateLimited_Forever_GivesUpAfterMaxRetries() {
        var handler = ScriptedHandler.Always((HttpStatusCode)429);
        using var client = new IucnApiClient(Config(maxRateLimitRetries: 3), handler);

        await Assert.ThrowsAsync<IucnApiException>(() => client.GetTaxaSisAsync(1, CancellationToken.None));

        // initial try + 3 rate-limit retries = 4 calls
        Assert.Equal(4, handler.Calls);
    }

    [Fact]
    public async Task TransientServerError_Then_Succeeds() {
        var handler = new ScriptedHandler(HttpStatusCode.ServiceUnavailable, HttpStatusCode.OK);
        using var client = new IucnApiClient(Config(), handler);

        var response = await client.GetAssessmentAsync(9, CancellationToken.None);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task ClientError_NotRetried() {
        var handler = ScriptedHandler.Always(HttpStatusCode.NotFound);
        using var client = new IucnApiClient(Config(), handler);

        await Assert.ThrowsAsync<IucnApiException>(() => client.GetTaxaSisAsync(404, CancellationToken.None));

        Assert.Equal(1, handler.Calls); // 404 fails immediately, no retry
    }

    // Returns a scripted sequence of status codes (last one repeats if exhausted).
    private sealed class ScriptedHandler : HttpMessageHandler {
        private readonly HttpStatusCode[] _sequence;
        private int _index;
        public int Calls { get; private set; }

        public ScriptedHandler(params HttpStatusCode[] sequence) => _sequence = sequence;

        public static ScriptedHandler Always(HttpStatusCode code) => new(code);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            Calls++;
            var code = _index < _sequence.Length ? _sequence[_index] : _sequence[^1];
            _index++;
            var response = new HttpResponseMessage(code) {
                Content = new StringContent(code == HttpStatusCode.OK
                    ? "{\"sis_id\":1,\"assessment_id\":1}"
                    : "{\"error\":\"err\"}")
            };
            return Task.FromResult(response);
        }
    }
}
