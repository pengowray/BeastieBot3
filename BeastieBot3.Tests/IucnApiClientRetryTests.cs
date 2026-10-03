using System.Net;
using System.Net.Http.Headers;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;

namespace BeastieBot3.Tests;

// Pins the IUCN API client's retry/backoff contract — in particular that a 429 (rate limit)
// is waited out and retried on its own budget rather than failing fast, which is what bit a
// long cache-infraranks run — and the pacing after a 429 (IucnApiPace). Uses an injected fake
// handler so no real HTTP happens, and a fake TimeProvider whose delays are recorded, not slept.
public class IucnApiClientRetryTests {
    private static IucnApiConfiguration Config(int maxRateLimitRetries = 10) => new(
        BaseUri: new Uri("https://example.test"),
        Token: "test-token",
        Timeout: TimeSpan.FromSeconds(30),
        MaxConcurrency: 1,
        InitialDelay: TimeSpan.FromMilliseconds(5),
        MaxDelay: TimeSpan.FromMilliseconds(20),
        RateLimitWait: TimeSpan.FromSeconds(60),
        MaxRateLimitRetries: maxRateLimitRetries);

    private static IucnApiClient Client(ScriptedHandler handler, FakeClock clock, int maxRateLimitRetries = 10) =>
        new(Config(maxRateLimitRetries), handler, clock.Delay, clock);

    // ---- retries ----

    [Fact]
    public async Task RateLimited_Then_Succeeds_RetriesPastThe429() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock,
            (HttpStatusCode)429,
            (HttpStatusCode)429,
            HttpStatusCode.OK);
        using var client = Client(handler, clock);

        var response = await client.GetTaxaSisAsync(123, CancellationToken.None);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(3, handler.Calls); // two 429s waited out, third succeeds
    }

    [Fact]
    public async Task RateLimited_Forever_GivesUpAfterMaxRetries() {
        var clock = new FakeClock();
        var handler = ScriptedHandler.Always(clock, (HttpStatusCode)429);
        using var client = Client(handler, clock, maxRateLimitRetries: 3);

        await Assert.ThrowsAsync<IucnApiException>(() => client.GetTaxaSisAsync(1, CancellationToken.None));

        // initial try + 3 rate-limit retries = 4 calls
        Assert.Equal(4, handler.Calls);
    }

    [Fact]
    public async Task TransientServerError_Then_Succeeds() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock, HttpStatusCode.ServiceUnavailable, HttpStatusCode.OK);
        using var client = Client(handler, clock);

        var response = await client.GetAssessmentAsync(9, CancellationToken.None);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task ClientError_NotRetried() {
        var clock = new FakeClock();
        var handler = ScriptedHandler.Always(clock, HttpStatusCode.NotFound);
        using var client = Client(handler, clock);

        await Assert.ThrowsAsync<IucnApiException>(() => client.GetTaxaSisAsync(404, CancellationToken.None));

        Assert.Equal(1, handler.Calls); // 404 fails immediately, no retry
    }

    // ---- pacing after a 429 ----

    [Fact]
    public async Task NoPacing_BeforeTheFirst429() {
        var clock = new FakeClock();
        var handler = ScriptedHandler.Always(clock, HttpStatusCode.OK);
        using var client = Client(handler, clock);

        for (var i = 0; i < 3; i++) await client.GetTaxaSisAsync(i, CancellationToken.None);

        Assert.Empty(clock.Delays);
        Assert.Equal(new[] { 0.0, 0.0 }, Gaps(handler));
    }

    // No Retry-After: the retry waits RateLimitWait, and later requests start at least 1 s apart.
    [Fact]
    public async Task After429_WaitsTheConfiguredTime_ThenSpacesRequests() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock, (HttpStatusCode)429, HttpStatusCode.OK);
        using var client = Client(handler, clock);

        for (var i = 0; i < 3; i++) await client.GetTaxaSisAsync(i, CancellationToken.None);

        // 429, retry after 60 s, then two more requests 1 s apart.
        Assert.Equal(new[] { 60.0, 1.0, 1.0 }, Gaps(handler));
    }

    // Two 429s in a row (the usual first 429 of a run) leave the interval at 1.5 s.
    [Fact]
    public async Task Each429_SlowsThePaceFurther() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock, (HttpStatusCode)429, (HttpStatusCode)429, HttpStatusCode.OK);
        using var client = Client(handler, clock);

        for (var i = 0; i < 2; i++) await client.GetTaxaSisAsync(i, CancellationToken.None);

        Assert.Equal(new[] { 60.0, 60.0, 1.5 }, Gaps(handler));
    }

    [Fact]
    public async Task RetryAfter_IsUsedWhenPresent_AndThePaceStillSlows() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock, (HttpStatusCode)429, HttpStatusCode.OK) {
            RetryAfter = TimeSpan.FromSeconds(7),
        };
        using var client = Client(handler, clock);

        for (var i = 0; i < 2; i++) await client.GetTaxaSisAsync(i, CancellationToken.None);

        Assert.Equal(new[] { 7.0, 1.0 }, Gaps(handler));
    }

    // A 404 is an answer, so a tombstone re-check (almost all 404s) lets the pace relax: after
    // 100 answers, 1 s × 0.95 is under 1 s, so the interval goes back to zero.
    [Fact]
    public async Task Answers_IncludingNotFound_RelaxThePace() {
        var clock = new FakeClock();
        var script = new List<HttpStatusCode> { (HttpStatusCode)429, HttpStatusCode.NotFound };
        script.AddRange(Enumerable.Repeat(HttpStatusCode.NotFound, 99));
        script.Add(HttpStatusCode.OK);
        script.Add(HttpStatusCode.OK);
        var handler = new ScriptedHandler(clock, script.ToArray());
        using var client = Client(handler, clock);

        for (var i = 0; i < 100; i++) {
            await Assert.ThrowsAsync<IucnApiException>(() => client.GetTaxaSisAsync(i, CancellationToken.None));
        }
        await client.GetTaxaSisAsync(1000, CancellationToken.None);
        await client.GetTaxaSisAsync(1001, CancellationToken.None);

        var gaps = Gaps(handler);
        Assert.Equal(60.0, gaps[0]);
        Assert.All(gaps.Skip(1).Take(99), gap => Assert.Equal(1.0, gap));
        Assert.Equal(1.0, gaps[100]);   // reserved when the request with the 100th answer started
        Assert.Equal(0.0, gaps[101]);
    }

    // ---- setting the system clock back ----

    // The gate measures time on the monotonic clock, so setting the system clock back an hour
    // between two requests does not make the second one wait.
    [Fact]
    public async Task SystemClockSetBack_DoesNotDelayTheNextRequest() {
        var clock = new FakeClock();
        var handler = ScriptedHandler.Always(clock, HttpStatusCode.OK);
        using var client = Client(handler, clock);

        await client.GetTaxaSisAsync(1, CancellationToken.None);
        clock.SetSystemClockBack(TimeSpan.FromHours(1));
        await client.GetTaxaSisAsync(2, CancellationToken.None);

        Assert.Empty(clock.Delays);
        Assert.Equal(new[] { 0.0 }, Gaps(handler));
    }

    // The same after a 429: the pause and the paced slots ignore the system clock too. The
    // handler sets the system clock back an hour on every request, the 429 included.
    [Fact]
    public async Task SystemClockSetBack_DuringAPause_WaitsOnlyThePause() {
        var clock = new FakeClock();
        var handler = new ScriptedHandler(clock, HttpStatusCode.OK, (HttpStatusCode)429, HttpStatusCode.OK) {
            SetSystemClockBackOnEachCall = TimeSpan.FromHours(1),
        };
        using var client = Client(handler, clock);

        for (var i = 0; i < 3; i++) await client.GetTaxaSisAsync(i, CancellationToken.None);

        // OK, 429, retry after 60 s, then the next request 1 s later.
        Assert.Equal(new[] { 0.0, 60.0, 1.0 }, Gaps(handler));
        Assert.Equal(new[] { TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(1) }, clock.Delays);
    }

    // ---- IucnApiPace ----

    [Fact]
    public void Pace_StartsAtZero_AndOnlyA429StartsIt() {
        var pace = new IucnApiPace();
        for (var i = 0; i < 500; i++) pace.OnAnswered();
        Assert.Equal(TimeSpan.Zero, pace.Interval);

        pace.OnRateLimited();
        Assert.Equal(TimeSpan.FromSeconds(1), pace.Interval);
    }

    [Fact]
    public void Pace_MultipliesBy1Point5_UpTo5Seconds() {
        var pace = new IucnApiPace();
        var seen = new List<double>();
        for (var i = 0; i < 6; i++) {
            pace.OnRateLimited();
            seen.Add(pace.Interval.TotalSeconds);
        }
        Assert.Equal(new[] { 1.0, 1.5, 2.25, 3.375, 5.0, 5.0 }, seen);
    }

    [Fact]
    public void Pace_Relaxes5PercentPer100Answers_AndA429RestartsTheCount() {
        var pace = new IucnApiPace();
        pace.OnRateLimited();
        pace.OnRateLimited();   // 1.5 s

        for (var i = 0; i < 99; i++) pace.OnAnswered();
        Assert.Equal(1.5, pace.Interval.TotalSeconds);
        pace.OnRateLimited();   // 2.25 s, count back to 0
        for (var i = 0; i < 99; i++) pace.OnAnswered();
        Assert.Equal(2.25, pace.Interval.TotalSeconds);
        pace.OnAnswered();
        Assert.Equal(2.25 * 0.95, pace.Interval.TotalSeconds, 6);
    }

    [Fact]
    public void Pace_GoesBackToZero_OnceUnderOneSecond() {
        var pace = new IucnApiPace();
        pace.OnRateLimited();
        pace.OnRateLimited();   // 1.5 s

        var answers = 0;
        while (pace.Interval > TimeSpan.Zero && answers < 10_000) {
            pace.OnAnswered();
            answers++;
        }
        Assert.Equal(TimeSpan.Zero, pace.Interval);
        Assert.Equal(800, answers);   // 1.5 × 0.95^7 ≈ 1.05 s; the 8th step goes under 1 s
    }

    // Seconds between the starts of consecutive requests, on the monotonic clock.
    private static double[] Gaps(ScriptedHandler handler) =>
        handler.Starts.Zip(handler.Starts.Skip(1), (a, b) => Math.Round((b - a).TotalSeconds, 6)).ToArray();

    // Two clocks the test moves by hand: the monotonic timestamp (GetTimestamp, one tick per
    // TimeSpan tick) and the system clock (GetUtcNow). A delay moves both forward; setting the
    // system clock back moves only the system clock.
    private sealed class FakeClock : TimeProvider {
        private long _timestamp;
        private DateTimeOffset _utcNow = new(2026, 10, 3, 0, 0, 0, TimeSpan.Zero);

        public List<TimeSpan> Delays { get; } = new();

        // Time since the clock was created, on the monotonic clock.
        public TimeSpan Elapsed => TimeSpan.FromTicks(_timestamp);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _timestamp;
        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void SetSystemClockBack(TimeSpan by) => _utcNow -= by;

        public Task Delay(TimeSpan wait, CancellationToken cancellationToken) {
            Delays.Add(wait);
            _timestamp += wait.Ticks;
            _utcNow += wait;
            return Task.CompletedTask;
        }
    }

    // Returns a scripted sequence of status codes (last one repeats if exhausted) and records when
    // each request started on the fake monotonic clock.
    private sealed class ScriptedHandler : HttpMessageHandler {
        private readonly FakeClock _clock;
        private readonly HttpStatusCode[] _sequence;
        private int _index;
        public int Calls { get; private set; }
        public List<TimeSpan> Starts { get; } = new();
        public TimeSpan? RetryAfter { get; init; }
        public TimeSpan? SetSystemClockBackOnEachCall { get; init; }

        public ScriptedHandler(FakeClock clock, params HttpStatusCode[] sequence) {
            _clock = clock;
            _sequence = sequence;
        }

        public static ScriptedHandler Always(FakeClock clock, HttpStatusCode code) => new(clock, code);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            Calls++;
            Starts.Add(_clock.Elapsed);
            if (SetSystemClockBackOnEachCall is { } step) _clock.SetSystemClockBack(step);
            var code = _index < _sequence.Length ? _sequence[_index] : _sequence[^1];
            _index++;
            var response = new HttpResponseMessage(code) {
                Content = new StringContent(code == HttpStatusCode.OK
                    ? "{\"sis_id\":1,\"assessment_id\":1}"
                    : "{\"error\":\"err\"}")
            };
            if (code == (HttpStatusCode)429 && RetryAfter is { } retryAfter) {
                response.Headers.RetryAfter = new RetryConditionHeaderValue(retryAfter);
            }
            return Task.FromResult(response);
        }
    }
}
