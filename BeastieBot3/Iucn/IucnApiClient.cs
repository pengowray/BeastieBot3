using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using Spectre.Console;

// HTTP client for IUCN Red List API v4 (api.iucnredlist.org). Configuration from
// IucnApiConfiguration (IUCN_API_TOKEN env var required). Limits concurrency with a semaphore,
// retries 5xx/timeouts with exponential backoff (2s→60s default), and handles 429 Too Many
// Requests by waiting (Retry-After, or IUCN_API_RATELIMIT_SECONDS when the header is missing)
// and then sending requests further apart for the rest of the run (IucnApiPace).
// Endpoints: /api/v4/taxa/sis/{sisId}, /api/v4/assessment/{assessmentId}.
// Used by the `iucn api cache-*` and `discover-by-family` commands.

namespace BeastieBot3.Iucn;

internal sealed class IucnApiClient : IDisposable {
    // Max attempts for transient 5xx/timeout errors (rate-limit 429s have their own, larger budget).
    private const int MaxTransientAttempts = 5;

    private readonly HttpClient _httpClient;
    private readonly SemaphoreSlim _semaphore;
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;
    private readonly TimeSpan _rateLimitWait;
    private readonly int _maxRateLimitRetries;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<DateTimeOffset> _now;

    // Shared gate, guarded by _rateLock. Every request start, retries included, takes the next
    // free time slot: no earlier than _pausedUntil (set by a 429, so concurrent workers back off
    // together instead of each using up its own retries), and at least _pace.Interval after the
    // previous start (measured start to start, so a slow answer does not add to the wait).
    private readonly object _rateLock = new();
    private readonly IucnApiPace _pace;
    private DateTimeOffset _pausedUntil = DateTimeOffset.MinValue;
    private DateTimeOffset _nextSlot = DateTimeOffset.MinValue;

    public IucnApiClient(IucnApiConfiguration configuration)
        : this(configuration, new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
            MaxConnectionsPerServer = configuration.MaxConcurrency
        }) {
    }

    // Test/advanced seam: inject the message handler (e.g. a fake that returns 429s) so the
    // retry/backoff logic can be exercised without real HTTP, and the delay and clock so the
    // waits can be recorded instead of slept.
    internal IucnApiClient(IucnApiConfiguration configuration, HttpMessageHandler handler,
        Func<TimeSpan, CancellationToken, Task>? delay = null, Func<DateTimeOffset>? now = null, IucnApiPace? pace = null) {
        _delay = delay ?? Task.Delay;
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _pace = pace ?? new IucnApiPace();
        _httpClient = new HttpClient(handler) {
            BaseAddress = configuration.BaseUri,
            Timeout = configuration.Timeout
        };

        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", configuration.Token);
        _httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("BeastieBot3/1.0 (+https://github.com/pengowray/BeastieBot3)");

        _semaphore = new SemaphoreSlim(configuration.MaxConcurrency, configuration.MaxConcurrency);
        _initialDelay = configuration.InitialDelay;
        _maxDelay = configuration.MaxDelay;
        _rateLimitWait = configuration.RateLimitWait;
        _maxRateLimitRetries = configuration.MaxRateLimitRetries;
    }

    public Task<IucnApiResponse> GetTaxaSisAsync(long sisId, CancellationToken cancellationToken) =>
        SendAsync($"/api/v4/taxa/sis/{sisId}", cancellationToken);

    public Task<IucnApiResponse> GetAssessmentAsync(long assessmentId, CancellationToken cancellationToken) =>
        SendAsync($"/api/v4/assessment/{assessmentId}", cancellationToken);

    // IUCN Red List API v4 information endpoint returning the current published
    // release version (e.g. { "red_list_version": "2025-2" }). Used by the web UI
    // freshness check; if IUCN changes this path the caller degrades gracefully.
    public Task<IucnApiResponse> GetRedListVersionAsync(CancellationToken cancellationToken) =>
        SendAsync("/api/v4/information/red_list_version", cancellationToken);

    public Task<IucnApiResponse> GetTaxaFamilyListAsync(CancellationToken cancellationToken) =>
        SendAsync("/api/v4/taxa/family/", cancellationToken);

    public Task<IucnApiResponse> GetTaxaByFamilyAsync(string familyName, int page, CancellationToken cancellationToken) =>
        SendAsync($"/api/v4/taxa/family/{Uri.EscapeDataString(familyName)}?page={page}", cancellationToken);

    private async Task<IucnApiResponse> SendAsync(string relativeUrl, CancellationToken cancellationToken) {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            var url = relativeUrl.StartsWith("/", StringComparison.Ordinal) ? relativeUrl : "/" + relativeUrl;
            var transientAttempt = 0;   // 5xx / network / timeout
            var rateLimitAttempt = 0;   // 429
            var delay = _initialDelay;

            while (true) {
                cancellationToken.ThrowIfCancellationRequested();

                await WaitForTurnAsync(cancellationToken).ConfigureAwait(false);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                HttpResponseMessage response;
                try {
                    response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                    throw;
                }
                catch (Exception ex) {
                    // Network failure or timeout — retry on the transient budget.
                    if (++transientAttempt >= MaxTransientAttempts) {
                        throw new IucnApiException(url, null, ex.Message, transientAttempt, ex);
                    }
                    await _delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = NextDelay(delay);
                    continue;
                }

                using (response) {
                    var payload = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

                    // Rate limited: wait the Retry-After (or the configured window), send requests
                    // further apart from now on, and retry on a separate, larger budget. A 429 is
                    // transient and clears once the window passes.
                    if (response.StatusCode == HttpStatusCode.TooManyRequests) {
                        if (++rateLimitAttempt > _maxRateLimitRetries) {
                            throw new IucnApiException(url, response.StatusCode, payload, rateLimitAttempt);
                        }
                        var wait = RetryAfter(response) ?? _rateLimitWait;
                        var interval = OnRateLimited(wait);
                        AnsiConsole.MarkupLineInterpolated(
                            $"[yellow]IUCN API answered 429 Too Many Requests.[/] Waiting {Seconds(wait)}, then sending at most one request every {Seconds(interval)}. Retry {rateLimitAttempt} of {_maxRateLimitRetries}.");
                        continue;   // WaitForTurnAsync waits out the pause
                    }

                    OnAnswered();

                    if (response.IsSuccessStatusCode) {
                        return new IucnApiResponse(url, payload, response.StatusCode, response.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(payload));
                    }

                    // Other transient server errors: exponential backoff on the transient budget.
                    if (IsTransientStatus(response.StatusCode)) {
                        if (++transientAttempt >= MaxTransientAttempts) {
                            throw new IucnApiException(url, response.StatusCode, payload, transientAttempt);
                        }
                        var wait = RetryAfter(response) ?? delay;
                        await _delay(wait, cancellationToken).ConfigureAwait(false);
                        delay = NextDelay(delay);
                        continue;
                    }

                    // Anything else (4xx other than 429) is not retryable.
                    throw new IucnApiException(url, response.StatusCode, payload, transientAttempt + 1);
                }
            }
        }
        finally {
            _semaphore.Release();
        }
    }

    private static bool IsTransientStatus(HttpStatusCode statusCode) =>
        statusCode is HttpStatusCode.RequestTimeout
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout
            or HttpStatusCode.InternalServerError;

    // Reads the Retry-After header (absolute date or delta seconds) if the server sent one.
    private static TimeSpan? RetryAfter(HttpResponseMessage response) {
        if (response.Headers.RetryAfter is not { } retryAfter) return null;
        if (retryAfter.Delta is { } delta && delta > TimeSpan.Zero) return delta;
        if (retryAfter.Date is { } date) {
            var wait = date - DateTimeOffset.UtcNow;
            if (wait > TimeSpan.Zero) return wait;
        }
        return null;
    }

    // Takes the next free time slot (see _rateLock) and waits for it.
    private async Task WaitForTurnAsync(CancellationToken cancellationToken) {
        TimeSpan wait;
        lock (_rateLock) {
            var now = _now();
            var slot = now;
            if (_nextSlot > slot) slot = _nextSlot;
            if (_pausedUntil > slot) slot = _pausedUntil;
            _nextSlot = slot + _pace.Interval;
            wait = slot - now;
        }
        if (wait > TimeSpan.Zero) {
            await _delay(wait, cancellationToken).ConfigureAwait(false);
        }
    }

    // Pauses every worker for `wait` and slows the pace. Returns the new interval between requests.
    private TimeSpan OnRateLimited(TimeSpan wait) {
        lock (_rateLock) {
            var until = _now() + wait;
            if (until > _pausedUntil) _pausedUntil = until;
            _pace.OnRateLimited();
            return _pace.Interval;
        }
    }

    private void OnAnswered() {
        lock (_rateLock) {
            _pace.OnAnswered();
        }
    }

    private static string Seconds(TimeSpan span) =>
        span.TotalSeconds < 10 ? $"{span.TotalSeconds:0.##} s" : $"{span.TotalSeconds:N0} s";

    private TimeSpan NextDelay(TimeSpan current) {
        var doubled = TimeSpan.FromMilliseconds(current.TotalMilliseconds * 2);
        return doubled <= _maxDelay ? doubled : _maxDelay;
    }

    public void Dispose() {
        _httpClient.Dispose();
        _semaphore.Dispose();
    }
}

// The shortest time between the starts of two requests, raised by each 429 Too Many Requests.
//
// IUCN's API answers 429 without a Retry-After header. A 2026-10-03 run of 1,502 requests at the
// commands' default --sleep-ms 250 (about 2 requests a second) got a 429 about every 100 requests;
// the retry after a 60 s wait got a second 429 14 times out of 15, and the retry after 120 s
// always worked. That fits a sliding limit of about 100 requests per 2 minutes, or one request
// every 1.2 s. A simulation of that limit reproduces the run without pacing (44 minutes, 30 429s)
// and gives about 38 minutes and 4 429s with the pacing below (20,000 requests: 9.8 hours and 404
// 429s without, 8.5 hours and 26 with). The limit itself sets the floor: about 30 minutes.
//
//   - Until the first 429 the interval is zero: the commands' own --sleep-ms sets the pace.
//   - Each 429 multiplies the interval by 1.5, starting at 1 s (anything shorter does nothing,
//     because --sleep-ms 250 plus the answer time already spaces requests about 0.5 s apart),
//     up to 5 s.
//   - After every 100 answers in a row that are not 429, the interval shrinks by 5%. Once it is
//     below 1 s it goes back to zero, so a single 429 does not slow the rest of a long run. From
//     1.5 s that takes about 800 answers.
//
// The first 429 of a run still costs two waits whatever the pace: the requests sent before it are
// still inside the server's window 60 s later. Only the later 429s are fewer and cheaper.
// Not thread-safe; IucnApiClient calls it under its lock.
internal sealed class IucnApiPace {
    public static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan MaxInterval = TimeSpan.FromSeconds(5);
    public const double SlowerFactor = 1.5;
    public const double FasterFactor = 0.95;
    public const int AnswersBeforeFaster = 100;

    private int _answersSince;

    public TimeSpan Interval { get; private set; } = TimeSpan.Zero;

    public void OnRateLimited() {
        var slower = Interval * SlowerFactor;
        if (slower < MinInterval) slower = MinInterval;
        Interval = slower > MaxInterval ? MaxInterval : slower;
        _answersSince = 0;
    }

    // Any answer other than a 429, including a 404 (a tombstone re-check is almost all 404s).
    public void OnAnswered() {
        if (Interval == TimeSpan.Zero) return;
        if (++_answersSince < AnswersBeforeFaster) return;
        _answersSince = 0;
        var faster = Interval * FasterFactor;
        Interval = faster < MinInterval ? TimeSpan.Zero : faster;
    }
}

internal sealed record IucnApiResponse(string Url, string Body, HttpStatusCode StatusCode, long PayloadBytes);

internal sealed class IucnApiException : Exception {
    public IucnApiException(string url, HttpStatusCode? statusCode, string responseBody, int attempt, Exception? inner = null)
        : base($"IUCN API request to {url} failed with status {(int?)statusCode ?? 0} on attempt {attempt}" + (string.IsNullOrWhiteSpace(responseBody) ? string.Empty : $" Body: {responseBody}"), inner) {
        Url = url;
        StatusCode = statusCode;
        ResponseBody = responseBody;
        Attempt = attempt;
    }

    public string Url { get; }
    public HttpStatusCode? StatusCode { get; }
    public string ResponseBody { get; }
    public int Attempt { get; }
}
