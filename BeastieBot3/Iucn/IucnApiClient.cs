using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using Spectre.Console;

// HTTP client for IUCN Red List API v4 (apiv4.iucnredlist.org). Configuration from
// IucnApiConfiguration (IUCN_API_TOKEN env var required). Implements concurrency
// limiting via semaphore and exponential backoff (2s→90s) for 429/5xx responses.
// Endpoints: /api/v4/taxa/sis/{sisId}, /api/v4/assessment/{assessmentId}.
// Used by IucnApiCacheTaxaCommand and IucnApiCacheAssessmentsCommand.

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

    // Shared rate-limit gate: when any request gets a 429, every worker waits until this
    // instant before firing again, so concurrent requests back off together instead of all
    // hammering through their own retries. Guarded by _rateLock.
    private readonly object _rateLock = new();
    private DateTimeOffset _pausedUntil = DateTimeOffset.MinValue;

    public IucnApiClient(IucnApiConfiguration configuration)
        : this(configuration, new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
            MaxConnectionsPerServer = configuration.MaxConcurrency
        }) {
    }

    // Test/advanced seam: inject the message handler (e.g. a fake that returns 429s) so the
    // retry/backoff logic can be exercised without real HTTP.
    internal IucnApiClient(IucnApiConfiguration configuration, HttpMessageHandler handler) {
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

                // Honour a rate-limit pause any worker discovered, so concurrent requests
                // don't all blow through their retries against a known-throttled endpoint.
                await WaitForRateLimitWindowAsync(cancellationToken).ConfigureAwait(false);

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
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = NextDelay(delay);
                    continue;
                }

                using (response) {
                    var payload = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode) {
                        return new IucnApiResponse(url, payload, response.StatusCode, response.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(payload));
                    }

                    // Rate limited: wait the Retry-After (or the configured window) and retry on a
                    // separate, larger budget — a 429 is transient and clears once the window passes.
                    if (response.StatusCode == HttpStatusCode.TooManyRequests) {
                        if (++rateLimitAttempt > _maxRateLimitRetries) {
                            throw new IucnApiException(url, response.StatusCode, payload, rateLimitAttempt);
                        }
                        var wait = RetryAfter(response) ?? _rateLimitWait;
                        BeginRateLimitPause(wait);
                        AnsiConsole.MarkupLineInterpolated(
                            $"[yellow]IUCN API rate limited[/] — waiting {wait.TotalSeconds:N0}s then retrying ({rateLimitAttempt}/{_maxRateLimitRetries})…");
                        await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    // Other transient server errors: exponential backoff on the transient budget.
                    if (IsTransientStatus(response.StatusCode)) {
                        if (++transientAttempt >= MaxTransientAttempts) {
                            throw new IucnApiException(url, response.StatusCode, payload, transientAttempt);
                        }
                        var wait = RetryAfter(response) ?? delay;
                        await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
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

    private async Task WaitForRateLimitWindowAsync(CancellationToken cancellationToken) {
        TimeSpan wait;
        lock (_rateLock) {
            wait = _pausedUntil - DateTimeOffset.UtcNow;
        }
        if (wait > TimeSpan.Zero) {
            await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
        }
    }

    private void BeginRateLimitPause(TimeSpan wait) {
        lock (_rateLock) {
            var until = DateTimeOffset.UtcNow + wait;
            if (until > _pausedUntil) _pausedUntil = until;
        }
    }

    private TimeSpan NextDelay(TimeSpan current) {
        var doubled = TimeSpan.FromMilliseconds(current.TotalMilliseconds * 2);
        return doubled <= _maxDelay ? doubled : _maxDelay;
    }

    public void Dispose() {
        _httpClient.Dispose();
        _semaphore.Dispose();
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
