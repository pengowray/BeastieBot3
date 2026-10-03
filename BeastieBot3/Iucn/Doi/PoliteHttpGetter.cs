using System.Diagnostics;
using System.Net;

// GET requests to one public service at a fixed pace, with retries. Used for doi.org's handle API
// and Crossref's REST API by `iucn resolve-dois`.
//
//   - Requests start at least MinInterval apart, measured from the start of one request to the
//     start of the next, so a slow answer does not add to the wait.
//   - 429 Too Many Requests: waits for Retry-After (or RateLimitWait when the header is missing),
//     doubles MinInterval for the rest of the run (at most MaxInterval) and tries again.
//   - 408, 500, 502, 503, 504 and network errors: waits 2 s, 4 s, 8 s ... (at most 2 minutes, or
//     Retry-After when the server sends one) and tries again.
//   - After MaxAttempts tries for one URL, throws PoliteHttpException. Any other status is
//     returned to the caller with its body: a 404 from doi.org is an answer ("no such DOI").

namespace BeastieBot3.Iucn.Doi;

internal sealed record HttpGetResult(int Status, string Body);

internal sealed class PoliteHttpException : Exception {
    public PoliteHttpException(string url, string message, Exception? inner = null)
        : base(message, inner) {
        Url = url;
    }

    public string Url { get; }
}

internal sealed class PoliteHttpGetter {
    public static readonly TimeSpan MaxInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan FirstBackoff = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(2);

    private readonly HttpClient _http;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private TimeSpan? _lastStart;

    public PoliteHttpGetter(HttpClient http, TimeSpan minInterval, Func<TimeSpan, CancellationToken, Task>? delay = null) {
        _http = http;
        MinInterval = minInterval < TimeSpan.Zero ? TimeSpan.Zero : minInterval;
        _delay = delay ?? Task.Delay;
    }

    public TimeSpan MinInterval { get; private set; }
    public int MaxAttempts { get; init; } = 6;
    public TimeSpan RateLimitWait { get; init; } = TimeSpan.FromSeconds(60);

    /// Requests sent, retries included.
    public int Requests { get; private set; }
    public int RateLimited { get; private set; }
    public int Retried { get; private set; }

    /// Called with a message whenever the getter waits before trying again.
    public Action<string>? OnRetry { get; init; }

    public async Task<HttpGetResult> GetAsync(string url, CancellationToken cancellationToken) {
        var backoff = FirstBackoff;
        for (var attempt = 1; ; attempt++) {
            await WaitForTurnAsync(cancellationToken).ConfigureAwait(false);
            Requests++;
            HttpResponseMessage response;
            try {
                response = await _http.GetAsync(url, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false);
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                throw;
            } catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException) {
                if (attempt >= MaxAttempts) {
                    throw new PoliteHttpException(url, $"No answer after {attempt} tries: {ex.Message}", ex);
                }
                Retried++;
                OnRetry?.Invoke($"No answer from {Host(url)} ({ex.Message}). Trying again in {Seconds(backoff)}.");
                await _delay(backoff, cancellationToken).ConfigureAwait(false);
                backoff = Next(backoff);
                continue;
            }

            using (response) {
                var status = (int)response.StatusCode;
                if (response.StatusCode == HttpStatusCode.TooManyRequests) {
                    RateLimited++;
                    if (attempt >= MaxAttempts) {
                        throw new PoliteHttpException(url, $"Still rate limited (HTTP 429) after {attempt} tries.");
                    }
                    var wait = RetryAfter(response) ?? RateLimitWait;
                    var slower = MinInterval * 2 < TimeSpan.FromMilliseconds(250) ? TimeSpan.FromMilliseconds(250) : MinInterval * 2;
                    MinInterval = slower > MaxInterval ? MaxInterval : slower;
                    OnRetry?.Invoke($"{Host(url)} answered 429 Too Many Requests. Waiting {Seconds(wait)}, then sending at most one request every {Seconds(MinInterval)}.");
                    await _delay(wait, cancellationToken).ConfigureAwait(false);
                    continue;
                }
                if (IsTransient(response.StatusCode)) {
                    if (attempt >= MaxAttempts) {
                        throw new PoliteHttpException(url, $"HTTP {status} {response.ReasonPhrase} after {attempt} tries.");
                    }
                    Retried++;
                    var wait = RetryAfter(response) ?? backoff;
                    OnRetry?.Invoke($"{Host(url)} answered HTTP {status} {response.ReasonPhrase}. Trying again in {Seconds(wait)}.");
                    await _delay(wait, cancellationToken).ConfigureAwait(false);
                    backoff = Next(backoff);
                    continue;
                }
                var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                return new HttpGetResult(status, body);
            }
        }
    }

    private async Task WaitForTurnAsync(CancellationToken cancellationToken) {
        if (_lastStart is { } last) {
            var wait = last + MinInterval - _clock.Elapsed;
            if (wait > TimeSpan.Zero) {
                await _delay(wait, cancellationToken).ConfigureAwait(false);
            }
        }
        _lastStart = _clock.Elapsed;
    }

    private static bool IsTransient(HttpStatusCode status) =>
        status is HttpStatusCode.RequestTimeout or HttpStatusCode.InternalServerError or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout;

    private static TimeSpan Next(TimeSpan backoff) {
        var next = backoff * 2;
        return next > MaxBackoff ? MaxBackoff : next;
    }

    internal static TimeSpan? RetryAfter(HttpResponseMessage response) {
        if (response.Headers.RetryAfter is not { } retryAfter) {
            return null;
        }
        if (retryAfter.Delta is { } delta && delta > TimeSpan.Zero) {
            return delta;
        }
        if (retryAfter.Date is { } date) {
            var wait = date - DateTimeOffset.UtcNow;
            if (wait > TimeSpan.Zero) {
                return wait;
            }
        }
        return null;
    }

    private static string Host(string url) => Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri.Host : url;

    private static string Seconds(TimeSpan span) =>
        span.TotalSeconds < 10 ? $"{span.TotalSeconds:0.##} s" : $"{span.TotalSeconds:N0} s";
}
