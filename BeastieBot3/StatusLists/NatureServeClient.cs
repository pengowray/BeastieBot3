using System.Net;
using System.Net.Http.Headers;
using System.Text;

// Sends NatureServe Explorer requests one at a time, at least DelayBetweenRequests apart, and tries
// again after a wait when the answer is 429, 408, a 5xx or a network error.

namespace BeastieBot3.StatusLists;

internal sealed class NatureServeException : Exception {
    public HttpStatusCode? StatusCode { get; }

    public NatureServeException(string message, HttpStatusCode? statusCode = null, Exception? inner = null) : base(message, inner) {
        StatusCode = statusCode;
    }
}

internal sealed class NatureServeClient : IDisposable {
    public const string UserAgent = "BeastieBot3/1.0 (+https://github.com/pengowray/BeastieBot3)";
    public static readonly TimeSpan DelayBetweenRequests = TimeSpan.FromMilliseconds(500);

    // Waits before each new try of a failed request.
    private static readonly TimeSpan[] RetryWaits = {
        TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2),
    };

    private readonly HttpClient _http;
    private readonly Action<string>? _onRetry;
    private DateTime _lastRequestUtc = DateTime.MinValue;

    public NatureServeClient(Action<string>? onRetry = null) {
        _http = new HttpClient { Timeout = TimeSpan.FromMinutes(2) };
        _http.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _onRetry = onRetry;
    }

    public async Task<NatureServePage> SearchAsync(string prefix, int page, DateTime? modifiedSinceUtc, CancellationToken cancellationToken) {
        var json = await PostAsync(NatureServeSearch.SearchUrl, NatureServeSearch.BuildRequest(prefix, page, modifiedSinceUtc), cancellationToken)
            .ConfigureAwait(false);
        return NatureServeSearch.ParsePage(json);
    }

    public async Task<IReadOnlyList<long>> UnpublishedSinceAsync(DateTime? sinceUtc, CancellationToken cancellationToken) {
        var json = await PostAsync(NatureServeSearch.UnpublishedUrl, NatureServeSearch.BuildUnpublishedRequest(sinceUtc), cancellationToken)
            .ConfigureAwait(false);
        return NatureServeSearch.ParseUnpublished(json);
    }

    private async Task<string> PostAsync(string url, string body, CancellationToken cancellationToken) {
        for (var attempt = 0; ; attempt++) {
            var wait = _lastRequestUtc + DelayBetweenRequests - DateTime.UtcNow;
            if (wait > TimeSpan.Zero) {
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
            }
            _lastRequestUtc = DateTime.UtcNow;

            string problem;
            TimeSpan? retryAfter = null;
            HttpStatusCode? status = null;
            try {
                using var content = new StringContent(body, Encoding.UTF8, "application/json");
                using var response = await _http.PostAsync(url, content, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode) {
                    return await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                }
                status = response.StatusCode;
                problem = $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}";
                if (!IsTransient(response.StatusCode)) {
                    throw new NatureServeException(problem, status);
                }
                retryAfter = response.Headers.RetryAfter?.Delta;
            } catch (HttpRequestException ex) {
                problem = ex.Message;
            } catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested) {
                problem = "no answer within 2 minutes";
                _ = ex;
            }

            if (attempt >= RetryWaits.Length) {
                throw new NatureServeException($"{problem} (tried {attempt + 1} times)", status);
            }
            var delay = retryAfter is { } after && after > RetryWaits[attempt] && after < TimeSpan.FromMinutes(10) ? after : RetryWaits[attempt];
            _onRetry?.Invoke($"{problem}. Trying again in {delay.TotalSeconds:0} s.");
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsTransient(HttpStatusCode status) =>
        status is HttpStatusCode.TooManyRequests or HttpStatusCode.RequestTimeout || (int)status >= 500;

    public void Dispose() => _http.Dispose();
}
