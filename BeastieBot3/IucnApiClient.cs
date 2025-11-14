using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BeastieBot3;

internal sealed class IucnApiClient : IDisposable {
    private readonly HttpClient _httpClient;
    private readonly SemaphoreSlim _semaphore;
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;

    public IucnApiClient(IucnApiConfiguration configuration) {
        var handler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
            MaxConnectionsPerServer = configuration.MaxConcurrency
        };

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
    }

    public Task<IucnApiResponse> GetTaxaSisAsync(long sisId, CancellationToken cancellationToken) =>
        SendAsync($"/api/v4/taxa/sis/{sisId}", cancellationToken);

    public Task<IucnApiResponse> GetAssessmentAsync(long assessmentId, CancellationToken cancellationToken) =>
        SendAsync($"/api/v4/assessment/{assessmentId}", cancellationToken);

    private async Task<IucnApiResponse> SendAsync(string relativeUrl, CancellationToken cancellationToken) {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            var attempt = 0;
            var delay = _initialDelay;
            var url = relativeUrl.StartsWith("/", StringComparison.Ordinal) ? relativeUrl : "/" + relativeUrl;

            while (true) {
                cancellationToken.ThrowIfCancellationRequested();
                attempt++;

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                try {
                    var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    var payload = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode) {
                        return new IucnApiResponse(url, payload, response.StatusCode, response.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(payload));
                    }

                    if (!ShouldRetry(response.StatusCode, attempt)) {
                        throw new IucnApiException(url, response.StatusCode, payload, attempt);
                    }

                    delay = await DelayWithRetryAfterAsync(response, delay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) {
                    throw;
                }
                catch (IucnApiException) {
                    throw;
                }
                catch (Exception ex) when (attempt < 5) {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = NextDelay(delay);
                    if (attempt >= 5) {
                        throw new IucnApiException(url, null, ex.Message, attempt, ex);
                    }
                }
            }
        }
        finally {
            _semaphore.Release();
        }
    }

    private static bool ShouldRetry(HttpStatusCode statusCode, int attempt) {
        if (attempt >= 5) {
            return false;
        }

        return statusCode is HttpStatusCode.TooManyRequests
            or HttpStatusCode.RequestTimeout
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout
            or HttpStatusCode.InternalServerError;
    }

    private async Task<TimeSpan> DelayWithRetryAfterAsync(HttpResponseMessage response, TimeSpan currentDelay, CancellationToken cancellationToken) {
        if (response.Headers.RetryAfter is { } retryAfter) {
            if (retryAfter.Date.HasValue) {
                var wait = retryAfter.Date.Value - DateTimeOffset.UtcNow;
                if (wait > TimeSpan.Zero) {
                    await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                    return currentDelay;
                }
            }
            else if (retryAfter.Delta.HasValue) {
                await Task.Delay(retryAfter.Delta.Value, cancellationToken).ConfigureAwait(false);
                return currentDelay;
            }
        }

        await Task.Delay(currentDelay, cancellationToken).ConfigureAwait(false);
        return NextDelay(currentDelay);
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
