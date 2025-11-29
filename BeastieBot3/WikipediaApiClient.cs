using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace BeastieBot3;

internal sealed class WikipediaApiClient : IDisposable {
    private readonly HttpClient _actionClient;
    private readonly HttpClient _restClient;
    private readonly WikipediaConfiguration _configuration;
    private readonly SemaphoreSlim _actionSemaphore = new(1, 1);
    private readonly SemaphoreSlim _restSemaphore = new(1, 1);
    private DateTime _nextActionAllowed = DateTime.MinValue;
    private DateTime _nextRestAllowed = DateTime.MinValue;

    public WikipediaApiClient(WikipediaConfiguration configuration) {
        _configuration = configuration;

        var actionHandler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };
        _actionClient = new HttpClient(actionHandler) {
            BaseAddress = configuration.ActionEndpoint,
            Timeout = configuration.Timeout
        };
        ConfigureDefaultHeaders(_actionClient.DefaultRequestHeaders, configuration.UserAgent, "application/json");

        var restHandler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };
        _restClient = new HttpClient(restHandler) {
            BaseAddress = configuration.RestEndpoint,
            Timeout = configuration.Timeout
        };
        ConfigureDefaultHeaders(_restClient.DefaultRequestHeaders, configuration.UserAgent, "text/html");
    }

    public async Task<WikipediaQueryResult> QueryPageAsync(string title, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(title)) {
            throw new ArgumentException("Title must be provided", nameof(title));
        }

        await _actionSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            _nextActionAllowed = await EnforceRateLimitAsync(_nextActionAllowed, _configuration.ActionDelay, cancellationToken).ConfigureAwait(false);
            var response = await SendActionRequestAsync(title, cancellationToken).ConfigureAwait(false);
            return ParseQueryResponse(title, response);
        }
        finally {
            _actionSemaphore.Release();
        }
    }

    public async Task<WikipediaMobileHtmlResult> GetMobileHtmlAsync(string canonicalTitle, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(canonicalTitle)) {
            throw new ArgumentException("Title must be provided", nameof(canonicalTitle));
        }

        await _restSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            _nextRestAllowed = await EnforceRateLimitAsync(_nextRestAllowed, _configuration.RestDelay, cancellationToken).ConfigureAwait(false);
            var slug = WikipediaTitleHelper.ToSlug(canonicalTitle);
            var relative = $"page/mobile-html/{slug}";
            var response = await SendWithRetryAsync(
                _restClient,
                () => new HttpRequestMessage(HttpMethod.Get, relative),
                cancellationToken).ConfigureAwait(false);
            return new WikipediaMobileHtmlResult(canonicalTitle, response.Body, response.StatusCode, response.PayloadBytes);
        }
        finally {
            _restSemaphore.Release();
        }
    }

    private async Task<WikipediaHttpResponse> SendActionRequestAsync(string title, CancellationToken cancellationToken) {
        var now = DateTime.UtcNow;
        var requestFactory = () => new HttpRequestMessage(HttpMethod.Post, string.Empty) {
            Content = new FormUrlEncodedContent(new Dictionary<string, string> {
                ["action"] = "query",
                ["format"] = "json",
                ["formatversion"] = "2",
                ["redirects"] = "1",
                ["prop"] = "info|pageprops|categories|revisions",
                ["inprop"] = "displaytitle",
                ["ppprop"] = "disambiguation|setindex",
                ["cllimit"] = "max",
                ["rvslots"] = "main",
                ["rvprop"] = "ids|timestamp|content",
                ["rvlimit"] = "1",
                ["titles"] = title
            })
        };

        return await SendWithRetryAsync(_actionClient, requestFactory, cancellationToken).ConfigureAwait(false);
    }

    private async Task<WikipediaHttpResponse> SendWithRetryAsync(HttpClient client, Func<HttpRequestMessage> requestFactory, CancellationToken cancellationToken) {
        const int maxAttempts = 5;
        var attempt = 0;
        var delay = TimeSpan.FromMilliseconds(500);
        while (true) {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;
            using var request = requestFactory();
            try {
                var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
                var payload = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                var payloadBytes = response.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(payload);

                if (response.IsSuccessStatusCode) {
                    return new WikipediaHttpResponse(request.RequestUri?.ToString() ?? string.Empty, payload, response.StatusCode, payloadBytes);
                }

                if (!ShouldRetry(response.StatusCode, attempt)) {
                    throw new WikipediaApiException(request.RequestUri?.ToString() ?? string.Empty, response.StatusCode, payload, attempt);
                }

                delay = await DelayAfterFailureAsync(response, delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) {
                throw;
            }
            catch (WikipediaApiException) {
                throw;
            }
            catch (Exception ex) when (attempt < maxAttempts) {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = NextDelay(delay);
                if (attempt >= maxAttempts) {
                    throw new WikipediaApiException(client.BaseAddress?.ToString() ?? "wikipedia", null, ex.Message, attempt, ex);
                }
            }
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

    private static async Task<TimeSpan> DelayAfterFailureAsync(HttpResponseMessage response, TimeSpan currentDelay, CancellationToken cancellationToken) {
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

    private static TimeSpan NextDelay(TimeSpan current) {
        var doubled = TimeSpan.FromMilliseconds(current.TotalMilliseconds * 2);
        var cap = TimeSpan.FromSeconds(30);
        return doubled <= cap ? doubled : cap;
    }

    private static void ConfigureDefaultHeaders(HttpRequestHeaders headers, string userAgent, string accept) {
        headers.Accept.Clear();
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
        headers.UserAgent.ParseAdd(userAgent);
    }

    private static WikipediaQueryResult ParseQueryResponse(string requestedTitle, WikipediaHttpResponse response) {
        using var document = JsonDocument.Parse(response.Body);
        if (!document.RootElement.TryGetProperty("query", out var query)) {
            throw new WikipediaApiException(response.Url, response.StatusCode, "Missing query object", 1);
        }

        var redirects = ParseRedirects(query);
        var normalized = ParseNormalizations(query);
        var pages = query.TryGetProperty("pages", out var pagesElement) ? pagesElement : default;
        if (pages.ValueKind != JsonValueKind.Array || pages.GetArrayLength() == 0) {
            return WikipediaQueryResult.Missing(requestedTitle, normalized, redirects, "missing", null, response.StatusCode, response.PayloadBytes);
        }

        var page = pages[0];
        if (page.TryGetProperty("missing", out _)) {
            var missingReason = page.TryGetProperty("missingreason", out var reasonElement) ? reasonElement.GetString() : null;
            return WikipediaQueryResult.Missing(requestedTitle, normalized, redirects, "missing", missingReason, response.StatusCode, response.PayloadBytes);
        }

        var canonicalTitle = page.TryGetProperty("title", out var titleElement) ? titleElement.GetString() ?? requestedTitle : requestedTitle;
        var displayTitle = page.TryGetProperty("pageprops", out var propsElement)
            && propsElement.TryGetProperty("displaytitle", out var displayElement)
                ? displayElement.GetString() ?? canonicalTitle
                : canonicalTitle;

        var categories = ParseCategories(page);
        var wikitext = ParseWikitext(page);
        var pageProps = page.TryGetProperty("pageprops", out var ppElement) ? ppElement : default;
        var isDisambiguation = HasPageProp(pageProps, "disambiguation") || ContainsCategory(categories, "disambiguation pages");
        var isSetIndex = HasPageProp(pageProps, "setindex") || ContainsCategory(categories, "set index articles");
        var pageId = page.TryGetProperty("pageid", out var pageIdElement) ? pageIdElement.GetInt64() : (long?)null;
        var revid = page.TryGetProperty("revisions", out var revisionsElement) && revisionsElement.ValueKind == JsonValueKind.Array && revisionsElement.GetArrayLength() > 0
            ? revisionsElement[0].TryGetProperty("revid", out var revidElement) ? revidElement.GetInt64() : (long?)null
            : (long?)null;

        return WikipediaQueryResult.Found(
            requestedTitle,
            canonicalTitle,
            displayTitle,
            normalized,
            redirects,
            categories,
            wikitext,
            isDisambiguation,
            isSetIndex,
            pageId,
            revid,
            response.StatusCode,
            response.PayloadBytes);
    }

    private static IReadOnlyList<WikipediaRedirectStep> ParseRedirects(JsonElement query) {
        if (!query.TryGetProperty("redirects", out var redirectsElement) || redirectsElement.ValueKind != JsonValueKind.Array) {
            return Array.Empty<WikipediaRedirectStep>();
        }

        var list = new List<WikipediaRedirectStep>();
        foreach (var redirect in redirectsElement.EnumerateArray()) {
            var from = redirect.TryGetProperty("from", out var fromElement) ? fromElement.GetString() : null;
            var to = redirect.TryGetProperty("to", out var toElement) ? toElement.GetString() : null;
            if (string.IsNullOrWhiteSpace(from) || string.IsNullOrWhiteSpace(to)) {
                continue;
            }

            list.Add(new WikipediaRedirectStep(from, to));
        }

        return list;
    }

    private static IReadOnlyDictionary<string, string> ParseNormalizations(JsonElement query) {
        if (!query.TryGetProperty("normalized", out var normalizedElement) || normalizedElement.ValueKind != JsonValueKind.Array) {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in normalizedElement.EnumerateArray()) {
            var from = item.TryGetProperty("from", out var fromElement) ? fromElement.GetString() : null;
            var to = item.TryGetProperty("to", out var toElement) ? toElement.GetString() : null;
            if (string.IsNullOrWhiteSpace(from) || string.IsNullOrWhiteSpace(to)) {
                continue;
            }

            map[from] = to;
        }

        return map;
    }

    private static IReadOnlyList<string> ParseCategories(JsonElement page) {
        if (!page.TryGetProperty("categories", out var categoriesElement) || categoriesElement.ValueKind != JsonValueKind.Array) {
            return Array.Empty<string>();
        }

        var list = new List<string>();
        foreach (var category in categoriesElement.EnumerateArray()) {
            var title = category.TryGetProperty("title", out var titleElement) ? titleElement.GetString() : null;
            if (string.IsNullOrWhiteSpace(title)) {
                continue;
            }

            const string prefix = "Category:";
            if (title.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) {
                title = title[prefix.Length..];
            }

            list.Add(title);
        }

        return list;
    }

    private static string? ParseWikitext(JsonElement page) {
        if (!page.TryGetProperty("revisions", out var revisions) || revisions.ValueKind != JsonValueKind.Array || revisions.GetArrayLength() == 0) {
            return null;
        }

        var revision = revisions[0];
        if (!revision.TryGetProperty("slots", out var slots) || slots.ValueKind != JsonValueKind.Object) {
            return null;
        }

        if (!slots.TryGetProperty("main", out var mainSlot) || mainSlot.ValueKind != JsonValueKind.Object) {
            return null;
        }

        if (!mainSlot.TryGetProperty("content", out var contentElement)) {
            return null;
        }

        return contentElement.GetString();
    }

    private static bool HasPageProp(JsonElement props, string name) {
        if (props.ValueKind != JsonValueKind.Object) {
            return false;
        }

        return props.TryGetProperty(name, out _);
    }

    private static bool ContainsCategory(IReadOnlyList<string> categories, string value) {
        foreach (var category in categories) {
            if (category.Equals(value, StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }

        return false;
    }

    private static async Task<DateTime> EnforceRateLimitAsync(DateTime nextAllowed, TimeSpan delay, CancellationToken cancellationToken) {
        var now = DateTime.UtcNow;
        if (nextAllowed > now) {
            var wait = nextAllowed - now;
            if (wait > TimeSpan.Zero) {
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
            }
        }

        return DateTime.UtcNow + delay;
    }

    public void Dispose() {
        _actionSemaphore.Dispose();
        _restSemaphore.Dispose();
        _actionClient.Dispose();
        _restClient.Dispose();
    }
}

internal sealed record WikipediaHttpResponse(string Url, string Body, HttpStatusCode StatusCode, long PayloadBytes);

internal sealed record WikipediaMobileHtmlResult(string Title, string Html, HttpStatusCode StatusCode, long PayloadBytes);

internal sealed record WikipediaRedirectStep(string FromTitle, string ToTitle);

internal sealed class WikipediaApiException : Exception {
    public WikipediaApiException(string url, HttpStatusCode? statusCode, string responseBody, int attempt, Exception? inner = null)
        : base($"Wikipedia request to {url} failed with status {(int?)statusCode ?? 0} on attempt {attempt}" + (string.IsNullOrWhiteSpace(responseBody) ? string.Empty : $" Body: {responseBody}"), inner) {
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

internal sealed record WikipediaQueryResult(
    bool Exists,
    string RequestedTitle,
    string? CanonicalTitle,
    string? DisplayTitle,
    IReadOnlyDictionary<string, string> NormalizedTitles,
    IReadOnlyList<WikipediaRedirectStep> Redirects,
    IReadOnlyList<string> Categories,
    string? Wikitext,
    bool IsDisambiguation,
    bool IsSetIndex,
    long? PageId,
    long? RevisionId,
    HttpStatusCode StatusCode,
    long PayloadBytes,
    string? MissingReason
) {
    public static WikipediaQueryResult Missing(
        string requestedTitle,
        IReadOnlyDictionary<string, string> normalizedTitles,
        IReadOnlyList<WikipediaRedirectStep> redirects,
        string reason,
        string? detail,
        HttpStatusCode statusCode,
        long payloadBytes) => new(
            Exists: false,
            RequestedTitle: requestedTitle,
            CanonicalTitle: null,
            DisplayTitle: null,
            NormalizedTitles: normalizedTitles,
            Redirects: redirects,
            Categories: Array.Empty<string>(),
            Wikitext: null,
            IsDisambiguation: false,
            IsSetIndex: false,
            PageId: null,
            RevisionId: null,
            StatusCode: statusCode,
            PayloadBytes: payloadBytes,
            MissingReason: string.IsNullOrWhiteSpace(detail) ? reason : detail);

    public static WikipediaQueryResult Found(
        string requestedTitle,
        string canonicalTitle,
        string displayTitle,
        IReadOnlyDictionary<string, string> normalizedTitles,
        IReadOnlyList<WikipediaRedirectStep> redirects,
        IReadOnlyList<string> categories,
        string? wikitext,
        bool isDisambiguation,
        bool isSetIndex,
        long? pageId,
        long? revisionId,
        HttpStatusCode statusCode,
        long payloadBytes) => new(
            Exists: true,
            RequestedTitle: requestedTitle,
            CanonicalTitle: canonicalTitle,
            DisplayTitle: displayTitle,
            NormalizedTitles: normalizedTitles,
            Redirects: redirects,
            Categories: categories,
            Wikitext: wikitext,
            IsDisambiguation: isDisambiguation,
            IsSetIndex: isSetIndex,
            PageId: pageId,
            RevisionId: revisionId,
            StatusCode: statusCode,
            PayloadBytes: payloadBytes,
            MissingReason: null);
}
