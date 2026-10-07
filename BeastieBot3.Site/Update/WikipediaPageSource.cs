using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Update;

/// The wikitext of an English Wikipedia page, for the status update page (/update?page=Title).
/// Title: the page's own title (after any redirect).
public sealed record WikipediaPageText(string Title, string Text, long RevisionId, DateTimeOffset? Timestamp);

public enum WikipediaPageError {
    /// The site has no Wikipedia user agent (SiteOptions.WikipediaUserAgent), so it does not ask.
    NotConfigured,
    /// English Wikipedia has no such page or revision.
    NotFound,
    /// The page's wikitext is larger than the status update page takes (UpdateModel.MaxTextBytes).
    TooLarge,
    /// Wikipedia did not answer, or answered with an error.
    Failed,
    /// The site has loaded as many pages as it may this minute (RateLimitOptions.WikipediaLoadsPerMinute).
    Busy,
}

public sealed record WikipediaPageResult(WikipediaPageText? Page, WikipediaPageError? Error) {
    public static WikipediaPageResult Of(WikipediaPageText page) => new(page, null);
    public static WikipediaPageResult Failure(WikipediaPageError error) => new(null, error);
}

public interface IWikipediaPageSource {
    /// The page's current wikitext, or that of revisionId when it is given.
    Task<WikipediaPageResult> GetAsync(string title, long? revisionId, CancellationToken cancellationToken);
}

/// Reads a page from English Wikipedia's action API (revisions, main slot, redirects followed), with
/// the User-Agent that Wikimedia's policy asks for. Answers are kept for a few minutes, so a page
/// checked again with other options is not downloaded again.
public sealed class WikipediaPageSource : IWikipediaPageSource {
    public const string ApiUrl = "https://en.wikipedia.org/w/api.php";
    private static readonly TimeSpan Keep = TimeSpan.FromMinutes(5);

    private readonly HttpClient _http;
    private readonly string? _userAgent;
    private readonly int _maxTextBytes;
    private readonly MemoryCache _cache = new(new MemoryCacheOptions { SizeLimit = 64L * 1024 * 1024 });
    // Loads from Wikipedia per minute, all clients together; null: no limit.
    private readonly System.Threading.RateLimiting.RateLimiter? _loads;

    public WikipediaPageSource(HttpClient http, IOptions<SiteOptions> options)
        : this(http, options.Value.WikipediaUserAgent, Pages.UpdateModel.MaxTextBytes, options.Value.RateLimits.WikipediaLoadsPerMinute) {
    }

    internal WikipediaPageSource(HttpClient http, string? userAgent, int maxTextBytes, int loadsPerMinute = 0) {
        _loads = loadsPerMinute > 0
            ? new System.Threading.RateLimiting.SlidingWindowRateLimiter(new System.Threading.RateLimiting.SlidingWindowRateLimiterOptions {
                PermitLimit = loadsPerMinute, Window = TimeSpan.FromMinutes(1), SegmentsPerWindow = 6, QueueLimit = 0,
            })
            : null;
        _http = http;
        _http.Timeout = TimeSpan.FromSeconds(15);
        _http.MaxResponseContentBufferSize = 4L * maxTextBytes + 1024 * 1024;
        _userAgent = string.IsNullOrWhiteSpace(userAgent) ? null : userAgent.Trim();
        _maxTextBytes = maxTextBytes;
    }

    public async Task<WikipediaPageResult> GetAsync(string title, long? revisionId, CancellationToken cancellationToken) {
        if (_userAgent is null) {
            return WikipediaPageResult.Failure(WikipediaPageError.NotConfigured);
        }
        var key = revisionId is { } rev ? "rev|" + rev : "title|" + title;
        if (_cache.TryGetValue(key, out WikipediaPageResult? kept) && kept is not null) {
            return kept;
        }
        using var permit = _loads?.AttemptAcquire();
        if (permit is { IsAcquired: false }) {
            return WikipediaPageResult.Failure(WikipediaPageError.Busy);
        }
        var parameters = new Dictionary<string, string> {
            ["action"] = "query",
            ["format"] = "json",
            ["formatversion"] = "2",
            ["prop"] = "revisions",
            ["rvprop"] = "ids|timestamp|content",
            ["rvslots"] = "main",
            ["redirects"] = "1",
        };
        if (revisionId is { } r) {
            parameters["revids"] = r.ToString(System.Globalization.CultureInfo.InvariantCulture);
        } else {
            parameters["titles"] = title;
        }
        var url = ApiUrl + "?" + string.Join('&', parameters.Select(p => $"{p.Key}={Uri.EscapeDataString(p.Value)}"));
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("User-Agent", _userAgent);
        string body;
        try {
            using var response = await _http.SendAsync(request, cancellationToken);
            if (response.StatusCode != HttpStatusCode.OK) {
                return WikipediaPageResult.Failure(WikipediaPageError.Failed);
            }
            body = await response.Content.ReadAsStringAsync(cancellationToken);
        } catch (Exception e) when (e is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
            return WikipediaPageResult.Failure(WikipediaPageError.Failed);
        }
        var result = Read(body, _maxTextBytes);
        if (result.Error is null or WikipediaPageError.NotFound) {
            _cache.Set(key, result, new MemoryCacheEntryOptions {
                AbsoluteExpirationRelativeToNow = Keep,
                Size = 1 + (result.Page?.Text.Length ?? 0) * 2L,
            });
        }
        return result;
    }

    /// Reads an action API answer (formatversion=2): the page, or why there is none.
    internal static WikipediaPageResult Read(string body, int maxTextBytes) {
        try {
            using var json = JsonDocument.Parse(body);
            var root = json.RootElement;
            if (root.TryGetProperty("error", out _)) {
                return WikipediaPageResult.Failure(WikipediaPageError.Failed);
            }
            if (!root.TryGetProperty("query", out var query)) {
                return WikipediaPageResult.Failure(WikipediaPageError.Failed);
            }
            if (query.TryGetProperty("badrevids", out _)) {
                return WikipediaPageResult.Failure(WikipediaPageError.NotFound);
            }
            if (!query.TryGetProperty("pages", out var pages) || pages.GetArrayLength() == 0) {
                return WikipediaPageResult.Failure(WikipediaPageError.NotFound);
            }
            var page = pages[0];
            if (page.TryGetProperty("missing", out _) || page.TryGetProperty("invalid", out _)
                || !page.TryGetProperty("revisions", out var revisions) || revisions.GetArrayLength() == 0) {
                return WikipediaPageResult.Failure(WikipediaPageError.NotFound);
            }
            var revision = revisions[0];
            var text = revision.GetProperty("slots").GetProperty("main").GetProperty("content").GetString() ?? string.Empty;
            if (Encoding.UTF8.GetByteCount(text) > maxTextBytes) {
                return WikipediaPageResult.Failure(WikipediaPageError.TooLarge);
            }
            DateTimeOffset? timestamp = revision.TryGetProperty("timestamp", out var ts) && ts.TryGetDateTimeOffset(out var t) ? t : null;
            return WikipediaPageResult.Of(new WikipediaPageText(page.GetProperty("title").GetString() ?? string.Empty, text,
                revision.GetProperty("revid").GetInt64(), timestamp));
        } catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException) {
            return WikipediaPageResult.Failure(WikipediaPageError.Failed);
        }
    }
}
