using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;

// Action API client that can log in and save pages, for WikipediaPostDraftsCommand. Separate from
// WikipediaApiClient, which only reads and has no session.
//
// Login uses a bot password (Special:BotPasswords): the username is "Account@BotName" and the
// password is the one MediaWiki generated for it, never the account's own password. The session
// lives in this client's cookie container and ends when the process exits.
//
// Every save carries assert=user (fails instead of editing logged out), maxlag=5 (waits when the
// database replicas lag) and watchlist=nochange (posting 150 drafts does not fill the watchlist).

namespace BeastieBot3.Wikipedia;

internal sealed record PageRevisionInfo(string Title, bool Exists, string? Sha1, long Size);

internal enum EditStatus {
    Created,
    Updated,
    Unchanged,
    Failed,
}

internal sealed record EditOutcome(EditStatus Status, string? Message = null);

internal sealed class WikipediaEditClient : IDisposable {
    private const int MaxTitlesPerQuery = 50;
    private const int MaxLagSeconds = 5;
    private static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(60);
    // A 2 MB list with thousands of templates can take minutes to parse on save.
    private static readonly TimeSpan EditTimeout = TimeSpan.FromMinutes(5);

    private readonly HttpClient _http;
    private readonly Uri _endpoint;
    private string? _csrfToken;

    public WikipediaEditClient(WikipediaConfiguration configuration) {
        _endpoint = configuration.ActionEndpoint;
        var handler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.All,
            CookieContainer = new CookieContainer(),
            UseCookies = true,
        };
        _http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        _http.DefaultRequestHeaders.UserAgent.ParseAdd(configuration.UserAgent);
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public string? LoggedInAs { get; private set; }

    // Returns null on success, or the reason MediaWiki gave for refusing the login.
    public async Task<string?> LoginAsync(string username, string password, CancellationToken ct) {
        using var tokenDoc = await GetAsync("action=query&meta=tokens&type=login", ct).ConfigureAwait(false);
        var loginToken = tokenDoc.RootElement.GetProperty("query").GetProperty("tokens").GetProperty("logintoken").GetString();

        using var loginDoc = await PostFormAsync(new Dictionary<string, string> {
            ["action"] = "login",
            ["lgname"] = username,
            ["lgpassword"] = password,
            ["lgtoken"] = loginToken ?? string.Empty,
        }, ReadTimeout, ct).ConfigureAwait(false);
        var login = loginDoc.RootElement.GetProperty("login");
        if (login.GetProperty("result").GetString() != "Success") {
            return login.TryGetProperty("reason", out var reason) ? reason.GetString() : login.GetProperty("result").GetString();
        }
        LoggedInAs = login.GetProperty("lgusername").GetString();

        using var csrfDoc = await GetAsync("action=query&meta=tokens&type=csrf", ct).ConfigureAwait(false);
        _csrfToken = csrfDoc.RootElement.GetProperty("query").GetProperty("tokens").GetProperty("csrftoken").GetString();
        return null;
    }

    // Latest revision of each title, keyed by the title as given. Works without logging in.
    public async Task<IReadOnlyDictionary<string, PageRevisionInfo>> GetLatestRevisionsAsync(IReadOnlyList<string> titles, CancellationToken ct) {
        var result = new Dictionary<string, PageRevisionInfo>(StringComparer.Ordinal);
        for (var start = 0; start < titles.Count; start += MaxTitlesPerQuery) {
            var batch = titles.Skip(start).Take(MaxTitlesPerQuery).ToList();
            using var doc = await PostFormAsync(new Dictionary<string, string> {
                ["action"] = "query",
                ["prop"] = "revisions",
                ["rvprop"] = "sha1|size",
                ["titles"] = string.Join('|', batch),
            }, ReadTimeout, ct).ConfigureAwait(false);
            var query = doc.RootElement.GetProperty("query");

            // MediaWiki answers with its own form of each title; map it back to the one asked for.
            var askedFor = batch.ToDictionary(t => t, t => t, StringComparer.Ordinal);
            if (query.TryGetProperty("normalized", out var normalized)) {
                foreach (var n in normalized.EnumerateArray()) {
                    askedFor[n.GetProperty("to").GetString()!] = n.GetProperty("from").GetString()!;
                }
            }
            foreach (var page in query.GetProperty("pages").EnumerateArray()) {
                var title = page.GetProperty("title").GetString()!;
                var key = askedFor.TryGetValue(title, out var original) ? original : title;
                if (page.TryGetProperty("missing", out _) || !page.TryGetProperty("revisions", out var revisions)) {
                    result[key] = new PageRevisionInfo(title, false, null, 0);
                    continue;
                }
                var rev = revisions[0];
                result[key] = new PageRevisionInfo(
                    title,
                    true,
                    rev.TryGetProperty("sha1", out var sha1) ? sha1.GetString() : null,
                    rev.TryGetProperty("size", out var size) ? size.GetInt64() : 0);
            }
        }
        return result;
    }

    public async Task<EditOutcome> EditAsync(string title, string text, string summary, CancellationToken ct) {
        if (_csrfToken is null) {
            throw new InvalidOperationException("Log in before editing.");
        }

        var fields = new Dictionary<string, string> {
            ["action"] = "edit",
            ["title"] = title,
            ["text"] = text,
            ["summary"] = summary,
            ["assert"] = "user",
            ["watchlist"] = "nochange",
            ["md5"] = Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(text))).ToLowerInvariant(),
            // The token goes last, so a request cut off in transit cannot save a truncated page.
            ["token"] = _csrfToken,
        };

        JsonDocument doc;
        try {
            doc = await PostMultipartAsync(fields, EditTimeout, ct).ConfigureAwait(false);
        }
        catch (TimeoutException) {
            // The save can finish on the server after the response times out. Check before reporting
            // a failure, so the run does not post the page twice or report a saved page as failed.
            await Task.Delay(TimeSpan.FromSeconds(15), ct).ConfigureAwait(false);
            var after = await GetLatestRevisionsAsync(new[] { title }, ct).ConfigureAwait(false);
            var expected = DraftPageBuilder.Sha1Hex(DraftPageBuilder.Normalize(text));
            return after.TryGetValue(title, out var info) && info.Sha1 == expected
                ? new EditOutcome(EditStatus.Updated, "saved, but the response timed out")
                : new EditOutcome(EditStatus.Failed, $"no response after {EditTimeout.TotalMinutes:0} minutes, and the page does not have the new text");
        }

        using (doc) {
            var root = doc.RootElement;
            if (root.TryGetProperty("error", out var error)) {
                return new EditOutcome(EditStatus.Failed, DescribeError(error));
            }
            var edit = root.GetProperty("edit");
            if (edit.GetProperty("result").GetString() != "Success") {
                return new EditOutcome(EditStatus.Failed, DescribeFailure(edit));
            }
            if (edit.TryGetProperty("nochange", out _)) {
                return new EditOutcome(EditStatus.Unchanged);
            }
            return new EditOutcome(edit.TryGetProperty("new", out _) ? EditStatus.Created : EditStatus.Updated);
        }
    }

    private static string DescribeError(JsonElement error) {
        var code = error.TryGetProperty("code", out var c) ? c.GetString() : "error";
        var info = error.TryGetProperty("info", out var i) ? i.GetString() : null;
        return info is null ? code ?? "error" : $"{code}: {info}";
    }

    // A save refused by an edit filter, the spam blacklist or a captcha comes back as result=Failure.
    private static string DescribeFailure(JsonElement edit) {
        if (edit.TryGetProperty("code", out var code)) {
            var info = edit.TryGetProperty("info", out var i) ? i.GetString() : null;
            return info is null ? code.GetString() ?? "Failure" : $"{code.GetString()}: {info}";
        }
        if (edit.TryGetProperty("captcha", out _)) {
            return "captcha required";
        }
        if (edit.TryGetProperty("spamblacklist", out var spam)) {
            return $"spam blacklist: {spam.GetString()}";
        }
        return edit.GetRawText();
    }

    private Task<JsonDocument> GetAsync(string query, CancellationToken ct) =>
        SendAsync(() => new HttpRequestMessage(HttpMethod.Get, $"{_endpoint}?{query}&format=json&formatversion=2"), ReadTimeout, ct);

    private Task<JsonDocument> PostFormAsync(Dictionary<string, string> fields, TimeSpan timeout, CancellationToken ct) =>
        SendAsync(() => new HttpRequestMessage(HttpMethod.Post, _endpoint) {
            Content = new FormUrlEncodedContent(WithFormat(fields)),
        }, timeout, ct);

    // Multipart, as the API documentation recommends for large text: no 3x size from URL encoding.
    private Task<JsonDocument> PostMultipartAsync(Dictionary<string, string> fields, TimeSpan timeout, CancellationToken ct) =>
        SendAsync(() => {
            var content = new MultipartFormDataContent();
            foreach (var (name, value) in WithFormat(fields)) {
                content.Add(new StringContent(value, Encoding.UTF8), name);
            }
            return new HttpRequestMessage(HttpMethod.Post, _endpoint) { Content = content };
        }, timeout, ct);

    // Format and maxlag go before the caller's fields so the token stays last.
    private static IEnumerable<KeyValuePair<string, string>> WithFormat(Dictionary<string, string> fields) {
        yield return new("format", "json");
        yield return new("formatversion", "2");
        yield return new("maxlag", MaxLagSeconds.ToString());
        foreach (var field in fields) {
            yield return field;
        }
    }

    // Retries replica lag (error code maxlag), rate limiting and 5xx responses. A request that runs
    // past its timeout throws TimeoutException and is not retried here: for an edit, the caller has
    // to find out whether it was saved first.
    private async Task<JsonDocument> SendAsync(Func<HttpRequestMessage> requestFactory, TimeSpan timeout, CancellationToken ct) {
        const int maxAttempts = 8;
        var delay = TimeSpan.FromSeconds(5);
        for (var attempt = 1; ; attempt++) {
            using var request = requestFactory();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(timeout);
            HttpResponseMessage response;
            string payload;
            try {
                response = await _http.SendAsync(request, timeoutCts.Token).ConfigureAwait(false);
                payload = await response.Content.ReadAsStringAsync(timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) {
                throw new TimeoutException($"No response from {_endpoint.Host} within {timeout.TotalSeconds:0} seconds.");
            }

            using (response) {
                var retryAfter = response.Headers.RetryAfter?.Delta;
                if ((int)response.StatusCode == 429 || (int)response.StatusCode >= 500) {
                    if (attempt >= maxAttempts) {
                        throw new HttpRequestException($"{(int)response.StatusCode} {response.ReasonPhrase} from {_endpoint.Host} after {attempt} attempts.");
                    }
                    await Task.Delay(retryAfter ?? delay, ct).ConfigureAwait(false);
                    delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 120));
                    continue;
                }
                response.EnsureSuccessStatusCode();

                var doc = JsonDocument.Parse(payload);
                if (doc.RootElement.TryGetProperty("error", out var error)
                    && error.TryGetProperty("code", out var code)
                    && code.GetString() is "maxlag" or "ratelimited"
                    && attempt < maxAttempts) {
                    doc.Dispose();
                    var wait = code.GetString() == "ratelimited" ? TimeSpan.FromSeconds(60) : retryAfter ?? delay;
                    await Task.Delay(wait, ct).ConfigureAwait(false);
                    delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 120));
                    continue;
                }
                return doc;
            }
        }
    }

    public void Dispose() => _http.Dispose();
}
