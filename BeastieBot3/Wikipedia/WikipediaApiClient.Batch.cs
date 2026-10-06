using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

// Batched action API queries, up to 50 titles per request, for `wikipedia fetch-group-titles`:
//   QueryPagesAsync            the same page data as QueryPageAsync (wikitext, categories,
//                              disambiguation flags, redirects followed), for each title.
//   QueryIncomingRedirectsAsync the redirects in the article namespace that point at each title
//                              (prop=redirects), with the section each one points at.
// MediaWiki splits a large answer over several responses ("continue"); every response is merged
// into one result per title by WikipediaBatchResponse, which is pure so tests can feed it JSON.

namespace BeastieBot3.Wikipedia;

internal sealed partial class WikipediaApiClient {
    /// The most titles one action API request may name (for a client without the apihighlimits right).
    public const int MaxTitlesPerRequest = 50;

    // A response split into more parts than this is a loop, not a large answer.
    private const int MaxContinuations = 200;

    /// <summary>
    /// Page data for each of <paramref name="titles"/> (at most <see cref="MaxTitlesPerRequest"/>),
    /// keyed by the title as given. A title with no page, or one MediaWiki cannot have, gets a
    /// result with <c>Exists = false</c>.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, WikipediaQueryResult>> QueryPagesAsync(IReadOnlyList<string> titles,
        CancellationToken cancellationToken) {
        var parameters = new Dictionary<string, string> {
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
        };
        var (batch, status, bytes) = await RunBatchAsync(titles, parameters, cancellationToken).ConfigureAwait(false);
        return batch.PageResults(titles, status, bytes);
    }

    /// <summary>
    /// The redirects in the article namespace that point at each of <paramref name="titles"/> (at
    /// most <see cref="MaxTitlesPerRequest"/>), keyed by the title as given. A title that is itself a
    /// redirect is followed first, and the result names the page it reached.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, WikipediaIncomingRedirects>> QueryIncomingRedirectsAsync(IReadOnlyList<string> titles,
        CancellationToken cancellationToken) {
        var parameters = new Dictionary<string, string> {
            ["action"] = "query",
            ["format"] = "json",
            ["formatversion"] = "2",
            ["redirects"] = "1",
            ["prop"] = "redirects",
            ["rdprop"] = "title|fragment",
            ["rdnamespace"] = "0",
            ["rdlimit"] = "max",
        };
        var (batch, _, _) = await RunBatchAsync(titles, parameters, cancellationToken).ConfigureAwait(false);
        return batch.IncomingRedirects(titles);
    }

    private async Task<(WikipediaBatchResponse Batch, HttpStatusCode Status, long Bytes)> RunBatchAsync(IReadOnlyList<string> titles,
        Dictionary<string, string> parameters, CancellationToken cancellationToken) {
        if (titles.Count == 0 || titles.Count > MaxTitlesPerRequest) {
            throw new ArgumentException($"Give between 1 and {MaxTitlesPerRequest} titles.", nameof(titles));
        }
        parameters["titles"] = string.Join('|', titles);
        var batch = new WikipediaBatchResponse();
        var status = HttpStatusCode.OK;
        long bytes = 0;
        IReadOnlyDictionary<string, string> continuation = new Dictionary<string, string>();
        for (var part = 0; part < MaxContinuations; part++) {
            var form = new Dictionary<string, string>(parameters);
            foreach (var (key, value) in continuation) {
                form[key] = value;
            }
            WikipediaHttpResponse response;
            await _actionSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                _nextActionAllowed = await EnforceRateLimitAsync(_nextActionAllowed, _configuration.ActionDelay, cancellationToken).ConfigureAwait(false);
                response = await SendWithRetryAsync(_actionClient,
                    () => new HttpRequestMessage(HttpMethod.Post, string.Empty) { Content = new FormUrlEncodedContent(form) },
                    cancellationToken).ConfigureAwait(false);
            }
            finally {
                _actionSemaphore.Release();
            }
            status = response.StatusCode;
            bytes += response.PayloadBytes;
            using var document = JsonDocument.Parse(response.Body);
            if (document.RootElement.TryGetProperty("error", out var error)) {
                throw new WikipediaApiException(response.Url, response.StatusCode, error.GetRawText(), 1);
            }
            continuation = batch.Add(document.RootElement);
            if (continuation.Count == 0) {
                return (batch, status, bytes);
            }
        }
        throw new WikipediaApiException("wikipedia batch", status, $"The answer was still continuing after {MaxContinuations} requests.", MaxContinuations);
    }
}

/// The redirects that point at a page: Target is the page the requested title reached.
internal sealed record WikipediaIncomingRedirects(string RequestedTitle, string? Target, bool Exists, IReadOnlyList<WikipediaIncomingRedirect> Redirects);

/// A redirect to a page; Fragment is the section it points at, when it points at one.
internal sealed record WikipediaIncomingRedirect(string Title, string? Fragment);

/// <summary>
/// The parts of one batched action API answer, merged by page title. Each <see cref="Add"/> takes
/// one response and returns its "continue" values (empty when the answer is complete).
/// </summary>
internal sealed class WikipediaBatchResponse {
    private sealed class Page {
        public required string Title { get; init; }
        public bool Missing { get; set; }
        public string? MissingReason { get; set; }
        public bool Invalid { get; set; }
        public string? InvalidReason { get; set; }
        public long? PageId { get; set; }
        public long? RevisionId { get; set; }
        public string? Wikitext { get; set; }
        public string? DisplayTitle { get; set; }
        public HashSet<string> PageProps { get; } = new(StringComparer.Ordinal);
        public List<string> Categories { get; } = new();
        public HashSet<string> CategorySet { get; } = new(StringComparer.Ordinal);
        public List<WikipediaIncomingRedirect> Redirects { get; } = new();
        public HashSet<string> RedirectSet { get; } = new(StringComparer.Ordinal);
    }

    private readonly Dictionary<string, Page> _pages = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _normalized = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _redirects = new(StringComparer.Ordinal);

    public IReadOnlyDictionary<string, string> Add(JsonElement root) {
        if (root.TryGetProperty("query", out var query)) {
            ReadPairs(query, "normalized", _normalized);
            ReadPairs(query, "redirects", _redirects);
            if (query.TryGetProperty("pages", out var pages) && pages.ValueKind == JsonValueKind.Array) {
                foreach (var element in pages.EnumerateArray()) {
                    AddPage(element);
                }
            }
        }
        var continuation = new Dictionary<string, string>(StringComparer.Ordinal);
        if (root.TryGetProperty("continue", out var next) && next.ValueKind == JsonValueKind.Object) {
            foreach (var property in next.EnumerateObject()) {
                continuation[property.Name] = property.Value.ValueKind == JsonValueKind.String
                    ? property.Value.GetString() ?? string.Empty
                    : property.Value.GetRawText();
            }
        }
        return continuation;
    }

    private static void ReadPairs(JsonElement query, string name, Dictionary<string, string> into) {
        if (!query.TryGetProperty(name, out var list) || list.ValueKind != JsonValueKind.Array) {
            return;
        }
        foreach (var item in list.EnumerateArray()) {
            var from = item.TryGetProperty("from", out var f) ? f.GetString() : null;
            var to = item.TryGetProperty("to", out var t) ? t.GetString() : null;
            if (!string.IsNullOrWhiteSpace(from) && !string.IsNullOrWhiteSpace(to)) {
                into[from] = to;
            }
        }
    }

    private void AddPage(JsonElement element) {
        var title = element.TryGetProperty("title", out var t) ? t.GetString() : null;
        if (string.IsNullOrEmpty(title)) {
            return;
        }
        if (!_pages.TryGetValue(title, out var page)) {
            _pages[title] = page = new Page { Title = title };
        }
        if (element.TryGetProperty("missing", out _)) {
            page.Missing = true;
            page.MissingReason ??= element.TryGetProperty("missingreason", out var r) ? r.GetString() : null;
        }
        if (element.TryGetProperty("invalid", out _)) {
            page.Invalid = true;
            page.InvalidReason ??= element.TryGetProperty("invalidreason", out var r) ? r.GetString() : null;
        }
        if (element.TryGetProperty("pageid", out var id) && id.ValueKind == JsonValueKind.Number) {
            page.PageId = id.GetInt64();
        }
        if (element.TryGetProperty("pageprops", out var props) && props.ValueKind == JsonValueKind.Object) {
            foreach (var property in props.EnumerateObject()) {
                page.PageProps.Add(property.Name);
                if (property.Name == "displaytitle" && property.Value.ValueKind == JsonValueKind.String) {
                    page.DisplayTitle = property.Value.GetString();
                }
            }
        }
        if (element.TryGetProperty("categories", out var categories) && categories.ValueKind == JsonValueKind.Array) {
            foreach (var category in categories.EnumerateArray()) {
                var name = category.TryGetProperty("title", out var c) ? c.GetString() : null;
                if (string.IsNullOrWhiteSpace(name)) {
                    continue;
                }
                const string prefix = "Category:";
                if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) {
                    name = name[prefix.Length..];
                }
                if (page.CategorySet.Add(name)) {
                    page.Categories.Add(name);
                }
            }
        }
        if (element.TryGetProperty("revisions", out var revisions) && revisions.ValueKind == JsonValueKind.Array
            && revisions.GetArrayLength() > 0) {
            var revision = revisions[0];
            if (revision.TryGetProperty("revid", out var revid) && revid.ValueKind == JsonValueKind.Number) {
                page.RevisionId = revid.GetInt64();
            }
            if (revision.TryGetProperty("slots", out var slots) && slots.TryGetProperty("main", out var main)
                && main.TryGetProperty("content", out var content) && content.ValueKind == JsonValueKind.String) {
                page.Wikitext = content.GetString();
            }
        }
        if (element.TryGetProperty("redirects", out var incoming) && incoming.ValueKind == JsonValueKind.Array) {
            foreach (var redirect in incoming.EnumerateArray()) {
                var name = redirect.TryGetProperty("title", out var r) ? r.GetString() : null;
                if (string.IsNullOrWhiteSpace(name) || !page.RedirectSet.Add(name)) {
                    continue;
                }
                var fragment = redirect.TryGetProperty("fragment", out var f) ? f.GetString() : null;
                page.Redirects.Add(new WikipediaIncomingRedirect(name, string.IsNullOrWhiteSpace(fragment) ? null : fragment));
            }
        }
    }

    // The page a requested title reaches: its normalized form, then up to five redirect hops.
    private (string Final, List<WikipediaRedirectStep> Steps, Dictionary<string, string> Normalized) Resolve(string requested) {
        var normalized = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var title = requested;
        if (_normalized.TryGetValue(title, out var n)) {
            normalized[title] = n;
            title = n;
        }
        var steps = new List<WikipediaRedirectStep>();
        var seen = new HashSet<string>(StringComparer.Ordinal) { title };
        for (var hop = 0; hop < 5 && _redirects.TryGetValue(title, out var to); hop++) {
            steps.Add(new WikipediaRedirectStep(title, to));
            if (!seen.Add(to)) {
                break;
            }
            title = to;
        }
        return (title, steps, normalized);
    }

    public IReadOnlyDictionary<string, WikipediaQueryResult> PageResults(IReadOnlyList<string> requested, HttpStatusCode status, long bytes) {
        var results = new Dictionary<string, WikipediaQueryResult>(StringComparer.Ordinal);
        foreach (var title in requested.Distinct(StringComparer.Ordinal)) {
            var (final, steps, normalized) = Resolve(title);
            if (!_pages.TryGetValue(final, out var page) || page.Missing) {
                results[title] = WikipediaQueryResult.Missing(title, normalized, steps, "missing", page?.MissingReason, status, 0);
                continue;
            }
            if (page.Invalid) {
                results[title] = WikipediaQueryResult.Missing(title, normalized, steps, "invalid-title", page.InvalidReason, status, 0);
                continue;
            }
            var isDisambiguation = page.PageProps.Contains("disambiguation")
                || page.Categories.Any(c => c.Equals("disambiguation pages", StringComparison.OrdinalIgnoreCase));
            var isSetIndex = page.PageProps.Contains("setindex")
                || page.Categories.Any(c => c.Equals("set index articles", StringComparison.OrdinalIgnoreCase));
            results[title] = WikipediaQueryResult.Found(title, page.Title, page.DisplayTitle ?? page.Title, normalized, steps,
                page.Categories.ToList(), page.Wikitext, isDisambiguation, isSetIndex, page.PageId, page.RevisionId, status,
                requested.Count == 0 ? 0 : bytes / requested.Count);
        }
        return results;
    }

    public IReadOnlyDictionary<string, WikipediaIncomingRedirects> IncomingRedirects(IReadOnlyList<string> requested) {
        var results = new Dictionary<string, WikipediaIncomingRedirects>(StringComparer.Ordinal);
        foreach (var title in requested.Distinct(StringComparer.Ordinal)) {
            var (final, _, _) = Resolve(title);
            if (!_pages.TryGetValue(final, out var page) || page.Missing || page.Invalid) {
                results[title] = new WikipediaIncomingRedirects(title, null, false, Array.Empty<WikipediaIncomingRedirect>());
                continue;
            }
            results[title] = new WikipediaIncomingRedirects(title, page.Title, true, page.Redirects.ToList());
        }
        return results;
    }
}
