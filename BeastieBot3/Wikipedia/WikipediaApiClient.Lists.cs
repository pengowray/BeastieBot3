using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

// Action API list queries for `wikipedia fetch-species-lists`:
//   ListEmbeddedInAsync       the pages that use a template (list=embeddedin), in one namespace;
//   ListCategoryMembersAsync  the pages and subcategories of a category (list=categorymembers).
// Each follows "continue" until the list is complete, 500 entries per request.

namespace BeastieBot3.Wikipedia;

/// A page in a list answer: its title and namespace (0 for articles, 14 for categories).
internal sealed record WikipediaListedPage(string Title, int Namespace);

internal sealed partial class WikipediaApiClient {
    public const int ArticleNamespace = 0;
    public const int CategoryNamespace = 14;

    /// The pages in <paramref name="ns"/> that use <paramref name="template"/> ("Template:IUCN status"),
    /// directly or through another template.
    public Task<IReadOnlyList<WikipediaListedPage>> ListEmbeddedInAsync(string template, int ns, CancellationToken cancellationToken) =>
        RunListAsync("embeddedin", new Dictionary<string, string> {
            ["eititle"] = template,
            ["einamespace"] = ns.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["eilimit"] = "max",
        }, cancellationToken);

    /// The articles and subcategories of <paramref name="category"/> ("Category:Lists of plants").
    public Task<IReadOnlyList<WikipediaListedPage>> ListCategoryMembersAsync(string category, CancellationToken cancellationToken) =>
        RunListAsync("categorymembers", new Dictionary<string, string> {
            ["cmtitle"] = category,
            ["cmnamespace"] = $"{ArticleNamespace}|{CategoryNamespace}",
            ["cmlimit"] = "max",
        }, cancellationToken);

    private async Task<IReadOnlyList<WikipediaListedPage>> RunListAsync(string list, Dictionary<string, string> listParameters,
        CancellationToken cancellationToken) {
        var parameters = new Dictionary<string, string>(listParameters) {
            ["action"] = "query",
            ["format"] = "json",
            ["formatversion"] = "2",
            ["list"] = list,
        };
        var pages = new List<WikipediaListedPage>();
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
            using var document = JsonDocument.Parse(response.Body);
            var root = document.RootElement;
            if (root.TryGetProperty("error", out var error)) {
                throw new WikipediaApiException(response.Url, response.StatusCode, error.GetRawText(), 1);
            }
            pages.AddRange(ReadListedPages(root, list));
            continuation = ReadContinuation(root);
            if (continuation.Count == 0) {
                return pages;
            }
        }
        throw new WikipediaApiException("wikipedia list", System.Net.HttpStatusCode.OK,
            $"The list was still continuing after {MaxContinuations} requests.", MaxContinuations);
    }

    internal static IEnumerable<WikipediaListedPage> ReadListedPages(JsonElement root, string list) {
        if (!root.TryGetProperty("query", out var query) || !query.TryGetProperty(list, out var items) || items.ValueKind != JsonValueKind.Array) {
            yield break;
        }
        foreach (var item in items.EnumerateArray()) {
            var title = item.TryGetProperty("title", out var t) ? t.GetString() : null;
            var ns = item.TryGetProperty("ns", out var n) && n.ValueKind == JsonValueKind.Number ? n.GetInt32() : -1;
            if (!string.IsNullOrWhiteSpace(title)) {
                yield return new WikipediaListedPage(title, ns);
            }
        }
    }

    private static IReadOnlyDictionary<string, string> ReadContinuation(JsonElement root) {
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
}
