using System.Globalization;
using System.Text.Json;
using BeastieBot3.SiteBuild;
using GbifDoi = BeastieBot3.Iucn.Gbif.IucnDoi;

// Crossref's list of every DOI under IUCN's prefix 10.2305. IUCN registers its Red List DOIs with
// Crossref (doi.org/ra/<doi> answers "Crossref"), and Crossref's REST API lists a prefix's works
// 1,000 at a time with a cursor:
//
//   https://api.crossref.org/prefixes/10.2305/works?rows=1000&select=DOI,resource&cursor=*
//
// In October 2026 the prefix had 256,151 works: 255,060 Red List assessments (type "dataset") and
// about 1,100 books, reports and journal articles. Each item gives the DOI (in lower case) and
// resource.primary.URL, the page it points to: https://www.iucnredlist.org/species/<taxon>/<assessment>.
// The list took 258 requests and about 3 minutes on 2026-10-03. Crossref's public pool allows 5 requests a second
// and one at a time (x-rate-limit-limit, x-concurrency-limit); this sends one request at a time.
//
// The User-Agent names the project, never a person's email address, so the requests go to the
// public pool rather than the "polite" pool that asks for one.

namespace BeastieBot3.Iucn.Doi;

/// One page of the list: the works that are Red List assessments, and the cursor for the next page.
/// UnreadRlts lists DOIs that contain ".RLTS." but do not have the usual form, so are left out.
internal sealed record CrossrefPage(long? TotalResults, string? NextCursor, int ItemCount, IReadOnlyList<CrossrefIucnWork> Works,
    IReadOnlyList<string> UnreadRlts);

internal static class CrossrefIucnWorks {
    public const string Prefix = "10.2305";
    public const int PageSize = 1000;
    public const string DefaultBaseUrl = "https://api.crossref.org/";

    public static string PageUrl(string cursor, string baseUrl = DefaultBaseUrl) =>
        $"{(baseUrl.EndsWith('/') ? baseUrl : baseUrl + "/")}prefixes/{Prefix}/works?rows={PageSize.ToString(CultureInfo.InvariantCulture)}&select=DOI,resource&cursor={Uri.EscapeDataString(cursor)}";

    /// Reads one page of Crossref's answer. Items that are not Red List assessment DOIs are counted
    /// in ItemCount but left out of Works. Pure.
    public static CrossrefPage ParsePage(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("message", out var message) || message.ValueKind != JsonValueKind.Object) {
            throw new InvalidDataException("Crossref's answer has no message object.");
        }
        long? total = message.TryGetProperty("total-results", out var t) && t.ValueKind == JsonValueKind.Number && t.TryGetInt64(out var n) ? n : null;
        var next = message.TryGetProperty("next-cursor", out var c) && c.ValueKind == JsonValueKind.String ? c.GetString() : null;
        var works = new List<CrossrefIucnWork>();
        var unread = new List<string>();
        var count = 0;
        if (message.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array) {
            foreach (var item in items.EnumerateArray()) {
                count++;
                if (ParseItem(item) is { } work) {
                    works.Add(work);
                } else if (item.ValueKind == JsonValueKind.Object && item.TryGetProperty("DOI", out var doi)
                    && doi.ValueKind == JsonValueKind.String && doi.GetString() is { } text
                    && text.Contains(".rlts.", StringComparison.OrdinalIgnoreCase)) {
                    unread.Add(text);
                }
            }
        }
        return new CrossrefPage(total, next, count, works, unread);
    }

    /// One item as a Red List assessment DOI, or null when its DOI does not name an assessment.
    public static CrossrefIucnWork? ParseItem(JsonElement item) {
        if (item.ValueKind != JsonValueKind.Object || !item.TryGetProperty("DOI", out var doiElement) || doiElement.ValueKind != JsonValueKind.String) {
            return null;
        }
        if (IucnDoiSelector.TryParse(doiElement.GetString()) is not { } doi) {
            return null;
        }
        string? url = null;
        if (item.TryGetProperty("resource", out var resource) && resource.ValueKind == JsonValueKind.Object
            && resource.TryGetProperty("primary", out var primary) && primary.ValueKind == JsonValueKind.Object
            && primary.TryGetProperty("URL", out var urlElement) && urlElement.ValueKind == JsonValueKind.String) {
            url = urlElement.GetString();
        }
        var page = GbifDoi.ParseAssessmentUrl(url);
        return new CrossrefIucnWork(doi.ToString(), doi.TaxonId, doi.AssessmentId, doi.Release, doi.Language, url,
            page?.TaxonId, page?.AssessmentId);
    }

    /// Downloads the whole list into the store, one page per request, and marks the listing complete
    /// when Crossref returns an empty page. Returns the listing id.
    public static async Task<long> DownloadAsync(PoliteHttpGetter getter, IucnDoiCacheStore store, Action<CrossrefPage>? onPage,
        CancellationToken cancellationToken, string baseUrl = DefaultBaseUrl) {
        var listingId = store.StartListing(DateTime.UtcNow);
        var cursor = "*";
        while (true) {
            cancellationToken.ThrowIfCancellationRequested();
            var url = PageUrl(cursor, baseUrl);
            var response = await getter.GetAsync(url, cancellationToken).ConfigureAwait(false);
            if (response.Status != 200) {
                throw new PoliteHttpException(url, $"Crossref answered HTTP {response.Status}.");
            }
            CrossrefPage page;
            try {
                page = ParsePage(response.Body);
            } catch (Exception ex) when (ex is JsonException or InvalidDataException) {
                throw new PoliteHttpException(url, $"Crossref's answer could not be read: {ex.Message}", ex);
            }
            store.AddListingPage(listingId, page.Works, page.TotalResults, page.ItemCount);
            onPage?.Invoke(page);
            if (page.ItemCount == 0 || string.IsNullOrEmpty(page.NextCursor)) {
                break;
            }
            cursor = page.NextCursor;
        }
        store.CompleteListing(listingId, DateTime.UtcNow);
        return listingId;
    }
}
