using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Web;

/// One suggestion for the search box. Names, ids and the category only: under the IUCN Red List
/// Terms of Use the site has no API for any other assessment field.
public sealed record Suggestion(long TaxonId, string Name, string? CommonName, string? Category);

public static class SiteEndpoints {
    public const int MaxSuggestions = 10;

    /// Search text longer than this is cut; no name in the data is longer.
    public const int MaxQueryLength = 100;

    public static void MapSiteEndpoints(this IEndpointRouteBuilder endpoints) {
        // "ok", or "unavailable: " and the reason (no file paths). The database file is checked
        // again at most every 30 seconds (SiteDatabase), so this is cheap to poll.
        endpoints.MapGet("/healthz", (SiteDatabase db) => db.IsReady
            ? Results.Text("ok", "text/plain")
            : Results.Text("unavailable: " + db.PublicNotReadyReason, "text/plain", statusCode: StatusCodes.Status503ServiceUnavailable));

        endpoints.MapGet("/api/suggest", (SiteQueries queries, HttpContext context, CancellationToken cancellationToken) => {
            context.Response.Headers.CacheControl = "public, max-age=300";
            context.Response.Headers["X-Robots-Tag"] = "noindex";
            var text = QueryText(context.Request);
            if (IdQuery.Parse(text) is { } ids && queries.FindByIds(ids) is { Count: > 0 } idHits) {
                return Results.Json(idHits.Select(h => h.Taxon).DistinctBy(t => t.TaxonId)
                    .Select(t => new Suggestion(t.TaxonId, t.ScientificName, t.CommonNameEn, SuggestCode(t))));
            }
            if (FtsQuery.IsTooShort(text)) {
                return Results.Json(Array.Empty<Suggestion>());
            }
            var result = queries.Search(text, MaxSuggestions, countAll: false, cancellationToken: cancellationToken);
            var suggestions = result.Hits.Select(h => new Suggestion(
                h.Taxon.TaxonId,
                h.Taxon.ScientificName,
                h.Taxon.CommonNameEn,
                SuggestCode(h.Taxon)));
            return Results.Json(suggestions);
        }).CacheOutput(SiteCachePolicies.Suggest);
    }

    // The stored code with its case ("LR/nt", "nt" and "NT" are different categories), and CR(PE)
    // or CR(PEW) for a possibly extinct CR.
    private static string? SuggestCode(TaxonSummary taxon) => taxon.Category switch {
        null => null,
        "CR" when taxon.PossiblyExtinct => "CR(PE)",
        "CR" when taxon.PossiblyExtinctInTheWild => "CR(PEW)",
        var code => code,
    };

    /// The search text of a request: the first "q" parameter, normalised. The search page,
    /// /api/suggest and their output cache keys all read it this way, so a cached response always
    /// belongs to its key (a second "q" parameter cannot change the response but not the key).
    public static string QueryText(HttpRequest request) => NormalizeQuery(FirstQueryValue(request, "q"));

    /// The first value of a query parameter, or null.
    public static string? FirstQueryValue(HttpRequest request, string name) =>
        request.Query.TryGetValue(name, out var values) && values.Count > 0 ? values[0] : null;

    /// Trimmed, with runs of whitespace collapsed, and at most MaxQueryLength characters.
    public static string NormalizeQuery(string? q) {
        if (string.IsNullOrWhiteSpace(q)) {
            return string.Empty;
        }
        var collapsed = string.Join(' ', q.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return collapsed.Length > MaxQueryLength ? collapsed[..MaxQueryLength].TrimEnd() : collapsed;
    }
}
