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
        endpoints.MapGet("/healthz", (SiteDatabase db) => db.IsReady
            ? Results.Text("ok", "text/plain")
            : Results.Text("unavailable", "text/plain", statusCode: StatusCodes.Status503ServiceUnavailable));

        endpoints.MapGet("/api/suggest", (string? q, SiteQueries queries, HttpContext context) => {
            context.Response.Headers.CacheControl = "public, max-age=300";
            context.Response.Headers["X-Robots-Tag"] = "noindex";
            var text = NormalizeQuery(q);
            if (text.Length < 2) {
                return Results.Json(Array.Empty<Suggestion>());
            }
            var result = queries.Search(text, MaxSuggestions, countAll: false);
            var suggestions = result.Hits.Select(h => new Suggestion(
                h.Taxon.TaxonId,
                h.Taxon.ScientificName,
                h.Taxon.CommonNameEn,
                SuggestCode(h.Taxon)));
            return Results.Json(suggestions);
        });
    }

    // The stored code with its case ("LR/nt", "nt" and "NT" are different categories), and CR(PE)
    // or CR(PEW) for a possibly extinct CR.
    private static string? SuggestCode(TaxonSummary taxon) => taxon.Category switch {
        null => null,
        "CR" when taxon.PossiblyExtinct => "CR(PE)",
        "CR" when taxon.PossiblyExtinctInTheWild => "CR(PEW)",
        var code => code,
    };

    /// Trimmed, with runs of whitespace collapsed, and at most MaxQueryLength characters.
    public static string NormalizeQuery(string? q) {
        if (string.IsNullOrWhiteSpace(q)) {
            return string.Empty;
        }
        var collapsed = string.Join(' ', q.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return collapsed.Length > MaxQueryLength ? collapsed[..MaxQueryLength].TrimEnd() : collapsed;
    }
}
