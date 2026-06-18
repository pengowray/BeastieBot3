using BeastieBot3.Configuration;
using BeastieBot3.WikipediaLists;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Counts-only list-size impact for a taxa group — the same computation as `wikipedia preview-impact`,
// for the tuning UI.
//
//   GET /api/lists/impact?group=<g>&splitRank=<class|order|family>&budget=<N>
//
// Returns page-option sizes (combined threatened vs separate CR/EN/VU, NT, DD, LC) and, when a
// splitRank is given, per-sub-page sizes — each as renderable bullets AND canonical species, with a
// budget verdict (the group's size_budget unless overridden). Read-only; never generates wikitext.

public static class ListImpactEndpoints {
    public static void MapListImpactEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/lists/impact", (string? group, string? splitRank, int? budget, PathsService paths) => {
            if (string.IsNullOrWhiteSpace(group)) {
                return Results.BadRequest(new { error = "group is required" });
            }
            var configPath = Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
            string databasePath;
            try {
                databasePath = IucnDatasetResolver.Resolve(paths, null, null);
            } catch (Exception ex) {
                return Results.Json(new { error = "No IUCN database: " + ex.Message }, statusCode: 503);
            }
            try {
                var record = ListImpactService.Compute(databasePath, configPath, group!, splitRank, budget,
                    paths.GetWikipediaOutputDirectory());
                if (record is null) {
                    return Results.NotFound(new { error = $"Unknown taxa group '{group}'." });
                }
                return Results.Json(record);
            } catch (Exception ex) {
                return Results.Json(new { error = ex.Message }, statusCode: 500);
            }
        });
    }
}
