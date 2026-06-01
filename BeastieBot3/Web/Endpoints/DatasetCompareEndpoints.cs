using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Status;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Read-only "are the two IUCN datasets the same?" endpoint. Compares the CSV
// main DB against the API-cache projection (built by `iucn api project-view`)
// across version, dates, and the canonical species/global counts, so the user
// can pick a dataset for list/chart generation with confidence. Both sides are
// computed with the identical query set (DatasetStatsService) — small deltas are
// expected and explained in the UI (API excludes delisted taxa lacking a latest
// assessment; some latest assessments may not be downloaded yet).

public static class DatasetCompareEndpoints {
    public static void MapDatasetCompareEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/dataset-compare", () => {
            var paths = new PathsService();

            var csv = SafeCompute(() => paths.ResolveIucnDatabasePath(null));
            var apiPath = SafeResolve(() => paths.ResolveIucnApiProjectedPath(null));
            var api = apiPath is null
                ? new DatasetStats { Exists = false }
                : DatasetStatsService.Compute(apiPath);

            return Results.Json(new {
                generatedAt = DateTimeOffset.UtcNow,
                csv = Describe(csv),
                api = Describe(api),
                comparison = BuildComparison(csv, api),
            });
        });
    }

    private static DatasetStats SafeCompute(Func<string> resolvePath) {
        try { return DatasetStatsService.Compute(resolvePath()); }
        catch (Exception ex) { return new DatasetStats { Exists = false, Error = ex.Message }; }
    }

    private static string? SafeResolve(Func<string> resolvePath) {
        try { return resolvePath(); }
        catch { return null; }
    }

    private static object Describe(DatasetStats s) => new {
        exists = s.Exists,
        path = s.Path,
        version = s.Version,
        lastModified = s.LastModified,
        sizeBytes = s.SizeBytes,
        totalAssessments = s.TotalAssessments,
        distinctTaxa = s.DistinctTaxa,
        globalSpecies = s.GlobalSpecies,
        byCategory = s.ByCategory.Select(c => new { category = c.Category, count = c.Count }),
        error = s.Error,
        built = s.Exists && s.Error is null,
    };

    // Flat, render-ready comparison rows: headline metrics then per-category counts.
    private static IReadOnlyList<object> BuildComparison(DatasetStats csv, DatasetStats api) {
        var rows = new List<object>();
        rows.Add(NumRow("Total assessments", csv.TotalAssessments, api.TotalAssessments));
        rows.Add(NumRow("Distinct taxa", csv.DistinctTaxa, api.DistinctTaxa));
        rows.Add(NumRow("Global species", csv.GlobalSpecies, api.GlobalSpecies));

        var csvCats = csv.ByCategory.ToDictionary(c => c.Category, c => c.Count, StringComparer.OrdinalIgnoreCase);
        var apiCats = api.ByCategory.ToDictionary(c => c.Category, c => c.Count, StringComparer.OrdinalIgnoreCase);
        // Preserve the canonical order the service already sorted CSV into, then append API-only.
        var ordered = csv.ByCategory.Select(c => c.Category)
            .Concat(api.ByCategory.Select(c => c.Category))
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var cat in ordered) {
            long? c = csvCats.TryGetValue(cat, out var cv) ? cv : (csv.Exists ? 0 : (long?)null);
            long? a = apiCats.TryGetValue(cat, out var av) ? av : (api.Exists ? 0 : (long?)null);
            rows.Add(NumRow(cat, c, a, category: true));
        }
        return rows;
    }

    private static object NumRow(string label, long? csv, long? api, bool category = false) => new {
        label,
        category,
        csv,
        api,
        equal = csv.HasValue && api.HasValue && csv.Value == api.Value,
        delta = (csv.HasValue && api.HasValue) ? api.Value - csv.Value : (long?)null,
    };
}
