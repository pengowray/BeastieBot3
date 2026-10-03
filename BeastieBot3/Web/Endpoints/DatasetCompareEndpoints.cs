using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
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
//
// The API projection counts as built only when its newest build record has an end time, the rule
// IucnApiCacheStateReader uses for the workflow lights. `project-view` empties the projection
// before it writes, so a build that stopped part way leaves an empty file whose stats would
// otherwise read as a complete projection of 0 assessments.

public static class DatasetCompareEndpoints {
    public static void MapDatasetCompareEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/dataset-compare", (PathsService paths) => {
            var csv = SafeCompute(() => paths.ResolveIucnDatabasePath(null));
            var apiPath = SafeResolve(() => paths.ResolveIucnApiProjectedPath(null));
            var api = apiPath is null
                ? new DatasetStats { Exists = false }
                : DatasetStatsService.Compute(apiPath);
            var apiBuild = apiPath is null ? null : SafeReadBuild(apiPath);

            return Results.Json(Response(csv, api, apiBuild, DateTimeOffset.UtcNow));
        });
    }

    /// <summary>
    /// The response body. <paramref name="apiBuild"/> is the projection's build state; null when it
    /// could not be read, in which case the stats alone decide whether it is built.
    /// </summary>
    internal static object Response(DatasetStats csv, DatasetStats api, IucnProjectionState? apiBuild, DateTimeOffset generatedAt) {
        var csvBuilt = IsBuilt(csv, null);
        var apiBuilt = IsBuilt(api, apiBuild);
        return new {
            generatedAt,
            csv = Describe(csv, csvBuilt, null),
            api = Describe(api, apiBuilt, apiBuild?.UnfinishedBuildStartedAt),
            comparison = BuildComparison(csv, csvBuilt, api, apiBuilt),
        };
    }

    /// A database is built when its file could be read as an IUCN dataset and, for the API
    /// projection, its newest build record has an end time.
    internal static bool IsBuilt(DatasetStats stats, IucnProjectionState? build) =>
        stats.Exists && stats.Error is null && (build is null || build.Exists);

    private static DatasetStats SafeCompute(Func<string> resolvePath) {
        try { return DatasetStatsService.Compute(resolvePath()); }
        catch (Exception ex) { return new DatasetStats { Exists = false, Error = ex.Message }; }
    }

    private static string? SafeResolve(Func<string> resolvePath) {
        try { return resolvePath(); }
        catch { return null; }
    }

    private static IucnProjectionState? SafeReadBuild(string path) {
        try { return IucnApiCacheStateReader.ReadProjectionFile(Path.GetFullPath(path)); }
        catch { return null; }
    }

    // A database that is not built has no counts to show: an unfinished projection's tables are
    // empty, and "0" beside the CSV release's counts would read as a dataset with nothing in it.
    private static object Describe(DatasetStats s, bool built, DateTime? unfinishedBuildStartedAt) => new {
        exists = s.Exists,
        path = s.Path,
        version = built ? s.Version : null,
        lastModified = s.LastModified,
        sizeBytes = s.SizeBytes,
        totalAssessments = built ? s.TotalAssessments : null,
        distinctTaxa = built ? s.DistinctTaxa : null,
        globalSpecies = built ? s.GlobalSpecies : null,
        byCategory = (built ? s.ByCategory : Array.Empty<DatasetCategoryCount>()).Select(c => new { category = c.Category, count = c.Count }),
        error = s.Error,
        built,
        unfinishedBuildStartedAt,
        partial = built ? s.IsPartial : null,
        latestNotDownloaded = built ? s.LatestNotDownloaded : null,
    };

    // Flat, render-ready comparison rows: headline metrics then per-category counts. A side that
    // is not built has no value in any row, so no row shows a difference for it.
    private static IReadOnlyList<object> BuildComparison(DatasetStats csv, bool csvBuilt, DatasetStats api, bool apiBuilt) {
        var csvCategories = csvBuilt ? csv.ByCategory : Array.Empty<DatasetCategoryCount>();
        var apiCategories = apiBuilt ? api.ByCategory : Array.Empty<DatasetCategoryCount>();
        var rows = new List<object>();
        rows.Add(NumRow("Total assessments", csvBuilt ? csv.TotalAssessments : null, apiBuilt ? api.TotalAssessments : null));
        rows.Add(NumRow("Distinct taxa", csvBuilt ? csv.DistinctTaxa : null, apiBuilt ? api.DistinctTaxa : null));
        rows.Add(NumRow("Global species", csvBuilt ? csv.GlobalSpecies : null, apiBuilt ? api.GlobalSpecies : null));

        var csvCats = csvCategories.ToDictionary(c => c.Category, c => c.Count, StringComparer.OrdinalIgnoreCase);
        var apiCats = apiCategories.ToDictionary(c => c.Category, c => c.Count, StringComparer.OrdinalIgnoreCase);
        // Preserve the canonical order the service already sorted CSV into, then append API-only.
        var ordered = csvCategories.Select(c => c.Category)
            .Concat(apiCategories.Select(c => c.Category))
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var cat in ordered) {
            long? c = csvCats.TryGetValue(cat, out var cv) ? cv : (csvBuilt ? 0 : (long?)null);
            long? a = apiCats.TryGetValue(cat, out var av) ? av : (apiBuilt ? 0 : (long?)null);
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
