using System;
using System.IO;
using BeastieBot3.Configuration;

// Resolves which IUCN relational database `generate-lists` / `generate-charts`
// should open, given a --dataset choice:
//   csv (default) -> the CSV-imported main DB (Datastore:IUCN_sqlite_from_cvs)
//   api           -> the CSV-shaped projection of the API cache, built by
//                    `iucn api project-view` (Datastore:IUCN_api_projected_sqlite)
// Both expose the same view_assessments_html_taxonomy_html, so the query layer is
// dataset-agnostic. A --database override applies within the chosen dataset.

namespace BeastieBot3.WikipediaLists;

internal static class IucnDatasetResolver {
    public static string Resolve(PathsService paths, string? dataset, string? databaseOverride) {
        var ds = string.IsNullOrWhiteSpace(dataset) ? "csv" : dataset.Trim().ToLowerInvariant();
        switch (ds) {
            case "csv":
                return paths.ResolveIucnDatabasePath(databaseOverride);
            case "api":
                var path = paths.ResolveIucnApiProjectedPath(databaseOverride);
                if (!File.Exists(path)) {
                    throw new InvalidOperationException(
                        $"IUCN API projection not found at {path}.\n" +
                        "Build it first with:  iucn api project-view  (after caching via `iucn api cache-all`).");
                }
                return path;
            default:
                throw new InvalidOperationException($"Unknown --dataset '{dataset}'. Use 'csv' (default) or 'api'.");
        }
    }
}
