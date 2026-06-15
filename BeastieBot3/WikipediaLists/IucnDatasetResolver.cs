using System;
using System.IO;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;
using Spectre.Console;

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
                WarnIfPartial(path);
                return path;
            default:
                throw new InvalidOperationException($"Unknown --dataset '{dataset}'. Use 'csv' (default) or 'api'.");
        }
    }

    // The projection records is_partial / latest_not_downloaded in import_metadata when some
    // taxa's latest assessment JSON wasn't downloaded. Warn so list/chart output isn't silently
    // built from an incomplete dataset. Best-effort: a pre-coverage projection lacks the column.
    private static void WarnIfPartial(string projectionPath) {
        try {
            var cs = new SqliteConnectionStringBuilder { DataSource = projectionPath, Mode = SqliteOpenMode.ReadOnly };
            using var connection = new SqliteConnection(cs.ConnectionString);
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COALESCE(latest_not_downloaded, 0) FROM import_metadata WHERE is_partial = 1 ORDER BY id DESC LIMIT 1";
            var result = cmd.ExecuteScalar();
            if (result is null || result is DBNull) return;
            var missing = Convert.ToInt64(result);
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Warning:[/] the API projection is partial — {missing:N0} taxa are missing (latest assessment not downloaded). Re-run [yellow]iucn api cache-assessments[/] then [yellow]iucn api project-view[/] for full coverage.");
        } catch (SqliteException) {
            // Pre-coverage projection (no is_partial column) or unreadable metadata — skip the warning.
        }
    }
}
