using BeastieBot3.Col;
using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// "Is my imported Catalogue of Life release out of date?" endpoint — the CoL counterpart to
// /api/iucn-version, but fully OFFLINE. It compares two local facts, so it catches both ways a
// CoL update goes wrong:
//   loaded — the release inside Datastore:COL_sqlite
//   input  — the newest ColDP archive sitting in Datasets:COL_dir
// If the input folder holds a newer release than the loaded DB, you either haven't re-run
// `col import` or haven't repointed COL_sqlite — either way the badge goes amber.
//
// The comparison itself lives in ColUpdateStateReader, shared with the workflow step lights so
// the banner and the steps can't disagree about which release is loaded.

public static class ColVersionEndpoints {
    public static void MapColVersionEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/col-version", (HttpContext ctx, PathsService paths) => {
            if (ctx.Request.Query.TryGetValue("refresh", out var r) && r == "1") {
                ColUpdateStateReader.ClearZipCache();
            }

            var state = ColUpdateStateReader.Read(paths);
            return Results.Json(new {
                loaded = state.Loaded?.Label,
                loadedVersion = state.Loaded?.Version,
                loadedIssued = state.Loaded?.Issued,
                loadedComplete = state.Loaded?.Complete ?? false,
                loadedExists = state.Loaded?.Exists ?? false,
                loadedPath = state.Loaded?.Path,
                input = state.Input?.Label,
                inputVersion = state.Input?.Version,
                inputIssued = state.Input?.Issued,
                inputDir = state.Input?.Dir,
                archiveCount = state.Input?.ArchiveCount ?? 0,
                fresh = state.Fresh,
                status = state.Status,
                message = state.Message,
            });
        });
    }
}
