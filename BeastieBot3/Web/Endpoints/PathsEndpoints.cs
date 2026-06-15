using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Read-only configuration introspection. The dashboard uses this to surface
// the resolved paths.ini source file and every key/value pair without needing
// to spawn a `show-paths` job.

public static class PathsEndpoints {
    public static void MapPathsEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/paths", (PathsService svc) => {
            return Results.Json(new {
                source = svc.SourceFilePath,
                baseDirectory = svc.BaseDirectory,
                values = svc.GetAll(),
            });
        });
    }
}
