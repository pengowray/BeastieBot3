using BeastieBot3.Web.Status;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Read-only dashboard endpoint. Returns one entry per configured data source
// (SQLite databases, input/output directories) with row counts and file
// metadata. Safe to call repeatedly: WAL reads do not block running imports.

public static class StatusEndpoints {
    public static void MapStatusEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/status", (StatusService svc) => {
            return Results.Json(new {
                generatedAt = DateTimeOffset.UtcNow,
                sources = svc.Collect(),
            });
        });
    }
}
