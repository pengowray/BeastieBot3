using BeastieBot3.Web.Commands;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Catalogue endpoint. Returns the full set of CLI commands the web UI can
// dispatch — sourced from the [CommandInfo] assembly scan — including each
// command's classification and its reflected form schema.

public static class CommandsEndpoints {
    public static void MapCommandsEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/commands", () => {
            var list = CommandRegistry.All.Select(c => new {
                path = c.Path,
                description = c.Description,
                kind = c.Kind.ToString().ToLowerInvariant(),
                reason = c.Reason,
                rerun = c.Rerun.ToString().ToLowerInvariant(),
                rerunNote = c.RerunNote,
                examples = c.Examples,
                branch = c.Branch,
                form = CommandReflector.BuildSchema(c.Type),
            });
            return Results.Json(list);
        });
    }
}
