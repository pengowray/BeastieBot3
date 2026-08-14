using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Commands;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Catalogue endpoint. Returns the full set of CLI commands the web UI can
// dispatch — sourced from the [CommandInfo] assembly scan — including each
// command's classification and its reflected form schema.
//
// /api/commands/preflight additionally answers "what would this actually do right
// now", by inspecting the configured files. The confirmation dialog uses it so it can
// describe the real situation instead of the command's fixed warning text. Only
// commands with a preflight implementation return anything.

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

        app.MapGet("/api/commands/preflight", (PathsService paths, string path, string? args) => {
            var argv = (args ?? string.Empty).Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (path == "iucn import") {
                var pre = IucnImportPreflight.Describe(
                    paths,
                    force: argv.Contains("--force"),
                    replaceRelease: argv.Contains("--replace-release"));
                return Results.Json(new {
                    supported = true,
                    confirm = pre.Confirm,
                    headline = pre.Headline,
                    details = pre.Details,
                    warning = pre.Warning,
                });
            }

            return Results.Json(new { supported = false });
        });
    }
}
