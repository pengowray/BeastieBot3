using BeastieBot3.Configuration;
using BeastieBot3.Web.Commands;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Catalogue endpoint. Returns the full set of CLI commands the web UI can
// dispatch — sourced from the [CommandInfo] assembly scan — including each
// command's classification and its reflected form schema.
//
// /api/commands/preflight answers "does this run need confirmation, and what does it
// delete?" for a command and its options (CommandPreflight). The web UI asks it before
// every run and asks the user only when it says confirm. `supported: false` means
// nothing to confirm and nothing to show.

public static class CommandsEndpoints {
    public static void MapCommandsEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/commands", () => {
            var list = CommandRegistry.All.Select(c => new {
                path = c.Path,
                description = c.Description,
                kind = c.Kind.ToString().ToLowerInvariant(),
                reason = c.Reason,
                confirm = CommandPreflight.ConfirmMode(c.Info),
                promptOption = c.Info.PromptOption,
                rerun = c.Rerun.ToString().ToLowerInvariant(),
                rerunNote = c.RerunNote,
                examples = c.Examples,
                branch = c.Branch,
                form = CommandReflector.BuildSchema(c.Type),
            });
            return Results.Json(list);
        });

        app.MapGet("/api/commands/preflight", (PathsService paths, string path, string? args) => {
            var cmd = CommandRegistry.FindByPath(path);
            if (cmd is null) {
                return Results.Json(new { supported = false });
            }

            var argv = (args ?? string.Empty).Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var pre = CommandPreflight.Describe(cmd.Info, argv, paths);
            if (pre is null) {
                return Results.Json(new { supported = false });
            }
            return Results.Json(new {
                supported = true,
                confirm = pre.Confirm,
                headline = pre.Headline,
                details = pre.Details,
                warning = pre.Warning,
                addArgs = pre.AddArgs,
            });
        });
    }
}
