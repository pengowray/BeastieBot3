using BeastieBot3.Web.Flows;
using BeastieBot3.Web.Jobs;
using BeastieBot3.Web.Status;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Workflow endpoints.
//   GET /api/flows           -> summary list (id, title, description)
//   GET /api/flows/{id}      -> full snapshot with per-step status + last-run

public static class FlowsEndpoints {
    public static void MapFlowsEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/flows", () => {
            var list = FlowCatalogue.All.Select(f => new {
                id = f.Id,
                title = f.Title,
                description = f.Description,
                stepCount = f.Steps.Count,
            });
            return Results.Json(list);
        });

        app.MapGet("/api/flows/{id}", (string id, JobHistoryStore? history, JobRegistry? registry) => {
            var flow = FlowCatalogue.Find(id);
            if (flow is null) return Results.NotFound();
            var evaluator = new FlowEvaluator(new StatusService(), history, registry);
            return Results.Json(evaluator.Snapshot(flow));
        });
    }
}
