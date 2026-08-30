using System.Text.Json;
using BeastieBot3.Web.Commands;
using BeastieBot3.Web.Jobs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// REST + SSE endpoints for the job runner.
//
//   POST /api/jobs                  body { command, args[] }  -> enqueue
//   GET  /api/jobs                                            -> recent jobs
//   GET  /api/jobs/{id}                                       -> single job status
//   GET  /api/jobs/{id}/stream                                -> SSE output stream
//   POST /api/jobs/{id}/cancel                                -> request cancellation

public static class JobsEndpoints {
    private static readonly HashSet<string> Forbidden = new(StringComparer.OrdinalIgnoreCase) {
        "serve",
    };

    public static void MapJobsEndpoints(this IEndpointRouteBuilder app) {
        app.MapPost("/api/jobs", async (HttpContext ctx, JobRunner runner) => {
            var req = await JsonSerializer.DeserializeAsync<EnqueueRequest>(
                ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Command)) {
                return Results.BadRequest(new { error = "command is required" });
            }
            var command = req.Command.Trim();
            if (Forbidden.Contains(command)) {
                return Results.BadRequest(new { error = $"command '{command}' cannot be invoked via the web UI" });
            }
            // Only dispatch commands that actually exist in the attribute-driven command tree;
            // a typo or an injected garbage string returns 400 instead of being split and handed
            // to Spectre to fail at runtime (and closes the door on non-tree input).
            if (CommandRegistry.FindByPath(command) is null) {
                return Results.BadRequest(new { error = $"unknown command '{command}'" });
            }
            var job = runner.Enqueue(command, req.Args ?? Array.Empty<string>());
            return Results.Json(Describe(job), JsonOpts);
        });

        app.MapGet("/api/jobs", (JobRegistry reg) =>
            Results.Json(reg.All().Select(Describe), JsonOpts));

        app.MapGet("/api/jobs/{id}", (string id, JobRegistry reg) => {
            var job = reg.Get(id);
            return job is null
                ? Results.NotFound()
                : Results.Json(Describe(job), JsonOpts);
        });

        app.MapPost("/api/jobs/{id}/cancel", (string id, JobRegistry reg) => {
            var job = reg.Get(id);
            if (job is null) return Results.NotFound();
            var cts = job.CancellationSource;
            if (cts is null || job.Status is not (JobStatus.Pending or JobStatus.Running)) {
                return Results.BadRequest(new {
                    error = $"job is {job.Status.ToString().ToLowerInvariant()}; only running jobs can be cancelled",
                });
            }
            try { cts.Cancel(); } catch (ObjectDisposedException) { /* completed concurrently */ }
            return Results.Json(Describe(job), JsonOpts);
        });

        app.MapGet("/api/jobs/{id}/stream", async (string id, HttpContext ctx, JobRegistry reg) => {
            var job = reg.Get(id);
            if (job is null) {
                ctx.Response.StatusCode = 404;
                return;
            }

            ctx.Response.Headers.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            ctx.Response.Headers["X-Accel-Buffering"] = "no";

            var (history, reader) = job.Output.Subscribe();
            if (!string.IsNullOrEmpty(history)) {
                await WriteSseChunk(ctx, history).ConfigureAwait(false);
            }
            if (reader is not null) {
                var buffer = new System.Text.StringBuilder();
                try {
                    await foreach (var chunk in reader.ReadAllAsync(ctx.RequestAborted)) {
                        // Spectre writes through TextWriter.Write(char) in places, so one
                        // chunk can be one character. Drain whatever else is already queued
                        // and send it as a single event -- otherwise the browser does a full
                        // render pass per character.
                        buffer.Clear();
                        buffer.Append(chunk);
                        while (buffer.Length < MaxCoalescedChunkChars && reader.TryRead(out var more)) {
                            buffer.Append(more);
                        }
                        await WriteSseChunk(ctx, buffer.ToString()).ConfigureAwait(false);
                    }
                } catch (OperationCanceledException) {
                    // Client disconnected. Nothing to do.
                    return;
                }
            }
            await WriteSseEvent(ctx, "status", JsonSerializer.Serialize(Describe(job), JsonOpts)).ConfigureAwait(false);
            await WriteSseEvent(ctx, "done", "").ConfigureAwait(false);
        });
    }

    // Cap on how much output one SSE event carries when draining the queue.
    private const int MaxCoalescedChunkChars = 64 * 1024;

    // Output chunks travel as a JSON string. SSE treats a bare \r as a line
    // terminator, so a raw progress-bar redraw ("\rProgress 40%") lost everything
    // after the \r and the client's overwrite path never fired.
    private static Task WriteSseChunk(HttpContext ctx, string chunk) =>
        WriteSseEvent(ctx, "chunk", JsonSerializer.Serialize(chunk, JsonOpts));

    private static async Task WriteSseEvent(HttpContext ctx, string eventName, string data) {
        // SSE requires every line of data prefixed with "data: ". Split on \n and
        // re-join so multi-line chunks are correctly framed.
        var sb = new System.Text.StringBuilder();
        sb.Append("event: ").Append(eventName).Append('\n');
        foreach (var line in data.Split('\n')) {
            sb.Append("data: ").Append(line).Append('\n');
        }
        sb.Append('\n');
        await ctx.Response.WriteAsync(sb.ToString(), ctx.RequestAborted).ConfigureAwait(false);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted).ConfigureAwait(false);
    }

    private static object Describe(Job j) => new {
        id = j.Id,
        command = j.Command,
        args = j.Args,
        commandLine = j.CommandLine,
        status = j.Status.ToString().ToLowerInvariant(),
        exitCode = j.ExitCode,
        error = j.Error,
        createdAt = j.CreatedAt,
        startedAt = j.StartedAt,
        completedAt = j.CompletedAt,
    };

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web) {
        WriteIndented = false,
    };

    private sealed class EnqueueRequest {
        public string? Command { get; set; }
        public string[]? Args { get; set; }
    }
}
