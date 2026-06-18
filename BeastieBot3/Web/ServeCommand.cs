using System.ComponentModel;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using BeastieBot3.Web.Jobs;
using BeastieBot3.Web.Status;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.Web;

// Boots a local Kestrel host that exposes the web UI and REST/SSE endpoints.
// Localhost-only, single-process. No user auth (it's a personal tool bound to
// loopback), but mutating requests are guarded against cross-origin/DNS-rebinding
// abuse (see the Origin/Host middleware below). The command blocks until Ctrl+C;
// pressing Ctrl+C shuts the host down cleanly.

internal sealed class ServeCommand : AsyncCommand<ServeCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-p|--port <PORT>")]
        [Description("Port to listen on. Defaults to 8080.")]
        [DefaultValue(8080)]
        public int Port { get; init; } = 8080;

        [CommandOption("--host <HOST>")]
        [Description("Host/IP to bind to. Defaults to 127.0.0.1 (localhost-only).")]
        [DefaultValue("127.0.0.1")]
        public string Host { get; init; } = "127.0.0.1";

        [CommandOption("--job-history <PATH>")]
        [Description("SQLite path for persisted job history. Defaults to web_jobs.sqlite next to the executable.")]
        public string? JobHistoryPath { get; init; }

        [CommandOption("--max-concurrent <N>")]
        [Description("Maximum number of jobs allowed to run in parallel. Defaults to 4.")]
        [DefaultValue(4)]
        public int MaxConcurrent { get; init; } = 4;
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken ct) {
        var url = $"http://{settings.Host}:{settings.Port}";

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(url);

        // Quiet the default ASP.NET request log spam: it floods the terminal
        // alongside the actual job output.
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o => {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        var jobHistoryPath = settings.JobHistoryPath
            ?? Path.Combine(AppContext.BaseDirectory, "web_jobs.sqlite");
        var jobHistoryStore = JobHistoryStore.Open(jobHistoryPath);

        // Install AsyncLocal-aware console proxies BEFORE creating the job
        // runner. Concurrent jobs each push their own tee writers into these
        // proxies; the proxies route per-task via AsyncLocal so jobs cannot
        // see each other's output.
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        var originalAnsi = AnsiConsole.Console;
        var routing = new ConsoleRoutingSuite(originalOut, originalErr, originalAnsi);
        Console.SetOut(routing.StdOut);
        Console.SetError(routing.StdErr);
        AnsiConsole.Console = routing.Ansi;

        if (settings.MaxConcurrent < 1) {
            AnsiConsole.MarkupLine("[red]--max-concurrent must be at least 1.[/]");
            return -1;
        }
        // One PathsService for the whole host: paths.ini is parsed once instead of
        // per request, and every endpoint/service shares the same resolved config.
        builder.Services.AddSingleton<PathsService>();
        builder.Services.AddSingleton<StatusService>();
        builder.Services.AddSingleton(jobHistoryStore);
        builder.Services.AddSingleton(routing);
        builder.Services.AddSingleton(sp => new JobRegistry(sp.GetRequiredService<JobHistoryStore>()));
        builder.Services.AddSingleton(sp => new JobRunner(
            sp.GetRequiredService<JobRegistry>(),
            sp.GetRequiredService<ConsoleRoutingSuite>(),
            settings.MaxConcurrent));

        var app = builder.Build();
        app.Lifetime.ApplicationStopping.Register(() => {
            // Restore the originals so any post-shutdown logging from the host
            // goes back through the real terminal writers.
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
            AnsiConsole.Console = originalAnsi;
            jobHistoryStore.Dispose();
        });

        // Localhost-only hardening. The server binds to loopback, but a malicious web page in
        // the user's browser could still try to drive it (CSRF / DNS-rebinding), and those POSTs
        // would run real commands or rewrite rules/*. Reject mutating requests whose stated origin
        // or Host header isn't a loopback host. Read-only GETs are unaffected (so the read-only
        // Playwright smoke tests and normal browsing keep working).
        var allowedHosts = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            settings.Host, "127.0.0.1", "localhost", "::1", "[::1]"
        };
        app.Use(async (httpContext, next) => {
            var method = httpContext.Request.Method;
            var isMutating = HttpMethods.IsPost(method) || HttpMethods.IsPut(method)
                || HttpMethods.IsDelete(method) || HttpMethods.IsPatch(method);
            if (isMutating) {
                var origin = httpContext.Request.Headers["Origin"].ToString();
                if (string.IsNullOrEmpty(origin)) {
                    origin = httpContext.Request.Headers["Referer"].ToString();
                }
                if (!string.IsNullOrEmpty(origin)
                    && (!Uri.TryCreate(origin, UriKind.Absolute, out var originUri) || !allowedHosts.Contains(originUri.Host))) {
                    httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                    await httpContext.Response.WriteAsync("Cross-origin request rejected (localhost-only server).").ConfigureAwait(false);
                    return;
                }
                if (!allowedHosts.Contains(httpContext.Request.Host.Host)) {
                    httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                    await httpContext.Response.WriteAsync("Unexpected Host header (localhost-only server).").ConfigureAwait(false);
                    return;
                }
            }
            await next().ConfigureAwait(false);
        });

        var wwwroot = Path.Combine(AppContext.BaseDirectory, "Web", "wwwroot");
        if (Directory.Exists(wwwroot)) {
            app.UseDefaultFiles(new DefaultFilesOptions {
                FileProvider = new PhysicalFileProvider(wwwroot),
            });
            app.UseStaticFiles(new StaticFileOptions {
                FileProvider = new PhysicalFileProvider(wwwroot),
            });
        } else {
            AnsiConsole.MarkupLine($"[yellow]Warning:[/] wwwroot directory not found at [grey]{wwwroot}[/]");
        }

        app.MapJobsEndpoints();
        app.MapPathsEndpoints();
        app.MapStatusEndpoints();
        app.MapDatasetCompareEndpoints();
        app.MapIucnVersionEndpoints();
        app.MapCommandsEndpoints();
        app.MapFlowsEndpoints();
        app.MapFilesEndpoints();
        app.MapPreviewEndpoints();
        app.MapRulesEditorEndpoints();
        app.MapTaxaGroupingEndpoints();

        AnsiConsole.MarkupLine($"[green]BeastieBot3 web UI[/] listening on [cyan]{url}[/]");
        AnsiConsole.MarkupLine($"[grey]Job history:[/] {jobHistoryPath}");
        AnsiConsole.MarkupLine($"[grey]Max concurrent jobs:[/] {settings.MaxConcurrent}");
        AnsiConsole.MarkupLine("[grey]Press Ctrl+C to stop.[/]");

        await app.RunAsync(ct).ConfigureAwait(false);
        return 0;
    }
}
