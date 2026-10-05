using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Web;

public static class SiteMiddleware {
    public const string ContentSecurityPolicy =
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
        "object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

    /// Security headers on every response, error pages included. They are added when the response
    /// starts, because the exception handler clears headers set earlier.
    public static IApplicationBuilder UseSecurityHeaders(this IApplicationBuilder app) => app.Use(async (context, next) => {
        context.Response.OnStarting(() => {
            var headers = context.Response.Headers;
            headers.ContentSecurityPolicy = ContentSecurityPolicy;
            headers.XContentTypeOptions = "nosniff";
            headers.XFrameOptions = "DENY";
            headers["Referrer-Policy"] = "strict-origin-when-cross-origin";
            headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=(), payment=(), usb=(), browsing-topics=()";
            headers["Cross-Origin-Opener-Policy"] = "same-origin";
            return Task.CompletedTask;
        });
        await next(context);
    });

    /// The site answers GET and HEAD, and POST only on the status update page (UpdateModel.Path),
    /// whose request body may be larger than the server's limit for every other request. Anything
    /// else gets 405 with a plain-text body, so the status code page is not run for a method no
    /// page handles.
    public static IApplicationBuilder UseGetAndHeadOnly(this IApplicationBuilder app) => app.Use(async (context, next) => {
        var method = context.Request.Method;
        if (HttpMethods.IsGet(method) || HttpMethods.IsHead(method)) {
            await next(context);
            return;
        }
        if (HttpMethods.IsPost(method) && IsUpdatePath(context.Request.Path)) {
            if (context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } limit) {
                limit.MaxRequestBodySize = Pages.UpdateModel.MaxBodyBytes;
            }
            await next(context);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
        context.Response.Headers.Allow = IsUpdatePath(context.Request.Path) ? "GET, HEAD, POST" : "GET, HEAD";
        context.Response.ContentType = "text/plain; charset=utf-8";
        await context.Response.WriteAsync("Method not allowed");
    });

    /// The status update page's path, in any letter case, with or without a final "/".
    public static bool IsUpdatePath(PathString path) =>
        string.Equals(path.Value?.TrimEnd('/'), Pages.UpdateModel.Path, StringComparison.OrdinalIgnoreCase);

    /// While the database is not ready, every page answers 503 (the status code page explains).
    /// /healthz and the error pages still run.
    public static IApplicationBuilder UseDatabaseGate(this IApplicationBuilder app) => app.Use(async (context, next) => {
        var path = context.Request.Path;
        if (path.StartsWithSegments("/healthz") || path.StartsWithSegments("/error")) {
            await next(context);
            return;
        }
        var db = context.RequestServices.GetRequiredService<SiteDatabase>();
        if (!db.IsReady) {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            context.Response.Headers.RetryAfter = "60";
            return;
        }
        await next(context);
    });
}
