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

    /// The site only answers GET and HEAD. Anything else gets 405 with a plain-text body, so the
    /// status code page is not run for a method no page handles.
    public static IApplicationBuilder UseGetAndHeadOnly(this IApplicationBuilder app) => app.Use(async (context, next) => {
        if (HttpMethods.IsGet(context.Request.Method) || HttpMethods.IsHead(context.Request.Method)) {
            await next(context);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
        context.Response.Headers.Allow = "GET, HEAD";
        context.Response.ContentType = "text/plain; charset=utf-8";
        await context.Response.WriteAsync("Method not allowed");
    });

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
