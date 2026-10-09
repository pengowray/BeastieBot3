using System.Net;
using System.Text.Encodings.Web;
using System.Text.Unicode;
using BeastieBot3.Site;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.WebEncoders;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel => {
    kestrel.AddServerHeader = false;
    // The site answers GET and HEAD, so requests need no body or long headers. The one exception is
    // POST /update, whose larger body limit UseGetAndHeadOnly sets for that request.
    kestrel.Limits.MaxRequestBodySize = 16 * 1024;
    kestrel.Limits.MaxRequestHeadersTotalSize = 16 * 1024;
    kestrel.Limits.MaxRequestLineSize = 4 * 1024;
});

builder.Services.Configure<SiteOptions>(builder.Configuration.GetSection(SiteOptions.Section));
builder.Services.TryAddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SiteDatabase>();
builder.Services.AddSingleton<SiteQueries>();
// One client for the life of the site, so the pages it keeps (WikipediaPageSource) are shared.
builder.Services.AddSingleton<BeastieBot3.Site.Update.IWikipediaPageSource>(services => new BeastieBot3.Site.Update.WikipediaPageSource(
    new HttpClient(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(10) }),
    services.GetRequiredService<Microsoft.Extensions.Options.IOptions<SiteOptions>>()));

builder.Services.Configure<ForwardedHeadersOptions>(options => {
    // Trust X-Forwarded-For/Proto only from the reverse proxy on the same machine (Caddy).
    options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
    options.KnownIPNetworks.Clear();
    options.KnownProxies.Clear();
    options.KnownProxies.Add(IPAddress.Loopback);
    options.KnownProxies.Add(IPAddress.IPv6Loopback);
    options.ForwardLimit = 1;
});

builder.Services.AddRouting(options => options.LowercaseUrls = true);
builder.Services.Configure<WebEncoderOptions>(options => options.TextEncoderSettings = new TextEncoderSettings(UnicodeRanges.All));
// The status update page is /update-statuses too; its form still posts to /update (UpdateModel.Path).
builder.Services.AddRazorPages(options => options.Conventions.AddPageRoute("/Update", "update-statuses"));
builder.Services.AddSiteRateLimits();
builder.Services.AddOutputCache(options => {
    options.SizeLimit = 128 * 1024 * 1024;
    options.AddSitePolicies();
});

var app = builder.Build();

app.UseForwardedHeaders();
app.UseSecurityHeaders();
app.UseGetAndHeadOnly();
if (!app.Environment.IsDevelopment()) {
    app.UseExceptionHandler("/error/500");
    app.UseHsts();
}
app.UseStatusCodePagesWithReExecute("/error/{0}");
app.UseStaticFiles(new StaticFileOptions {
    OnPrepareResponse = context => {
        // Links to site.css and site.js carry a version hash (asp-append-version), so a versioned
        // request can be cached for a long time.
        context.Context.Response.Headers.CacheControl = context.Context.Request.Query.ContainsKey("v")
            ? "public, max-age=31536000, immutable"
            : "public, max-age=86400";
    },
});
app.UseDatabaseGate();
app.UseRouting();
app.UseRateLimiter();
app.UseOutputCache();

app.MapSiteEndpoints();
app.MapRazorPages();

app.Services.GetRequiredService<SiteDatabase>().CheckAtStartup();

app.Run();

public partial class Program;
