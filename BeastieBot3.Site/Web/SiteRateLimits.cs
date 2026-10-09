using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Web;

/// Requests per client per minute, in four buckets: search pages, the name-suggest API, texts sent
/// to the status update page (POST /update), and every other page. Taxon, group and name pages also
/// count against an hourly and a daily limit per client (RateLimitOptions.TaxonPagesPerHour and
/// TaxonPagesPerDay). Static files are served before the limiter and are not counted; /healthz and the
/// error pages (including the 429 page itself) are never limited. Searches and suggestions also
/// and status updates share one limit on how many run at the same time (RateLimitOptions.ConcurrentSearches). A
/// rejected request gets status 429 and the status code page renders the "Too many requests" page.
public static class SiteRateLimits {
    public static IServiceCollection AddSiteRateLimits(this IServiceCollection services) {
        services.AddRateLimiter(_ => { });
        services.AddOptions<RateLimiterOptions>().Configure<IOptions<SiteOptions>>((options, site) => {
            var limits = site.Value.RateLimits;
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            options.OnRejected = (context, _) => {
                if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter)) {
                    context.HttpContext.Response.Headers.RetryAfter =
                        ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
                }
                return ValueTask.CompletedTask;
            };
            options.GlobalLimiter = BuildGlobalLimiter(limits);
        });
        return services;
    }

    /// The per-client limits, chained with one limit on the searches running at the same time
    /// across all clients. A full-text search can take a quarter of a second of CPU, so this bounds
    /// the load whatever the queries are.
    ///
    /// The per-client limit comes first, so a client over its limit is turned away before it can
    /// take a place in the shared queue. The cost: a request that has to wait in the queue uses two
    /// of its client's permits, because the middleware tries once without waiting and then waits,
    /// and a fixed-window permit cannot be given back.
    internal static PartitionedRateLimiter<HttpContext> BuildGlobalLimiter(RateLimitOptions limits) {
        var perClient = PartitionedRateLimiter.Create<HttpContext, string>(context => {
            var path = context.Request.Path;
            if (IsUnlimited(path)) {
                return RateLimitPartition.GetNoLimiter("unlimited");
            }
            var client = ClientKey(context.Connection.RemoteIpAddress);
            if (path.StartsWithSegments("/api")) {
                return PerMinute("suggest|" + client, limits.SuggestPerMinute);
            }
            if (IsSearch(path)) {
                return PerMinute("search|" + client, limits.SearchPerMinute);
            }
            if (IsUpdateRun(context)) {
                return PerMinute("update|" + client, limits.UpdatesPerMinute);
            }
            return PerMinute("page|" + client, limits.PagesPerMinute);
        });
        var searchesAtOnce = PartitionedRateLimiter.Create<HttpContext, string>(context => {
            var path = context.Request.Path;
            if (path.StartsWithSegments("/api") || IsSearch(path) || IsUpdateRun(context)) {
                return RateLimitPartition.GetConcurrencyLimiter("searches", _ => new ConcurrencyLimiterOptions {
                    PermitLimit = Math.Max(1, limits.ConcurrentSearches),
                    QueueLimit = Math.Max(0, limits.SearchQueueLength),
                    QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                });
            }
            return RateLimitPartition.GetNoLimiter("unlimited");
        });
        var taxonPagesPerHour = TaxonPageLimiter(limits.TaxonPagesPerHour, "hour", TimeSpan.FromHours(1));
        var taxonPagesPerDay = TaxonPageLimiter(limits.TaxonPagesPerDay, "day", TimeSpan.FromDays(1));
        return PartitionedRateLimiter.CreateChained(perClient, taxonPagesPerHour, taxonPagesPerDay, searchesAtOnce);
    }

    /// The pages whose number per client is also limited per hour and per day.
    internal static bool IsTaxonPage(PathString path) =>
        path.StartsWithSegments("/species") || path.StartsWithSegments("/taxa") || path.StartsWithSegments("/name");

    private static PartitionedRateLimiter<HttpContext> TaxonPageLimiter(int permits, string name, TimeSpan window) =>
        PartitionedRateLimiter.Create<HttpContext, string>(context => {
            if (permits <= 0 || !IsTaxonPage(context.Request.Path)) {
                return RateLimitPartition.GetNoLimiter("unlimited");
            }
            var key = $"taxon-{name}|{ClientKey(context.Connection.RemoteIpAddress)}";
            return RateLimitPartition.GetFixedWindowLimiter(key, _ => new FixedWindowRateLimiterOptions {
                PermitLimit = permits,
                Window = window,
                QueueLimit = 0,
            });
        });

    // A text sent to the status update page, or a page it loads from Wikipedia (GET with page=):
    // both run the status updater, and a load also asks Wikipedia.
    private static bool IsUpdateRun(HttpContext context) =>
        SiteMiddleware.IsUpdatePath(context.Request.Path)
        && (HttpMethods.IsPost(context.Request.Method) || context.Request.Query.ContainsKey(Pages.UpdateModel.PageField));

    private static bool IsUnlimited(PathString path) =>
        path.StartsWithSegments("/healthz") || path.StartsWithSegments("/error");

    private static RateLimitPartition<string> PerMinute(string key, int permits) =>
        RateLimitPartition.GetFixedWindowLimiter(key, _ => new FixedWindowRateLimiterOptions {
            PermitLimit = Math.Max(1, permits),
            Window = TimeSpan.FromMinutes(1),
            QueueLimit = 0,
        });

    /// The partition for a client address: IPv4 addresses one by one, IPv6 addresses by their /64
    /// network, since one IPv6 client can use any address in its /64.
    internal static string ClientKey(IPAddress? address) {
        if (address is null) {
            return "unknown";
        }
        if (address.IsIPv4MappedToIPv6) {
            address = address.MapToIPv4();
        }
        if (address.AddressFamily != AddressFamily.InterNetworkV6) {
            return address.ToString();
        }
        var bytes = address.GetAddressBytes();
        Array.Clear(bytes, 8, 8);
        return new IPAddress(bytes) + "/64";
    }

    // The pages that run a search: /search, and the cite tool and list maker with a query.
    private static bool IsSearch(PathString path) =>
        path.StartsWithSegments("/search") || path.StartsWithSegments(Web.ToolPaths.Cite) || path.StartsWithSegments(Web.ToolPaths.ListMaker);
}
