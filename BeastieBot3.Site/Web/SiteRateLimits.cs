using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Web;

/// Requests per client per minute, in three buckets: search pages, the name-suggest API, and every
/// other page. Static files are served before the limiter and are not counted; /healthz and the
/// error pages (including the 429 page itself) are never limited. A rejected request gets status 429
/// and the status code page renders the "Too many requests" page.
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
            options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context => {
                var path = context.Request.Path;
                if (path.StartsWithSegments("/healthz") || path.StartsWithSegments("/error")) {
                    return RateLimitPartition.GetNoLimiter("unlimited");
                }
                var client = ClientKey(context.Connection.RemoteIpAddress);
                if (path.StartsWithSegments("/api")) {
                    return PerMinute("suggest|" + client, limits.SuggestPerMinute);
                }
                if (path.StartsWithSegments("/search")) {
                    return PerMinute("search|" + client, limits.SearchPerMinute);
                }
                return PerMinute("page|" + client, limits.PagesPerMinute);
            });
        });
        return services;
    }

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
}
