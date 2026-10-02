namespace BeastieBot3.Site.Web;

public static class SiteUrls {
    /// The absolute URL of a path on this site ("/species/22823"), for canonical links. Built from
    /// Site:BaseUrl when it is an absolute http or https URL. Otherwise from the request: its scheme,
    /// its host in lower case, and its port only when it is not the scheme's default. Output-cached
    /// pages are shared by every spelling of the host that differs only in case, so the host must
    /// not be copied as the first visitor typed it.
    public static string Absolute(string? baseUrl, HttpRequest request, string path) {
        if (!string.IsNullOrWhiteSpace(baseUrl)
            && Uri.TryCreate(baseUrl.Trim(), UriKind.Absolute, out var configured)
            && (configured.Scheme == Uri.UriSchemeHttps || configured.Scheme == Uri.UriSchemeHttp)) {
            return configured.GetLeftPart(UriPartial.Path).TrimEnd('/') + path;
        }
        var scheme = request.Scheme.ToLowerInvariant();
        var host = request.Host.Host.ToLowerInvariant();
        var port = request.Host.Port;
        var defaultPort = scheme == "https" ? 443 : 80;
        var authority = port is { } p && p != defaultPort ? $"{host}:{p}" : host;
        return $"{scheme}://{authority}{path}";
    }
}
