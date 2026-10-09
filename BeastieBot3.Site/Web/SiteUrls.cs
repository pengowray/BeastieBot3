namespace BeastieBot3.Site.Web;

public static class SiteUrls {
    /// A group page: "/taxa/family/felidae", with "?kingdom=plantae" or "?parent=Moraceae" when
    /// another group has the same rank and name. listQuery is the list options ("?style=sci&h=family"
    /// or empty), joined to that.
    public static string Group(Data.GroupRow group, string listQuery = "") {
        var url = $"/taxa/{Uri.EscapeDataString(group.Rank)}/{Uri.EscapeDataString(group.Name.ToLowerInvariant())}";
        var query = listQuery.TrimStart('?');
        if (group.LinkQuery is { } pick) {
            query = query.Length == 0 ? pick : pick + "&" + query;
        }
        return query.Length == 0 ? url : url + "?" + query;
    }

    /// The list page of a group: "/taxa/family/felidae/list", with the pick of Group and the list options.
    public static string GroupList(Data.GroupRow group, string listQuery = "") {
        var url = Group(group);
        var q = url.IndexOf('?');
        var path = q < 0 ? url : url[..q];
        var query = q < 0 ? "" : url[(q + 1)..];
        var options = listQuery.TrimStart('?');
        query = options.Length == 0 ? query : query.Length == 0 ? options : query + "&" + options;
        return path + "/list" + (query.Length == 0 ? "" : "?" + query);
    }

    /// The page of a species from the Catalogue of Life or Wikidata that is not on the IUCN Red List:
    /// "/col/4QHKG" when the Catalogue of Life has it, else "/wikidata/Q1003".
    public static string Extra(Lists.ExtraSpeciesRow species) => species.ColId is { } colId
        ? "/col/" + Uri.EscapeDataString(colId)
        : "/wikidata/" + Uri.EscapeDataString(species.WikidataQid ?? string.Empty);

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
