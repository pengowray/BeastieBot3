using BeastieBot3.Shared.SiteData;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Web;

/// Output cache policies. Every cached response carries the tag DatabaseTag, so SiteDatabase can
/// evict them all when the database file is replaced.
public static class SiteCachePolicies {
    /// Output cache policy of the taxon pages.
    public const string Species = "species";

    /// Output cache policy of the group pages.
    public const string Group = "group";

    /// Output cache policy of the search page.
    public const string Search = "search";

    /// Output cache policy of /api/suggest.
    public const string Suggest = "suggest";

    /// Tag on every cached response.
    public const string DatabaseTag = "site-db";

    /// The query parameters a taxon page reads (Species.cshtml.cs and WikitextOptions).
    public static readonly string[] SpeciesQueryKeys = ["assessment", "authors", "fullnames", "access", "opts", "ref", "refname", "amp", "q", Pages.IucnReference.QueryKey];

    private static readonly TimeSpan Lifetime = TimeSpan.FromHours(1);

    public static void AddSitePolicies(this OutputCacheOptions options) {
        // Taxon pages are the same for everyone with the same options. Only the parameters the page
        // reads are part of the key, so made-up parameters cannot fill the cache with copies. "Today" as
        // the access date makes a page depend on the date too (the server's UTC date).
        options.AddPolicy(Species, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery(SpeciesQueryKeys)
            .VaryByValue(_ => new KeyValuePair<string, string>("utc-day", DateTime.UtcNow.ToString("yyyy-MM-dd"))));

        // Group pages vary by the list options and by what picks one of two groups with the same name.
        options.AddPolicy(Group, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([.. Lists.GroupListQuery.Keys, .. Lists.SpeciesTableQuery.Keys, "kingdom", "parent"]));

        // The search page shows the query as typed (heading, title, search box), so the key is the
        // query with whitespace collapsed, case kept: two spellings that differ only in case would
        // otherwise be served one visitor's spelling. Only 200 responses are stored, never the
        // redirect to a taxon page.
        options.AddPolicy(Search, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([])
            .VaryByValue(context => new KeyValuePair<string, string>("q", SiteEndpoints.QueryText(context.Request)))
            .VaryByValue(context => new KeyValuePair<string, string>("all", SiteEndpoints.FirstQueryValue(context.Request, "all") == "1" ? "1" : "0")));

        // Suggestions never repeat the query, so spellings that fold to the same text (case,
        // accents, spaces) share one entry. The folded text decides the exact matches and the
        // full-text query, so it decides the response.
        options.AddPolicy(Suggest, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([])
            .VaryByValue(context => new KeyValuePair<string, string>("q", SiteNameKey.Fold(SiteEndpoints.QueryText(context.Request)))));
    }
}
