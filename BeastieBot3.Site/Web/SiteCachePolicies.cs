using BeastieBot3.Shared.SiteData;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Web;

/// Output cache policies. Every cached response carries the tag DatabaseTag, so SiteDatabase can
/// evict them all when the database file is replaced.
public static class SiteCachePolicies {
    /// Output cache policy of the taxon pages.
    public const string Species = "species";

    /// Output cache policy of the wikitext pages of taxa.
    public const string SpeciesWikitext = "species-wikitext";

    /// Output cache policy of the Wikidata pages of taxa.
    public const string SpeciesWikidata = "species-wikidata";

    /// Output cache policy of the group pages.
    public const string Group = "group";

    /// Output cache policy of the list pages of groups.
    public const string GroupList = "group-list";

    /// Output cache policy of the search page.
    public const string Search = "search";

    /// Output cache policy of /api/suggest.
    public const string Suggest = "suggest";

    /// Tag on every cached response.
    public const string DatabaseTag = "site-db";

    /// The query parameters a taxon's wikitext page reads (SpeciesWikitext.cshtml.cs and WikitextOptions).
    public static readonly string[] SpeciesQueryKeys = ["assessment", "authors", "fullnames", "access", "opts", "ref", "refname", "amp", Pages.IucnReference.QueryKey, Pages.WikitextOptions.GreenStatusYearKey, Pages.WikitextOptions.WikiKey];

    private static readonly TimeSpan Lifetime = TimeSpan.FromHours(1);

    public static void AddSitePolicies(this OutputCacheOptions options) {
        // Taxon pages vary by the search text (q) that the line about the search repeats. The wikitext
        // options are in the key too: the page sends an address with them to the wikitext page, and
        // without them in the key such an address would get the cached page instead.
        options.AddPolicy(Species, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([.. SpeciesQueryKeys, "q"]));

        // Wikitext pages are the same for everyone with the same options. Only the parameters the page
        // reads are part of the key, so made-up parameters cannot fill the cache with copies. "Today" as
        // the access date makes a page depend on the date too (the server's UTC date).
        options.AddPolicy(SpeciesWikitext, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery(SpeciesQueryKeys)
            .VaryByValue(_ => new KeyValuePair<string, string>("utc-day", DateTime.UtcNow.ToString("yyyy-MM-dd"))));

        // Wikidata pages vary only by the assessment shown: they have no options, and nothing on them
        // depends on the date.
        options.AddPolicy(SpeciesWikidata, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery(["assessment"]));

        // Group pages vary by what picks one of two groups with the same name and by the search text
        // (q) that the link back to the search results repeats. The list options are in the key too:
        // the page sends an address with them to the list page.
        options.AddPolicy(Group, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([.. Lists.GroupListQuery.Keys, .. Lists.SpeciesTableQuery.Keys, "kingdom", "parent", "q"]));

        // List pages vary by the list options and by what picks one of two groups with the same name.
        // The list references give today's date as the access date.
        options.AddPolicy(GroupList, policy => policy
            .Expire(Lifetime)
            .Tag(DatabaseTag)
            .SetVaryByQuery([.. Lists.GroupListQuery.Keys, .. Lists.SpeciesTableQuery.Keys, "kingdom", "parent"])
            .VaryByValue(_ => new KeyValuePair<string, string>("utc-day", DateTime.UtcNow.ToString("yyyy-MM-dd"))));

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
