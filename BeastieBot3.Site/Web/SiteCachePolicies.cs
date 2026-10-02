namespace BeastieBot3.Site.Web;

public static class SiteCachePolicies {
    /// Output cache policy of the taxon pages (Program.cs).
    public const string Species = "species";

    /// The query parameters a taxon page reads (Species.cshtml.cs and WikitextOptions).
    public static readonly string[] SpeciesQueryKeys = ["assessment", "authors", "access", "opts", "ref", "refname", "amp", "q"];
}
