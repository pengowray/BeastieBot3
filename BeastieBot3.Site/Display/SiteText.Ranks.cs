namespace BeastieBot3.Site.Display;

// The species page's comparison of ranks in IUCN, on this site, in the Catalogue of Life, Wikidata, English Wikipedia and Wikispecies.

public static partial class SiteText {
    public const string HeadingRanks = "Classification in other sources";

    public static string RanksIntro(int differences) =>
        "The taxon's ranks in each source. \"This site\" is IUCN's classification with Catalogue of Life groups between its ranks; \"English Wikipedia\" is the taxobox of the taxon's article; \"Wikispecies\" is the Taxonavigation section of the taxon's Wikispecies page. Groups between two main ranks are listed under the higher one."
        + (differences == 0 ? " Every main rank has the same name as in IUCN's classification." : differences == 1
            ? " 1 main rank has another name than in IUCN's classification (marked ≠)."
            : $" {differences} main ranks have another name than in IUCN's classification (marked ≠).");

    public const string RanksIucn = "IUCN";
    public const string RanksThisSite = "This site";
    public const string RanksCol = "Catalogue of Life";
    public const string RanksWikidata = "Wikidata";
    public const string RanksWikipedia = "English Wikipedia";
    public const string RanksWikispecies = "Wikispecies";
    public const string RanksNoRank = "no rank";

    public static string RanksUnrankedFolded(int n) => $"{n} groups with no rank";
    public const string RanksDiffersTitle = "Another name than in IUCN's classification";

    public static string RankRowLabel(string rank) => rank == ClassificationComparison.AboveKingdom
        ? "Above kingdom"
        : char.ToUpperInvariant(rank[0]) + rank[1..];
}
