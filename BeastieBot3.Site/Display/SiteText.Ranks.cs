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

    /// The line that opens the collapsed clades and groups with no rank of one cell.
    public static string RanksFolded(int clades, int unranked) => (clades, unranked) switch {
        (_, 0) => clades == 1 ? "1 clade" : $"{clades} clades",
        (0, _) => unranked == 1 ? "1 group with no rank" : $"{unranked} groups with no rank",
        _ => $"{clades} {(clades == 1 ? "clade" : "clades")} and {unranked} {(unranked == 1 ? "group" : "groups")} with no rank",
    };
    public const string RanksDiffersTitle = "Another name than in IUCN's classification";

    public static string RankRowLabel(string rank) => rank == ClassificationComparison.AboveKingdom
        ? "Above kingdom"
        : char.ToUpperInvariant(rank[0]) + rank[1..];
}
