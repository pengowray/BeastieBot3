namespace BeastieBot3.Site.Display;

// The species page's comparison of ranks in IUCN, on this site, in the Catalogue of Life, Wikidata, English Wikipedia and Wikispecies.

public static partial class SiteText {
    public const string HeadingComparativeClassification = "Classification";

    public static string RanksIntro(int differences) =>
        (differences == 0 ? "Every main rank has the same name as in IUCN's classification." : differences == 1
            ? "1 main rank has another name than in IUCN's classification (marked ≠)."
            : $"{differences} main ranks have another name than in IUCN's classification (marked ≠).");

    /// The box that shows the hidden rows above order (not kingdom, phylum or class, which are
    /// always shown): "Show the 3 other ranks above order". rank: order, else family or genus.
    public static string RanksShowAbove(int rows, string rank) =>
        rows == 1 ? $"Show the other rank above {rank}" : $"Show the {rows} other ranks above {rank}";

    public const string RanksIucn = "IUCN";
    public const string RanksCol = "Catalogue of Life";
    public const string RanksWikidata = "Wikidata";
    public const string RanksWikipedia = "English Wikipedia";
    public const string RanksWikipediaHelp = "The taxobox of the taxon's English Wikipedia article";
    public const string RanksWikispecies = "Wikispecies";
    public const string RanksWikispeciesHelp = "The Taxonavigation section of the taxon's Wikispecies page";
    public const string RanksNoRank = "no rank";

    public const string RanksDiffersTitle = "Another name than in IUCN's classification";

    public static string RankRowLabel(string rank) => char.ToUpperInvariant(rank[0]) + rank[1..];
}
