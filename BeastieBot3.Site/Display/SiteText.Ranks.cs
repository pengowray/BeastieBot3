namespace BeastieBot3.Site.Display;

// The species page's comparison of ranks in IUCN, on this site, in the Catalogue of Life, Wikidata, English Wikipedia and Wikispecies.

public static partial class SiteText {
    public const string HeadingRanks = "Classification in other sources";

    public static string RanksIntro(int differences) =>
        "\"Wikispecies\" is the Taxonavigation section of the taxon's Wikispecies page."
        + (differences == 0 ? " Every main rank has the same name as in IUCN's classification." : differences == 1
            ? " 1 main rank has another name than in IUCN's classification (marked ≠)."
            : $" {differences} main ranks have another name than in IUCN's classification (marked ≠).");

    public static string RanksShowMinor(int rows) => $"Show all ranks ({rows} more)";
    public const string RanksShowMinorHelp =
        "Also shows the groups that are not in IUCN's or the Catalogue of Life's classification: clades and other groups that only Wikidata, the English Wikipedia taxobox or Wikispecies has.";

    public const string RanksIucn = "IUCN";
    public const string RanksCol = "Catalogue of Life";
    public const string RanksWikidata = "Wikidata";
    public const string RanksWikipedia = "English Wikipedia taxobox";
    public const string RanksWikispecies = "Wikispecies";
    public const string RanksNoRank = "no rank";

    public const string RanksDiffersTitle = "Another name than in IUCN's classification";

    public static string RankRowLabel(string rank) => char.ToUpperInvariant(rank[0]) + rank[1..];
}
