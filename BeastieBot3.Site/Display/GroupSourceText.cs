using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Display;

// The words of the list option for species sources (IUCN, the Catalogue of Life, Wikidata) and of the
// notices about possible duplicates on a group page (GroupListSources, ListSourceMerge).

public static class GroupSourceText {
    public const string Legend = "Species sources";
    public const string SourceIucn = "IUCN Red List";
    public const string SourceCol = "Catalogue of Life (CoL)";
    public const string SourceWikidata = "Wikidata";
    public const string OrderLabel = "Order of preference";
    public const string Help = "Species from Catalogue of Life or Wikidata that are not on the IUCN Red List are listed under Not Evaluated (NE). They are added only under genera or families that IUCN has. If no box is ticked, the list uses the IUCN Red List only.";
    public const string OrderHelp = "When sources spell a species’ name differently, the list uses the spelling from the first ticked source in this order. When two entries are likely the same species, the list keeps the entry from the source that comes first.";

    /// "IUCN", "CoL", "Wikidata".
    public static string ShortName(ListSource source) => source switch {
        ListSource.Col => "CoL",
        ListSource.Wikidata => "Wikidata",
        _ => "IUCN",
    };

    /// "IUCN, then CoL, then Wikidata".
    public static string OrderOption(IReadOnlyList<ListSource> order) => string.Join(", then ", order.Select(ShortName));

    // The list
    public static string ExtraCountSuffix(int species) => species switch {
        0 => string.Empty,
        1 => ", including 1 species from CoL or Wikidata that is not on the IUCN Red List",
        _ => $", including {SiteFormat.Number(species)} species from CoL or Wikidata that are not on the IUCN Red List",
    };
    public const string TooLongNote = "This count includes likely duplicates, so a list with these options could have fewer lines.";
    public const string PreviewNote = "For species that are not on the IUCN Red List, small “CoL” and “Wikidata” links after the line go to the species’ Catalogue of Life page and Wikidata item. These links are in the preview only, not in the wikitext.";
    public const string PreviewColLink = "CoL";
    public const string PreviewWikidataLink = "Wikidata";

    // Notices
    public const string NoticesHeading = "Possible duplicates";
    public static string LeftOutHeading(int count) => $"Left out of the list ({SiteFormat.Number(count)})";
    public static string KeptHeading(int count) => $"Kept in the list ({SiteFormat.Number(count)})";
    public const string LikelySame = "likely the same species as";
    public const string MaySame = "may be the same species";
    public const string NotInList = "not in this list";

    public static string Reason(string reason) => reason switch {
        "iucn-synonym" => "IUCN lists one name as a synonym of the other.",
        "col-synonym" => "Catalogue of Life lists one name as a synonym of the other.",
        "wikidata-synonym" => "Wikidata lists one name as a taxon synonym of the other.",
        "gender-ending" => "Same genus, and the epithets differ only in the Latin gender ending.",
        "spelling" => "Same genus, and the epithets differ by one or two letters.",
        "other-genus" => "Same epithet in another genus of the same family. The species may have been moved to another genus.",
        _ => reason,
    };
}
