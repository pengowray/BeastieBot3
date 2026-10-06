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
    public const string OtherGenera = "Species in genera not on the IUCN Red List (listed under their family)";
    public const string Help = "Species from Catalogue of Life or Wikidata that are not on the IUCN Red List are listed under Not Evaluated (NE). Each one is placed in the IUCN genus with the same name, or in its family when IUCN does not have the genus. If no box is ticked, the list uses the IUCN Red List only.";
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
    public const string TooLongNote = "This count may include species that Catalogue of Life and Wikidata list under different names, so a list with these options could have fewer lines.";
    public const string PreviewNote = "For species that are not on the IUCN Red List, small “CoL” and “Wikidata” links after the line go to the species’ Catalogue of Life page and Wikidata item. These links are in the preview only, not in the wikitext.";
    public const string PreviewColLink = "CoL";
    public const string PreviewWikidataLink = "Wikidata";

    // Notices
    public const string NoticesHeading = "Possible duplicates";
    public static string LeftOutHeading(int entries) => $"Left out of the list ({Entries(entries)})";
    public static string KeptHeading(int pairs) => $"Both entries kept ({Pairs(pairs)})";
    public const string LeftOutRule = "Of two entries that are likely the same species, the one from the source later in the order of preference is left out.";

    /// The line under the list's size that links to the panel: "Possible duplicates: 302 entries
    /// left out, 5 pairs with both entries kept". The link is on the first part (NoticesHeading).
    public static string SummaryCounts(int leftOutEntries, int keptPairs) => string.Join(", ", new[] {
        leftOutEntries > 0 ? $"{Entries(leftOutEntries)} left out" : null,
        keptPairs > 0 ? $"{Pairs(keptPairs)} with both entries kept" : null,
    }.Where(p => p is not null));

    // Column headings
    public const string ColumnLeftOut = "Left out";
    public const string ColumnLikelySame = "Likely the same species as";
    public const string ColumnInList = "In the list";
    public const string ColumnMaySame = "May be the same species as";

    /// The state of an entry, after its source: "in this list", "left out", "in another genus".
    /// groupRank: the page's IUCN rank, or null for a Catalogue of Life group (an entry outside such
    /// a group need not have a rank of that kind).
    public static string State(NoticeState state, string? groupRank) => state switch {
        NoticeState.InList => "in this list",
        NoticeState.LeftOut => "left out",
        _ => groupRank is null ? "outside this group" : $"in another {groupRank}",
    };

    /// "41 pairs: Catalogue of Life lists one name as a synonym of the other."
    public static string ReasonSummary(string reason, int pairs) => $"{Pairs(pairs)}: {Reason(reason)}";

    /// A collapsed run: "Rana names" in each column (the genus in italics, then this word), and the
    /// box that shows its pairs.
    public const string RunNamesWord = "names";
    public static string RunShow(int pairs) => $"Show {Pairs(pairs)}";

    private static string Entries(int count) => count == 1 ? "1 entry" : $"{SiteFormat.Number(count)} entries";
    private static string Pairs(int count) => count == 1 ? "1 pair" : $"{SiteFormat.Number(count)} pairs";

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
