using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Display;

// The words of the bullet list options for the taxon authority and the references after each line
// (ListLineOptions, _ListLineOptions.cshtml). The reference choices reuse the species tables' labels
// in GroupText where they fit.

public static class GroupLineText {
    public const string AuthorityLegend = "Taxon authority";
    public const string AuthorityNone = "None";
    public const string AuthoritySmall = "Small text";
    public const string AuthoritySmallExample = "Panthera leo <small>(Linnaeus, 1758)</small>";
    public const string AuthorityPlain = "Normal text";
    public const string AuthorityPlainExample = "Panthera leo (Linnaeus, 1758)";
    public const string AuthorityHelp = "Authorities are from the IUCN Red List or Catalogue of Life. Species found only in Wikidata have none.";

    public const string ReferencesInline = "Full citation in each line";
    public const string ReferencesHelp = "Each line cites its source: the IUCN assessment, the Catalogue of Life page or the Wikidata item. Red List taxa with no global assessment have no reference.";
    public const string ReferencesWikidataHelp = "English Wikipedia does not accept Wikidata as a reliable source, so replace Wikidata references before publishing.";

    public static string TooLongWithReferences(int taxa, int max) =>
        $"Too many taxa: {SiteFormat.Number(taxa)}. Lists with references have at most {SiteFormat.Number(max)} taxa, about the number of lines that fit on one Wikipedia page with room for the rest of the article. Choose “No references”, fewer Red List categories, or a smaller taxon from the table above.";

    // The status in another list after each line (ListLineOptions.OtherList, OtherStatusLineEnd).
    public const string OtherListLegend = "Other conservation status";
    public const string OtherListNone = "None";
    public const string OtherListHelp = """
        “Add the status to each line” adds each taxon's status in the chosen list to the end of its line in a bulleted list, after {{IUCN status}} and any reference. Statuses from the EPBC Act and the US Endangered Species Act use {{EPBC status}} and {{ESA status}}; statuses from other lists are plain text, for example (Great Britain Red List: Near Threatened). No reference is added for statuses from the chosen list.

        “Only taxa on this list” limits the wikitext to the taxa that have a status in the chosen list and are in a category ticked under “Red List categories”. The counts in “Red List categories” then include only taxa on the chosen list.

        “Both” limits the wikitext to the same taxa as “Only taxa on this list”, and adds each taxon's status in the chosen list to the end of its line.

        Under “Statuses in this list”, a taxon stays in the wikitext while at least one of its statuses in the chosen list is ticked, so a bird with “Vulnerable (breeding)” and “Least Concern (non-breeding)” on the Great Britain Red List stays while either status is ticked. National red lists give “Not Evaluated” or “Not Applicable” to introduced species such as fallow deer and American mink; untick those statuses under “Statuses in this list” to leave those species out. The number after each status counts the taxa with that status in the ticked Red List categories, whether or not the status itself is ticked. A status you untick stays unticked on other groups' list pages with the same options, and when you choose another list that has the same status.

        Only IUCN Red List taxa are matched to the chosen list. “Only taxa on this list” and “Both” leave out species from the Catalogue of Life and Wikidata (“Species sources”), and a taxon that is on the chosen list but not on the IUCN Red List is never in the wikitext.

        Species tables never show statuses from the chosen list. With species tables, “Add the status to each line” gives the same tables as “None”, and “Both” gives the same tables as “Only taxa on this list”.
        """;

    // What the chosen list does (ListLineOptions.OtherUse).
    public const string OtherUseLine = "Add the status to each line";
    public const string OtherUseOnly = "Only taxa on this list";
    public const string OtherUseBoth = "Both";
    /// Above the checkboxes of the chosen list's statuses (OtherStatusFilter).
    public const string OtherStatusesLabel = "Statuses in this list";
    /// Under the choices, shown only for species tables.
    public const string OtherListTablesNote = "Species tables never show statuses from the chosen list.";

    /// The note above the wikitext for the chosen list's use.
    public static string OtherListNote(OtherListUse use, int taxa, string heading) => use switch {
        OtherListUse.OnlyListed => OnlyListedNote(taxa, heading),
        OtherListUse.Both => BothNote(taxa, heading),
        _ => OtherListCount(taxa, heading),
    };

    /// Above the wikitext with "Only taxa on this list" (taxa is at least 1: an empty list has no wikitext).
    public static string OnlyListedNote(int taxa, string heading) => taxa == 1
        ? $"{heading}: the 1 taxon in the wikitext has a status in this list."
        : $"{heading}: all {SiteFormat.Number(taxa)} taxa in the wikitext have a status in this list.";

    /// Above the wikitext with "Both" (taxa is at least 1).
    public static string BothNote(int taxa, string heading) => taxa == 1
        ? $"{heading}: the 1 taxon in the wikitext has a status in this list, added at the end of its line."
        : $"{heading}: all {SiteFormat.Number(taxa)} taxa in the wikitext have a status in this list, added at the end of their lines.";

    /// Above the wikitext: "EPBC Act: 85 taxa in the wikitext have a status in this list, added at the end of their lines."
    public static string OtherListCount(int taxa, string heading) => taxa switch {
        0 => $"{heading}: none of the taxa in the wikitext has a status in this list.",
        1 => $"{heading}: 1 taxon in the wikitext has a status in this list, added at the end of its line.",
        _ => $"{heading}: {SiteFormat.Number(taxa)} taxa in the wikitext have a status in this list, added at the end of their lines.",
    };

    /// The end of a list line: " (EPBC Act: {{EPBC status|VU}})", " (Great Britain Red List: Near
    /// Threatened)"; label: OtherStatusChoice.LineLabel; statuses: joined by OtherStatusLineEnd.
    public static string OtherStatusLineEnd(string label, string statuses) => $" ({label}: {statuses})";
}
