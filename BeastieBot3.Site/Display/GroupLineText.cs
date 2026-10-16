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
        Statuses from the EPBC Act and the US Endangered Species Act are written with {{EPBC status}} and {{ESA status}}; statuses from other lists are plain text, with no reference.

        Only IUCN Red List taxa are matched to the chosen list, so “Only taxa on this list” leaves out species from the Catalogue of Life and Wikidata. A taxon with more than one status in the chosen list, such as “Vulnerable (breeding)” and “Least Concern (non-breeding)”, stays while any of its statuses is ticked.
        """;

    // What the chosen list does (ListLineOptions.OtherOnLines, OtherOnly).
    public const string OtherOnLines = "Add the status to each line";
    public const string OtherOnly = "Only taxa on this list";
    /// The hidden legend of the checkboxes of the chosen list's statuses (OtherStatusFilter).
    public const string OtherStatusesLabel = "Statuses in this list";

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
