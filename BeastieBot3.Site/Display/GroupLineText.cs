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
    public const string OtherListLegend = "Status in another list";
    public const string OtherListNone = "None";
    public const string OtherListHelp = "Adds each taxon's status in the chosen list at the end of its line, after {{IUCN status}} and any reference. The EPBC Act and the US Endangered Species Act are written with their templates, {{EPBC status}} and {{ESA status}}; other lists as text. Bulleted lists only.";
    public static string OtherListCount(int taxa, string heading) =>
        $"{SiteFormat.Number(taxa)} {(taxa == 1 ? "taxon" : "taxa")} in this list {(taxa == 1 ? "has" : "have")} a status in {heading}.";
    /// The end of a list line: " (EPBC Act: {{EPBC status|VU}})".
    public static string OtherStatusLineEnd(string heading, string statuses) => $" ({heading}: {statuses})";
}
