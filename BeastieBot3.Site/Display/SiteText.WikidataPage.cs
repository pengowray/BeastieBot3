namespace BeastieBot3.Site.Display;

// The Wikidata page of a taxon (/species/{id}/wikidata): the assessment's Wikidata item, the
// QuickStatements commands for it, and the taxon item's IUCN status (P141).

public static partial class SiteText {
    public const string WikidataPageLabel = "Wikidata references";
    /// The link to the Wikidata page: "Wikidata references for " and the taxon's name.
    public const string WikidataPageFor = "Wikidata references for ";
    public static string WikidataPageTitle(string name) => WikidataPageFor + name;
    public const string WikidataPageHelp =
        "Find the Wikidata item of any IUCN assessment of this taxon, and get QuickStatements commands to create the item, add its missing statements, or update the taxon item's IUCN conservation status (P141).";

    public const string HeadingWikidataItem = "Wikidata item of the assessment";

    // The tools page's entry.
    public const string ToolsWikidataHeading = "Wikidata references for a taxon";
    public const string ToolsWikidataText =
        "The Wikidata item of any IUCN assessment of a taxon, with QuickStatements commands that create the item or add what it lacks, and the taxon item's IUCN conservation status (P141) with the commands that bring it up to date.";

    public static string WikidataChooseAssessment(string? version) =>
        (version is null ? "No current assessment in the IUCN Red List." : $"No current assessment in IUCN Red List version {version}.")
        + " For the Wikidata item and QuickStatements commands of an assessment, select “Show Wikidata item” in its row below.";

    public static string WikidataEarlierAssessment(string category, int? year, string? versionNote = null) =>
        (year is null ? $"Wikidata item of an earlier assessment: {category}." : $"Wikidata item of an earlier assessment: {category}, published {year}.")
        + WithNote(versionNote);
    public static string WikidataRegionalAssessment(string region, string category, int? year, string? versionNote = null) =>
        (year is null ? $"Wikidata item of {RegionalName(region)}: {category}." : $"Wikidata item of {RegionalName(region)}: {category}, published {year}.")
        + WithNote(versionNote);
    public const string ShowLatestWikidata = "Show the item of the latest assessment";

    // The assessment tables' column on the Wikidata page.
    public const string ColWikidata = "Wikidata";
    public const string ShowWikidata = "Show Wikidata item";
    public static string ShowWikidataAccessible(string? region, int? year, string? versionNote = null) {
        var assessment = region is null ? "the assessment" : RegionalName(region);
        var text = year is null ? $"Show the Wikidata item of {assessment}" : $"Show the Wikidata item of {assessment} published in {year}";
        return versionNote is null ? text : $"{text} ({char.ToLowerInvariant(versionNote[0])}{versionNote[1..]})";
    }
    public static string ShowWikidataOtherIdAccessible(long taxonId, int? year, string? versionNote = null) {
        var text = year is null
            ? $"See IUCN id {taxonId} for the Wikidata item of the assessment"
            : $"See IUCN id {taxonId} for the Wikidata item of its {year} assessment";
        return versionNote is null ? text : $"{text} ({char.ToLowerInvariant(versionNote[0])}{versionNote[1..]})";
    }
}
