namespace BeastieBot3.Site.Display;

// The tools: the Tools menu in the header, the row of tool tabs and the pages behind them (/tools,
// /cite, /update-statuses, /species-list-maker), and the tool pages of a taxon: its wikitext page
// (/species/{id}/wikitext) and its Wikidata page (/species/{id}/wikidata). The update page's own
// strings are in UpdateText, and a group's list page's in GroupText.

public static partial class SiteText {
    // The Tools menu, the tool tabs and /tools: one line per tool.
    public const string HeadingTools = "Tools";
    public const string ToolTabsLabel = "Tools";
    public const string ToolCite = "Cite an assessment";
    public const string ToolCiteLine = "Enter a species name or IUCN taxon ID to get the {{cite iucn}} citation, taxobox status and Wikidata references for the species.";
    public const string ToolUpdateLink = "Update IUCN statuses";
    public const string ToolUpdateHelp = "Paste the wikitext of a Wikipedia article or list, or enter its title, to bring its IUCN statuses up to date.";
    public const string ToolUpdateLine = "Paste the wikitext or address of an English Wikipedia article or list to update the IUCN statuses in it.";
    public const string ToolListMaker = "Make a species list";
    public const string ToolListMakerLine = "Enter a genus, family or order to get its species as a Wikipedia list or species tables.";

    // /cite
    public static string CiteResultsTitle(string query) => $"Cite {query}";
    public const string CiteInputLabel = "Common name, scientific name or IUCN ID";
    public const string CitePlaceholder = "Polar bear, Ursus maritimus, 22823";
    public const string CiteButton = "Cite";
    public const string CiteForLegend = "Cite for";
    public const string CiteForWikipedia = "Wikipedia";
    public const string CiteForWikidata = "Wikidata";
    public const string CiteChooseTaxon = "Choose a taxon";
    public static string CiteNothingFound(string query) => $"No taxon found for “{query}”.";

    // /species-list-maker
    public const string ListMakerInputLabel = "Genus, family, order or other group";
    public const string ListMakerPlaceholder = "Felidae, cats, Ursus";
    public const string ListMakerButton = "Make list";
    public const string ListMakerChooseGroup = "Choose a group";
    public static string ListMakerNothingFound(string query) => $"No group found for “{query}”.";
    /// When the text names a species: ListMakerSpeciesBefore + the species + ListMakerSpeciesAfter, then its groups.
    public const string ListMakerSpeciesBefore = "";
    public const string ListMakerSpeciesAfter = " is a species. Make a list of its genus, family or order:";
    /// ListMakerUpdateBefore + link ToolUpdateLink + ListMakerUpdateAfter.
    public const string ListMakerUpdateBefore = "To update a list that is already on Wikipedia, use ";
    public const string ListMakerUpdateAfter = ".";

    // The wikitext page of a taxon.
    /// The link to it: "Wikitext and citations for " and the taxon's name.
    public const string ToolWikitextFor = "Wikitext and citations for ";
    public const string ToolWikitextHelp =
        "{{cite iucn}}, {{IUCN status}} and taxobox status lines for any IUCN assessment of this taxon.";
    public static string WikitextPageTitle(string name) => ToolWikitextFor + name;
    public const string WikitextPageLabel = "Wikitext and citations";
    /// The link back to the taxon page: "Taxon page for " and the taxon's name.
    public const string TaxonPageFor = "Taxon page for ";
    public static string WikitextChooseAssessment(string? version) =>
        (version is null ? "No current assessment in the IUCN Red List." : $"No current assessment in IUCN Red List version {version}.")
        + " To get wikitext, select “Show wikitext” beside one of the assessments below.";

    // The Wikidata page of a taxon: the assessment's Wikidata item, the QuickStatements commands for
    // it, and the taxon item's IUCN status (P141).
    public const string WikidataPageLabel = "Wikidata references";
    /// The link to the Wikidata page: "Wikidata references for " and the taxon's name.
    public const string WikidataPageFor = "Wikidata references for ";
    public static string WikidataPageTitle(string name) => WikidataPageFor + name;
    public const string WikidataPageHelp =
        "Find the Wikidata item of any IUCN assessment of this taxon, and get QuickStatements commands to create the item, add its missing statements, or update the taxon item's IUCN conservation status (P141).";

    public const string HeadingWikidataItem = "Wikidata item of the assessment";

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
