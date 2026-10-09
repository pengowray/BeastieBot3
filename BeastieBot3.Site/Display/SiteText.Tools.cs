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

    // The tabs under a taxon's name (PageTabs.ForTaxon).
    public const string TaxonTabsLabel = "Pages for this taxon";
    public const string TabTaxonPage = "Taxon page";
    public const string TabCitations = "Citations";

    /// The Tools section at the bottom of the taxon page: "Citations for " and the taxon's name, and
    /// the help line after it.
    public const string ToolCitationsFor = "Citations for ";
    public const string ToolCitationsHelp =
        "For any IUCN assessment of this taxon: {{cite iucn}}, {{IUCN status}} and taxobox status lines for Wikipedia, and QuickStatements commands for Wikidata.";

    /// The browser title of the citations pages: "Ursus maritimus (Polar bear): Citations for Wikipedia".
    /// name: the taxon page's title, the scientific name with the common name in brackets.
    public static string CitationsPageTitle(string name, bool wikidata) => $"{name}: Citations for {(wikidata ? "Wikidata" : "Wikipedia")}";

    /// The taxon page's title: "Ursus maritimus (Polar bear)", or the scientific name alone.
    public static string TaxonTitle(string scientificName, string? commonName) =>
        commonName is null ? scientificName : $"{scientificName} ({commonName})";

    // The citations page for a Wikipedia.
    public static string WikitextChooseAssessment(string? version) =>
        (version is null ? "No current assessment in the IUCN Red List." : $"No current assessment in IUCN Red List version {version}.")
        + " To get wikitext, select “Show wikitext” beside one of the assessments below.";

    // The citations page for Wikidata: the assessment's Wikidata item, the QuickStatements commands
    // for it, and the taxon item's IUCN status (P141).
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
