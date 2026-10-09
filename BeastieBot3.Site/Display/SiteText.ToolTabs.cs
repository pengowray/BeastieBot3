namespace BeastieBot3.Site.Display;

// The tool tabs and the pages behind them: /tools, /cite, /update-statuses, /species-list-maker.

public static partial class SiteText {
    public const string ToolTabsLabel = "Tools";

    public const string ToolCite = "Cite an assessment";
    public const string ToolCiteLine = "Enter a species name or IUCN taxon ID to get the {{cite iucn}} citation, taxobox status and Wikidata references for the species.";
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
    /// ListMakerUpdateBefore + link "Update IUCN statuses in wikitext" + ListMakerUpdateAfter.
    public const string ListMakerUpdateBefore = "To update a list that is already on Wikipedia, use ";
    public const string ListMakerUpdateAfter = ".";
}
