namespace BeastieBot3.Site.Display;

// The tools: the Tools menu in the header, the tools page (/tools), the Tools section at the bottom
// of taxon and group pages, and the headers of the wikitext and list pages. Each tool has one name
// everywhere: "Wikitext and citations" (for a taxon), "Wikipedia list" (of a group) and
// "Update IUCN statuses in wikitext".

public static partial class SiteText {
    public const string HeadingTools = "Tools";
    public const string NavToolsAll = "All tools";

    public const string ToolsIntro = "Tools for adding IUCN Red List statuses and citations to Wikipedia and Wikidata.";

    public const string ToolUpdateLink = "Update IUCN statuses in wikitext";
    public const string ToolUpdateHelp = "Paste the wikitext of a Wikipedia article or list, or enter its title, to bring its IUCN statuses up to date.";
    public const string ToolsUpdateText =
        "Paste the wikitext of a Wikipedia article or list, or enter its title, to update its {{IUCN status}} templates, species tables and taxobox status lines to the latest assessments. For a list, the report also shows which taxa of the list's group are missing from the list.";

    /// The wikitext page of a taxon: "Wikitext and citations for " and the taxon's name.
    public const string ToolWikitextFor = "Wikitext and citations for ";
    public const string ToolWikitextHelp =
        "{{cite iucn}}, {{IUCN status}} and taxobox status lines for any IUCN assessment of this taxon, and QuickStatements commands for Wikidata.";
    public const string ToolsWikitextHeading = "Wikitext and citations for a taxon";
    public const string ToolsWikitextText =
        "{{cite iucn}}, {{IUCN status}} and taxobox status lines for any IUCN assessment of a taxon, with {{cite Q}} and QuickStatements commands for Wikidata.";
    public const string ToolsWhereTaxon =
        "Search for a taxon, then choose this tool in the Tools menu at the top of its page or in the Tools section at the bottom.";

    public const string ToolsListHeading = "Wikipedia list of a group";
    public const string ToolsListText =
        "The taxa in a genus, family, order or other group as a bulleted list or species tables, with a choice of headings, IUCN categories and references. The list can also include species from the Catalogue of Life and Wikidata that are not on the IUCN Red List.";
    public const string ToolsWhereGroup =
        "Search for the group by its scientific name, or select it in the classification at the top of a taxon page, then choose this tool in the Tools menu or in the Tools section at the bottom of the group's page.";
    public const string ToolsExample = "Example:";

    // The wikitext page of a taxon.
    public static string WikitextPageTitle(string name) => ToolWikitextFor + name;
    public const string WikitextPageLabel = "Wikitext and citations";
    /// The link back to the taxon page: "Taxon page for " and the taxon's name.
    public const string TaxonPageFor = "Taxon page for ";
    public static string WikitextChooseAssessment(string? version) =>
        (version is null ? "No current assessment in the IUCN Red List." : $"No current assessment in IUCN Red List version {version}.")
        + " To get wikitext, select “Show wikitext” beside one of the assessments below.";

    // Taxon page, names in other languages: the grey line under a name in another script.
    public const string TransliterationTitle = "Automatic transliteration into Latin letters";
}
