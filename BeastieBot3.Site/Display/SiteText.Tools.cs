namespace BeastieBot3.Site.Display;

// The tools: the Tools menu in the header, the tools page (/tools), the Tools section at the bottom
// of taxon and group pages, and the headers of the wikitext and list pages.

public static partial class SiteText {
    public const string HeadingTools = "Tools";
    public const string NavToolsAll = "All tools";

    public const string ToolsIntro = "Tools for adding IUCN Red List statuses and citations to Wikipedia and Wikidata.";

    public const string ToolUpdateLink = "Update IUCN statuses in wikitext";
    public const string ToolUpdateHelp = "Paste a Wikipedia page or list, or give its title, to bring its statuses up to date.";
    public const string ToolsUpdateText =
        "Paste the wikitext of a Wikipedia article or list, or give its title. The page updates {{IUCN status}} templates, species tables and taxobox status lines to the latest assessments, and lists the taxa a list is missing.";

    public const string ToolWikitextLink = "Wikitext and citations";
    public const string ToolWikitextHelp = "{{cite iucn}}, {{IUCN status}} and taxobox status lines for any of this taxon's assessments, and Wikidata commands.";
    public const string ToolsWikitextText =
        "{{cite iucn}}, {{IUCN status}} and taxobox status lines for any assessment of a taxon, with citation options, a {{cite Q}} citation and QuickStatements commands for Wikidata.";
    public const string ToolsWhereTaxon = "Find the taxon, then follow \"Wikitext and citations\" in the Tools section at the bottom of its page.";

    public const string ToolsListHeading = "Wikipedia lists of a group";
    public const string ToolsListText =
        "A bulleted list or species tables of the taxa in a family, order or other group, with headings and categories to choose.";
    public const string ToolsWhereGroup = "Open the group's page from a taxon page's classification, then follow the list link in its Tools section.";
    public const string ToolsExample = "Example:";

    /// The list link in the Tools section of a taxon page: the list page of its genus.
    public static string ToolGenusListLink(string genus) => $"Wikipedia list of the genus {genus}";
    public const string ToolGenusListHelp = "The genus's species as a list or species tables, with options.";

    // The wikitext page of a taxon.
    public static string WikitextPageTitle(string scientificName) => $"Wikitext for {scientificName}";
    public const string WikitextPageLabel = "Wikitext and citations";
    public const string BackToTaxonPage = "Back to the taxon page";
    public const string WikitextChooseAssessment = "Choose an assessment in the tables below to get its wikitext.";
}
