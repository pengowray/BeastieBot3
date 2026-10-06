namespace BeastieBot3.Site.Display;

// The words of the group page's species tables ({{Species table}}) and its list type choice.

public static partial class GroupText {
    // List type
    public const string OptionListType = "List type";
    public const string ListTypeBullets = "Bulleted list";
    public const string ListTypeTables = "Species tables, one per genus";
    public const string ListTypeTablesExample = "{{Species table}}";
    public const string ListTypeTablesHelp =
        "Species tables, one per genus, list species only, without subspecies or varieties. This site has no data for image, range, size, habitat or diet.";

    // Table options
    public const string OptionTables = "Table options";
    public const string SpeciesCountHelp =
        "The species count in each table caption is the number of species in the genus on the Red List, including species in Red List categories you have not ticked.";
    public const string OptionReferences = "References";
    public const string ReferencesNone = "No references";
    public const string ReferencesListDefined = "List-defined references";
    public const string ReferencesInline = "Full citation in each row";
    public const string ReferencesHelp =
        "Rows without a reference have no link to the IUCN assessment. List-defined references are in one {{reflist|refs=}} at the end of the wikitext; you can move them to the article's References section.";
    public const string OptionRefNames = "Ref names";
    public const string RefNamesCommon = "IUCN and the English name (IUCNBananaserotine)";
    public const string RefNamesId = "iucn- and the IUCN taxon ID (iucn-44923)";
    public const string RefNamesSci = "Scientific name (Afronycteris nanus)";
    public const string RefNamesHelp = "For a species with no English name, the first kind of ref name is IUCN and the scientific name.";
    public const string OptionCitationTemplate = "Citation template";
    public const string CitationTemplateIucn = "{{cite iucn}}";
    public const string CitationTemplateQ = "{{cite Q}} if the assessment has a Wikidata item, otherwise {{cite iucn}}";
    public const string OptionColumns = "Columns";
    public const string ColumnsAll = "All columns, with image, range, size, habitat and diet blank";
    public const string ColumnsNoDiet = "No Diet line (no-diet=yes)";
    public const string ColumnsNoEcology = "No Size and ecology column (no-ecology=yes)";
    public const string OptionSummary = "{{IUCN statuses}} box at the top (species count for each Red List category)";

    // Table output
    public static string TableSize(int species, int tables) =>
        $"{SiteFormat.Number(species)} species in {SiteFormat.Number(tables)} {(tables == 1 ? "table" : "tables")}";
    public static string TablesTooLong(int count, int max, bool withReferences) => withReferences
        ? $"Too many species: {SiteFormat.Number(count)}. Tables with references have at most {SiteFormat.Number(max)} rows, about the number that fit on one Wikipedia page. Choose fewer Red List categories, a smaller taxon from the table above, no references, or a bulleted list."
        : $"Too many species: {SiteFormat.Number(count)}. Tables without references have at most {SiteFormat.Number(max)} rows, about the number that fit on one Wikipedia page. Choose fewer Red List categories, a smaller taxon from the table above, or a bulleted list.";
    public const string TablePreviewNote =
        "Simplified preview without the Range and Size and ecology columns. Each status code links to its Wikipedia article and to the assessment on the IUCN Red List website.";
    public const string CopyTablesAccessible = "Copy the tables wikitext";
}
