namespace BeastieBot3.Site.Display;

// The words of the group pages (/taxa/{rank}/{name}), the list options and the classification on
// taxon pages. Kept apart from SiteText so each file stays short enough to review.

public static class GroupText {
    // Classification on a taxon page
    public const string ShowColGroups = "Show Catalogue of Life groups";
    public const string ColMarker = "CoL";
    public const string ColMarkerTitle = "Group from the Catalogue of Life, between two of IUCN's ranks";
    public const string RuleMarker = "not assigned by IUCN";
    public const string RuleMarkerTitle = "IUCN gives this rank as NOT ASSIGNED. This value comes from the Catalogue of Life and is the one the Wikipedia lists use.";
    public const string ColNamesTitle = "English names from the Catalogue of Life";

    // Group page: heading and facts
    public static string Title(string rankHeading, string? commonName) =>
        commonName is null ? rankHeading : $"{rankHeading} ({commonName})";
    public const string HeadingClassification = "Classification";
    public const string LabelSpecies = "Species";
    public const string LabelInfra = "Subspecies and varieties";
    public const string LabelSubpopulations = "Subpopulations";
    public const string LabelThreatened = "Threatened species";
    public const string LabelExtinct = "Extinct species";
    public const string ThreatenedNote = "Critically Endangered, Endangered or Vulnerable";
    public const string ExtinctNote = "Extinct or Extinct in the Wild";
    public const string HeadingCategories = "Species by category";
    public const string CategoriesNote = "Latest global assessment of each species.";
    public const string ColumnCategory = "Category";
    public const string ColumnSpecies = "Species";
    public const string ColumnInfra = "Subspecies and varieties";
    public const string HeadingOtherNames = "English names from the Catalogue of Life";
    public const string HeadingLinks = "Links";
    public const string LinkWikipedia = "English Wikipedia";
    public const string LinkCol = "Catalogue of Life";
    public const string ColGroupNote = "This group is from the Catalogue of Life. IUCN does not use it; it is shown because nearly all the species IUCN puts in the IUCN group above it are in it.";
    public const string RuleGroupNote = "IUCN gives this rank as NOT ASSIGNED for these species. The site uses the value the Wikipedia lists use.";

    // Group page: groups inside
    public static string HeadingChildren(int count) => count == 1 ? "1 group in it" : $"{SiteFormat.Number(count)} groups in it";
    public const string ColumnGroup = "Group";
    public const string ColumnEnglishName = "English name";
    public const string ColumnThreatened = "Threatened";
    public const string ColumnExtinct = "Extinct";

    // Group page: two or more groups with the rank and name
    public static string ChooseHeading(string rank, string name) => $"{rank} {name}: choose a group";
    public static string ChooseLine(int count) => $"{count} groups have this rank and name. Choose one:";
    public static string NotFoundHeading(string rank, string name) => $"No {rank.ToLowerInvariant()} named {name}";
    public const string NotFoundLine = "The site has no group with this rank and name. Search for the name instead:";

    // Group page: the list
    public const string HeadingList = "Wikipedia list";
    public static string ListSize(int lines) => lines == 1 ? "1 line" : $"{SiteFormat.Number(lines)} lines";
    public static string TooLong(int lines, int max) =>
        $"This list would have {SiteFormat.Number(lines)} lines. The site makes lists of up to {SiteFormat.Number(max)} lines, about as many {{{{IUCN status}}}} templates as one Wikipedia page can hold. Choose fewer categories, or a smaller group from the table above.";
    public const string EmptyList = "No taxa in this group match these options.";
    public static string LevelsSkipped(int count) =>
        count == 1 ? "1 heading rank is left out: Wikipedia headings go down to level 6." : $"{count} heading ranks are left out: Wikipedia headings go down to level 6.";
    public const string TabWikitext = "Wikitext";
    public const string TabPreview = "Preview";
    public const string CopyList = "Copy list";
    public const string CopyListAccessible = "Copy the list wikitext";
    public const string ListUpdated = "List updated";
    public const string ListTooManyRequests = "Too many requests: the list was not updated. Wait a minute, then select Update list.";
    public const string PreviewNote = "Names link to this site's taxon pages. In the wikitext they link to Wikipedia.";

    // List options
    public const string OptionsHeading = "List options";
    public const string OptionFormat = "Line format";
    public const string StyleSci = "Scientific name first";
    public const string StyleSciExample = "Panthera leo, lion";
    public const string StyleCommon = "Common name first";
    public const string StyleCommonExample = "Lion (Panthera leo)";
    public const string StyleCommonOnly = "Common name only";
    public const string StyleCommonOnlyExample = "Lion";
    public const string DefaultMark = "(default for this group)";
    public const string OptionHeadings = "Headings";
    public const string OptionStatusSections = "A section for each category";
    public const string OptionRankHeadings = "A heading for each:";
    public const string NoRanks = "No ranks below this group.";
    public const string ColRankNote = "Catalogue of Life";
    public const string OptionHeadingNames = "English name under each heading";
    public const string OptionTopLevel = "Top heading level";
    public const string OptionCategories = "Categories";
    public const string OptionTaxa = "Taxa";
    public const string InfraNone = "Species only";
    public const string InfraSeparate = "Subspecies and varieties after the species";
    public const string InfraUnder = "Subspecies and varieties under their species";
    public const string OptionSubpopulations = "Subpopulations";
    public const string OptionSort = "Order of names";
    public const string SortFirst = "By the first name on each line";
    public const string SortSci = "By scientific name";
    public const string SortCommon = "By English name";
    public const string OptionTemplate = "{{IUCN status}} after each name";
    public const string UpdateList = "Update list";
    public const string ResetOptions = "Reset options";

    // Search results
    public const string SearchGroupsHeading = "Groups";
    public const string SearchTaxaHeading = "Taxa";
    public static string SearchGroupSpecies(int species, string kingdom) =>
        $"{SiteFormat.Number(species)} {(species == 1 ? "species" : "species")}, {SiteFormat.TitleCase(kingdom)}";
}
