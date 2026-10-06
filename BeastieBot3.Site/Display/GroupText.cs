namespace BeastieBot3.Site.Display;

// The words of the pages of higher taxa (/taxa/{rank}/{name}), the list options and the
// classification on taxon pages. Kept apart from SiteText so each file stays short enough to review.
// "Group" is the code's word for a higher taxon; pages say "taxon" or the rank.

public static class GroupText {
    // Classification on a taxon page
    public static string ShowColRanks(int count) =>
        count == 1 ? "Show 1 rank from the Catalogue of Life" : $"Show {count} ranks from the Catalogue of Life";
    public const string ColMarker = "CoL";
    public const string ColMarkerTitle = "From the Catalogue of Life. IUCN uses only kingdom, phylum, class, order, family and genus.";
    public const string RuleMarker = "IUCN: NOT ASSIGNED";
    public static string RuleMarkerTitle(string rank) => $"IUCN gives the {rank} as NOT ASSIGNED. This {rank} is from the Catalogue of Life.";
    public const string ColNamesTitle = "Common names from the Catalogue of Life (unchecked)";

    // Page of a higher taxon: heading and facts
    public static string Title(string rankHeading, string? commonName) =>
        commonName is null ? rankHeading : $"{rankHeading} ({commonName})";
    public const string HeadingClassification = "Classification";
    public const string LabelSpecies = "Assessed species";
    public const string LabelInfra = "Subspecies and varieties";
    public const string LabelSubpopulations = "Subpopulations";
    public const string LabelThreatened = "Threatened species";
    public const string ThreatenedNote = "Critically Endangered, Endangered or Vulnerable";
    public const string LabelExtinct = "Extinct or Extinct in the Wild";
    public const string HeadingCategories = "Species by Red List category";
    public const string CategoriesNote = "Counts use the latest global assessment of each taxon.";
    public const string ColumnCategory = "Category";
    public const string ColumnSpecies = "Species";
    public const string ColumnInfra = "Subspecies and varieties";
    public const string OtherNamesLabel = "Common names in the Catalogue of Life (unchecked):";
    public const string LinkWikipedia = "English Wikipedia";
    public const string LinkCol = "Catalogue of Life";

    /// For a taxon from the Catalogue of Life: "Suborder Feliformia is from the Catalogue of Life.
    /// IUCN does not use suborders. The site shows it inside IUCN's order Carnivora because nearly
    /// all of its species are in that order." iucnParentRank and iucnParentName: the nearest IUCN
    /// taxon above it.
    public static string ColGroupNote(string heading, string rank, string? iucnParentRank, string? iucnParentName) {
        var first = $"{heading} is from the Catalogue of Life.";
        var ranks = rank == "unranked" ? "IUCN does not use it." : $"IUCN does not use {RankPlural(rank)}.";
        return iucnParentRank is null || iucnParentName is null
            ? $"{first} {ranks}"
            : $"{first} {ranks} The site shows it inside IUCN's {iucnParentRank} {iucnParentName} because nearly all of its species are in that {iucnParentRank}.";
    }

    public static string RuleGroupNote(string rank) =>
        $"IUCN gives the {rank} of these species as NOT ASSIGNED. This {rank} is from the Catalogue of Life.";

    // Page of a higher taxon: the taxa directly in it
    /// "Genera in Felidae (14)" when every row has one rank, else "Taxa in Felidae (16)".
    public static string HeadingChildren(string? sharedRank, string name, int count) =>
        sharedRank is null
            ? $"Taxa in {name} ({SiteFormat.Number(count)})"
            : $"{Capitalize(RankPlural(sharedRank))} in {name} ({SiteFormat.Number(count)})";
    public const string ColumnTaxon = "Taxon";
    public const string ColumnCommonName = "Common name";
    public const string ColumnThreatened = "Threatened";
    public const string ColumnExtinct = "Extinct (EX, EW)";

    // Two or more taxa with the rank and name, or none
    public static string ChooseHeading(int count, string rank, string name) => $"{count} {RankPlural(rank)} named {name}";
    public static string NotFoundHeading(string rank, string name) => $"{Capitalize(rank)} {name} not found on this site";
    public static string NotFoundLine(string name) => $"Search for {name}:";

    // The list
    public const string HeadingList = "Wikipedia list";
    public static string ListSize(int taxa) => taxa == 1 ? "1 taxon" : $"{SiteFormat.Number(taxa)} taxa";
    public static string TooLong(int taxa, int max) =>
        $"Too many taxa: {SiteFormat.Number(taxa)}. Lists have at most {SiteFormat.Number(max)} taxa, about the number of {{{{IUCN status}}}} templates one Wikipedia page can hold. Choose fewer Red List categories, or a smaller taxon from the table above.";
    public static string EmptyList(string name) => $"No taxa in {name} match these options.";
    public static string RanksLeftOut(IEnumerable<string> ranks) =>
        $"{Capitalize(string.Join(", ", ranks))} headings left out: Wikipedia headings stop at level 6. Choose fewer ranks, or top heading level 2.";
    public const string WikitextLabel = "Wikitext";
    public const string CopyListAccessible = "Copy the list wikitext";
    public const string Preview = "Preview";
    public const string PreviewNote = "Approximate preview of how Wikipedia displays this wikitext. Names link to English Wikipedia articles. Each category code links to the Wikipedia article for that category, and the small superscript link (such as “IUCN 2016”) links to the assessment on the IUCN Red List website.";
    public const string ListUpdated = "List updated";
    public const string ListTooManyRequests = "Too many requests: the list was not updated. Wait a minute, then select Update list.";

    // List options
    public const string OptionsHeading = "List options";
    public const string OptionFormat = "Line format";
    public const string StyleSci = "Scientific name first";
    public const string StyleSciExample = "Panthera leo, lion";
    public const string StyleCommon = "Common name first";
    public const string StyleCommonExample = "Lion (Panthera leo)";
    public const string StyleCommonOnly = "Common name only";
    public const string StyleCommonOnlyExample = "Lion";
    public const string DefaultMark = "(default)";
    public const string OptionTemplate = "{{IUCN status}} after each name";
    public const string OptionHeadings = "Headings";
    public const string OptionStatusSections = "A heading for each Red List category";
    public const string OptionRankHeadings = "Headings for these ranks:";
    public static string NoRanks(string rank, string name) => $"No ranks below {rank} {name}.";
    public const string OptionHeadingNames = "“Members of … are called …” under each heading";
    public const string OptionTopLevel = "Top heading level";
    public static string LevelOption(int level) => $"Level {level} ({new string('=', level)})";
    public const string OptionCategories = "Red List categories";
    public const string CategoriesHelp = "Not Evaluated: taxa with no global IUCN assessment, such as taxa with only a regional assessment (for example Europe) and species from Catalogue of Life or Wikidata that are not on the IUCN Red List. Their lines have no {{IUCN status}} after the name. If no box is ticked, the list includes every category.";
    public const string OptionTaxa = "Taxa";
    public const string InfraNone = "Species only";
    public const string InfraSeparate = "Subspecies and varieties in a separate section";
    public const string InfraUnder = "Subspecies and varieties under each species";
    public const string OptionSubpopulations = "Subpopulations";
    public const string OptionSort = "Sort";
    public const string SortFirst = "By the first name on each line";
    public const string SortSci = "By scientific name";
    public const string SortCommon = "By common name";
    public const string UpdateList = "Update list";
    public const string ResetOptions = "Reset options";

    // Search results
    public const string SearchGroupsHeading = "Higher taxa";
    public const string SearchTaxaHeading = "Assessed taxa";
    public static string SearchGroupSpecies(int species, string kingdom) =>
        $"{SiteFormat.Number(species)} assessed species, {SiteFormat.TitleCase(kingdom)}";

    /// "genera", "families", "orders", "classes", "phyla", "subfamilies", "tribes".
    public static string RankPlural(string rank) => rank switch {
        "genus" => "genera",
        "phylum" => "phyla",
        "subphylum" => "subphyla",
        "infraphylum" => "infraphyla",
        _ when rank.EndsWith("ss", StringComparison.Ordinal) => rank + "es",
        _ when rank.EndsWith('y') => rank[..^1] + "ies",
        _ => rank + "s",
    };

    public static string Capitalize(string text) =>
        text.Length == 0 ? text : char.ToUpperInvariant(text[0]) + text[1..];
}
