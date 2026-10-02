namespace BeastieBot3.Site.Display;

// Every sentence and label the site shows, in one place so the wording can be reviewed and changed
// without reading the pages. Pages and SiteFormat fill in the values. Strings that contain HTML are
// noted; every other string is plain text and is HTML-encoded where it is used.

public static class SiteText {
    public const string SiteName = "Beastie Bot Species Status";
    public const string Description =
        "Unofficial site for looking up the IUCN Red List category of any species and copying wikitext to cite the assessment on Wikipedia.";

    // Layout
    public const string SkipToContent = "Skip to main content";
    public const string NavAbout = "About";
    public const string FooterLine1 = "This site is unofficial and is not affiliated with or endorsed by IUCN.";
    public const string SourceCode = "Source code";

    // Home page
    public const string SearchLabel = "Search for a taxon";
    public const string SearchPlaceholder = "Scientific name, common name or synonym";
    public const string SearchButton = "Search";
    public const string ExamplesLabel = "Examples:";
    public const string WhatHeading = "On each taxon page:";
    public const string What1 = "The category, criteria and population trend of the latest global assessment";
    public const string What2 = "{{cite iucn}}, {{IUCN status}} and taxobox wikitext to copy into Wikipedia";
    public const string What3 = "Earlier and regional assessments, names, and links to Wikipedia, Wikidata and the Catalogue of Life";

    public static string DatasetSummary(string version, long globalTaxa, long assessments) =>
        $"IUCN Red List version {version}: {SiteFormat.Number(globalTaxa)} taxa with a global assessment, {SiteFormat.Number(assessments)} assessments in total";

    // Search results
    public static string SearchHeading(string query) => $"Results for “{query}”";
    public static string SearchCount(long n) => n == 1 ? "1 taxon found" : $"{SiteFormat.Number(n)} taxa found";
    public static string SearchTruncated(int shown, long n) =>
        $"Showing the first {shown} of {SiteFormat.Number(n)} taxa. Type more of the name to narrow the search.";
    public const string MatchSynonymLabel = "Matched synonym:";
    public const string MatchCommonNameLabel = "Matched common name:";
    public static string NoResults(string query) =>
        $"No taxa found for “{query}”. Check the spelling, or search for the scientific name. If the spelling is right, the taxon may not be on the IUCN Red List.";
    public const string TooShort = "Search term too short. Enter at least 2 letters or digits.";
    public const string NoGlobalShort = "No global assessment";

    public static string? KindLabel(string kind) => kind switch {
        "subspecies" => "Subspecies",
        "variety" => "Variety",
        "subpopulation" => "Subpopulation",
        _ => null,
    };

    // Taxon page: arriving from a search
    public static string ArrivedSynonym(string query) => $"“{query}” is a synonym of this taxon.";
    public static string ArrivedCommonName(string query) => $"“{query}” is a common name of this taxon.";
    public static string ArrivedAllResults(string query) => $"See all search results for “{query}”";

    // Taxon page: headings
    public const string HeadingStatus = "Latest global assessment";
    public const string HeadingStatusNoGlobal = "IUCN Red List status";
    public const string HeadingWikitext = "Wikitext for Wikipedia";
    public const string HeadingHistory = "Assessment history";
    public const string HeadingRegional = "Regional assessments";
    public const string HeadingNames = "Names";
    public const string HeadingLinks = "Links to other sites";
    public const string HeadingClassification = "Classification";

    /// "Subspecies", "Subspecies and subpopulations", "Subspecies, varieties and subpopulations".
    public static string HeadingChildren(bool subspecies, bool varieties, bool subpopulations) {
        var parts = new List<string>();
        if (subspecies) {
            parts.Add("subspecies");
        }
        if (varieties) {
            parts.Add("varieties");
        }
        if (subpopulations) {
            parts.Add("subpopulations");
        }
        if (parts.Count == 0) {
            return string.Empty;
        }
        var text = parts.Count switch {
            1 => parts[0],
            2 => $"{parts[0]} and {parts[1]}",
            _ => $"{parts[0]}, {parts[1]} and {parts[2]}",
        };
        return char.ToUpperInvariant(text[0]) + text[1..];
    }

    public static readonly IReadOnlyList<string> RankLabels = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus"];

    /// "Subspecies of", "Variety of", "Subpopulation of"; the parent's linked name follows.
    public static string? ParentLinePrefix(string kind) => kind switch {
        "subspecies" => "Subspecies of",
        "variety" => "Variety of",
        "subpopulation" => "Subpopulation of",
        _ => null,
    };

    // Taxon page: status summary
    public const string LabelCategory = "Category";
    public const string LabelCriteria = "Criteria";
    public const string CriteriaNone = "None listed";
    public const string LabelTrend = "Population trend";
    public const string LabelAssessed = "Date assessed";
    public const string LabelPublished = "Year published";
    public const string LabelVersion = "Data from Red List version";
    public const string LabelRegion = "Region";
    public const string IucnLink = "Read the full assessment on the IUCN Red List website";
    public const string CategoryCrPe = "Critically Endangered (Possibly Extinct)";
    public const string CategoryCrPew = "Critically Endangered (Possibly Extinct in the Wild)";
    public static string OldCategoryLabel(string iucnName) => $"{iucnName} (1994 or earlier categories)";
    public static string UnknownCategoryLabel(string code) => $"{code} (old IUCN category)";
    /// With OldCategoryLabel: the label of a code such as "NT" on an assessment that uses an earlier
    /// version of the categories, which IUCN gives no name.
    public const string CategoryNotNamed = "No name given by IUCN";

    /// "No global assessment. This taxon has " + link("{n} regional assessments") + ".".
    public const string NoGlobalBefore = "No global assessment. This taxon has ";
    public static string NoGlobalLinkText(int n) => n == 1 ? "1 regional assessment" : $"{SiteFormat.Number(n)} regional assessments";
    public const string NoGlobalAfter = ".";

    // Taxon page: wikitext boxes
    public const string LabelCite = "{{cite iucn}} citation";
    public const string LabelStatus = "{{IUCN status}} template";
    /// "{{Speciesbox}} status parameters", "{{Subspeciesbox}} status parameters", "Taxobox status
    /// parameters" (TaxoboxTemplate).
    public static string TaxoboxLabel(string taxobox) => $"{char.ToUpperInvariant(taxobox[0])}{taxobox[1..]} status parameters";
    public const string Copy = "Copy";
    public const string Copied = "Copied";
    public const string CopyFailed = "Copy failed. Select the wikitext and copy it by hand.";
    public static string CopyAccessible(string template) => $"Copy {template} wikitext";

    // Taxon page: citation options form. Strings ending in Html contain markup.
    public const string OptionsLegend = "Citation options";
    public const string AuthorsLabel = "Author names";
    public const string AuthorsAuthorHtml = "<code>|author=Surname, I.</code> <code>|author2=</code> … (used in most articles)";
    public const string AuthorsLastFirstHtml = "<code>|last1=Surname</code> <code>|first1=I.</code> (as in the {{cite iucn}} documentation)";
    public const string AccessLabel = "Access date";
    public static string AccessDownload(string date) => $"Date downloaded from IUCN ({date})";
    public static string AccessToday(string date) => $"Today ({date})";
    public const string AccessNone = "No access date";
    public const string AccessHelp = "Date downloaded from IUCN: the date this site downloaded the details of this assessment from the IUCN Red List API.";
    public const string RefWrapHtml = "Wrap the citation in <code>&lt;ref&gt;</code> tags";
    public const string RefName = "Ref name";
    public const string RefNameHelp = "Use a ref name that no other citation in the article uses, unless this citation replaces the citation with that name.";
    public const string AmpHtml = "Add <code>|name-list-style=amp</code>";
    public const string AmpHelp = "Puts “&” before the last author, as IUCN does.";
    public const string UpdateWikitext = "Update wikitext";

    // Taxon page: notes about the wikitext
    public const string DoiGbif = "DOI from GBIF's copy of the IUCN checklist.";
    public const string DoiWikidata = "DOI from Wikidata.";
    public const string NoDoi = "No DOI found in IUCN's citation text, GBIF or Wikidata. {{cite iucn}} works without a DOI.";
    public const string AuthorsUnsplit = "Check these author names, given exactly as IUCN wrote them:";
    public static string EarlierAssessment(string category, int? year) =>
        year is null ? $"Wikitext for an earlier assessment: {category}." : $"Wikitext for an earlier assessment: {category}, published {year}.";
    public static string RegionalAssessment(string region, string category, int? year) =>
        year is null ? $"Wikitext for the {region} assessment: {category}." : $"Wikitext for the {region} assessment: {category}, published {year}.";
    /// taxoboxLabel: TaxoboxTemplate.Label.
    public static string TaxoboxGlobalOnly(string taxoboxLabel) => $"{taxoboxLabel} are given for global assessments only.";
    public const string ShowLatestWikitext = "Show wikitext for the latest assessment";
    /// taxobox: TaxoboxTemplate.Noun ("{{Speciesbox}}", "the taxobox").
    public static string NoTemplateCode(string taxobox) => $"{{{{IUCN status}}}} and {taxobox} have no code for this category.";
    /// taxobox: TaxoboxTemplate.Name ("{{Speciesbox}}", "taxobox").
    public static string NoTemplateEarlierVersion(string taxobox) =>
        $"This assessment uses an earlier version of the IUCN categories, so no {{{{IUCN status}}}} or {taxobox} wikitext is given for it.";

    /// The no-citation note: reason, then (when there is an {{IUCN status}} box) the available
    /// line, then "To cite the assessment, use " + link("its page on the IUCN Red List website") + ".".
    public const string NoCitationReason = "No citation for this assessment yet: its details have not been downloaded from the IUCN Red List.";
    public const string NoCitationStatusAvailable = "{{IUCN status}} wikitext is available.";
    public const string NoCitationBefore = "To cite the assessment, use ";
    public const string NoCitationLink = "its page on the IUCN Red List website";
    public const string NoCitationAfter = ".";
    public const string IucnCitationLabel = "Citation as given by IUCN";

    // Taxon page: assessment history and regional tables
    public const string ColPublished = "Year published";
    public const string ColAssessed = "Date assessed";
    public const string ColCategory = "Category";
    public const string ColCriteria = "Criteria";
    public const string ColWikitext = "Wikitext";
    public const string ColRegion = "Region";
    public const string ColAssessment = "Assessment";
    public const string ShowWikitext = "Show wikitext";
    public static string ShowWikitextAccessible(int? year) =>
        year is null ? "Show wikitext for this assessment" : $"Show wikitext for the assessment published in {year}";
    public const string Shown = "Shown";
    public const string Latest = "Latest";
    public const string IucnSiteLink = "IUCN Red List website";

    // Taxon page: names
    public const string NamesEnglish = "English common names";
    public const string NamesOtherLanguages = "Common names in other languages";
    public const string NamesSynonyms = "Synonyms";
    public const string ColName = "Name";
    public const string ColSource = "Source";
    public const string IucnMainName = "IUCN's main English name";
    public const string NoEnglishName = "No English common name found";
    public const string LanguageNotGiven = "Language not given";
    public static string ShowMore(int n) => $"Show {SiteFormat.Number(n)} more";

    public static string SourceLabel(string source) => source switch {
        "iucn" => "IUCN Red List",
        "col" => "Catalogue of Life",
        "wikidata" => "Wikidata",
        "wikipedia" => "Wikipedia",
        _ => source,
    };

    // Taxon page: links to other sites
    public const string LinkWikipedia = "English Wikipedia article";
    public const string LinkWikipediaNone = "No English Wikipedia article found";
    public const string LinkWikidata = "Wikidata item";
    public const string LinkCol = "Catalogue of Life";
    public const string LinkSprat = "SPRAT profile";
    public const string SpratFullName = "Species Profile and Threats Database, Australian Government";
    public const string EpbcFullName = "Environment Protection and Biodiversity Conservation Act 1999";

    /// "Listed as Endangered under Australia's " + abbr("EPBC Act").
    public static string EpbcBefore(string status) => $"Listed as {status} under Australia's ";
    public const string EpbcAbbr = "EPBC Act";

    // Taxon page: data note. Before + link("About page") + after.
    public static string DataNoteBefore(string version, string dateRange) =>
        $"Data from IUCN Red List version {version}, downloaded from the IUCN Red List API {dateRange}. Other data sources and their licences are listed on the ";
    public static string DataNoteBeforeNoDates(string version) =>
        $"Data from IUCN Red List version {version}. Other data sources and their licences are listed on the ";
    public const string AboutPageLink = "About page";
    public const string DataNoteAfter = ".";

    // Name lookup page
    public static string LookupHeading(string name) => $"Taxa with the name “{name}”";
    public static string LookupLine(int n, string name) =>
        $"{SiteFormat.Number(n)} taxa have “{name}” as their scientific name, a common name or a synonym. Select one to see its page.";

    // Footer line 2: before + link("IUCN Red List Terms of Use") + middle + link("About page") + ".".
    public static string FooterLine2Before(string version) => $"Data from IUCN Red List version {version}, used under the ";
    public const string TermsLink = "IUCN Red List Terms of Use";
    public const string FooterLine2Middle = ", and from the other sources listed on the ";

    // Error pages
    public const string NotFoundHeading = "Page not found";
    public const string NotFoundLine = "Check the address, or search for a taxon.";
    public static string TaxonNotFoundHeading(long id) => $"No taxon with IUCN id {id}";
    public static string TaxonNotFoundLine(string version) => $"This site uses IUCN Red List version {version}. Check the id, or search for the taxon by name.";
    public const string TaxonNotFoundLineNoVersion = "Check the id, or search for the taxon by name.";
    public const string TooManyHeading = "Too many requests";
    public const string TooManyLine = "Try again in a minute.";
    public const string ServerErrorHeading = "Server error";
    public static string ServerErrorLine(string contact) => $"Try again in a few minutes. If the error happens again, report it to {contact}.";
    public const string UnavailableHeading = "Site unavailable";
    public const string UnavailableLine = "Try again in a few minutes.";
    public static string OtherErrorHeading(int code) => $"Error {code}";

    // Fallback when Site:ContactText is not configured.
    public const string ContactFallback = "the person who runs this site";

    public const string TermsUrl = "https://www.iucnredlist.org/terms/terms-of-use";
}
