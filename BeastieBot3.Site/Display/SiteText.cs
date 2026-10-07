using System.Globalization;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Display;

// Every sentence and label the site shows, in one place so the wording can be reviewed and changed
// without reading the pages. Pages and SiteFormat fill in the values. Strings that contain HTML are
// noted; every other string is plain text and is HTML-encoded where it is used.

public static partial class SiteText {
    public const string SiteName = "Beastie Bot Species Status";
    public const string Description =
        "Unofficial site for looking up the IUCN Red List category of any species and copying wikitext to cite the assessment on Wikipedia.";

    // Layout
    public const string SkipToContent = "Skip to main content";
    public const string NavAbout = "About";
    public const string FooterLine1 = "This site is unofficial and is not affiliated with or endorsed by IUCN.";
    public const string SourceCode = "Source code";

    // Layout: theme control in the header. theme.js shows it and keeps the choice in the browser;
    // without JavaScript it stays hidden and the site follows the system setting.
    public const string ThemeLabel = "Theme";
    public const string ThemeSystem = "System";
    public const string ThemeLight = "Light";
    public const string ThemeDark = "Dark";

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
    public const string MatchTaxonIdLabel = "Matched IUCN taxon ID:";
    public const string MatchAssessmentIdLabel = "Matched IUCN assessment ID:";
    /// "14871490 (Global, 2016)".
    public static string MatchAssessmentId(long assessmentId, string? scope, int? year) {
        var details = string.Join(", ", new[] { scope, year?.ToString(CultureInfo.InvariantCulture) }.Where(p => !string.IsNullOrEmpty(p)));
        var id = assessmentId.ToString(CultureInfo.InvariantCulture);
        return details.Length == 0 ? id : $"{id} ({details})";
    }
    public static string AssessmentIdNotFound(long assessmentId) =>
        $"This site has no assessment with IUCN assessment ID {assessmentId.ToString(CultureInfo.InvariantCulture)}.";
    public static string NoResults(string query) =>
        $"No taxa found for “{query}”. Check the spelling, or search for the scientific name. If the spelling is right, the taxon may not be on the IUCN Red List.";
    /// Before the links to searches with a misspelled word corrected, when a search found nothing.
    public const string SimilarNames = "Similar names:";

    public const string TooShort = "Search term too short. Enter at least 2 letters or digits.";
    public const string NoGlobalShort = "No global assessment";
    /// In a list of taxa, in place of the category, for a taxon that is not in the release.
    public const string NoCurrentShort = "No current assessment";

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

    // Taxon page: names
    public const string NamesEnglish = "English common names";
    public const string NamesOtherLanguages = "Common names in other languages";
    public const string NamesSynonyms = "Synonyms";
    public const string ColName = "Name";
    public const string ColSource = "Source";
    public const string ColSynonym = "Synonym";
    /// Around the authority a source gives for a synonym, when that differs from the one shown:
    /// "Catalogue of Life (authority: Phipps, 1774)".
    public const string SourceAuthorityBefore = " (authority: ";
    public const string SourceAuthorityAfter = ")";
    public const string IucnMainName = "IUCN's main English name";
    public const string NoEnglishName = "No English common name found";
    public const string LanguageNotGiven = "Language not given";
    public static string ShowMoreLanguages(int n) => n == 1 ? "Show 1 more language" : $"Show {SiteFormat.Number(n)} more languages";
    public static string ShowMoreSynonyms(int n) => n == 1 ? "Show 1 more synonym" : $"Show {SiteFormat.Number(n)} more synonyms";

    public static string SourceLabel(string source) => source switch {
        "iucn" => "IUCN Red List",
        "col" => "Catalogue of Life",
        "wikidata" => "Wikidata",
        "wikipedia" => "Wikipedia",
        "wikipedia-taxobox" => "Wikipedia taxobox",
        "mdd" => "Mammal Diversity Database",
        "amphibiaweb" => "AmphibiaWeb",
        _ => source,
    };

    // Taxon page: links to other sites
    public const string LinkWikipedia = "English Wikipedia article";
    public const string LinkWikipediaNone = "No English Wikipedia article found";
    public const string LinkWikidata = "Wikidata item";
    public const string LinkCol = "Catalogue of Life";
    public const string LinkSprat = "SPRAT profile";
    public const string LinkSpratPlural = "SPRAT profiles";
    public const string SpratFullName = "Species Profile and Threats Database, Australian Government";
    public const string EpbcFullName = "Environment Protection and Biodiversity Conservation Act 1999";

    /// "Listed as Endangered under Australia's " + abbr("EPBC Act") + ".", then, for a listing of a
    /// population, EpbcPopulationOnly; for a listing under another name, EpbcListedNameBefore +
    /// italic name + ".".
    public static string EpbcBefore(string status) => $"Listed as {status} under Australia's ";
    public const string EpbcAbbr = "EPBC Act";
    public const string EpbcAfter = ".";
    /// population: the population as SPRAT names it ("combined populations of Qld, NSW and the ACT").
    public static string EpbcPopulationOnly(string population) => $"This listing applies only to the {population}.";
    public const string EpbcListedNameBefore = "The listing uses the name ";

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
    public static string TaxonNotFoundLine(string version) =>
        $"This id is not in IUCN Red List version {version}, and this site has no earlier assessments with this id. Check the id, or search for the taxon by name.";
    public const string TaxonNotFoundLineNoVersion =
        "This site has no assessments with this id. Check the id, or search for the taxon by name.";
    public const string TooManyHeading = "Too many requests";
    public const string TooManyLine = "Try again in a minute.";
    /// The wait before the limit lets this address in again, from the Retry-After header.
    public static string TooManyWait(int seconds) => seconds switch {
        <= 60 => TooManyLine,
        < 2 * 3600 => $"Try again in {(seconds + 59) / 60} minutes.",
        _ => $"Try again in {(seconds + 3599) / 3600} hours.",
    };
    public const string TooManyTaxonPages = "This address has opened more taxon and group pages than the site allows in an hour or a day.";
    /// Status line under the citation options when a live update was refused by the rate limit.
    public const string WikitextTooManyRequests = "Too many requests: the wikitext was not updated. Wait a minute, then select Update wikitext.";
    public const string ServerErrorHeading = "Server error";
    public static string ServerErrorLine(string contact) => $"Try again in a few minutes. If the error happens again, report it to {contact}.";
    public const string UnavailableHeading = "Site unavailable";
    public const string UnavailableLine = "Try again in a few minutes.";
    public static string OtherErrorHeading(int code) => $"Error {code}";

    // Fallback when Site:ContactText is not configured.
    public const string ContactFallback = "the person who runs this site";

    public const string TermsUrl = "https://www.iucnredlist.org/terms/terms-of-use";

    // About page: the data sources, their licences and their citations. The rest of the About
    // page text is in Pages/About.cshtml.
    public const string LicenceCcBy = "https://creativecommons.org/licenses/by/4.0/";
    public const string LicenceCcBySa = "https://creativecommons.org/licenses/by-sa/4.0/";
    public const string LicenceCcByNc = "https://creativecommons.org/licenses/by-nc/4.0/";
    public const string MddUrl = "https://www.mammaldiversity.org/";
    /// The Mammal Diversity Database's releases on Zenodo (the concept DOI, which leads to the newest).
    public const string MddDoiUrl = "https://doi.org/10.5281/zenodo.4139722";
    public const string AmphibiaWebUrl = "https://amphibiaweb.org/";
    public const string LicenceCc0 = "https://creativecommons.org/publicdomain/zero/1.0/";
    public const string IucnRedListUrl = "https://www.iucnredlist.org";
    public const string WikidataUrl = "https://www.wikidata.org";
    public const string EnglishWikipediaUrl = "https://en.wikipedia.org";
    public const string WikispeciesUrl = "https://species.wikimedia.org";
    public const string CatalogueOfLifeUrl = "https://www.catalogueoflife.org";
    public const string SpratUrl = "https://www.environment.gov.au/cgi-bin/sprat/public/sprat.pl";
    public const string CrossrefUrl = "https://www.crossref.org/documentation/retrieve-metadata/rest-api/";
    /// Crossref's licensing statement: bibliographic metadata is facts, in the public domain (CC0).
    public const string CrossrefLicenceUrl = "https://www.crossref.org/documentation/retrieve-metadata/";

    /// DOI of GBIF's copy of the IUCN checklist, used when the database has none in its meta table.
    public const string GbifChecklistDoi = "10.15468/0qnb58";

    public const string SpratLicensor =
        "Department of Climate Change, Energy, the Environment and Water (DCCEEW), Australian Government";

    /// About page, Version cell of Wikidata and English Wikipedia: their data comes from caches
    /// downloaded over time, so the date is the latest it can be, the day the database was built.
    public static string CachedUpTo(string date) => $"Downloaded on various dates up to {date}";

    /// About page, Version cell of Crossref: `iucn resolve-dois` checks each assessment's DOI on its
    /// own date, so the date is the newest check.
    public static string DoisCheckedUpTo(string date) => $"DOIs checked on various dates up to {date}";
}
