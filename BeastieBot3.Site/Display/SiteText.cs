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
    public static string NoResults(string query) =>
        $"No taxa found for “{query}”. Check the spelling, or search for the scientific name. If the spelling is right, the taxon may not be on the IUCN Red List.";
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

    /// "No global assessment. This taxon has been assessed in " + link("{n} regions") + ".". n is the
    /// number of regions; the Regional assessments table lists the latest assessment in each.
    public const string NoGlobalBefore = "No global assessment. This taxon has been assessed in ";
    public static string NoGlobalLinkText(int regions) => regions == 1 ? "1 region" : $"{SiteFormat.Number(regions)} regions";
    public const string NoGlobalAfter = ".";

    // Taxon page: a taxon that is not in the release (an old IUCN id, or a taxon IUCN no longer
    // assesses). The status section has the first line, and the second when a taxon in the release
    // has the same name: before + italic name + middle + link("IUCN id {id}") + ".".
    public static string NoCurrentLine(string? version) =>
        version is null ? "No current assessment in the IUCN Red List." : $"No current assessment in IUCN Red List version {version}.";
    public static string CurrentTaxonBefore(string? version) =>
        version is null ? "The IUCN Red List lists " : $"IUCN Red List version {version} lists ";
    public const string CurrentTaxonMiddle = " under ";
    public static string IucnIdLink(long id) => $"IUCN id {id}";
    public const string CurrentTaxonAfter = ".";

    /// Under the assessment history of a taxon in the release, for each taxon with the same name that
    /// is not in the release: before + link("IUCN id {id}") + after.
    public const string EarlierIdBefore = "Earlier assessments of a taxon with this name are under ";
    public const string EarlierIdAfter = ".";

    // Taxon page: wikitext boxes
    public const string LabelCite = "{{cite iucn}} citation";
    public const string LabelStatus = "{{IUCN status}} template";
    /// "{{Speciesbox}} status parameters", "{{Subspeciesbox}} status parameters", "Taxobox status
    /// parameters" (TaxoboxTemplate).
    public static string TaxoboxLabel(string taxobox) => $"{char.ToUpperInvariant(taxobox[0])}{taxobox[1..]} status parameters";
    public const string Copy = "Copy";
    public const string Copied = "Copied";
    // site.js selects the wikitext when copying fails, so the message says it is selected.
    public const string CopyFailed = "Copy failed. The wikitext is selected: copy it with Ctrl+C (⌘C on a Mac) or your browser's Copy command.";
    public const string CopyFailedButton = "Copy failed";
    public static string CopyAccessible(string template) => $"Copy {template} wikitext";

    // Taxon page: citation options form. Strings ending in Html contain markup.
    public const string OptionsLegend = "Citation options";
    public const string AuthorsLabel = "Author names";
    public const string AuthorsAuthorHtml = "<code>|author=Surname, I.</code> <code>|author2=</code> … (used in most articles)";
    public const string AuthorsLastFirstHtml = "<code>|last1=Surname</code> <code>|first1=I.</code> (as in the {{cite iucn}} documentation)";
    public const string FullGivenNames = "Full given names instead of initials";

    /// Help line of the full given names option. withNames: how many authors IUCN gives full given
    /// names for, out of people (authors who are not organisations). given and published: one of
    /// them, as "Catherine" and "Sayer, C.".
    public static string FullGivenNamesHelp(int withNames, int people, string given, string published) {
        var example = $"“{given}” for “{published}”";
        if (people <= 1) {
            return $"IUCN gives this author's full given names: {example}.";
        }
        if (withNames >= people) {
            return people == 2
                ? $"IUCN gives full given names for both authors, such as {example}."
                : $"IUCN gives full given names for all {people} authors, such as {example}.";
        }
        var others = people - withNames;
        return $"IUCN gives full given names for {withNames} of the {people} authors, such as {example}. "
            + (others == 1 ? "The other author is written as in IUCN's citation." : $"The other {others} authors are written as in IUCN's citation.");
    }
    public const string AccessLabel = "Access date";
    public static string AccessDownload(string date) => $"Date downloaded from IUCN ({date})";
    public static string AccessToday(string date) => $"Today ({date}, UTC)";
    public const string AccessNone = "No access date";
    public const string AccessHelp = "Date downloaded from IUCN: the date this site downloaded the details of this assessment from the IUCN Red List API.";
    public const string RefWrapHtml = "Wrap the citation in <code>&lt;ref&gt;</code> tags";
    public const string RefName = "Ref name";
    public const string RefNameHelp = "Use a ref name that no other citation in the article uses, unless this citation replaces the citation with that name.";
    public const string AmpHtml = "Add <code>|name-list-style=amp</code>";
    public const string AmpHelp = "Puts “&” before the last author, as IUCN does.";
    /// The form's submit button, for browsers without JavaScript. site.js hides it and updates the
    /// wikitext as soon as an option changes, then shows WikitextUpdated in a status line.
    public const string UpdateWikitext = "Update wikitext";
    public const string WikitextUpdated = "Wikitext updated";

    // Taxon page: {{cite Q}} and QuickStatements, the last part of the wikitext section. The site
    // never edits Wikidata; the reader runs the QuickStatements commands with their own account.
    public const string HeadingWikidataCite = "{{cite Q}} citation from Wikidata";
    /// Before a link to the item ("Q123").
    public const string WikidataItemBefore = "Wikidata item for this assessment: ";
    public const string LabelCiteQ = "{{cite Q}} citation";
    /// Link("English Wikipedia guidance") + after: the 2017 deletion discussion of Template:Cite Q
    /// and WP:Citing sources#Wikidata.
    public const string CiteQGuidanceLink = "English Wikipedia guidance";
    public const string CiteQGuidanceUrl = "https://en.wikipedia.org/wiki/Wikipedia:Citing_sources#Wikidata";
    public const string CiteQGuidanceAfter =
        ": check every detail that {{cite Q}} takes from Wikidata, and don't use {{cite Q}} in an article whose citations mostly give authors as “Last, First” or in Vancouver style.";
    /// statements: "publisher (P123), DOI (P356)".
    public static string MissingStatements(string statements) => $"The item is missing these statements: {statements}.";
    /// After MissingStatements, in the same paragraph.
    public const string AddStatementsLine = "To add them, run these commands in QuickStatements with your Wikidata account.";
    public const string LabelAddStatements = "QuickStatements commands to add the missing statements";
    public const string NoWikidataItem = "No Wikidata item found for this assessment.";
    /// Before + link("search Wikidata") + after.
    public const string SearchWikidataBefore = "Before creating an item, ";
    public const string SearchWikidataLink = "search Wikidata";
    public const string SearchWikidataAfter = " in case one was added after this site's data was downloaded.";
    public const string CreateItemLine =
        "To create the item, run these commands in QuickStatements with your Wikidata account. Then cite the new item with {{cite Q|<new item id>}}.";
    public const string LabelCreateItem = "QuickStatements commands to create the item";
    public const string OpenInQuickStatements = "Open in QuickStatements";
    public const string CopyQuickStatements = "Copy QuickStatements commands";

    /// The English labels of the properties the assessment item model uses, as Wikidata gives them.
    public static string WikidataPropertyLabel(string property) {
        var label = property switch {
            "P31" => "instance of",
            "P50" => "author",
            "P123" => "publisher",
            "P356" => "DOI",
            "P407" => "language of work or name",
            "P478" => "volume",
            "P577" => "publication date",
            "P921" => "main subject",
            "P953" => "full work available at URL",
            "P1433" => "published in",
            "P1476" => "title",
            "P2093" => "author name string",
            "P2322" => "article ID",
            _ => null,
        };
        return label is null ? property : $"{label} ({property})";
    }

    /// QuickStatements terms: 'L' label, 'D' description, 'A' alias, 'S' sitelink, with the language
    /// code or site: "label (en)".
    public static string WikidataTermLabel(char kind, string language) => kind switch {
        'L' => $"label ({language})",
        'D' => $"description ({language})",
        'A' => $"alias ({language})",
        _ => $"sitelink ({language})",
    };

    // Taxon page: notes about the wikitext
    public const string DoiGbif = "DOI from GBIF's copy of the IUCN checklist.";
    public const string DoiWikidata = "DOI from Wikidata.";
    public const string DoiResolved = "DOI found in Crossref's list of IUCN DOIs, or by checking possible DOIs at doi.org.";
    public const string NoDoi = "No DOI found in IUCN's citation text, GBIF or Wikidata. {{cite iucn}} works without a DOI.";
    public const string AuthorsUnsplit = "Check these author names, given exactly as IUCN wrote them:";
    /// versionNote: VersionNote's text ("Replaced by the errata version"), added as a sentence.
    public static string EarlierAssessment(string category, int? year, string? versionNote = null) =>
        (year is null ? $"Wikitext for an earlier assessment: {category}." : $"Wikitext for an earlier assessment: {category}, published {year}.")
        + WithNote(versionNote);
    public static string RegionalAssessment(string region, string category, int? year, string? versionNote = null) =>
        (year is null ? $"Wikitext for the {region} assessment: {category}." : $"Wikitext for the {region} assessment: {category}, published {year}.")
        + WithNote(versionNote);
    private static string WithNote(string? note) => note is null ? string.Empty : $" {note}.";
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
    /// The accessible name of a row's "Show wikitext" link. region: null for a global assessment.
    /// versionNote: the row's VersionNote, so an errata version and the assessment it replaced
    /// have different names.
    public static string ShowWikitextAccessible(string? region, int? year, string? versionNote = null) {
        var assessment = region is null ? "the assessment" : $"the {region} assessment";
        var text = year is null ? $"Show wikitext for {assessment}" : $"Show wikitext for {assessment} published in {year}";
        return versionNote is null ? text : $"{text} ({char.ToLowerInvariant(versionNote[0])}{versionNote[1..]})";
    }

    // Notes on errata and amended versions in the assessment tables. IUCN publishes an errata
    // version with the year of the assessment it corrects; an amended version usually has a later
    // year. Both replace the earlier assessment, which IUCN still lists.
    public static string ErrataVersion(int publishedYear) => $"Errata version, published {publishedYear}";
    public static string AmendedVersion(int amendsYear) => $"Amended version of the {amendsYear} assessment";
    public const string ReplacedByErrata = "Replaced by the errata version";
    public const string ReplacedByAmended = "Replaced by the amended version";
    public const string ReplacedByLater = "Replaced by a later version";
    public const string Shown = "Shown";
    public const string Latest = "Latest";
    public const string IucnSiteLink = "IUCN Red List website";

    // Taxon page: old IUCN ids. A taxon not in the release (an old id) is linked to the taxon in the
    // release with the same scientific name, and to the one taxon whose IUCN synonyms list its name.
    // When a linked id has a global assessment, the history section is a combined assessment history
    // (Pages/Shared/_CombinedHistory.cshtml): a legend line for each id, then one table.
    public const string HeadingCombinedHistory = "Combined assessment history";

    /// "Global assessments of IUCN ids 2790 and 2785, newest first."; with more than 3 ids, their number.
    public static string CombinedIntro(IReadOnlyList<long> ids) {
        if (ids.Count > 3) {
            return $"Global assessments of {ids.Count} IUCN ids, newest first.";
        }
        var list = ids.Count == 1 ? $"{ids[0]}"
            : string.Join(", ", ids.Take(ids.Count - 1)) + $" and {ids[^1]}";
        return $"Global assessments of IUCN ids {list}, newest first.";
    }

    public const string ColIucnId = "IUCN id";
    /// The tag beside the page's own id, in the legend and in the IUCN id column.
    public const string ThisPageTag = "This page";

    /// A legend line: strong("IUCN id 2790") + " " + tag(ThisPageTag), or link("IUCN id 2785"); then
    /// LegendBeforeName + italic name + LegendAfterName(...); then, for a linked id, LegendSameName,
    /// or SynonymOfBefore + italic name + SynonymOfMiddle + italic name + ".".
    public const string LegendBeforeName = " (";

    /// After the name in a legend line: whether the id is in the release, and its global assessments:
    /// "): in Red List version 2026-1; 1 global assessment, published 2026."
    public static string LegendAfterName(bool inRelease, string? version, int globalCount, int? firstYear, int? lastYear) {
        var release = version is null
            ? (inRelease ? "in the current Red List version" : "not in the current Red List version")
            : (inRelease ? $"in Red List version {version}" : $"not in Red List version {version}");
        var assessments = globalCount == 0 ? "no global assessments"
            : globalCount == 1 ? $"1 global assessment, published {lastYear}"
            : firstYear == lastYear ? $"{globalCount} global assessments, published {lastYear}"
            : $"{globalCount} global assessments, published {firstYear} to {lastYear}";
        return $"): {release}; {assessments}.";
    }

    /// After the legend line of an id with the same scientific name as the page's taxon.
    public static string LegendSameName(long pageTaxonId) => $"Same scientific name as IUCN id {pageTaxonId}.";

    /// In the Wikitext column, for a row of another IUCN id: a link to that id's page, which shows
    /// the assessment's wikitext. The accessible name starts with the visible text.
    public static string ShowWikitextOtherId(long taxonId) => $"See IUCN id {taxonId}";
    public static string ShowWikitextOtherIdAccessible(long taxonId, int? year, string? versionNote = null) {
        var text = year is null
            ? $"See IUCN id {taxonId} for the wikitext of the assessment"
            : $"See IUCN id {taxonId} for the wikitext of its {year} assessment";
        return versionNote is null ? text : $"{text} ({char.ToLowerInvariant(versionNote[0])}{versionNote[1..]})";
    }

    /// Under the combined table, when the newest global assessment of one or more ids has taxonomic
    /// notes: TaxonomicNotesBefore + link(TaxonomicNotesLink) [+ ListSeparator + link ...] +
    /// TaxonomicNotesAfter. The links go to the assessments on the IUCN Red List website.
    public const string TaxonomicNotesBefore = "IUCN's taxonomic notes may explain why these assessments are under more than one IUCN id: see the ";
    public static string TaxonomicNotesLink(int? year, long taxonId) =>
        year is null ? $"newest assessment of IUCN id {taxonId}" : $"{year} assessment of IUCN id {taxonId}";
    /// Before link number index (from 0) of count: "", ", the " or " and the ".
    public static string ListSeparator(int index, int count) => index == 0 ? string.Empty : index == count - 1 ? " and the " : ", the ";
    public const string TaxonomicNotesAfter = " on the IUCN Red List website.";

    /// Under the combined table, for another id with regional assessments: before + link("IUCN id
    /// {id}", to the Regional assessments section of its page) + after.
    public const string OtherRegionalBefore = "Regional assessments of ";
    public const string OtherRegionalAfter = " are on its page.";

    /// IUCN lists one name as a synonym of another: SynonymOfBefore + italic name + SynonymOfMiddle +
    /// italic name, then "." in a legend line and under the history, or, in the status section of an
    /// old id's page, SynonymOfBeforeLink + link("IUCN id {id}") + SynonymOfAfter.
    public const string SynonymOfBefore = "IUCN lists ";
    public const string SynonymOfMiddle = " as a synonym of ";
    public const string SynonymOfBeforeLink = " (";
    public const string SynonymOfAfter = ").";

    /// Under the assessment history of a taxon in the release, for an old id whose name IUCN lists as
    /// a synonym of it and that has no global assessment: EarlierSynonymIdBefore + italic name +
    /// EarlierSynonymIdMiddle + link("IUCN id {id}") + ". " + the synonym sentence.
    public const string EarlierSynonymIdBefore = "Earlier assessments of ";
    public const string EarlierSynonymIdMiddle = " are under ";

    // Taxon page: names
    public const string NamesEnglish = "English common names";
    public const string NamesOtherLanguages = "Common names in other languages";
    public const string NamesSynonyms = "Synonyms";
    public const string ColName = "Name";
    public const string ColSource = "Source";
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
    public const string LicenceCc0 = "https://creativecommons.org/publicdomain/zero/1.0/";
    public const string IucnRedListUrl = "https://www.iucnredlist.org";
    public const string WikidataUrl = "https://www.wikidata.org";
    public const string EnglishWikipediaUrl = "https://en.wikipedia.org";
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
