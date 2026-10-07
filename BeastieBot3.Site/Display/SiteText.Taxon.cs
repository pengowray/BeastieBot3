using System.Globalization;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Display;

// Strings of the taxon page (Pages/Species.cshtml) other than its Wikidata part and its names and
// links, which are in SiteText.Wikidata.cs and SiteText.cs.

public static partial class SiteText {
    // Taxon page: headings
    public const string HeadingStatus = "Latest global assessment";
    public const string HeadingStatusNoGlobal = "IUCN Red List status";
    public const string HeadingWikitext = "Wikitext for Wikipedia";
    public const string HeadingHistory = "Assessment history";
    public const string HeadingRegional = "Regional assessments";
    /// In place of HeadingRegional when every assessment in that table has no scope.
    public const string HeadingNoScope = "Assessments with no geographic scope";
    public const string HeadingNames = "Names";
    public const string HeadingLinks = "Links to other sites";
    /// The hidden heading of the line of groups under the taxon's name.
    public const string HeadingShortClassification = "Short classification";

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

    /// The heading of a species page's table of its subspecies, varieties and subpopulations: the
    /// kinds it has, or when it has none, the kinds its kingdom can have (animals have no varieties).
    public static string HeadingInfraTaxa(string? kingdom, bool subspecies, bool varieties, bool subpopulations) =>
        subspecies || varieties || subpopulations
            ? HeadingChildren(subspecies, varieties, subpopulations)
            : HeadingChildren(true, !IsAnimal(kingdom), true);

    public static string NoInfraTaxa(string? kingdom) => IsAnimal(kingdom)
        ? "IUCN has not assessed any subspecies or subpopulations of this species."
        : "IUCN has not assessed any subspecies, varieties or subpopulations of this species.";

    private static bool IsAnimal(string? kingdom) => string.Equals(kingdom, "ANIMALIA", StringComparison.OrdinalIgnoreCase);

    public const string HeadingSpecies = "Species";
    public const string SpeciesNotAssessedBefore = "IUCN has not assessed the species ";
    public const string SpeciesNotAssessedAfter = " as a whole.";

    /// "Other subspecies", "Other subspecies and subpopulations" of the same species.
    public static string HeadingOtherInfraTaxa(bool subspecies, bool varieties, bool subpopulations) =>
        "Other " + HeadingChildren(subspecies, varieties, subpopulations).ToLowerInvariant() + " of this species";

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
        (year is null ? $"Wikitext for {RegionalName(region)}: {category}." : $"Wikitext for {RegionalName(region)}: {category}, published {year}.")
        + WithNote(versionNote);
    /// "the Europe assessment", or for an assessment with no scope (region ""), "the assessment with no geographic scope".
    private static string RegionalName(string region) =>
        string.IsNullOrWhiteSpace(region) ? "the assessment with no geographic scope" : $"the {region} assessment";
    /// A region in the regional table and the status summary; "" is an assessment IUCN published with no scope.
    public static string RegionLabel(string region) => string.IsNullOrWhiteSpace(region) ? NoScopeLabel : region;
    public const string NoScopeLabel = "No scope given";
    /// Under the heading of a taxon IUCN assessed under a working name. marker: "sp. nov.";
    /// rank: "species", "subspecies" or "variety". hasSynonyms: the page has a Synonyms table.
    public static string ProvisionalName(string marker, string rank, bool hasSynonyms) =>
        $"Provisional name: \"{marker}\" means new {rank}. The {rank} had not been formally described when IUCN assessed it."
        + (hasSynonyms ? " If it has been described since, its published name may be in the Synonyms table." : "");
    /// The status summary of a taxon with no global assessment, some of whose current assessments
    /// have no scope. withNoGlobal: the line comes first, so it starts with "No global assessment.".
    public static string NoScopeLine(int count, bool withNoGlobal) =>
        (withNoGlobal ? "No global assessment. " : "")
        + (count == 1
            ? "IUCN published the current assessment of this taxon with no geographic scope."
            : $"IUCN published {count} current assessments of this taxon with no geographic scope.");
    /// Beside the IUCN link of an assessment that the IUCN API answers 404 for.
    public const string ApiNotFoundNote = "Not found in the IUCN API";
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
    public const string ColTaxon = "Taxon";
    public const string ColCriteria = "Criteria";
    public const string ColWikitext = "Wikitext";
    public const string ColRegion = "Region";
    public const string ColAssessment = "Assessment";
    public const string ShowWikitext = "Show wikitext";
    /// The accessible name of a row's "Show wikitext" link. region: null for a global assessment.
    /// versionNote: the row's VersionNote, so an errata version and the assessment it replaced
    /// have different names.
    public static string ShowWikitextAccessible(string? region, int? year, string? versionNote = null) {
        var assessment = region is null ? "the assessment" : RegionalName(region);
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


    // The status in the taxobox of the English Wikipedia article, in the Links section.
    public const string TaxoboxStatusHeading = "Status in the Wikipedia taxobox";
    public const string TaxoboxNoStatus = "The taxobox has no IUCN status.";
    public const string TaxoboxLatestIs = "The latest assessment is";
    public static string TaxoboxOtherSystem(string wikipediaSystem, string latestSystem) =>
        $"with status_system = {wikipediaSystem}. The latest assessment uses status_system = {latestSystem}.";
    public const string TaxoboxCitesLatest = "Up to date, with a reference to the latest assessment.";
    public static string TaxoboxCitesOther(int? year, long assessmentId, int? latestYear = null) => year is { } y
        ? latestYear is { } ly
            ? $"The same category as the latest assessment ({ly.ToString(CultureInfo.InvariantCulture)}), but the reference cites the {y.ToString(CultureInfo.InvariantCulture)} assessment."
            : $"The same category as the latest assessment, but the reference cites the {y.ToString(CultureInfo.InvariantCulture)} assessment."
        : $"The same category as the latest assessment, but the reference cites another assessment (ID {assessmentId.ToString(CultureInfo.InvariantCulture)}).";
    public const string TaxoboxCitesNone = "The same category as the latest assessment. The reference names no assessment.";
    public static string TaxoboxCopyDate(string date) =>
        $"From the copy of the article downloaded on {date}. The article may have changed since then.";
}
