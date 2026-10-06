using System.Globalization;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

/// Strings of the status update page (/update). The three results are named the same way in the
/// summary, the report table and the notes: "changed", "already up to date", "left as is".
public static partial class UpdateText {
    public const string Heading = "Update IUCN statuses in wikitext";
    public const string NavLink = "Update wikitext";

    /// The link on the home page.
    public const string HomeLink = "Update the IUCN statuses in an article or list";

    public static string Intro(string? version) =>
        "Paste the wikitext of an article or list. This page gives back the same wikitext with IUCN statuses changed to match the latest global assessments"
        + (version is null ? "." : $" in Red List {version}.")
        + " All other text is returned exactly as pasted.";

    public const string IntroItemsLabel = "Updated items:";

    public const string IntroTemplatesTitle = "{{IUCN status}} templates with a taxon id";
    public const string IntroTemplates = ", such as {{IUCN status|EN|4828/21289898|1|year=2015}}: the code, the ids and the year. EX and EW have no year, so the year is removed. An old taxon id is changed to the current id.";

    public const string IntroTablesTitle = "Status columns in wikitables";
    public const string IntroTables = ": cells that contain only a code (\"EN\", \"LR/nt\") or {{IUCN status|EN}}, in a column with \"IUCN\", \"Red List\" or \"status\" in its heading. Status columns named for other lists, such as EPBC or CITES, are not changed. The taxon is found by the scientific name in the same row.";

    public const string IntroTaxoboxesTitle = "Taxoboxes with a status parameter";
    public const string IntroTaxoboxes = ": status, status_system, and the {{cite iucn}} in status_ref when it cites an older assessment. The taxon is found by the taxobox's scientific name.";

    public const string IntroListsTitle = "{{IUCN status}} on list lines";
    public const string IntroLists = ", such as * [[Aye-aye]], ''Daubentonia madagascariensis'' {{IUCN status|EN}}: the code. The taxon is found by the scientific name earlier on the line.";

    public const string IntroSpeciesTablesTitle = "Species tables";
    public const string IntroSpeciesTables = ": the iucn-status and direction parameters of {{Species table/row}}. direction is set to the latest assessment's population trend, such as {{decrease|Population declining}}, and the reference after it is kept. population is not changed: when it differs from the number of mature individuals in the latest assessment, the row is listed under \"Population differences\" after the Items found table. range, size, habitat and diet are not checked. The taxon is found by the row's binomial, with the genus from the {{Species table}} above it.";

    public static string IntroLimits(int maxItems) =>
        $"Limits: 2 MB of text and {Count(maxItems)} items. Items after the first {Count(maxItems)} are left as they are.";

    public const string InputLabel = "Wikitext of an article or list";
    public const string Submit = "Update statuses";
    public const string NotSaved = "This site does not save your text or edit Wikipedia. Copy the updated wikitext back into the article yourself.";

    public const string OptionsLegend = "Also change";
    public const string OptionPossiblyExtinct = "CR to CR(PE) or CR(PEW) in table cells and species tables, for possibly extinct taxa";
    public const string OptionIds = "Add the taxon id and assessment id to {{IUCN status}} templates that lack them";
    public const string OptionYear = "Add year= to {{IUCN status}} templates that have no year";
    public const string OptionCitations = "Replace {{cite iucn}} citations of older assessments with citations of the latest ones";
    public const string OptionCiteQ = "Use {{cite Q}} instead of {{cite iucn}} in replaced citations when the latest assessment has a Wikidata item";
    public const string OptionCommonNames = "Match common names: when no scientific name in an item matches IUCN's, find the taxon by its English common name, if only one taxon has that name";

    // Offers above the result, shown when an option that is off would change items.
    public static string OfferPossiblyExtinct(int n) =>
        n == 1 ? "1 item kept CR for a possibly extinct taxon." : $"{Count(n)} items kept CR for possibly extinct taxa.";
    public const string OfferPossiblyExtinctButton = "Use CR(PE) and CR(PEW)";
    public static string OfferIds(int n) =>
        n == 1 ? "1 {{IUCN status}} template has no ids." : $"{Count(n)} {{{{IUCN status}}}} templates have no ids.";
    public const string OfferIdsButton = "Add ids";
    public static string OfferAssessmentIds(int n) =>
        n == 1 ? "1 {{IUCN status}} template has a taxon id but no assessment id." : $"{Count(n)} {{{{IUCN status}}}} templates have a taxon id but no assessment id.";
    public static string OfferYear(int n) =>
        n == 1 ? "1 {{IUCN status}} template has no year." : $"{Count(n)} {{{{IUCN status}}}} templates have no year.";
    public const string OfferYearButton = "Add years";
    public static string OfferCitations(int n) =>
        n == 1 ? "1 {{cite iucn}} cites an older assessment." : $"{Count(n)} {{{{cite iucn}}}} citations cite older assessments.";
    public const string OfferCitationsButton = "Replace citations";
    public static string OfferCommonNames(int n) =>
        n == 1 ? "1 item was not found by scientific name. Its English common name matches one taxon."
            : $"{Count(n)} items were not found by scientific name. Their English common names each match one taxon.";
    public const string OfferCommonNamesButton = "Match common names";

    public const string ResultHeading = "Result";

    public const string EditSummaryLabel = "Edit summary";
    public const string CopyEditSummaryAccessible = "Copy edit summary";
    public const string EditSummaryHelp = "A starting point for the edit summary on Wikipedia. Check it before you save.";
    public const string EditSummaryCredit = "assisted by Beastie Bot Species Status";

    public const string OutputLabel = "Updated wikitext (read only)";
    public const string CopyOutputAccessible = "Copy updated wikitext";
    public const string NoChanges = "No items were changed. The updated wikitext is the same as the text you pasted.";

    public static string Summary(int changed, int current, int left) {
        var total = changed + current + left;
        if (total == 0) {
            return "No IUCN statuses found. This page looks for {{IUCN status}} templates, status columns in wikitables, and taxoboxes with a status parameter.";
        }
        return $"{Items(total)} found: {Count(changed)} changed, {Count(current)} already up to date, {Count(left)} left as is.";
    }

    /// The summary when more items were found than are checked.
    public static string SummaryCapped(int maxItems, int changed, int current, int left, int rest) =>
        $"More than {Count(maxItems)} items found. The first {Count(changed + current + left)} were checked: {Count(changed)} changed, "
        + $"{Count(current)} already up to date, {Count(left)} left as is. The other {Items(rest)} {(rest == 1 ? "was" : "were")} not checked and "
        + $"{(rest == 1 ? "is" : "are")} unchanged. To update them, paste the rest of the text separately.";

    public const string ErrorEmpty = "No text to update. Paste the wikitext of an article or list.";
    public const string ErrorTooLarge = "Text too large: the limit is 2 MB. Split the text into parts and update each part separately.";
    public const string ErrorUnreadable = "Could not read the form. Reload the page and paste the text again.";

    public const string ReportHeading = "Items found";
    public const string FilterLegend = "Show:";
    public static string FilterChanged(int n) => $"Changed ({Count(n)})";
    public static string FilterCurrent(int n) => $"Already up to date ({Count(n)})";
    public static string FilterLeft(int n) => $"Left as is ({Count(n)})";
    public static string FilterAll(int n) => $"All items ({Count(n)})";
    public const string ColumnLine = "Line";
    public const string ColumnResult = "Result";
    public const string ColumnItem = "Item";
    public const string ColumnBefore = "Before";
    public const string ColumnAfter = "After";
    public const string ColumnTaxon = "Taxon";
    public const string ColumnNotes = "Notes";

    public const string PopulationHeading = "Population differences";
    public const string PopulationIntro = "In these rows, population differs from the number of mature individuals in the latest global assessment and was not changed, because IUCN often gives a band such as 2,500\u20139,999 or a best estimate with a range.";
    public const string PopulationKey = "IUCN mature individuals is the value as IUCN publishes it, with the year of the assessment. U means unknown. A number after a comma is a best estimate or a second range.";
    public const string ColumnPopulationNow = "Population now";
    public const string ColumnPopulationIucn = "IUCN mature individuals";
    public const string ColumnPopulationSuggested = "Suggested population";

    public static string PopulationIucnValue(string value, int? year) => year is { } y ? $"{value} ({y})" : value;

    private static string Count(int n) => n.ToString("N0", CultureInfo.InvariantCulture);

    private static string Items(int n) => n == 1 ? "1 item" : $"{Count(n)} items";
}
