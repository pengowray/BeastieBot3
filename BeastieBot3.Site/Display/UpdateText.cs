using System.Globalization;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

/// Strings of the status update page (/update). The three results are named the same way in the
/// summary, the report table and the notes: "changed", "already up to date", "left as is".
public static class UpdateText {
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

    /// "IUCN Red List 2026-1: Panthera tigris VU→EN, Ursus maritimus EN→VU; 3 other IUCN statuses updated
    /// (ids, years, references or trends); 2 IUCN citations updated (assisted by Beastie Bot Species Status)".
    /// With more changes than fit in EditSummary.MaxListLength: "42 IUCN statuses changed (12 to EN,
    /// 20 to VU, 10 to LC)". Null when nothing changed.
    // Most threatened first; codes not listed come last.
    private static int SeverityOrder(string code) {
        var i = Array.FindIndex(SeverityCodes, c => string.Equals(c, code.Replace(" ", ""), StringComparison.OrdinalIgnoreCase));
        return i < 0 ? SeverityCodes.Length : i;
    }

    private static readonly string[] SeverityCodes =
        ["EX", "EW", "CR(PE)", "CR(PEW)", "CR", "EN", "VU", "LR/cd", "NT", "LR/nt", "LC", "LR/lc", "DD", "NE"];

    public static string? EditSummary(string? version, IReadOnlyList<Update.EditSummary.CategoryChange> changes, int otherItems, int citations) {
        if (changes.Count == 0 && otherItems == 0 && citations == 0) {
            return null;
        }
        var parts = new List<string>();
        if (changes.Count > 0) {
            var list = string.Join(", ", changes.Select(c => $"{c.Name} {c.From}→{c.To}"));
            if (list.Length > Update.EditSummary.MaxListLength) {
                var byCategory = changes.GroupBy(c => c.To, StringComparer.OrdinalIgnoreCase)
                    .OrderBy(g => SeverityOrder(g.Key))
                    .Select(g => $"{Count(g.Count())} to {g.Key}");
                list = $"{Count(changes.Count)} IUCN statuses changed ({string.Join(", ", byCategory)})";
            }
            parts.Add(list);
        }
        if (otherItems > 0) {
            var other = changes.Count > 0 ? " other" : "";
            var unchanged = changes.Count > 0 ? "" : "; categories unchanged";
            parts.Add(otherItems == 1
                ? $"1{other} IUCN status updated (ids, year, reference or trend{unchanged})"
                : $"{Count(otherItems)}{other} IUCN statuses updated (ids, years, references or trends{unchanged})");
        }
        if (citations > 0) {
            parts.Add(citations == 1 ? "1 IUCN citation updated" : $"{Count(citations)} IUCN citations updated");
        }
        var prefix = version is null ? "IUCN Red List" : $"IUCN Red List {version}";
        return $"{prefix}: {string.Join("; ", parts)} ({EditSummaryCredit})";
    }
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

    public static string Outcome(StatusOutcome outcome) => outcome switch {
        StatusOutcome.Updated => "Changed",
        StatusOutcome.Current => "Already up to date",
        _ => "Left as is",
    };

    public static string OutcomeClass(StatusOutcome outcome) => outcome switch {
        StatusOutcome.Updated => "outcome-updated",
        StatusOutcome.Current => "outcome-current",
        _ => "outcome-not-updated",
    };

    public static string Kind(StatusItemKind kind) => kind switch {
        StatusItemKind.StatusTemplate => "{{IUCN status}} template",
        StatusItemKind.TableCell => "Table cell",
        StatusItemKind.ListLine => "List line",
        StatusItemKind.SpeciesTableRow => "Species table row",
        StatusItemKind.Citation => "{{cite iucn}}",
        _ => "Taxobox",
    };

    public static string Note(StatusNote note, StatusItemKind kind) {
        var several = note.Detail?.Contains(", ", StringComparison.Ordinal) == true;
        return note.Kind switch {
            StatusNoteKind.UsedCurrentTaxon => $"Old taxon id {note.Id} replaced with the current taxon id.",
            StatusNoteKind.TaxonNotFound => $"No taxon on this site has taxon id {note.Id}.",
            StatusNoteKind.NotInRelease => $"Taxon id {note.Id} is not in this Red List version, and no taxon in this version has the same scientific name.",
            StatusNoteKind.NoGlobalAssessment => "No global assessment. This taxon has regional assessments only.",
            StatusNoteKind.NoCode => $"The latest category, {note.Detail}, has no code in {{{{IUCN status}}}} or in taxoboxes.",
            StatusNoteKind.NoTaxonId => "Taxon not found: the template has no taxon id and is not in a status column of a table. Add the taxon id and assessment id, such as 4828/21289898.",
            StatusNoteKind.BadTaxonId => $"Could not read the ids \"{note.Detail}\". Expected taxon id/assessment id, such as 4828/21289898.",
            StatusNoteKind.NoName when kind == StatusItemKind.Taxobox => "No scientific name found in the taxobox (taxon=, genus= and species=, binomial= or trinomial=).",
            StatusNoteKind.NoName when kind == StatusItemKind.ListLine => "No scientific name found on this line before the template.",
            StatusNoteKind.NoName when kind == StatusItemKind.SpeciesTableRow => "No scientific name found in the row's binomial or name.",
            StatusNoteKind.NoName => "No scientific name found in this table row.",
            StatusNoteKind.NameNotFound when several => $"No taxon in this Red List version has any of the names {note.Detail}.",
            StatusNoteKind.NameNotFound => $"No taxon in this Red List version has the name {note.Detail}.",
            StatusNoteKind.NameAmbiguous when several => $"Ambiguous names: {note.Detail} match {note.Id} taxa.",
            StatusNoteKind.NameAmbiguous => $"Ambiguous name: {note.Detail} matches {note.Id} taxa.",
            StatusNoteKind.OtherStatusSystem => $"status_system is {note.Detail}, which is not an IUCN system. Taxobox left as is.",
            StatusNoteKind.UnknownStatusCode => $"status is {note.Detail}, which is not an IUCN category code.",
            StatusNoteKind.StatusSystemAdded => "Added status_system, which was missing.",
            StatusNoteKind.CitationReplaced => "Replaced the {{cite iucn}} in status_ref with a citation of the latest assessment.",
            StatusNoteKind.RefNotChecked => "Check status_ref: it has no {{cite iucn}}, so it was not checked.",
            StatusNoteKind.RefDefinedElsewhere => $"Check the reference \"{note.Detail}\": status_ref uses this named reference, which is defined elsewhere in the article. It was not checked.",
            StatusNoteKind.NoStatusRef => "No status_ref: the status changed, and the taxobox has no reference for it. The taxon's page on this site has a {{cite iucn}} to copy.",
            StatusNoteKind.NoCitation => "Check status_ref: it cites an older assessment, and this site has no citation for the latest assessment. status_ref was not changed.",
            StatusNoteKind.PossiblyExtinctKept => $"Kept as \"CR\". The latest assessment is {note.Detail}.",
            StatusNoteKind.IdsNotAdded => $"No ids. Add ids would add {note.Detail}.",
            StatusNoteKind.YearNotAdded => $"No year. Add years would add year={note.Detail}.",
            StatusNoteKind.IdsAdded => $"Added the ids {note.Detail}.",
            StatusNoteKind.AssessmentIdNotAdded => $"No assessment id. Add ids would write {note.Detail}.",
            StatusNoteKind.IdNotFoundMatchedByName => $"No taxon on this site has taxon id {note.Id}. Found by the scientific name {note.Detail} instead.",
            StatusNoteKind.IdNotInReleaseMatchedByName => $"Taxon id {note.Id} is not in this Red List version. Found by the scientific name {note.Detail} instead.",
            StatusNoteKind.YearAdded => $"Added year={note.Detail}.",
            StatusNoteKind.CitationWithoutIds => "No assessment id (such as e.T22823A14871490) in article-number, id, url or doi, so the citation was not checked.",
            StatusNoteKind.CitationOlder => "Cites an older assessment. Not replaced: select Replace citations above the result.",
            StatusNoteKind.CitationUpdated => "Replaced with a citation of the latest assessment.",
            StatusNoteKind.NoGenus => $"The binomial {note.Detail} is abbreviated, and no {{{{Species table}}}} above it gives the genus.",
            StatusNoteKind.NoPopulationTrend => "direction not checked: the latest assessment has no population trend.",
            StatusNoteKind.DirectionNotRecognised => $"direction not changed: it has no population trend template, such as {{{{decrease}}}}. To show the latest assessment's population trend, use {note.Detail}.",
            StatusNoteKind.MatchedBySynonym => $"Matched by the synonym {note.Detail}. The Taxon column shows IUCN's name.",
            StatusNoteKind.MatchedByCitation when note.Detail is not null =>
                $"No name in this item matches exactly one IUCN taxon, so the taxon was found by the taxon id in the IUCN citation in the reference named \"{note.Detail}\".",
            StatusNoteKind.MatchedByCitation =>
                "No name in this item matches exactly one IUCN taxon, so the taxon was found by the taxon id in the IUCN citation on the same row or line.",
            StatusNoteKind.MatchedByCommonName =>
                $"Matched by the English common name “{note.Detail}”. The Taxon column shows IUCN's scientific name.",
            StatusNoteKind.CommonNameNotUsed =>
                $"The English common name “{note.Detail}” matches one taxon, but common names were not used. To use them, click Match common names above the result.",
            _ => note.Kind.ToString(),
        };
    }

    private static string Count(int n) => n.ToString("N0", CultureInfo.InvariantCulture);

    private static string Items(int n) => n == 1 ? "1 item" : $"{Count(n)} items";
}
