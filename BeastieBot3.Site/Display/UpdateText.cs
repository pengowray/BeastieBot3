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

    public static string IntroLimits(int maxItems) =>
        $"Limits: 2 MB of text and {Count(maxItems)} items. Items after the first {Count(maxItems)} are left as they are.";

    public const string InputLabel = "Wikitext of an article or list";
    public const string Submit = "Update statuses";
    public const string NotSaved = "This site does not save your text or edit Wikipedia. Copy the updated wikitext back into the article yourself.";

    public const string ResultHeading = "Result";
    public const string OutputLabel = "Updated wikitext";
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
    public const string ColumnLine = "Line";
    public const string ColumnResult = "Result";
    public const string ColumnItem = "Item";
    public const string ColumnBefore = "Before";
    public const string ColumnAfter = "After";
    public const string ColumnTaxon = "Taxon";
    public const string ColumnNotes = "Notes";

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
            _ => note.Kind.ToString(),
        };
    }

    private static string Count(int n) => n.ToString("N0", CultureInfo.InvariantCulture);

    private static string Items(int n) => n == 1 ? "1 item" : $"{Count(n)} items";
}
