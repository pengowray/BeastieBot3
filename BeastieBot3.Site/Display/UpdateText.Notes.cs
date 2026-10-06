using System.Globalization;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's report: the result and item of each row, and its notes.

public static partial class UpdateText {
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
        StatusItemKind.ListLine or StatusItemKind.ListLineAdded => "List line",
        StatusItemKind.TableColumnAdded => "Table header row",
        StatusItemKind.TableRowAdded => "Table row",
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
            StatusNoteKind.MatchedByArticle =>
                $"Matched by the link to the Wikipedia article “{note.Detail}”. The Taxon column shows IUCN's scientific name.",
            StatusNoteKind.CommonNameNotUsed =>
                $"The English common name “{note.Detail}” matches one taxon, but common names were not used. To use them, click Match common names above the result.",
            StatusNoteKind.StatusInText when note.Detail?.StartsWith('(') == true =>
                $"The line already gives a status in its text: {note.Detail}.",
            StatusNoteKind.StatusInText => $"The line already gives a status with the image {note.Detail}.",
            StatusNoteKind.ColumnAdded => ColumnAdded(note),
            StatusNoteKind.ColumnLayout => ColumnLayout(note),
            StatusNoteKind.EmptyCellAdded => "Added an empty status cell.",
            StatusNoteKind.ColumnNotChosen => "Status column not added: the checkbox for this table is not ticked.",
            _ => note.Kind.ToString(),
        };
    }

    // "Added an IUCN status column after column 2. 14 of 15 rows have a status."
    private static string ColumnAdded(StatusNote note) {
        var parts = (note.Detail ?? "0/0").Split('/');
        var rows = parts.Length == 2 && int.TryParse(parts[1], CultureInfo.InvariantCulture, out var r) ? r : 0;
        var verb = parts[0] == "1" ? "has" : "have";
        return $"Added an IUCN status column after column {note.Id}. {parts[0]} of {Count(rows)} {(rows == 1 ? "row" : "rows")} {verb} a status.";
    }

    private static string ColumnLayout(StatusNote note) {
        var detail = note.Detail ?? string.Empty;
        if (detail.StartsWith(StatusUpdater.LayoutCellCount + ":", StringComparison.Ordinal)) {
            var parts = detail.Split(':');
            return $"Status column not added: the row on line {note.Id} has {parts[1]} cells and the header row has {parts[2]}.";
        }
        return detail switch {
            StatusUpdater.LayoutNoHeader => "Status column not added: the table has no header row at the top.",
            StatusUpdater.LayoutSecondHeader => $"Status column not added: line {note.Id} is a second header row.",
            _ => $"Status column not added: the row on line {note.Id} uses rowspan or colspan.",
        };
    }
}
