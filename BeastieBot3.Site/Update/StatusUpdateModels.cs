using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Update;

/// A taxon as the status updater needs it, with its latest global assessment (null when it has
/// none, or is not in the release).
public sealed record StatusTaxon(long TaxonId, string ScientificName, bool InRelease, long? CurrentTaxonId, AssessmentRow? LatestGlobal);

/// The site database as the status updater reads it.
public interface IStatusLookup {
    StatusTaxon? GetTaxon(long taxonId);

    /// The taxa in the release that have this scientific name (synonyms: false) or this IUCN
    /// synonym (synonyms: true), compared after SiteNameKey.Fold.
    IReadOnlyCollection<long> InReleaseTaxaWithName(string name, bool synonyms);

    /// The scope of an assessment ("Global", "Europe"), or null when the site has no such assessment.
    string? AssessmentScope(long assessmentId);
}

/// Changes the reader asks for on top of the default ones. Each is off by default.
public sealed record StatusUpdateOptions {
    /// Write CR(PE) or CR(PEW) for a possibly extinct taxon in a table cell or a species table row
    /// that holds a bare code, instead of keeping CR.
    public bool PossiblyExtinctCodes { get; init; }
    /// Add the taxon id and assessment id ("|4828/21289898|1") to {{IUCN status}} templates with none.
    public bool AddIds { get; init; }
    /// Add |year= to {{IUCN status}} templates with neither year= nor label= (not for EX and EW).
    public bool AddYear { get; init; }
    /// Replace {{cite iucn}} citations of an older global assessment, anywhere in the text, with a
    /// citation of the latest one. Those in a taxobox's status_ref are always replaced.
    public bool UpdateCitations { get; init; }
}

public enum StatusItemKind {
    /// {{IUCN status}} with a taxon id.
    StatusTemplate,
    /// A status cell of a wikitable: a code, or {{IUCN status}} with no ids.
    TableCell,
    /// The status lines of a taxobox.
    Taxobox,
    /// {{IUCN status}} with no ids on a list line ("* ''Genus species'' {{IUCN status|EN}}").
    ListLine,
    /// The iucn-status parameter of {{Species table/row}}.
    SpeciesTableRow,
    /// A {{cite iucn}} citation outside a taxobox's status_ref.
    Citation,
}

public enum StatusOutcome { Updated, Current, NotUpdated }

public enum StatusNoteKind {
    /// The template's taxon id is not in the release; the current taxon (Id) was used.
    UsedCurrentTaxon,
    /// No taxon with the template's taxon id (Id).
    TaxonNotFound,
    /// The taxon (Id) is not in the release and no taxon in the release has its name.
    NotInRelease,
    /// The taxon has no global assessment.
    NoGlobalAssessment,
    /// The latest global assessment's category (Detail) has no code in this template or taxobox.
    NoCode,
    /// {{IUCN status}} with no taxon id, outside a status column of a table.
    NoTaxonId,
    /// The ids parameter (Detail) is not "taxonId/assessmentId".
    BadTaxonId,
    /// No scientific name in the row (table cell) or the taxobox.
    NoName,
    /// No taxon in the release has the name (Detail: the names tried).
    NameNotFound,
    /// The names in the row or taxobox (Detail) name two or more taxa in the release.
    NameAmbiguous,
    /// The taxobox's status_system (Detail) is not an IUCN system.
    OtherStatusSystem,
    /// The taxobox's status (Detail) is not an IUCN category code ("fossil", "DOM").
    UnknownStatusCode,
    /// status_system was added.
    StatusSystemAdded,
    /// The {{cite iucn}} in status_ref was replaced with one for the latest assessment.
    CitationReplaced,
    /// status_ref has no {{cite iucn}}, so its reference was not checked.
    RefNotChecked,
    /// status_ref reuses a named reference defined elsewhere (Detail: the name).
    RefDefinedElsewhere,
    /// The taxobox has no status_ref.
    NoStatusRef,
    /// The {{cite iucn}} is for an older assessment, and the site has no citation for the latest one.
    NoCitation,
    /// A bare CR was kept for a taxon that is possibly extinct (Detail: CR(PE) or CR(PEW)).
    PossiblyExtinctKept,
    /// The template has no ids; StatusUpdateOptions.AddIds would add them (Detail: the ids).
    IdsNotAdded,
    /// The template has no year= or label=; StatusUpdateOptions.AddYear would add one (Detail: the year).
    YearNotAdded,
    /// The ids (Detail) were added.
    IdsAdded,
    /// The template names the taxon but no assessment; StatusUpdateOptions.AddIds would write both (Detail: the ids).
    AssessmentIdNotAdded,
    /// The template's taxon id (Id) is not on the site; the taxon (Detail: its name) was found by the name in the row or line.
    IdNotFoundMatchedByName,
    /// The template's taxon id (Id) is not in the release; the taxon (Detail) was found by name.
    IdNotInReleaseMatchedByName,
    /// year= (Detail) was added.
    YearAdded,
    /// The citation names no assessment (no T…A… id in article-number, id, url or doi).
    CitationWithoutIds,
    /// The citation is of an older global assessment; StatusUpdateOptions.UpdateCitations would replace it.
    CitationOlder,
    /// The {{cite iucn}} was replaced with a citation of the latest assessment (outside a taxobox).
    CitationUpdated,
    /// The row's binomial is abbreviated and no {{Species table}} above it gives the genus.
    NoGenus,
    /// The latest assessment has no population trend, so a species table row's direction was not checked.
    NoPopulationTrend,
    /// A species table row's direction has no trend template ({{decrease}} and so on), so it was left;
    /// Detail: the template for the latest trend.
    DirectionNotRecognised,
}

public sealed record StatusNote(StatusNoteKind Kind, string? Detail = null, long? Id = null);

/// One thing the updater found. Before and After are the template, cell content or taxobox lines
/// (After is null when nothing changed). Taxon is the taxon the latest assessment came from.
public sealed record StatusFinding(
    StatusItemKind Kind,
    int Line,
    StatusOutcome Outcome,
    string Before,
    string? After,
    StatusTaxon? Taxon,
    IReadOnlyList<StatusNote> Notes);

/// Text: the input with the updates applied. Findings: in the order of the text. NotChecked: items
/// found after the first MaxItems, which were left as they are.
public sealed record StatusUpdateResult(string Text, IReadOnlyList<StatusFinding> Findings, int NotChecked) {
    /// How many items have a note of this kind: used to offer an option that would change them.
    public int CountNotes(StatusNoteKind kind) => Findings.Count(f => f.Notes.Any(n => n.Kind == kind));

    public int Updated => Findings.Count(f => f.Outcome == StatusOutcome.Updated);
    public int Current => Findings.Count(f => f.Outcome == StatusOutcome.Current);
    public int NotUpdated => Findings.Count(f => f.Outcome == StatusOutcome.NotUpdated);
}
