using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Update;

/// A taxon as the status updater needs it, with its latest global assessment (null when it has
/// none, or is not in the release). Kind: TaxonKinds. NodeId: the lowest group the taxon is in
/// (higher_taxon), for a taxon in the release.
public sealed record StatusTaxon(long TaxonId, string ScientificName, bool InRelease, long? CurrentTaxonId, AssessmentRow? LatestGlobal,
    string Kind = TaxonKinds.Species, int? NodeId = null);

/// The site database as the status updater reads it.
public interface IStatusLookup {
    StatusTaxon? GetTaxon(long taxonId);

    /// The taxa in the release that have this name as a scientific name, a synonym (from any
    /// source), an English common name, or the title of their English Wikipedia article, compared
    /// after SiteNameKey.Fold.
    IReadOnlyCollection<long> InReleaseTaxaWithName(string name, StatusNameKind kind);

    /// The scope of an assessment ("Global", "Europe"), or null when the site has no such assessment.
    string? AssessmentScope(long assessmentId);

    /// The IUCN region whose assessments stand in for the global ones ("Europe"): a taxon's
    /// LatestGlobal is then its latest assessment in that region. Null: global assessments.
    string? Region => null;

    /// The taxon with its latest global assessment, whatever Region is: a taxobox shows the global status.
    StatusTaxon? GetGlobalTaxon(long taxonId) => GetTaxon(taxonId);
}

public enum StatusNameKind { Scientific, Synonym, EnglishCommonName, ArticleTitle }

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
    /// A replaced citation is {{cite Q}} for the latest assessment's Wikidata item, when it has one,
    /// instead of {{cite iucn}} (IucnReference).
    public bool CiteQ { get; init; }
    /// When no scientific name or synonym in a row or line names a taxon, use an English common name
    /// in it that names exactly one taxon.
    public bool MatchCommonNames { get; init; }
    /// Add {{IUCN status}} after the scientific name on list lines ("*" or "#") that name exactly one
    /// taxon and have no status. Off: the lines are only counted (StatusUpdateResult.ListLinesWithoutStatus).
    public bool AddToListLines { get; init; }
    /// Add an "IUCN status" column to wikitables whose rows name taxa and that have no status column.
    /// Off: the tables are only counted (StatusUpdateResult.TablesWithoutStatus).
    public bool AddStatusColumns { get; init; }
    /// Put a status added to a list line at the end of the line, before its references, instead of
    /// after the scientific name.
    public bool StatusAtLineEnd { get; init; }
    /// Add a reference to each status added to a list line or a new column: the named reference the
    /// text already defines for the assessment, else {{cite iucn}} (or {{cite Q}}, CiteQ) in a <ref>.
    public bool AddReferences { get; init; }
    /// The heading of an added status column; null for StatusUpdater.StatusColumnHeader.
    public string? ColumnHeader { get; init; }
    /// The tables that get a status column, by the line of their header row in the text; null for all.
    public IReadOnlySet<int>? ColumnTables { get; init; }
    /// Add an {{IUCN statuses}} box with the counts to a text that has species table rows and no box
    /// (IucnStatusesSummary). Off: StatusUpdateResult.SummaryMissing says one could be added.
    public bool AddStatusSummary { get; init; }
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
    /// A list line with no status, which StatusUpdateOptions.AddToListLines gives {{IUCN status}}.
    ListLineAdded,
    /// A wikitable with no status column, which StatusUpdateOptions.AddStatusColumns gives one; the
    /// finding is the header row.
    TableColumnAdded,
    /// A row of a table that got a status column: the new cell.
    TableRowAdded,
    /// {{IUCN statuses}}, the box of counts by category (IucnStatusesSummary); Before is empty when
    /// the box was added.
    StatusSummary,
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
    /// Comparing with a region's assessments (IStatusLookup.Region): the taxon has none there.
    NoRegionalAssessment,
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
    /// The template's taxon id (Id) is of another taxon than the one its row or line names (Detail);
    /// the template was not changed.
    IdOfAnotherTaxon,
    /// The template's taxon id (Id) is also on rows or lines that name other taxa (IdOfAnotherTaxon),
    /// and this row or line names no taxon the site has; the template was not changed.
    IdUsedForOtherTaxa,
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
    /// The taxon was found by a synonym (Detail), not by its scientific name.
    MatchedBySynonym,
    /// No name matched, or a name matched several taxa; the taxon was found by the taxon id in an IUCN
    /// citation in the item's row or line (Detail: the name of the reference it uses, or null for a citation written there).
    MatchedByCitation,
    /// The taxon was found by an English common name (Detail); StatusUpdateOptions.MatchCommonNames is on.
    MatchedByCommonName,
    /// No name matched, but an English common name (Detail) names exactly one taxon;
    /// StatusUpdateOptions.MatchCommonNames would use it.
    CommonNameNotUsed,
    /// No scientific name matched; the taxon was found by a link to its English Wikipedia article
    /// (Detail: the link's target as written).
    MatchedByArticle,
    /// The list line already gives a status in its text or as a status image (Detail), so no template was added.
    StatusInText,
    /// A status column was added to the table after column Id (1-based); Detail: "rows with a status/data rows".
    ColumnAdded,
    /// No status column was added. Detail: why (StatusUpdater.Layout*); Id: the line of the first row
    /// with the problem, or of the table when it has no header row at the top.
    ColumnLayout,
    /// The row got an empty status cell, because no status was found for it.
    EmptyCellAdded,
    /// No status column was added: the reader left the table out (StatusUpdateOptions.ColumnTables).
    ColumnNotChosen,
    /// {{IUCN statuses}}: what the counts were made from (Detail: "rows" for {{Species table/row}},
    /// "templates" for {{IUCN status}}; Id: how many).
    SummaryCountedFrom,
    /// {{IUCN statuses}}: the counts that changed (Detail: "en 6 → 7, ne 50 → 45").
    SummaryChanged,
    /// {{IUCN statuses}}: rows or templates (Id) with a code the box has no count for, left out.
    SummaryUncounted,
    /// {{IUCN statuses}} was added after the heading (Detail), or before the first species table when null.
    SummaryAdded,
    /// The text has two or more {{IUCN statuses}}, so none was changed.
    SummaryTwoOrMore,
    /// The text has nothing to count, so the {{IUCN statuses}} was not changed.
    SummaryNothingToCount,
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
/// found after the first MaxItems, which were left as they are. Populations: species table rows whose
/// population differs from IUCN's number of mature individuals, which are listed and not changed.
/// ListLinesWithoutStatus and TablesWithoutStatus: the list lines and tables that the options
/// AddToListLines and AddStatusColumns would give a status, counted when those options are off.
/// Members: the taxa the text lists (ListMember), for ListScope.
public sealed record StatusUpdateResult(string Text, IReadOnlyList<StatusFinding> Findings, int NotChecked,
    IReadOnlyList<PopulationSuggestion> Populations, int ListLinesWithoutStatus = 0, int TablesWithoutStatus = 0,
    IReadOnlyList<ListMember>? Members = null) {
    /// How many missing taxa ListPlacement put into Text.
    public int MissingAdded { get; init; }

    /// How many taxa now in another category ListPlacement took out of Text.
    public int TaxaRemoved { get; init; }

    /// The text has species table rows and no {{IUCN statuses}}, and StatusUpdateOptions.AddStatusSummary is off.
    public bool SummaryMissing { get; init; }

    /// How many items have a note of this kind: used to offer an option that would change them.
    public int CountNotes(StatusNoteKind kind) => Findings.Count(f => f.Notes.Any(n => n.Kind == kind));

    public int Updated => Findings.Count(f => f.Outcome == StatusOutcome.Updated);
    public int Current => Findings.Count(f => f.Outcome == StatusOutcome.Current);
    public int NotUpdated => Findings.Count(f => f.Outcome == StatusOutcome.NotUpdated);
}

/// A taxon the text lists: found on a list line, in a table row or a species table row, or by the
/// ids of an {{IUCN status}} template (not in a taxobox or a citation). Written: the name that found
/// it (a synonym or common name, else the taxon's scientific name). WrittenCode: the status code the
/// text gave it before any change, or null when it had none. Source: where the text lists it.
/// HasStatusTemplate: a list line that has {{IUCN status}}, or gets one.
public sealed record ListMember(StatusTaxon Taxon, int Line, string Written, string? WrittenCode,
    ListMemberSource Source = ListMemberSource.Other, bool HasStatusTemplate = false) {
    /// On a list line ("*" or "#").
    public bool OnListLine => Source == ListMemberSource.ListLine;
}

public enum ListMemberSource {
    /// An {{IUCN status}} with ids in prose.
    Other,
    /// A list line ("*" or "#").
    ListLine,
    /// A row of a wikitable.
    TableRow,
    /// A {{Species table/row}}.
    SpeciesTableRow,
}
