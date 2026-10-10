using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// What the two pages of a taxon share: the taxon page (/species/{id}, SpeciesModel) and its
/// wikitext page (/species/{id}/wikitext, SpeciesWikitextModel). Both show the taxon's assessment
/// tables; only the wikitext page has the "Show wikitext" column that picks the assessment to cite.
public abstract class TaxonPageModel : PageModel {
    protected readonly SiteDatabase _db;
    protected readonly SiteQueries _queries;
    protected readonly SiteOptions _options;

    protected TaxonPageModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options) {
        _db = db;
        _queries = queries;
        _options = options.Value;
    }

    public long RequestedTaxonId { get; private set; }
    public string? Version { get; private set; }
    public TaxonRow? Taxon { get; private set; }
    public TaxonSummary? Parent { get; private set; }

    /// False for a taxon that is not in the release: an old IUCN id, or a taxon IUCN no longer
    /// assesses. Its page has no status summary and no latest assessment.
    public bool InRelease => Taxon?.InRelease ?? true;

    public bool IsSpecies => Taxon?.Kind == TaxonKinds.Species;

    /// The taxa linked to this one in taxon_link. For a taxon not in the release: the taxa in the
    /// release with its name, or that IUCN lists its name as a synonym of. For a taxon in the
    /// release: the taxa not in the release (old ids) linked to it in those ways, and the taxa in the
    /// release linked to it by a working name or a provisional name.
    public IReadOnlyList<TaxonLinkRow> LinkedTaxa { get; private set; } = [];

    /// The old IUCN ids linked to this taxon, or for an old id the taxa in the release it is linked
    /// to (LinkedTaxa of the same-name and iucn-synonym kinds).
    public IReadOnlyList<TaxonLinkRow> EarlierIds { get; private set; } = [];

    /// For a taxon IUCN named with "_new" after the name of another taxon in the release
    /// ("Balaenoptera edeni_new"): that taxon. Null otherwise.
    public TaxonRow? WorkingNameOf { get; private set; }

    /// For a taxon in the release: the assessments IUCN published under a "_new" name of it
    /// (Balaenoptera edeni_new for Balaenoptera edeni), the latest in each scope of each such record.
    /// They are listed in the regional table after the taxon's own.
    public IReadOnlyList<LinkedAssessment> WorkingNameRows { get; private set; } = [];

    /// The global assessments of this taxon and of the linked taxa in one table, shown in place of
    /// the assessment history; null when no linked taxon has a global assessment.
    public CombinedHistory? Combined { get; private set; }

    /// The reasons IUCN's Table 7 gives for the category changes of the assessments in the history
    /// table (this taxon's, and in a combined history the linked taxa's), by assessment id.
    private IReadOnlyDictionary<long, CategoryChangeRow> ChangeReasons { get; set; } = new Dictionary<long, CategoryChangeRow>();

    /// The assessments in the history table that IUCN's summary tables list as Possibly Extinct, by
    /// assessment id. The history table marks only those whose own record has no such tag
    /// (ListedOnlyInTables).
    private IReadOnlyDictionary<long, IReadOnlyList<PossiblyExtinctListingRow>> PossiblyExtinctListings { get; set; }
        = new Dictionary<long, IReadOnlyList<PossiblyExtinctListingRow>>();

    /// The table listing of an assessment as Possibly Extinct (or in the Wild) when the assessment
    /// itself does not have that tag; null otherwise. A listing as PE of an assessment tagged PEW
    /// counts, so the difference shows.
    private PossiblyExtinctListingRow? ListedOnlyInTables(AssessmentRow assessment) =>
        PossiblyExtinctListings.TryGetValue(assessment.AssessmentId, out var listings)
            ? listings.FirstOrDefault(l => l.Tag == "PE" ? !assessment.PossiblyExtinct : !assessment.PossiblyExtinctInTheWild)
            : null;

    /// The PDF of the 2008 Table 7, which lists genuine changes only, for the footnote of a 2008 change
    /// with no reason; null when the page has no reasons or the database has no such table.
    private string? Table7Of2008 { get; set; }

    /// The parts of a history table that come from IUCN's summary tables, for its rows in the order
    /// shown (own: the row is this page's taxon's).
    public HistoryTableNotes TableNotes(IEnumerable<(AssessmentRow Row, bool Own)> rows) =>
        HistoryTableNotes.Build(rows.ToList(), ChangeReasons, ListedOnlyInTables, Taxon!.Kind, Table7Of2008);

    /// The taxon's IUCN Green Status assessment; null when it has none.
    public GreenStatusRow? GreenStatus { get; private set; }

    /// The status in the taxobox of the taxon's English Wikipedia article compared with the latest
    /// global assessment; null when there is no article, no taxobox about the taxon, or no latest
    /// global assessment.
    public TaxoboxStatusCheck? TaxoboxCheck { get; private set; }

    /// The taxon's Citations page at its box of taxobox status parameters for the latest global
    /// assessment, and that box's label ("{{Speciesbox}} status parameters"); null when that page has
    /// no such box (no citation parts, or a category the taxobox has no code for).
    public string? TaxoboxWikitextUrl { get; private set; }
    public string? TaxoboxWikitextLabel { get; private set; }

    /// The probable scopes of the page's assessments with no scope, by assessment id.
    private IReadOnlyDictionary<long, ProbableScopeRow> ProbableScopes { get; set; } = new Dictionary<long, ProbableScopeRow>();

    /// What _ScopeLabel shows for an assessment's scope: "No scope given" with its help and probable
    /// scope, or the region. place: where on the page ("status", "regional"), for the help's id.
    public ScopeLabelModel ScopeLabel(AssessmentRow assessment, string place) {
        var probable = ProbableScopes.GetValueOrDefault(assessment.AssessmentId);
        var help = SiteText.NoScopeHelp(_db.Snapshot?.NoScopeAssessmentCount, _db.Snapshot?.AssessmentCount)
            + (probable is null ? "" : "\n\n" + SiteText.ProbableScopeHelp(probable.Evidence));
        return new ScopeLabelModel(assessment.Scope, assessment.AssessmentId, place, help, probable);
    }

    public IReadOnlyList<AssessmentRow> GlobalHistory { get; private set; } = [];
    /// The rows of the Regional assessments table. For a taxon in the release, the latest assessment
    /// in each region. For a taxon not in the release, none is current, so every regional assessment,
    /// by region and then newest first.
    public IReadOnlyList<AssessmentRow> RegionalRows { get; private set; } = [];
    public AssessmentRow? LatestGlobal { get; private set; }

    /// The assessment in the status summary: the latest global one, or the latest regional one.
    public AssessmentRow? StatusAssessment { get; private set; }

    /// Every assessment of the taxon, global and regional.
    protected IReadOnlyList<AssessmentRow> Assessments { get; private set; } = [];

    /// Reads the taxon and its assessment tables. False when the site has no taxon with the id; the
    /// response is then a 404 and the page says so.
    protected bool LoadTaxon(long taxonId) {
        RequestedTaxonId = taxonId;
        Version = _db.Snapshot?.IucnRelease;
        Taxon = _queries.GetTaxon(taxonId);
        if (Taxon is null) {
            Response.StatusCode = StatusCodes.Status404NotFound;
            return false;
        }
        if (Taxon.ParentTaxonId is { } parentId) {
            Parent = _queries.GetSummary(parentId);
        }
        LinkedTaxa = _queries.GetLinkedTaxa(Taxon.TaxonId);
        EarlierIds = LinkedTaxa.Where(l => l.IsEarlierId).ToList();
        WorkingNameOf = LinkedTaxa.FirstOrDefault(l => l.IsWorkingName && l.IsFrom)?.Taxon;
        GreenStatus = _queries.GetGreenStatus(Taxon.TaxonId);
        LoadAssessments();
        WorkingNameRows = LinkedTaxa.Where(l => l.IsWorkingName && !l.IsFrom)
            .SelectMany(l => LatestPerScope(_queries.GetAssessments(l.Taxon.TaxonId).Where(a => !a.IsGlobal))
                .Select(a => new LinkedAssessment(a, l.Taxon)))
            .ToList();
        ProbableScopes = _queries.GetProbableScopes(Assessments.Concat(WorkingNameRows.Select(r => r.Assessment))
            .Where(a => a.HasNoScope).Select(a => a.AssessmentId).ToList());
        TaxoboxCheck = LatestGlobal is { } latest && Taxon.EnwikiTitle is not null && _queries.GetEnwikiTaxoboxStatus(Taxon.TaxonId) is { } taxobox
            ? TaxoboxStatusCheck.For(taxobox, latest, GlobalHistory)
            : null;
        // The same conditions as the box on the Citations page (SpeciesWikitextModel.BuildWikitext).
        if (LatestGlobal is { } latestGlobal && PartsOf(latestGlobal) is not null && IucnCategories.HasTaxoboxCode(latestGlobal)) {
            TaxoboxWikitextUrl = AssessmentToolModel.WikipediaPath(Taxon.TaxonId) + "#wikitext-speciesbox";
            TaxoboxWikitextLabel = TaxoboxTemplate.For(Taxon.Kind, Taxon.Kingdom).Label;
        }
        Combined = CombinedHistory.Build(Taxon, EarlierIds,
            id => id == Taxon.TaxonId ? Assessments : _queries.GetAssessments(id), _queries.GetTaxonomicNotesFlags);
        var historyTaxa = Combined?.Ids.Select(i => i.TaxonId).ToList() ?? [Taxon.TaxonId];
        ChangeReasons = _queries.GetCategoryChanges(historyTaxa);
        Table7Of2008 = ChangeReasons.Count == 0 ? null : _queries.GetSummaryTableUrl(7, "2008");
        PossiblyExtinctListings = _queries.GetPossiblyExtinctListings(historyTaxa);
        return true;
    }

    private void LoadAssessments() {
        var all = _queries.GetAssessments(Taxon!.TaxonId);
        Assessments = all;
        GlobalHistory = all.Where(a => a.IsGlobal).ToList();
        LatestGlobal = !Taxon.InRelease ? null
            : GlobalHistory.FirstOrDefault(a => a.AssessmentId == Taxon.LatestGlobalAssessmentId)
                ?? (Taxon.LatestGlobalAssessmentId is null ? null : GlobalHistory.FirstOrDefault(a => a.IsLatest));

        var regional = all.Where(a => !a.IsGlobal);
        RegionalRows = Taxon.InRelease
            ? LatestPerScope(regional)
            : regional
                .OrderBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
                .ThenByDescending(a => a.YearPublished ?? 0)
                .ThenByDescending(a => a.AssessmentDate, StringComparer.Ordinal)
                .ThenByDescending(a => a.AssessmentId)
                .ToList();

        // A taxon not in the release has no status summary: none of its assessments is current.
        StatusAssessment = !Taxon.InRelease ? null : LatestGlobal ?? RegionalRows
            .OrderByDescending(a => a.YearPublished ?? 0)
            .ThenByDescending(a => a.AssessmentDate)
            .FirstOrDefault();
    }

    // The latest assessment in each region: the row flagged latest, or the newest one when none is.
    // assessments: newest first (SiteQueries.GetAssessments).
    private static List<AssessmentRow> LatestPerScope(IEnumerable<AssessmentRow> assessments) => assessments
        .GroupBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
        .Select(g => g.FirstOrDefault(a => a.IsLatest) ?? g.First())
        .OrderBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
        .ToList();

    // citation_json written by `site build-db`. Unknown properties are ignored, so nothing but the
    // citation parts can reach the page.
    protected static IucnCitationParts? ReadParts(string? json) {
        try {
            return IucnCitationParts.FromJson(json);
        } catch (JsonException) {
            return null;
        }
    }

    private readonly Dictionary<long, IucnCitationParts?> _partsById = [];

    protected IucnCitationParts? PartsOf(AssessmentRow assessment) {
        if (!_partsById.TryGetValue(assessment.AssessmentId, out var parts)) {
            parts = ReadParts(assessment.CitationJson);
            _partsById[assessment.AssessmentId] = parts;
        }
        return parts;
    }

    /// The name in the title registered with Crossref for the assessment's DOI, when it names the
    /// taxon differently from the taxon's current name; null otherwise. taxon: the taxon the row is
    /// under (another IUCN id in a combined history); this page's taxon when null. The assessment
    /// tables show it under the year (or region) of the row.
    public string? DoiTitleName(AssessmentRow assessment, TaxonRow? taxon = null) =>
        PartsOf(assessment)?.RegisteredNameDifferentFrom((taxon ?? Taxon)?.ScientificName);

    /// A short note for the assessment tables when the assessment is an errata or amended version,
    /// or was replaced by one ("Replaced by the errata version"); null otherwise. An errata version
    /// keeps the year of the assessment it replaces, so without the note the two rows look the same.
    public string? VersionNote(AssessmentRow assessment) {
        if (assessment.ReplacedByAssessmentId is { } replacedBy) {
            var replacing = Assessments.FirstOrDefault(a => a.AssessmentId == replacedBy)
                ?? Combined?.Rows.FirstOrDefault(r => r.Assessment.AssessmentId == replacedBy)?.Assessment;
            var replacingParts = replacing is null ? null : PartsOf(replacing);
            return replacingParts?.ErrataYear is not null ? SiteText.ReplacedByErrata
                : replacingParts?.AmendsYear is not null ? SiteText.ReplacedByAmended
                : SiteText.ReplacedByLater;
        }
        var parts = PartsOf(assessment);
        if (parts?.ErrataYear is { } errataYear) {
            return SiteText.ErrataVersion(errataYear);
        }
        if (parts?.AmendsYear is { } amendsYear) {
            return SiteText.AmendedVersion(amendsYear);
        }
        return null;
    }
}
