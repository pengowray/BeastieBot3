using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// One IUCN id in a combined assessment history: the page's own taxon or a taxon linked to it.
/// LinkKind: how it is linked to the page's taxon (TaxonLinkKinds), null for the page's own taxon.
/// Tint: the colour of its rows and legend swatch, 1 to CombinedHistory.TintCount; null when the
/// table has more ids than tints. NewestGlobal: its newest global assessment, and whether IUCN's
/// taxonomic notes on it have text (NewestHasNotes).
public sealed record HistoryId(
    TaxonRow Taxon,
    bool IsThisPage,
    string? LinkKind,
    int? Tint,
    IReadOnlyList<AssessmentRow> Global,
    int RegionalCount,
    bool NewestHasNotes) {
    public long TaxonId => Taxon.TaxonId;
    public HistoryLink Link => IsThisPage ? HistoryLink.ThisPage
        : LinkKind != TaxonLinkKinds.IucnSynonym ? HistoryLink.SameName
        : Taxon.InRelease ? HistoryLink.ThisNameIsSynonymOfIt
        : HistoryLink.NameIsSynonymOfThisTaxon;
    public AssessmentRow? NewestGlobal => Global.Count > 0 ? Global[0] : null;
    public int? FirstYear => Global.Min(a => a.YearPublished);
    public int? LastYear => Global.Max(a => a.YearPublished);
    /// The CSS class of its rows and swatch ("id-tint-2"), or null.
    public string? TintClass => Tint is { } t ? $"id-tint-{t}" : null;
}

/// How an id in a combined history is linked to the page's taxon, as the page says it.
public enum HistoryLink {
    /// The page's own taxon.
    ThisPage,
    /// The two taxa have the same scientific name.
    SameName,
    /// The page is a taxon in the release, and IUCN lists this old id's name as its synonym.
    NameIsSynonymOfThisTaxon,
    /// The page is an old id, and IUCN lists the page's name as a synonym of this taxon in the release.
    ThisNameIsSynonymOfIt,
}

/// A row of the combined table: an assessment and the id it is under.
public sealed record CombinedRow(AssessmentRow Assessment, HistoryId Id);

/// The global assessments of a taxon and of the taxa linked to it in taxon_link (old ids that have
/// the same name, or whose name IUCN lists as a synonym), in one table, newest first. Shown in place
/// of the taxon's own assessment history when at least one linked taxon has a global assessment.
/// Regional assessments stay in each taxon's own Regional assessments table: none of an old id's is
/// current, and the table of a taxon in the release lists the latest assessment in each region.
public sealed class CombinedHistory {
    /// The number of row colours (id-tint-1 to id-tint-6 in site.css). A table with more ids has no
    /// colours, so that no colour stands for two ids; its IUCN id column still names each row's id.
    public const int TintCount = 6;

    /// Taxa in the release first, then old ids, each by id; so the taxon in the release has the same
    /// colour on its own page and on an old id's page.
    public IReadOnlyList<HistoryId> Ids { get; }
    public IReadOnlyList<CombinedRow> Rows { get; }
    /// True when the ids do not all have the same scientific name, so the table has a Name column.
    public bool ShowNames { get; }
    public HistoryId ThisPage => Ids.First(i => i.IsThisPage);
    public IEnumerable<HistoryId> Others => Ids.Where(i => !i.IsThisPage);
    /// The ids whose newest global assessment has taxonomic notes: taxa in the release first.
    public IReadOnlyList<HistoryId> WithNotes => Ids.Where(i => i.NewestHasNotes && i.NewestGlobal is not null).ToList();

    private CombinedHistory(IReadOnlyList<HistoryId> ids) {
        Ids = ids;
        Rows = ids.SelectMany(id => id.Global.Select(a => new CombinedRow(a, id)))
            .OrderByDescending(r => r.Assessment.YearPublished ?? 0)
            .ThenByDescending(r => r.Assessment.AssessmentDate, StringComparer.Ordinal)
            .ThenByDescending(r => r.Assessment.AssessmentId)
            .ToList();
        ShowNames = ids.Select(i => NameKey(i.Taxon)).Distinct(StringComparer.Ordinal).Count() > 1;
    }

    /// The combined history of a page, or null when no linked taxon has a global assessment (the page
    /// then shows its own history and a line for each linked taxon). assessments: every assessment of
    /// each taxon, newest first (SiteQueries.GetAssessments). notes: has_taxonomic_notes of the
    /// newest global assessment of each taxon (SiteQueries.GetTaxonomicNotesFlags).
    public static CombinedHistory? Build(TaxonRow page, IReadOnlyList<TaxonLinkRow> linked,
        Func<long, IReadOnlyList<AssessmentRow>> assessments, Func<IReadOnlyCollection<long>, IReadOnlyDictionary<long, bool?>> notes) {
        if (linked.Count == 0) {
            return null;
        }
        var members = new List<(TaxonRow Taxon, bool IsThisPage, string? Kind, IReadOnlyList<AssessmentRow> All)> {
            (page, true, null, assessments(page.TaxonId)),
        };
        foreach (var link in linked.Where(l => l.Taxon.TaxonId != page.TaxonId).DistinctBy(l => l.Taxon.TaxonId)) {
            members.Add((link.Taxon, false, link.Kind, assessments(link.Taxon.TaxonId)));
        }
        if (!members.Any(m => !m.IsThisPage && m.All.Any(a => a.IsGlobal))) {
            return null;
        }
        var ordered = members.OrderByDescending(m => m.Taxon.InRelease).ThenBy(m => m.Taxon.TaxonId).ToList();
        var newest = ordered.Select(m => m.All.FirstOrDefault(a => a.IsGlobal)?.AssessmentId).OfType<long>().ToList();
        var flags = notes(newest);
        var tinted = ordered.Count <= TintCount;
        var ids = ordered.Select((m, i) => {
            var global = m.All.Where(a => a.IsGlobal).ToList();
            var hasNotes = global.Count > 0 && flags.TryGetValue(global[0].AssessmentId, out var flag) && flag == true;
            return new HistoryId(m.Taxon, m.IsThisPage, m.Kind, tinted ? i + 1 : null, global, m.All.Count(a => !a.IsGlobal), hasNotes);
        }).ToList();
        return new CombinedHistory(ids);
    }

    private static string NameKey(TaxonRow taxon) => taxon.SubpopulationName is { } sub ? $"{taxon.ScientificName}|{sub}" : taxon.ScientificName;
}
