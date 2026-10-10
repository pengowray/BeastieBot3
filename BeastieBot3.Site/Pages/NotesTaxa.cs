using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// A global assessment whose taxonomic notes name other taxa.
public sealed record NotesAssessment(long AssessmentId, int? Year);

/// One taxon named in the notes. NameInNotes: the name the notes use when it is not the taxon's own
/// (the newest assessment's that has one); Assessments: the assessments whose notes name it, newest
/// first; Order: its place in the order the notes name them (newest assessment first, then the
/// order of mention).
public sealed record NotesTaxonEntry(TaxonSummary Taxon, string? NameInNotes, IReadOnlyList<NotesAssessment> Assessments, int Order) {
    /// Its sort key by IUCN category: the most threatened first, then Data Deficient, then none.
    public int CategoryRank => NotesTaxa.CategoryRank(Taxon.Category, Taxon.PossiblyExtinct, Taxon.PossiblyExtinctInTheWild);
}

/// One item of the list: the entry, the page's taxon (for the links to its assessments), and whether
/// the item lists the assessments whose notes name the taxon (not under an assessment's heading).
public sealed record NotesTaxonItem(NotesTaxonEntry Entry, long TaxonId, bool ShowAssessments);

/// The taxa of one assessment's notes, in the order the notes name them.
public sealed record NotesAssessmentGroup(NotesAssessment Assessment, IReadOnlyList<NotesTaxonEntry> Entries);

/// "Named in IUCN's taxonomic notes" on a taxon page: the taxa the notes of the taxon's global
/// assessments name, one entry per taxon (ByTaxon) and one group per assessment (ByAssessment).
/// The page shows ByTaxon; site.js can show ByAssessment instead and sort the entries.
public sealed record NotesTaxa(long TaxonId, IReadOnlyList<NotesTaxonEntry> ByTaxon, IReadOnlyList<NotesAssessmentGroup> ByAssessment) {
    public bool IsEmpty => ByTaxon.Count == 0;

    /// The grouping choice is offered only when the notes of two or more assessments name taxa.
    public bool OffersGrouping => ByAssessment.Count > 1;

    /// The sort and group choices are offered when there is something to reorder.
    public bool OffersChoices => ByTaxon.Count > 1 || OffersGrouping;

    /// rows: GetNotesTaxa's, newest assessment first and then in the order the notes name them.
    /// Assessments published in the same year (an assessment and its errata or amended version) count
    /// as one, linked to the newest, so no year is listed twice.
    public static NotesTaxa Build(long taxonId, IReadOnlyList<NotesTaxonRow> rows) {
        var byTaxon = new List<(TaxonSummary Taxon, string? Name, List<NotesAssessment> Assessments)>();
        var index = new Dictionary<long, int>();
        var groups = new List<(NotesAssessment Assessment, List<(TaxonSummary Taxon, string? Name)> Rows)>();
        foreach (var row in rows) {
            if (groups.Count == 0 || !SameYear(groups[^1].Assessment, row)) {
                groups.Add((new NotesAssessment(row.AssessmentId, row.AssessmentYear), []));
            }
            var assessment = groups[^1].Assessment;
            if (groups[^1].Rows.All(r => r.Taxon.TaxonId != row.Taxon.TaxonId)) {
                groups[^1].Rows.Add((row.Taxon, row.NameInNotes));
            }
            if (!index.TryGetValue(row.Taxon.TaxonId, out var at)) {
                index[row.Taxon.TaxonId] = byTaxon.Count;
                byTaxon.Add((row.Taxon, row.NameInNotes, [assessment]));
                continue;
            }
            var entry = byTaxon[at];
            if (!entry.Assessments.Contains(assessment)) {
                entry.Assessments.Add(assessment);
            }
            byTaxon[at] = entry with { Name = entry.Name ?? row.NameInNotes };
        }
        var entries = byTaxon.Select((e, i) => new NotesTaxonEntry(e.Taxon, e.Name, e.Assessments, i)).ToList();
        var order = entries.ToDictionary(e => e.Taxon.TaxonId, e => e.Order);
        return new NotesTaxa(taxonId, entries, groups
            .Select(g => new NotesAssessmentGroup(g.Assessment, g.Rows
                .Select(r => new NotesTaxonEntry(r.Taxon, r.Name, [g.Assessment], order[r.Taxon.TaxonId]))
                .ToList()))
            .ToList());
    }

    // Rows of the same year, or of the same assessment when it has no year, share a group.
    private static bool SameYear(NotesAssessment group, NotesTaxonRow row) =>
        group.Year is { } year ? row.AssessmentYear == year : row.AssessmentYear is null && row.AssessmentId == group.AssessmentId;

    private static readonly string[] Order = ["EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD"];

    /// EX first, then EW, CR (Possibly Extinct before Possibly Extinct in the Wild before other CR),
    /// EN, VU, NT (with LR/cd and LR/nt), LC (with LR/lc), DD, and anything else last.
    public static int CategoryRank(string? category, bool possiblyExtinct, bool possiblyExtinctInTheWild) {
        var code = category?.Trim() switch {
            "LR/cd" or "LR/nt" => "NT",
            "LR/lc" => "LC",
            var c => c,
        };
        var at = code is null ? -1 : Array.IndexOf(Order, code);
        if (at < 0) {
            return Order.Length * 3;
        }
        var tag = code == "CR" ? (possiblyExtinct ? 0 : possiblyExtinctInTheWild ? 1 : 2) : 0;
        return at * 3 + tag;
    }
}
