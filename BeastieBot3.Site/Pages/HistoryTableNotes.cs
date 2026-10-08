using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// What a "Reason for change" cell shows.
public enum ReasonCellKind {
    /// IUCN's Table 7 gives a reason code for the change of category this assessment brought.
    Reason,
    /// Published before 2007, the year of the first Table 7.
    BeforeTables,
    /// The oldest global assessment in the table.
    FirstAssessment,
    /// The same category as the assessment before it (LR/nt counts as NT, LR/lc as LC).
    NoChange,
    /// A change published in 2008: the 2008 table lists genuine changes only.
    NoneGiven,
    /// A change that no Table 7 row was matched to.
    NotFound,
}

/// One "Reason for change" cell. Code: the reason code for Kind Reason. Footnote: the number of the
/// footnote with the table the cell is from, or null.
public sealed record ReasonCell(ReasonCellKind Kind, string? Code, int? Footnote);

/// A footnote under a history table. A table footnote has the Table 7's version and PDF (and
/// Genuine2008: it is the 2008 table, which lists genuine changes only); a Possibly Extinct footnote
/// has its Text.
public sealed record HistoryFootnote(int Number, string? TableRelease, string? TableUrl, bool Genuine2008, string? Text);

/// The parts of a history table (the assessment history or the combined assessment history) that
/// come from IUCN's summary tables: whether it has a "Reason for change" column and each row's cell,
/// the badge of an assessment that the tables list as Possibly Extinct (in the Wild) without the
/// assessment's own tag, and the numbered footnotes, in the order the rows refer to them.
public sealed class HistoryTableNotes {
    private readonly Dictionary<long, ReasonCell> _cells = new();
    private readonly Dictionary<long, (int Footnote, string? Tag)> _listed = new();

    private readonly List<HistoryFootnote> _footnotes = new();

    private HistoryTableNotes(bool showReasonColumn) => ShowReasonColumn = showReasonColumn;

    public bool ShowReasonColumn { get; }
    public IReadOnlyList<HistoryFootnote> Footnotes => _footnotes;
    /// Whether a cell shows the dash of an assessment published before 2007, for the line under the table.
    public bool HasBeforeTables { get; private set; }

    public ReasonCell? CellFor(long assessmentId) => _cells.GetValueOrDefault(assessmentId);

    /// The badge of a row: the assessment's own, except that an assessment with neither tag that the
    /// tables list as PE or PEW shows CR (PE) or CR (PEW). A listed assessment's badge refers to its footnote.
    public BadgeModel BadgeFor(AssessmentRow assessment) {
        if (!_listed.TryGetValue(assessment.AssessmentId, out var l)) return BadgeModel.For(assessment);
        var badge = l.Tag is { } tag ? new BadgeModel(IucnCategories.Describe("CR", tag == "PE", tag == "PEW")) : BadgeModel.For(assessment);
        return badge with { Footnote = l.Footnote };
    }

    /// rows: the table's rows in the order shown, newest first (own: the row is this page's taxon's).
    /// table7Of2008: the URL of the 2008 Table 7, for its footnote; null when the site has none.
    public static HistoryTableNotes Build(IReadOnlyList<(AssessmentRow Row, bool Own)> rows,
        IReadOnlyDictionary<long, CategoryChangeRow> reasons, Func<AssessmentRow, PossiblyExtinctListingRow?> listedOnlyInTables,
        string taxonKind, string? table7Of2008) {
        var notes = new HistoryTableNotes(rows.Any(r => reasons.ContainsKey(r.Row.AssessmentId)));
        var footnotes = notes._footnotes;
        var tableFootnotes = new Dictionary<string, int>(StringComparer.Ordinal);
        int TableFootnote(string release, string url) {
            if (!tableFootnotes.TryGetValue(release, out var number)) {
                number = footnotes.Count + 1;
                tableFootnotes[release] = number;
                footnotes.Add(new HistoryFootnote(number, release, url, release == "2008", null));
            }
            return number;
        }

        for (var i = 0; i < rows.Count; i++) {
            var row = rows[i].Row;
            if (listedOnlyInTables(row) is { } listing) {
                var otherTag = listing.Tag == "PE"
                    ? (row.PossiblyExtinctInTheWild ? "PEW" : null)
                    : (row.PossiblyExtinct ? "PE" : null);
                var number = footnotes.Count + 1;
                footnotes.Add(new HistoryFootnote(number, null, null, false,
                    SiteText.ListedTagFootnote(listing.Tag, listing.Tables, listing.FirstRelease, listing.LastRelease, taxonKind, otherTag)));
                // An assessment with the other tag keeps its own badge.
                notes._listed[row.AssessmentId] = (number, row.PossiblyExtinct || row.PossiblyExtinctInTheWild ? null : listing.Tag);
            }
            if (!notes.ShowReasonColumn) continue;
            var before = i + 1 < rows.Count ? rows[i + 1].Row : null;
            ReasonCell cell;
            if (reasons.TryGetValue(row.AssessmentId, out var reason)) {
                cell = new ReasonCell(ReasonCellKind.Reason, reason.Reason, TableFootnote(reason.TableRelease, reason.TableUrl));
            } else if (row.YearPublished is < 2007) {
                cell = new ReasonCell(ReasonCellKind.BeforeTables, null, null);
                notes.HasBeforeTables = true;
            } else if (before is null) {
                cell = new ReasonCell(ReasonCellKind.FirstAssessment, null, null);
            } else if (Family(before.Category) == Family(row.Category)) {
                cell = new ReasonCell(ReasonCellKind.NoChange, null, null);
            } else if (row.YearPublished == 2008 && table7Of2008 is not null) {
                cell = new ReasonCell(ReasonCellKind.NoneGiven, null, TableFootnote("2008", table7Of2008));
            } else {
                cell = new ReasonCell(ReasonCellKind.NotFound, null, null);
            }
            notes._cells[row.AssessmentId] = cell;
        }
        return notes;
    }

    // A category as Table 7 counts it: LR/nt as NT, LR/lc as LC.
    private static string Family(string category) => category.Trim() switch {
        "LR/nt" => "NT",
        "LR/lc" => "LC",
        var c => c,
    };

    /// The id of footnote number n under the history table.
    public static string FootnoteId(int number) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"history-fn-{number}");
}
