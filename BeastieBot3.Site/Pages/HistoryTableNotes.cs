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
/// Genuine2008: it is the 2008 table, which lists genuine changes only); a Possibly Extinct footnote,
/// and the footnote of the names read from DOI titles, have their Text. Id: its element id.
public sealed record HistoryFootnote(int Number, string? TableRelease, string? TableUrl, bool Genuine2008, string? Text, string Id = "");

/// The notes of an assessment table (the assessment history, the combined assessment history, or the
/// regional assessments): from IUCN's summary tables, whether it has a "Reason for change" column and
/// each row's cell, and the badge of an assessment that the tables list as Possibly Extinct (in the
/// Wild) without the assessment's own tag; the name each assessment was published under when it is
/// not the current one (PublishedName); and the numbered footnotes, in the order the rows refer to
/// them. A name from Table 7 shares the footnote of that table with the row's reason.
public sealed class HistoryTableNotes {
    /// The prefix of the footnote ids of the history table, and of the regional assessments table.
    public const string HistoryPrefix = "history-fn-";
    public const string RegionalPrefix = "regional-fn-";

    private readonly Dictionary<long, ReasonCell> _cells = new();
    private readonly Dictionary<long, (int Footnote, string? Tag)> _listed = new();
    private readonly Dictionary<long, PublishedNameCell> _names = new();

    private readonly List<HistoryFootnote> _footnotes = new();
    private readonly string _prefix;

    private HistoryTableNotes(bool showReasonColumn, string prefix) {
        ShowReasonColumn = showReasonColumn;
        _prefix = prefix;
    }

    public bool ShowReasonColumn { get; }
    public IReadOnlyList<HistoryFootnote> Footnotes => _footnotes;
    /// Whether a cell shows the dash of an assessment published before 2007, for the line under the table.
    public bool HasBeforeTables { get; private set; }

    public ReasonCell? CellFor(long assessmentId) => _cells.GetValueOrDefault(assessmentId);

    /// The name the row's assessment was published under, with its footnote; null when it is the
    /// current name or not known.
    public PublishedNameCell? NameFor(long assessmentId) => _names.GetValueOrDefault(assessmentId);

    /// The reference to footnote number n of this table.
    public FootnoteRef Ref(int number) => new(number, Id(number));

    private string Id(int number) => _prefix + number.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// The badge of a row: the assessment's own, except that an assessment with neither tag that the
    /// tables list as PE or PEW shows CR (PE) or CR (PEW). A listed assessment's badge refers to its footnote.
    public BadgeModel BadgeFor(AssessmentRow assessment) {
        if (!_listed.TryGetValue(assessment.AssessmentId, out var l)) return BadgeModel.For(assessment);
        var badge = l.Tag is { } tag ? new BadgeModel(IucnCategories.Describe("CR", tag == "PE", tag == "PEW")) : BadgeModel.For(assessment);
        return badge with { Footnote = l.Footnote };
    }

    /// rows: the table's rows in the order shown, newest first (own: the row is this page's taxon's).
    /// table7Of2008: the URL of the 2008 Table 7, for its footnote; null when the site has none.
    /// publishedName: the name a row's assessment was published under (TaxonPageModel.PublishedNameOf).
    /// reasonColumn: false for the regional assessments table, which has none.
    public static HistoryTableNotes Build(IReadOnlyList<(AssessmentRow Row, bool Own)> rows,
        IReadOnlyDictionary<long, CategoryChangeRow> reasons, Func<AssessmentRow, PossiblyExtinctListingRow?> listedOnlyInTables,
        string taxonKind, string? table7Of2008, Func<AssessmentRow, PublishedName?>? publishedName = null,
        string prefix = HistoryPrefix, bool reasonColumn = true) {
        var notes = new HistoryTableNotes(reasonColumn && rows.Any(r => reasons.ContainsKey(r.Row.AssessmentId)), prefix);
        var footnotes = notes._footnotes;
        var tableFootnotes = new Dictionary<string, int>(StringComparer.Ordinal);
        int TableFootnote(string release, string url) {
            if (!tableFootnotes.TryGetValue(release, out var number)) {
                number = footnotes.Count + 1;
                tableFootnotes[release] = number;
                footnotes.Add(new HistoryFootnote(number, release, url, release == "2008", null, notes.Id(number)));
            }
            return number;
        }
        var doiFootnotes = new Dictionary<PublishedNameSource, int>();
        int DoiFootnote(PublishedNameSource source) {
            if (!doiFootnotes.TryGetValue(source, out var number)) {
                number = footnotes.Count + 1;
                doiFootnotes[source] = number;
                footnotes.Add(new HistoryFootnote(number, null, null, false, SiteText.PublishedNameFootnote(source), notes.Id(number)));
            }
            return number;
        }

        for (var i = 0; i < rows.Count; i++) {
            var row = rows[i].Row;
            // The name is under the year, so its footnote is numbered before the reason's.
            if (publishedName?.Invoke(row) is { } name) {
                var number = name is { Source: PublishedNameSource.SummaryTable, Change: { } change }
                    ? TableFootnote(change.TableRelease, change.TableUrl)
                    : DoiFootnote(name.Source);
                notes._names[row.AssessmentId] = new PublishedNameCell(name, notes.Ref(number));
            }
            if (listedOnlyInTables(row) is { } listing) {
                var otherTag = listing.Tag == "PE"
                    ? (row.PossiblyExtinctInTheWild ? "PEW" : null)
                    : (row.PossiblyExtinct ? "PE" : null);
                var number = footnotes.Count + 1;
                footnotes.Add(new HistoryFootnote(number, null, null, false,
                    SiteText.ListedTagFootnote(listing.Tag, listing.Tables, listing.FirstRelease, listing.LastRelease, taxonKind, otherTag), notes.Id(number)));
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

    // A category as Table 7 counts it: LR/nt as NT, LR/lc as LC. Unlike IucnStatusTemplate.CategoryOf,
    // LR/cd stays its own category, because Table 7 lists LR/cd to NT as a change (18 rows, all
    // non-genuine) and never lists LR/nt to NT or LR/lc to LC. A row's category has no possibly
    // extinct tag (the tags are flags on the row), so CR(PE) is CR here too; Table 7 does list CR to
    // CR(PE) as a change, and such a row shows its reason.
    private static string Family(string category) => category.Trim() switch {
        "LR/nt" => "NT",
        "LR/lc" => "LC",
        var c => c,
    };

    /// The id of footnote number n under the history table.
    public static string FootnoteId(int number) => FootnoteRef.History(number).Id;
}
