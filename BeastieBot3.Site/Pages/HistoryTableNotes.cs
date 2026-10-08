using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// One footnote of a [PE] or [PEW] marker in a history table: the row's assessment, the marker's
/// tag and the footnote's text.
public sealed record ListedTagFootnote(long AssessmentId, string Tag, string Text);

/// The parts of a history table (the assessment history or the combined assessment history) that
/// come from IUCN's summary tables: whether it has a "Reason for change" column, the Table 7 files
/// its reasons are from (version and PDF link, oldest first), whether a row published in 2008 has
/// another category than the assessment before it and no reason (the 2008 table lists genuine
/// changes only, so its change may have been non-genuine), and the footnotes of its [PE] markers.
public sealed class HistoryTableNotes {
    public bool ShowReasonColumn { get; private init; }
    public IReadOnlyList<(string Release, string Url)> ReasonTables { get; private init; } = [];
    public bool HasBlank2008Row { get; private init; }
    public IReadOnlyList<ListedTagFootnote> Footnotes { get; private init; } = [];

    public static HistoryTableNotes Build(IReadOnlyList<(AssessmentRow Row, bool Own)> rows,
        IReadOnlyDictionary<long, CategoryChangeRow> reasons, Func<AssessmentRow, PossiblyExtinctListingRow?> listedOnlyInTables,
        string taxonKind) {
        var used = rows.Select(r => reasons.GetValueOrDefault(r.Row.AssessmentId)).OfType<CategoryChangeRow>().ToList();
        var footnotes = new List<ListedTagFootnote>();
        foreach (var (row, own) in rows) {
            if (listedOnlyInTables(row) is not { } listing) continue;
            var otherTag = listing.Tag == "PE"
                ? (row.PossiblyExtinctInTheWild ? "PEW" : null)
                : (row.PossiblyExtinct ? "PE" : null);
            footnotes.Add(new ListedTagFootnote(row.AssessmentId, listing.Tag, SiteText.ListedTagFootnote(listing.Tag, row.YearPublished,
                own ? null : row.TaxonId, listing.Tables, listing.FirstRelease, listing.LastRelease, taxonKind, otherTag)));
        }
        return new HistoryTableNotes {
            ShowReasonColumn = used.Count > 0,
            ReasonTables = used
                .GroupBy(r => r.TableRelease, StringComparer.Ordinal)
                .Select(g => (g.Key, g.First().TableUrl))
                .OrderBy(t => t.Key, StringComparer.Ordinal)
                .ToList(),
            HasBlank2008Row = used.Count > 0 && rows.Any(r => r.Row.YearPublished == 2008 && !reasons.ContainsKey(r.Row.AssessmentId)
                && rows.Where(e => e.Row.YearPublished < 2008).MaxBy(e => (e.Row.YearPublished, e.Row.AssessmentDate)) is { } before
                && Family(before.Row.Category) != Family(r.Row.Category)),
            Footnotes = footnotes,
        };
    }

    // A category as Table 7 counts it: LR/nt as NT, LR/lc as LC.
    private static string Family(string category) => category.Trim() switch {
        "LR/nt" => "NT",
        "LR/lc" => "LC",
        var c => c,
    };

    /// The id of a marker's footnote, for the marker's link.
    public static string FootnoteId(long assessmentId) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"listed-tag-{assessmentId}");
}
