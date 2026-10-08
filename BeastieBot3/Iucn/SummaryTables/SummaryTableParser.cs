using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

// Reads the rows of IUCN's summary statistics Table 7 ("Species changing IUCN Red List Status") and
// Table 9 ("Possibly Extinct and Possibly Extinct in the Wild Species") from the lines of a PDF
// (PdfLines). The tables have changed layout over the years:
//
//   2007 to 2023: scientific name, common name, the two categories, reason for change and the Red
//     List version each change was first published in, under a heading line for each group
//     ("MAMMALS"). 2008 has no reason or version column: it lists only genuine changes, under
//     "Genuine improvements" and "Genuine deteriorations" headings. 2007 ends some groups with
//     "Species removed from the IUCN Red List for taxonomic reasons:", whose rows give a reason such
//     as "synonym of A. nigriceps" instead of a code.
//   2024 onwards: a Group column on every row instead of heading lines.
//   Table 9 (2014 to 2020): scientific name, common name, category (CR(PE) or CR(PEW)), the year the
//     species was first assessed as Possibly Extinct, and the date last recorded in the wild, which
//     can run over two or three lines around the row's other cells.
//
// The columns are found from the header row ("Scientific name", "Common name", "Category", ...) on
// each page; a page without one uses the columns of the page before. A row's words are grouped into
// cells by the gaps between them. The cells after the scientific and common names go to the
// category, reason and version columns in order when there is one for each column, and otherwise to
// the column whose header is nearest (the 2014-2 table's bird section is shifted to the left of its
// header).

namespace BeastieBot3.Iucn.SummaryTables;

/// What the text above a table says about it. Release is the version in the page header
/// ("2024-2", or "2007" for the 2007 and 2008 tables). PeriodFrom and PeriodTo are the two versions
/// a Table 7 compares ("between 2023 (IUCN Red List version 2023-1) and 2024 (...2024-2)").
/// DefinesError: the legend lists E (the previous listing was an error).
internal sealed record SummaryTableInfo(
    string? Title, string? Release, string? LastUpdated, string? PeriodFrom, string? PeriodTo, bool DefinesError);

/// One row of Table 7, every cell as printed. Group is the row's group cell or the group heading it
/// is under; Section is a heading inside a group (the 2008 table's "Genuine improvements"). Anomaly
/// says what could not be read; the row is kept.
internal sealed record CategoryChangeRow(
    int Page, int LineNo, string? Group, string? Section, string ScientificName, string? CommonName,
    string? OldCategoryText, string? NewCategoryText, string? ReasonText, string? VersionText, string? Anomaly) {
    public TableCategory Old => SummaryTableValues.ReadCategory(OldCategoryText);
    public TableCategory New => SummaryTableValues.ReadCategory(NewCategoryText);
    /// The reason cell's code; in the 2008 table, which lists only genuine changes, G.
    public string? Reason => SummaryTableValues.ReadReason(ReasonText)
        ?? (Section is not null && Section.StartsWith("Genuine", StringComparison.Ordinal) ? "G" : null);
    public string? Version => SummaryTableValues.ReadVersion(VersionText);
}

/// One row of Table 9, every cell as printed.
internal sealed record PossiblyExtinctRow(
    int Page, int LineNo, string? Group, string ScientificName, string? CommonName,
    string? CategoryText, string? YearText, string? LastRecorded, string? Anomaly) {
    public TableCategory Category => SummaryTableValues.ReadCategory(CategoryText);
    public int? YearAssessed => SummaryTableValues.ReadYear(YearText);
}

/// Unread: lines below a header row that are neither rows nor headings, for checking the parser.
internal sealed record SummaryTableParse<TRow>(SummaryTableInfo Info, IReadOnlyList<TRow> Rows, IReadOnlyList<string> Unread, bool FoundHeader);

internal static partial class SummaryTableParser {
    /// Stored with each file's rows. Increase it when a change to the parser changes the rows it
    /// reads, so `iucn summary-tables` reads every file again.
    public const int Version = 1;

    // Words closer than this (in points) are in the same cell. A space is 1.5 to 2.5 points wide.
    private const double CellGap = 4.0;
    // Header lines are within this distance (in points) of the "Scientific name" line.
    private const double HeaderSpan = 22;
    // A line of only date-last-recorded text belongs to the nearest row within this distance (points).
    private const double ContinuationSpan = 16;

    public static SummaryTableParse<CategoryChangeRow> ParseTable7(IReadOnlyList<PdfLine> lines) {
        var parse = Parse(lines, SummaryTableKind.CategoryChanges);
        var rows = parse.Rows.Select(row => {
            var c = row.Cells;
            c.TryGetValue(Column.Old, out var old);
            c.TryGetValue(Column.New, out var @new);
            c.TryGetValue(Column.Reason, out var reason);
            c.TryGetValue(Column.Version, out var version);
            // A long reason ("synonym of A. nigriceps") can reach the version column.
            if (version is not null && SummaryTableValues.ReadVersion(version) is null && reason is null) {
                (reason, version) = (version, null);
            }
            var anomalies = new List<string>();
            if (SummaryTableValues.ReadCategory(old).Category is null) anomalies.Add($"previous category \"{old}\"");
            if (SummaryTableValues.ReadCategory(@new).Category is null) anomalies.Add($"new category \"{@new}\"");
            var removed = row.Section?.StartsWith("Species removed", StringComparison.Ordinal) == true;
            if (c.HasReasonColumn && !removed && SummaryTableValues.ReadReason(reason) is null) anomalies.Add($"reason \"{reason}\"");
            if (c.HasVersionColumn && SummaryTableValues.ReadVersion(version) is null) anomalies.Add($"version \"{version}\"");
            return new CategoryChangeRow(row.Line.Page, row.LineNo, row.Group, row.Section, c.Scientific!, c.Common,
                old, @new, reason, version, anomalies.Count == 0 ? null : "Unread " + string.Join(", ", anomalies));
        }).ToList();
        return new SummaryTableParse<CategoryChangeRow>(parse.Info, rows, parse.Unread, parse.FoundHeader);
    }

    public static SummaryTableParse<PossiblyExtinctRow> ParseTable9(IReadOnlyList<PdfLine> lines) {
        var parse = Parse(lines, SummaryTableKind.PossiblyExtinct);
        var rows = parse.Rows.Select(row => {
            var c = row.Cells;
            c.TryGetValue(Column.Category, out var category);
            c.TryGetValue(Column.Year, out var year);
            var last = row.LastRecorded.Count == 0 ? null
                : string.Join(' ', row.LastRecorded.OrderByDescending(f => f.Baseline).Select(f => f.Text));
            var anomalies = new List<string>();
            if (category is null || !SummaryTableValues.IsPossiblyExtinct(category)) anomalies.Add($"category \"{category}\"");
            if (SummaryTableValues.ReadYear(year) is null) anomalies.Add($"year of assessment \"{year}\"");
            return new PossiblyExtinctRow(row.Line.Page, row.LineNo, row.Group, c.Scientific!, c.Common, category, year, last,
                anomalies.Count == 0 ? null : "Unread " + string.Join(", ", anomalies));
        }).ToList();
        return new SummaryTableParse<PossiblyExtinctRow>(parse.Info, rows, parse.Unread, parse.FoundHeader);
    }

    // ------------------------------------------------------------ layout

    private enum Column { Old, New, Reason, Version, Category, Year, LastRecorded }

    /// The columns of one page. GroupLeft is null when the table has no Group column. Right lists
    /// the columns after the common name, left to right, with the centre of each one's header.
    private sealed record Layout(double? GroupLeft, double ScientificLeft, double CommonLeft,
        IReadOnlyList<(Column Column, double Center)> Right);

    /// The cells of one line.
    private sealed class RowCells : Dictionary<Column, string> {
        public string? Group;
        public string? Scientific;
        public string? Common;
        public bool HasCommonCell;
        public bool HasReasonColumn;
        public bool HasVersionColumn;
    }

    /// A row before it is turned into a record. LastRecorded holds the Table 9 date text of the row's
    /// line and of the lines around it that hold only that text.
    private sealed record Row(RowCells Cells, PdfLine Line, int LineNo, string? Group, string? Section,
        List<(double Baseline, string Text)> LastRecorded);

    private sealed record ParseResult(SummaryTableInfo Info, IReadOnlyList<Row> Rows, IReadOnlyList<string> Unread, bool FoundHeader);

    private static ParseResult Parse(IReadOnlyList<PdfLine> lines, SummaryTableKind kind) {
        var rows = new List<Row>();
        var unread = new List<string>();
        var intro = new List<string>();
        Layout? layout = null;
        string? group = null;
        string? section = null;
        var lineNo = 0;
        foreach (var page in lines.GroupBy(l => l.Page).OrderBy(g => g.Key)) {
            var pageLines = page.ToList();
            var headerIndex = pageLines.FindIndex(IsHeaderLine);
            var firstTablePage = headerIndex >= 0 && layout is null;
            double? headerBottom = null;
            if (headerIndex >= 0) {
                var headerLine = pageLines[headerIndex];
                var block = pageLines.Where(l => Math.Abs(l.Baseline - headerLine.Baseline) <= HeaderSpan && IsHeaderBlockLine(l)).ToList();
                layout = ReadLayout(block, headerLine, kind) ?? layout;
                headerBottom = block.Min(l => l.Baseline);
            }
            var pageRows = new List<Row>();
            var continuations = new List<(PdfLine Line, string Text)>();
            foreach (var line in pageLines) {
                lineNo++;
                if (layout is null) {
                    intro.Add(line.Text);
                    continue;
                }
                if (headerBottom is { } bottom && line.Baseline >= bottom - 0.5) {
                    // Above or in the header row: the page header, and on the first page the title and notes.
                    if (firstTablePage && !IsHeaderBlockLine(line)) intro.Add(line.Text);
                    continue;
                }
                if (IsPageFurniture(line.Text)) continue;

                var text = line.Text.Trim();
                var cells = Split(line, layout);
                if (cells.Count == 0 && !cells.HasCommonCell && cells.Group is null && cells.Scientific is { } only) {
                    if (SectionPattern().Match(only) is { Success: true } genuine) {
                        section = genuine.Groups["kind"].Value.StartsWith("improve", StringComparison.OrdinalIgnoreCase)
                            ? "Genuine improvements" : "Genuine deteriorations";
                        continue;
                    }
                    if (only.StartsWith("Species removed", StringComparison.Ordinal)) {
                        section = only.TrimEnd(':', ' ');
                        continue;
                    }
                }
                if (layout.GroupLeft is null && IsGroupHeading(text)
                    && !line.Words.Any(w => SummaryTableValues.ReadCategory(w.Text).Category is not null)) {
                    // A heading can be long enough to reach the category columns:
                    // "CRUSTACEANS (Arthropoda: Branchiopoda, Cephalocardia, and Remipedia)".
                    group = text;
                    section = null;
                    continue;
                }
                if (cells.Scientific is null && !cells.HasCommonCell && cells.Group is null) {
                    if (cells.Count == 1 && cells.TryGetValue(Column.LastRecorded, out var fragment)) {
                        continuations.Add((line, fragment));
                        continue;
                    }
                    unread.Add($"p{line.Page}: {line.Text}");
                    continue;
                }
                if (cells.Count == 0 || cells.Scientific is null) {
                    unread.Add($"p{line.Page}: {line.Text}");
                    continue;
                }
                var dates = new List<(double, string)>();
                if (cells.TryGetValue(Column.LastRecorded, out var date)) dates.Add((line.Baseline, date));
                pageRows.Add(new Row(cells, line, lineNo, cells.Group ?? group, section, dates));
            }
            foreach (var (line, fragment) in continuations) {
                // The row whose date ran over more than one line has no date text on its own line,
                // so it comes before a nearer row that has.
                var nearest = pageRows
                    .Where(r => Math.Abs(r.Line.Baseline - line.Baseline) <= ContinuationSpan)
                    .OrderBy(r => r.Cells.ContainsKey(Column.LastRecorded) ? 1 : 0)
                    .ThenBy(r => Math.Abs(r.Line.Baseline - line.Baseline))
                    .FirstOrDefault();
                if (nearest is null) {
                    unread.Add($"p{line.Page}: {line.Text}");
                    continue;
                }
                nearest.LastRecorded.Add((line.Baseline, fragment));
            }
            rows.AddRange(pageRows);
        }
        return new ParseResult(ReadInfo(intro, lines), rows, unread, layout is not null);
    }

    private static bool IsHeaderLine(PdfLine line) {
        for (var i = 0; i + 1 < line.Words.Count; i++) {
            if (line.Words[i].Text == "Scientific" && line.Words[i + 1].Text.Equals("name", StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }
        return false;
    }

    // The words of the header row's lines. A data row never consists of these words only.
    private static readonly HashSet<string> HeaderWords = new(StringComparer.OrdinalIgnoreCase) {
        "IUCN", "Red", "List", "Category", "Categories", "Reason", "for", "change", "version", "Group", "Scientific", "name",
        "Common", "Year", "of", "Assessment", "Date", "last", "recorded", "in", "the", "wild",
    };

    [GeneratedRegex(@"^\(?\s*(?:19|20)\d{2}(?:[.\-\u2010]\d)?\s*\)?$")]
    private static partial Regex HeaderYearPattern();

    private static bool IsHeaderBlockLine(PdfLine line) =>
        line.Words.All(w => HeaderWords.Contains(w.Text) || HeaderYearPattern().IsMatch(w.Text));

    private static Layout? ReadLayout(IReadOnlyList<PdfLine> block, PdfLine headerLine, SummaryTableKind kind) {
        var words = block.SelectMany(l => l.Words).ToList();
        var scientific = headerLine.Words.First(w => w.Text == "Scientific");
        var common = words.FirstOrDefault(w => w.Text == "Common");
        if (common is null) return null;
        var group = words.FirstOrDefault(w => w.Text == "Group" && w.Left < scientific.Left);
        var categories = words.Where(w => w.Text == "Category").OrderBy(w => w.Left).ToList();
        var right = new List<(Column, double)>();
        if (kind == SummaryTableKind.CategoryChanges) {
            if (categories.Count < 2) return null;
            right.Add((Column.Old, categories[0].Center));
            right.Add((Column.New, categories[1].Center));
            if ((PhraseCenter(block, "Reason", "change") ?? WordCenter(words, "change")) is { } reason) right.Add((Column.Reason, reason));
            if (WordCenter(words, "version", after: categories[1].Right) is { } version) right.Add((Column.Version, version));
        } else {
            if (categories.Count < 1) return null;
            right.Add((Column.Category, categories[0].Center));
            var year = WordCenter(words, "Assessment") ?? WordCenter(words, "Year");
            if (year is null) return null;
            right.Add((Column.Year, year.Value));
            var lastWords = words.Where(w => w.Left > year.Value + 15).ToList();
            if (lastWords.Count > 0) right.Add((Column.LastRecorded, (lastWords.Min(w => w.Left) + lastWords.Max(w => w.Right)) / 2));
        }
        return new Layout(group?.Left, scientific.Left, common.Left, right);
    }

    private static double? WordCenter(List<PdfWord> words, string text, double after = double.MinValue) =>
        words.Where(w => w.Text.Equals(text, StringComparison.OrdinalIgnoreCase) && w.Left > after)
            .Select(w => (double?)w.Center).FirstOrDefault();

    /// The centre of "first ... last" when both are on one line, such as "Reason for change".
    private static double? PhraseCenter(IReadOnlyList<PdfLine> block, string first, string last) {
        foreach (var line in block) {
            var start = line.Words.FirstOrDefault(w => w.Text == first);
            var end = line.Words.FirstOrDefault(w => w.Text == last);
            if (start is not null && end is not null && end.Left > start.Left) return (start.Left + end.Right) / 2;
        }
        return null;
    }

    private static RowCells Split(PdfLine line, Layout layout) {
        var cells = new RowCells {
            HasReasonColumn = layout.Right.Any(r => r.Column == Column.Reason),
            HasVersionColumn = layout.Right.Any(r => r.Column == Column.Version),
        };
        var right = new List<PdfWord>();
        var starts = new List<double> { layout.ScientificLeft, layout.CommonLeft };
        if (layout.GroupLeft is { } groupLeft) starts.Add(groupLeft);
        foreach (var cell in Cells(line.Words, starts)) {
            if (layout.GroupLeft is { } groupColumn && cells.Group is null && cells.Scientific is null && Math.Abs(cell.Left - groupColumn) <= 6) {
                cells.Group = cell.Text;
            } else if (cell.Left < layout.CommonLeft - 2 && cells.Common is null && right.Count == 0) {
                cells.Scientific = Join(cells.Scientific, cell.Text);
            } else if (cells.Common is null && right.Count == 0 && Math.Abs(cell.Left - layout.CommonLeft) <= 6) {
                cells.Common = cell.Text;
                cells.HasCommonCell = true;
            } else if (cells.Common is null && right.Count == 0 && cell.Text == "0") {
                // A few tables print a number 0, aligned right, for a species with no common name.
                cells.HasCommonCell = true;
            } else {
                right.Add(cell);
            }
        }
        if (right.Count == layout.Right.Count) {
            for (var i = 0; i < right.Count; i++) cells[layout.Right[i].Column] = right[i].Text;
        } else {
            foreach (var cell in right) {
                var column = layout.Right.MinBy(r => Math.Abs(r.Center - cell.Center)).Column;
                cells[column] = cells.TryGetValue(column, out var existing) ? existing + " " + cell.Text : cell.Text;
            }
        }
        return cells;
    }

    private static string Join(string? a, string b) => a is null ? b : a + " " + b;

    /// The line's cells, left to right: words close together. Words of one font are joined first;
    /// then cells of different fonts are joined when they do not overlap, so "2019" and "\u20103" (a
    /// hyphen in another font) make one cell, while a group name that runs over the scientific name
    /// in another font stays apart from it. A word that starts at a column's left edge (the group,
    /// scientific name and common name columns are left-aligned) always starts a cell, and cells of
    /// different fonts are never joined across a column's left edge.
    private static IEnumerable<PdfWord> Cells(IReadOnlyList<PdfWord> words, IReadOnlyList<double> columnStarts) {
        var byFont = new List<PdfWord>();
        foreach (var font in words.GroupBy(w => w.Font)) {
            byFont.AddRange(JoinClose(font.OrderBy(w => w.Left), columnStarts, sameFont: true));
        }
        return JoinClose(byFont.OrderBy(c => c.Left), columnStarts, sameFont: false);
    }

    private static List<PdfWord> JoinClose(IEnumerable<PdfWord> words, IReadOnlyList<double> columnStarts, bool sameFont) {
        var cells = new List<PdfWord>();
        foreach (var word in words) {
            var current = cells.Count > 0 ? cells[^1] : null;
            if (current is not null && CanJoin(current, word, columnStarts, sameFont)) {
                cells[^1] = current with { Text = current.Text + " " + word.Text, Right = Math.Max(current.Right, word.Right) };
            } else {
                cells.Add(word);
            }
        }
        return cells;
    }

    private static bool CanJoin(PdfWord cell, PdfWord word, IReadOnlyList<double> columnStarts, bool sameFont) {
        var gap = word.Left - cell.Right;
        if (gap > CellGap) return false;
        foreach (var start in columnStarts) {
            if (Math.Abs(word.Left - start) <= 2) return false;
            if (!sameFont && cell.Left < start - 2 && word.Left >= start - 2) return false;
        }
        return sameFont || gap >= -1;
    }

    // ------------------------------------------------------------ lines that are not rows

    [GeneratedRegex(@"^Genuine\s+(?<kind>improvements?|deteriorations?)$", RegexOptions.IgnoreCase)]
    private static partial Regex SectionPattern();

    /// "MAMMALS", "FUNGI & PROTISTS", "BUTTERFLIES and MOTHS", "LAMPREYS, etc.",
    /// "MAMMALS (Mammalia)": the first word is in capitals. A scientific name never is.
    internal static bool IsGroupHeading(string text) {
        var first = text.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.TrimEnd(',', ':', ';', '.');
        return first is { Length: >= 3 } && first.All(c => char.IsUpper(c) || c == '&' || c == '-');
    }

    [GeneratedRegex(@"^(?:IUCN Red List version|(?:19|20)\d{2}(?:[.\-_]\d)?\s*Red List:|Last [Uu]pdated|THE IUCN RED LIST|Page \d+|\d+(?: of \d+)?$|Table \d+[a-z]?:|TM$|\u2122$)")]
    private static partial Regex FurniturePattern();

    private static bool IsPageFurniture(string text) => FurniturePattern().IsMatch(text.Trim());

    // ------------------------------------------------------------ the text above the table

    [GeneratedRegex(@"IUCN Red List version\s+(?<v>(?:19|20)\d{2}[.\-\u2010]\d)")]
    private static partial Regex ReleasePattern();

    [GeneratedRegex(@"^(?<y>(?:19|20)\d{2})\s*Red List:")]
    private static partial Regex OldReleasePattern();

    [GeneratedRegex(@"Last [Uu]pdated:?\s*(?<d>.+)$")]
    private static partial Regex LastUpdatedPattern();

    [GeneratedRegex(@"between\s+(?:19|20)\d{2}\s*\(IUCN Red List version\s+(?<from>[\d.\-\u2010]+)\)\s*and\s+(?:19|20)\d{2}\s*\(IUCN Red List version\s+(?<to>[\d.\-\u2010]+)\)")]
    private static partial Regex PeriodPattern();

    private static SummaryTableInfo ReadInfo(IReadOnlyList<string> intro, IReadOnlyList<PdfLine> lines) {
        var firstPage = lines.Where(l => l.Page == (lines.Count > 0 ? lines[0].Page : 1)).Select(l => l.Text).ToList();
        string? title = null, release = null, lastUpdated = null;
        foreach (var text in firstPage) {
            var t = text.Trim();
            if (title is null && t.StartsWith("Table ", StringComparison.Ordinal) && t.Contains(':')) title = SummaryTableValues.NormalizeDashes(t);
            if (release is null && ReleasePattern().Match(t) is { Success: true } r) release = SummaryTableValues.ReadVersion(r.Groups["v"].Value);
            if (release is null && OldReleasePattern().Match(t) is { Success: true } o) release = o.Groups["y"].Value;
            if (lastUpdated is null && LastUpdatedPattern().Match(t) is { Success: true } u) lastUpdated = u.Groups["d"].Value.Trim();
        }
        var joined = string.Join(' ', intro);
        var period = PeriodPattern().Match(joined);
        var definesError = joined.Contains("Previous listing was an Error", StringComparison.OrdinalIgnoreCase)
            || joined.Contains("E - Previous listing", StringComparison.OrdinalIgnoreCase);
        return new SummaryTableInfo(title, release, lastUpdated,
            period.Success ? SummaryTableValues.ReadVersion(period.Groups["from"].Value) : null,
            period.Success ? SummaryTableValues.ReadVersion(period.Groups["to"].Value) : null,
            definesError);
    }
}

internal enum SummaryTableKind {
    CategoryChanges = 7,
    PossiblyExtinct = 9,
}
