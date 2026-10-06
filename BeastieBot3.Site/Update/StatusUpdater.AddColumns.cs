using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Update;

// A status column added to wikitables whose rows name taxa (StatusUpdateOptions.AddStatusColumns).
public sealed partial class StatusUpdater {
    // ---------------------------------------------------------------- tables

    // Tables with no status column (no header names the status, and no data cell holds a status code
    // or {{IUCN status}}), not nested in or holding another table, with at least MinTableRows data rows,
    // at least half of which name one taxon. At most _maxItems rows are looked up.
    private IEnumerable<TableAddition> TablesWithoutStatus(WikitextScanner s, IReadOnlyList<WikiTable> tables) {
        var looked = 0;
        var nested = NestedTables(tables);
        foreach (var table in tables) {
            if (looked >= _maxItems) {
                yield break;
            }
            var data = table.Rows.Where(r => !r.IsHeaderRow).ToList();
            if (data.Count < MinTableRows || nested.Contains(table) || HasStatus(s, table)) {
                continue;
            }
            var rows = new List<(TableRow Row, StatusTaxonResolver.NameMatch Match)>();
            var nameColumns = new Dictionary<int, int>();
            var matched = 0;
            foreach (var row in data) {
                looked++;
                var cells = row.Cells.Concat(row.Spanning).ToList();
                var names = cells.SelectMany(c => NamesIn(s, c.Content)).Distinct().ToList();
                var match = _resolver.Resolve(names, s, null, notEvaluated: false, ArticleTitles(s, cells.Select(c => c.Content)));
                rows.Add((row, match));
                if (match.Taxon is null) {
                    continue;
                }
                matched++;
                // The cell with the scientific name, or with the link that found the taxon.
                var linked = match.HowFound is { Kind: StatusNoteKind.MatchedByArticle, Detail: { } title } ? title : null;
                // A name in italics or {{sp}}, or one IUCN has: a common name in a link can look like a
                // binomial ("[[Snow leopard]]").
                if (row.Cells.FirstOrDefault(c => linked is null
                        ? WritesScientificName(s, c.Content) || NamesIn(s, c.Content).Any(IsKnownName)
                        : ArticleLinks(s, c.Content).Any(l => string.Equals(l.Title, linked, StringComparison.OrdinalIgnoreCase))) is { } nameCell) {
                    var column = nameCell.Column + nameCell.Colspan - 1;
                    nameColumns[column] = nameColumns.GetValueOrDefault(column) + 1;
                }
            }
            if (matched * 2 < data.Count) {
                continue;
            }
            var last = table.Rows.Max(r => r.Cells.Count == 0 ? 0 : r.Cells[^1].Column + r.Cells[^1].Colspan - 1);
            var after = nameColumns.Count > 0 ? nameColumns.MaxBy(kv => kv.Value).Key : last;
            yield return new TableAddition(table, rows, after, LayoutProblem(s, table));
        }
    }

    // The tables nested in another table or holding one, in one pass over the tables in order of start.
    private static HashSet<WikiTable> NestedTables(IReadOnlyList<WikiTable> tables) {
        var nested = new HashSet<WikiTable>();
        var open = new Stack<WikiTable>();
        foreach (var table in tables) {
            while (open.Count > 0 && open.Peek().Span.End <= table.Span.Start) {
                open.Pop();
            }
            if (open.Count > 0) {
                nested.Add(table);
                nested.Add(open.Peek());
            }
            open.Push(table);
        }
        return nested;
    }

    private static bool HasStatus(WikitextScanner s, WikiTable table) {
        foreach (var row in table.Rows) {
            foreach (var cell in row.Cells) {
                if (cell.Content.Length > MaxStatusCellLength) {
                    continue;
                }
                if (cell.IsHeader && row.IsHeaderRow && IsStatusHeader(s.Masked[cell.Content.Start..cell.Content.End])) {
                    return true;
                }
                if (row.IsHeaderRow) {
                    continue;
                }
                var core = StatusPart(s, cell.Content);
                if ((core.Length <= MaxCodeLength && BareCode(s.Masked[core.Start..core.End]) is not null)
                    || s.TemplatesWithin(cell.Content).Any(t => t.Name == "iucn status")) {
                    return true;
                }
            }
        }
        return false;
    }

    // The first row that keeps a column from being added, as a ColumnLayout note: the column is added
    // only to a table with one header row, first, and rows with the same number of cells, none
    // spanning several rows or columns. Null when the column can be added.
    internal static StatusNote? LayoutProblem(WikitextScanner s, WikiTable table) {
        StatusNote Problem(TableRow row, string cause) =>
            new(StatusNoteKind.ColumnLayout, cause, s.LineOf(row.Cells.Count > 0 ? row.Cells[0].Whole.Start : table.Span.Start));
        if (table.Rows.Count == 0 || !table.Rows[0].IsHeaderRow) {
            return new StatusNote(StatusNoteKind.ColumnLayout, LayoutNoHeader, s.LineOf(table.Span.Start));
        }
        var cells = table.Rows[0].Cells.Count;
        for (var i = 0; i < table.Rows.Count; i++) {
            var row = table.Rows[i];
            if (row.Spanning.Count > 0 || row.Cells.Any(c => c.Colspan > 1 || c.Rowspan > 1)) {
                return Problem(row, LayoutSpan);
            }
            if (i > 0 && row.IsHeaderRow) {
                return Problem(row, LayoutSecondHeader);
            }
            if (row.Cells.Count != cells) {
                return Problem(row, $"{LayoutCellCount}:{row.Cells.Count}:{cells}");
            }
        }
        return null;
    }

    /// The causes in a ColumnLayout note's Detail. LayoutCellCount is followed by ":cells:header cells".
    public const string LayoutNoHeader = "no-header";
    public const string LayoutSpan = "span";
    public const string LayoutSecondHeader = "second-header";
    public const string LayoutCellCount = "cells";

    private (List<StatusFinding> Findings, List<Edit> Edits) AddColumn(WikitextScanner s, TableAddition addition) {
        var table = addition.Table;
        var header = table.Rows[0];
        var headerSpan = new TextSpan(s.Text.LastIndexOf('\n', Math.Max(0, header.Cells[0].Whole.Start - 1)) + 1, header.Cells[^1].Whole.End);
        var headerLine = s.LineOf(headerSpan.Start);
        var headerBefore = s.Original(headerSpan).TrimEnd('\r');
        if (_options.ColumnTables is { } chosen && !chosen.Contains(headerLine)) {
            return ([new StatusFinding(StatusItemKind.TableColumnAdded, headerLine, StatusOutcome.NotUpdated, headerBefore, null, null,
                [new StatusNote(StatusNoteKind.ColumnNotChosen)])], []);
        }
        if (addition.Layout is { } layout) {
            return ([new StatusFinding(StatusItemKind.TableColumnAdded, headerLine, StatusOutcome.NotUpdated, headerBefore, null, null,
                [layout])], []);
        }
        var c = addition.Column;
        var edits = new List<Edit>();
        var findings = new List<StatusFinding>();
        var headerEdit = NewCell(s, header, c, _options.ColumnHeader ?? StatusColumnHeader);
        var added = 0;
        foreach (var (row, match) in addition.Rows) {
            var cell = row.Cells[c];
            var line = s.LineOf(cell.Whole.Start);
            var before = s.Original(s.Core(cell.Content));
            var notes = new List<StatusNote>();
            if (match.HowFound is { } howFound) {
                notes.Add(howFound);
            }
            var taxon = match.Taxon;
            StatusNote? failure = taxon is null ? match.Failure
                : taxon.LatestGlobal is not { } global ? new StatusNote(StatusNoteKind.NoGlobalAssessment)
                : !IucnCategories.HasStatusTemplateCode(global) ? new StatusNote(StatusNoteKind.NoCode, global.Category)
                : null;
            if (failure is not null) {
                var empty = NewCell(s, row, c, string.Empty);
                edits.Add(empty);
                findings.Add(new StatusFinding(StatusItemKind.TableRowAdded, line, StatusOutcome.NotUpdated, before, null, taxon,
                    [.. notes, failure, new StatusNote(StatusNoteKind.EmptyCellAdded)]));
                continue;
            }
            var edit = NewCell(s, row, c, NewStatusTemplate(taxon!, taxon!.LatestGlobal!) + NewReference(s, taxon, taxon.LatestGlobal!));
            edits.Add(edit);
            added++;
            var span = s.Core(cell.Content);
            var after = Apply(s.Text, [edit], new TextSpan(span.Start, Math.Max(span.End, edit.End))).TrimEnd('\r', '\n');
            findings.Add(new StatusFinding(StatusItemKind.TableRowAdded, line, StatusOutcome.Updated, before, after, taxon, notes));
        }
        var headerAfter = Apply(s.Text, [headerEdit], headerSpan).TrimEnd('\r');
        findings.Insert(0, new StatusFinding(StatusItemKind.TableColumnAdded, headerLine, StatusOutcome.Updated, headerBefore, headerAfter, null,
            [new StatusNote(StatusNoteKind.ColumnAdded, $"{added}/{addition.Rows.Count}", c + 1)]));
        edits.Add(headerEdit);
        return (findings, edits);
    }

    // A new cell after cell column of the row, written the way the row writes its cells: on the same
    // line after "||" (or "!!" in a header row) when the row's cells share a line, else on a line of
    // its own starting with "|" (or "!").
    private static Edit NewCell(WikitextScanner s, TableRow row, int column, string content) {
        var cell = row.Cells[column];
        var header = row.IsHeaderRow;
        // At the end of the cell before its trailing spaces, after a comment in it.
        var at = cell.Whole.End;
        while (at > cell.Whole.Start && char.IsWhiteSpace(s.Text[at - 1])) {
            at--;
        }
        bool inline;
        if (column + 1 < row.Cells.Count) {
            var next = row.Cells[column + 1];
            inline = !s.Text.AsSpan(cell.Whole.End, Math.Max(0, next.Whole.Start - cell.Whole.End)).Contains('\n');
        } else {
            inline = cell.Whole.Start >= 2 && s.Masked[(cell.Whole.Start - 2)..cell.Whole.Start] is "||" or "!!";
        }
        if (inline) {
            // "a||b" gets "a || new ||b": a space before the next separator when the row has none.
            var space = at < s.Text.Length && s.Text[at] is '|' or '!' ? " " : string.Empty;
            return new Edit(at, at, $" {(header ? "!!" : "||")} {content}".TrimEnd() + space);
        }
        var newline = s.Text.IndexOf('\n', at);
        var eol = newline > 0 && s.Text[newline - 1] == '\r' ? "\r\n" : "\n";
        return new Edit(at, at, $"{eol}{(header ? "!" : "|")} {content}".TrimEnd(' '));
    }
}
