namespace BeastieBot3.Site.Update;

/// One cell of a wikitable. Whole: from the cell's first character after "|", "!", "||" or "!!" to
/// its end (the cell can run over several lines). Content: the part after the attributes.
/// Column: the 0-based column the cell starts in, counting colspan and rowspan of earlier cells.
public sealed record TableCell(bool IsHeader, TextSpan Whole, TextSpan Content, string Attributes, int Column, int Colspan);

/// Spanning: cells of earlier rows whose rowspan covers this row.
public sealed record TableRow(IReadOnlyList<TableCell> Cells, IReadOnlyList<TableCell> Spanning) {
    /// A row of header cells only.
    public bool IsHeaderRow => Cells.Count > 0 && Cells.All(c => c.IsHeader);
}

public sealed record WikiTable(TextSpan Span, IReadOnlyList<TableRow> Rows);

/// Wikitables found line by line, as MediaWiki reads them: "{|" starts a table, "|}" ends it, "|-"
/// starts a row, "|+" is the caption, "!" starts header cells (more on the line after "!!" or
/// "||"), "|" starts data cells (more after "||"), and any other line continues the cell above. A
/// line inside a template that started on an earlier line continues the cell too. Tables can be
/// nested in cells; the outer cell's content then includes the nested table.
public static class WikiTables {
    public static IReadOnlyList<WikiTable> Find(WikitextScanner scanner) {
        var masked = scanner.Masked;
        var tables = new List<WikiTable>();
        var stack = new Stack<Builder>();
        var lineStart = 0;
        while (lineStart <= masked.Length) {
            var newline = masked.IndexOf('\n', lineStart);
            var lineEnd = newline < 0 ? masked.Length : newline;
            var p = lineStart;
            while (p < lineEnd && (masked[p] == ' ' || masked[p] == '\t')) {
                p++;
            }
            if (!scanner.InsideTemplate(p)) {
                var top = stack.Count > 0 ? stack.Peek() : null;
                if (StartsWith(masked, p, lineEnd, "{|")) {
                    stack.Push(new Builder(lineStart));
                } else if (top is not null && StartsWith(masked, p, lineEnd, "|}")) {
                    top.CloseCell(lineStart - 1);
                    top.EndRow();
                    stack.Pop();
                    tables.Add(new WikiTable(new TextSpan(top.Start, lineEnd), top.Rows));
                } else if (top is not null && StartsWith(masked, p, lineEnd, "|-")) {
                    top.CloseCell(lineStart - 1);
                    top.EndRow();
                } else if (top is not null && StartsWith(masked, p, lineEnd, "|+")) {
                    top.CloseCell(lineStart - 1);
                } else if (top is not null && p < lineEnd && masked[p] is '|' or '!') {
                    top.CloseCell(lineStart - 1);
                    var header = masked[p] == '!';
                    foreach (var segment in SplitCells(masked, p + 1, lineEnd, header)) {
                        top.OpenCell(masked, segment, header);
                    }
                }
            }
            if (newline < 0) {
                break;
            }
            lineStart = newline + 1;
        }
        // A table with no "|}" ends at the end of the text, as in MediaWiki.
        while (stack.Count > 0) {
            var open = stack.Pop();
            open.CloseCell(masked.Length);
            open.EndRow();
            tables.Add(new WikiTable(new TextSpan(open.Start, masked.Length), open.Rows));
        }
        tables.Sort((a, b) => a.Span.Start.CompareTo(b.Span.Start));
        return tables;
    }

    private static bool StartsWith(string text, int p, int end, string prefix) =>
        end - p >= prefix.Length && string.CompareOrdinal(text, p, prefix, 0, prefix.Length) == 0;

    // The cells of one line, split at "||" (and "!!" on a header line) outside templates and links.
    private static List<int> SplitPoints(string masked, int start, int end, bool header) {
        var points = new List<int>();
        var braces = 0;
        var links = 0;
        for (var i = start; i < end - 1; i++) {
            var pair = (masked[i], masked[i + 1]);
            switch (pair) {
                case ('{', '{'):
                    braces++;
                    i++;
                    continue;
                case ('}', '}'):
                    braces = Math.Max(0, braces - 1);
                    i++;
                    continue;
                case ('[', '['):
                    links++;
                    i++;
                    continue;
                case (']', ']'):
                    links = Math.Max(0, links - 1);
                    i++;
                    continue;
            }
            if (braces == 0 && links == 0 && (pair == ('|', '|') || (header && pair == ('!', '!')))) {
                points.Add(i);
                i++;
            }
        }
        return points;
    }

    private static IEnumerable<TextSpan> SplitCells(string masked, int start, int end, bool header) {
        var from = start;
        foreach (var point in SplitPoints(masked, start, end, header)) {
            yield return new TextSpan(from, point);
            from = point + 2;
        }
        yield return new TextSpan(from, end);
    }

    // The first "|" outside templates and links: what comes before it is the cell's attributes.
    private static int AttributeBar(string masked, TextSpan cell) {
        var braces = 0;
        var links = 0;
        for (var i = cell.Start; i < cell.End; i++) {
            var c = masked[i];
            var next = i + 1 < cell.End ? masked[i + 1] : '\0';
            if (c == '{' && next == '{') { braces++; i++; continue; }
            if (c == '}' && next == '}') { braces = Math.Max(0, braces - 1); i++; continue; }
            if (c == '[' && next == '[') { links++; i++; continue; }
            if (c == ']' && next == ']') { links = Math.Max(0, links - 1); i++; continue; }
            if (c == '\n') {
                return -1;
            }
            if (c == '|' && braces == 0 && links == 0) {
                return i;
            }
        }
        return -1;
    }

    private static int SpanAttribute(string attributes, string name) {
        var m = System.Text.RegularExpressions.Regex.Match(attributes, name + @"\s*=\s*[""']?\s*(\d{1,3})", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return m.Success && int.TryParse(m.Groups[1].Value, out var n) && n > 0 ? n : 1;
    }

    private sealed class Builder(int start) {
        public int Start { get; } = start;
        public List<TableRow> Rows { get; } = [];
        private readonly List<PendingCell> _cells = [];
        private readonly List<int> _rowspans = [];
        private readonly List<(TableCell Cell, int Rows)> _spanning = [];
        private int _column;
        private PendingCell? _open;

        public void OpenCell(string masked, TextSpan segment, bool header) {
            var cell = new PendingCell(header, segment.Start, segment.End);
            var bar = AttributeBar(masked, segment);
            if (bar >= 0) {
                cell.Attributes = masked[segment.Start..bar];
                cell.ContentStart = bar + 1;
            } else {
                cell.ContentStart = segment.Start;
            }
            cell.Colspan = SpanAttribute(cell.Attributes, "colspan");
            var rowspan = SpanAttribute(cell.Attributes, "rowspan");
            cell.Rowspan = rowspan;
            while (_column < _rowspans.Count && _rowspans[_column] > 0) {
                _column++;
            }
            cell.Column = _column;
            for (var c = _column; c < _column + cell.Colspan; c++) {
                while (_rowspans.Count <= c) {
                    _rowspans.Add(0);
                }
                if (rowspan > 1) {
                    _rowspans[c] = rowspan;
                }
            }
            _column += cell.Colspan;
            _cells.Add(cell);
            _open = cell;
        }

        /// The open cell (the last one started) runs to end, the position of the newline before
        /// the line that ends it.
        public void CloseCell(int end) {
            if (_open is not null) {
                _open.End = Math.Max(_open.End, end);
                _open = null;
            }
        }

        public void EndRow() {
            _open = null;
            if (_cells.Count == 0) {
                return;
            }
            var cells = _cells.Select(c => new TableCell(c.Header, new TextSpan(c.Start, c.End),
                new TextSpan(c.ContentStart, c.End), c.Attributes, c.Column, c.Colspan)).ToList();
            Rows.Add(new TableRow(cells, _spanning.Select(s => s.Cell).ToList()));
            for (var i = _spanning.Count - 1; i >= 0; i--) {
                if (_spanning[i].Rows <= 1) {
                    _spanning.RemoveAt(i);
                } else {
                    _spanning[i] = (_spanning[i].Cell, _spanning[i].Rows - 1);
                }
            }
            for (var i = 0; i < cells.Count; i++) {
                if (_cells[i].Rowspan > 1) {
                    _spanning.Add((cells[i], _cells[i].Rowspan - 1));
                }
            }
            _cells.Clear();
            _column = 0;
            for (var c = 0; c < _rowspans.Count; c++) {
                if (_rowspans[c] > 0) {
                    _rowspans[c]--;
                }
            }
        }
    }

    private sealed class PendingCell(bool header, int start, int end) {
        public bool Header { get; } = header;
        public int Start { get; } = start;
        public int End { get; set; } = end;
        public int ContentStart { get; set; }
        public string Attributes { get; set; } = string.Empty;
        public int Column { get; set; }
        public int Colspan { get; set; } = 1;
        public int Rowspan { get; set; } = 1;
    }
}
