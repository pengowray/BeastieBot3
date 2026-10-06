using System.Text;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

// Missing species put into a wikitable: a new row next to a row of a species of the same genus, in a
// table with one header row, the same number of cells in every row and no rowspan or colspan.
public static partial class ListPlacement {
    private sealed partial class Placer {
        // Every line of a wikitable row, mapped to its table and row; read once.
        private Dictionary<int, (WikiTable Table, int Row)>? _tableRowOfLine;
        // Each table's data rows and their keys, read once per table.
        private readonly Dictionary<WikiTable, (List<TableRow> Data, (List<string?> Scientific, List<string?> Common) Keys)> _tableKeys = [];

        private (List<TableRow> Data, (List<string?> Scientific, List<string?> Common) Keys) TableData(WikiTable table) {
            if (!_tableKeys.TryGetValue(table, out var cached)) {
                var data = table.Rows.Where(r => !r.IsHeaderRow).ToList();
                var keys = data.Select(TableRowKeys).ToList();
                _tableKeys[table] = cached = (data, ([.. keys.Select(k => k.Scientific)], [.. keys.Select(k => k.Common)]));
            }
            return cached;
        }
        // The listed taxon of each table row, by table and row index.
        private Dictionary<(WikiTable, int), ListMember>? _tableRowMember;

        private (WikiTable Table, int Row)? TableRowOf(ListMember member) {
            if (_tableRowOfLine is null) {
                _tableRowOfLine = [];
                foreach (var table in WikiTables.Find(_scanner)) {
                    for (var i = 0; i < table.Rows.Count; i++) {
                        foreach (var cell in table.Rows[i].Cells) {
                            for (var l = _scanner.LineOf(cell.Whole.Start); l <= _scanner.LineOf(Math.Max(cell.Whole.Start, cell.Whole.End - 1)); l++) {
                                _tableRowOfLine.TryAdd(l, (table, i));
                            }
                        }
                    }
                }
            }
            return _tableRowOfLine.TryGetValue(member.Line, out var found) ? found : null;
        }

        private ListMember? MemberInRow(WikiTable table, int row) {
            _tableRowMember ??= Listed(ListMemberSource.TableRow).Select(m => (Member: m, Row: TableRowOf(m)))
                .Where(x => x.Row is not null).GroupBy(x => (x.Row!.Value.Table, x.Row.Value.Row)).ToDictionary(g => g.Key, g => g.First().Member);
            return _tableRowMember.GetValueOrDefault((table, row));
        }

        // "Title|" when the taxon's article is not its scientific name, for a link labelled with the name.
        private static string ArticlePrefix(ListTaxonRow taxon) =>
            taxon.ListArticleTitle is { } title && !string.Equals(title, taxon.ScientificName, StringComparison.Ordinal) ? title + "|" : string.Empty;

        private static int IndexOf(IReadOnlyList<TableRow> rows, TableRow row) {
            for (var i = 0; i < rows.Count; i++) {
                if (ReferenceEquals(rows[i], row)) {
                    return i;
                }
            }
            return -1;
        }

        // The scientific name (the first one in italics, a link or {{sp}}) and the common name (the
        // first link outside italics that is not the scientific name) of a row.
        private (string? Scientific, string? Common) TableRowKeys(TableRow row) {
            string? scientific = null;
            string? common = null;
            foreach (var cell in row.Cells) {
                // Only a name in italics or {{sp}}: a common name in a link can look like a binomial.
                var italics = StatusUpdater.NameOccurrencesIn(_scanner, cell.Content)
                    .Where(o => _scanner.Masked[o.Span.Start] is '\'' or '{').ToList();
                scientific ??= italics.Select(o => o.Name).FirstOrDefault();
                if (common is null) {
                    common = StatusUpdater.ArticleLinks(_scanner, cell.Content)
                        .Where(l => !italics.Any(i => i.Span.Start <= l.Span.Start && l.Span.End <= i.Span.End))
                        .Select(l => LinkLabel(_scanner.Original(l.Span)))
                        .FirstOrDefault(n => !StatusUpdater.IsScientificNameShape(n) || n != scientific);
                }
            }
            return (scientific, common);
        }

        // The table and data-row indexes of each genus's listed rows, read once per genus.
        private Dictionary<int, (WikiTable Table, List<int> Rows)?>? _genusTableRows;
        // Whether a table can take a row: read once per table.
        private readonly Dictionary<WikiTable, bool> _tableTakesRows = [];

        private PlacedTaxon? InTableRow(ListTaxonRow taxon) {
            _genusTableRows ??= Listed(ListMemberSource.TableRow).GroupBy(m => m.Taxon.NodeId!.Value).ToDictionary(g => g.Key, g => {
                var rows = g.Select(TableRowOf).Where(r => r is not null).Select(r => r!.Value).ToList();
                if (rows.Count == 0) {
                    return ((WikiTable, List<int>)?)null;
                }
                var table = rows[0].Table;
                var (data, _) = TableData(table);
                var index = data.Select((row, i) => (row, i)).ToDictionary(x => x.row, x => x.i, ReferenceEqualityComparer.Instance);
                return (table, rows.Where(r => r.Table == table).Select(r => index.GetValueOrDefault(table.Rows[r.Row], -1)).Where(i => i >= 0).ToList());
            });
            if (_genusTableRows.GetValueOrDefault(taxon.NodeId) is not { } genus || genus.Rows.Count == 0) {
                return null;
            }
            var table = genus.Table;
            if (!_tableTakesRows.TryGetValue(table, out var takesRows)) {
                _tableTakesRows[table] = takesRows = StatusUpdater.LayoutProblem(_scanner, table) is null;
            }
            if (!takesRows) {
                return null;
            }
            var (data, keys) = TableData(table);
            var (order, index) = Order(keys.Scientific, taxon.ScientificName, keys.Common, taxon.CommonNameEn);
            // After the row before its place; before the first row when it sorts first; after the
            // genus's last row when the table keeps no order.
            var genusRows = genus.Rows;
            int neighbourIndex;
            bool before;
            if (order == ListOrder.None) {
                neighbourIndex = genusRows.Max();
                before = false;
            } else if (index == 0) {
                neighbourIndex = 0;
                before = true;
            } else {
                neighbourIndex = index - 1;
                before = false;
            }
            var neighbour = data[neighbourIndex];
            (string? Scientific, string? Common) neighbourKeys = (keys.Scientific[neighbourIndex], keys.Common[neighbourIndex]);
            // A table getting a status column in this run: the new row gets its cell too.
            int? newColumn = _options.TablesWithNewColumn.TryGetValue(_scanner.LineOf(table.Rows[0].Cells[0].Whole.Start), out var after)
                ? after - 1
                : null;
            if (NewTableRow(neighbour, neighbourKeys, taxon, newColumn) is not { } rowText) {
                return null;
            }
            var rowStart = _lines.Start(_scanner.LineOf(neighbour.Cells[0].Whole.Start));
            var last = neighbour.Cells[^1].Whole;
            var rowEnd = _lines.End(_scanner.LineOf(Math.Max(last.Start, last.End - 1)));
            // The new row's own "|-" goes between it and its neighbour.
            var line = before ? rowText + "\n|-" : "|-\n" + rowText;
            var member = MemberInRow(table, IndexOf(table.Rows, neighbour));
            return new PlacedTaxon(taxon, line, before ? rowStart : rowEnd, neighbourKeys.Common ?? neighbourKeys.Scientific ?? string.Empty,
                member?.Taxon, _scanner.LineOf(neighbour.Cells[0].Whole.Start), before);
        }

        // The new row: the neighbour row's cells, attributes and line breaks, with the cell holding
        // its scientific name given the taxon's, the cell with its common name link the taxon's
        // common name, the status cell the taxon's status, and every other cell empty.
        private string? NewTableRow(TableRow neighbour, (string? Scientific, string? Common) keys, ListTaxonRow taxon, int? newColumn = null) {
            var sb = new StringBuilder();
            var named = false;
            var at = _lines.Start(_scanner.LineOf(neighbour.Cells[0].Whole.Start));
            foreach (var cell in neighbour.Cells) {
                var core = _scanner.Core(cell.Content);
                var content = _scanner.Masked[core.Start..core.End];
                string value;
                if (StatusUpdater.BareCode(content) is not null) {
                    value = GroupList.StatusCode(taxon);
                } else if (_scanner.TemplatesWithin(core).Any(t => t.Name == "iucn status")) {
                    value = StatusTemplate(taxon, _options);
                } else if (keys.Scientific is { } s && StatusUpdater.NameOccurrencesIn(_scanner, cell.Content)
                        .Any(o => o.Name == s && _scanner.Masked[o.Span.Start] is '\'' or '{')) {
                    value = ListScope.UnlinkLists($"''[[{ArticlePrefix(taxon)}{taxon.ScientificName}]]''");
                    named = true;
                } else if (keys.Common is { } c && StatusUpdater.ArticleLinks(_scanner, cell.Content).Any(l => LinkLabel(_scanner.Original(l.Span)) == c)) {
                    // The article the list lines link: the taxon's article, else its scientific name.
                    var target = taxon.ListArticleTitle ?? taxon.ScientificName;
                    var title = !string.Equals(target, taxon.CommonNameEn, StringComparison.Ordinal) ? target + "|" : string.Empty;
                    // A taxon with no English name gets its scientific name in the common name's cell.
                    value = taxon.CommonNameEn is { } name ? ListScope.UnlinkLists($"[[{title}{name}]]")
                        : keys.Scientific is null ? ListScope.UnlinkLists($"''[[{ArticlePrefix(taxon)}{taxon.ScientificName}]]''")
                        : string.Empty;
                    named |= value.Length > 0;
                } else {
                    value = string.Empty;
                }
                var (start, end) = core.Length == 0 ? (cell.Content.Start, cell.Content.Start) : (core.Start, core.End);
                sb.Append(_text, at, start - at).Append(value);
                at = end;
                if (newColumn == cell.Column) {
                    // The status cell of the column added in this run, written as the row writes cells.
                    var i = neighbour.Cells.ToList().IndexOf(cell);
                    var inline = i + 1 < neighbour.Cells.Count
                        ? !_text.AsSpan(cell.Whole.End, Math.Max(0, neighbour.Cells[i + 1].Whole.Start - cell.Whole.End)).Contains('\n')
                        : cell.Whole.Start >= 2 && _scanner.Masked[(cell.Whole.Start - 2)..cell.Whole.Start] is "||";
                    var status = taxon.Category is null ? string.Empty : StatusTemplate(taxon, _options);
                    sb.Append(_text, at, end - at);
                    sb.Append(inline ? " || " + status : "\n| " + status);
                }
            }
            sb.Append(_text, at, neighbour.Cells[^1].Whole.End - at);
            // A row with no cell for the taxon's name would be a row of empty cells.
            if (!named) {
                return null;
            }
            // Insertions gives the text's line ends to the lines put in.
            return sb.ToString().Replace("\r", string.Empty, StringComparison.Ordinal).TrimEnd('\n');
        }
    }
}
