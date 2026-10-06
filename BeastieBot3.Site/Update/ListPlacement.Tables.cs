using System.Text;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

// Missing species put into a wikitable: a new row next to a row of a species of the same genus, in a
// table with one header row, the same number of cells in every row and no rowspan or colspan.
public static partial class ListPlacement {
    private sealed partial class Placer {
        private IReadOnlyList<WikiTable>? _tables;

        private (WikiTable Table, int Row)? TableRowOf(ListMember member) {
            _tables ??= WikiTables.Find(_scanner);
            var start = _lines.Start(member.Line);
            var end = _lines.End(member.Line);
            foreach (var table in _tables) {
                if (table.Span.Start > end || table.Span.End < start) {
                    continue;
                }
                for (var i = 0; i < table.Rows.Count; i++) {
                    if (table.Rows[i].Cells.Any(c => c.Whole.Start <= end && c.Whole.End >= start)) {
                        return (table, i);
                    }
                }
            }
            return null;
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

        private PlacedTaxon? InTableRow(ListTaxonRow taxon) {
            var mates = Listed(ListMemberSource.TableRow).Where(m => m.Taxon.NodeId == taxon.NodeId)
                .Select(TableRowOf).Where(r => r is not null).Select(r => r!.Value).ToList();
            if (mates.Count == 0) {
                return null;
            }
            var table = mates[0].Table;
            if (StatusUpdater.LayoutProblem(_scanner, table) is not null) {
                return null;
            }
            var data = table.Rows.Where(r => !r.IsHeaderRow).ToList();
            var keys = data.Select(TableRowKeys).ToList();
            var (order, index) = Order([.. keys.Select(k => k.Scientific)], taxon.ScientificName, [.. keys.Select(k => k.Common)], taxon.CommonNameEn);
            // After the row before its place; before the first row when it sorts first; after the
            // genus's last row when the table keeps no order.
            var genusRows = mates.Where(m => m.Table == table).Select(m => data.IndexOf(table.Rows[m.Row])).Where(i => i >= 0).ToList();
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
            var neighbourKeys = keys[neighbourIndex];
            var rowText = NewTableRow(neighbour, neighbourKeys, taxon);
            var rowStart = _lines.Start(_scanner.LineOf(neighbour.Cells[0].Whole.Start));
            var rowEnd = neighbour.Cells[^1].Whole.End;
            // The new row's own "|-" goes between it and its neighbour.
            var line = before ? rowText + "\n|-" : "|-\n" + rowText;
            var member = _members.FirstOrDefault(m => m.Source == ListMemberSource.TableRow && TableRowOf(m) is { } r && r.Table == table
                && r.Row == table.Rows.ToList().IndexOf(neighbour));
            return new PlacedTaxon(taxon, line, before ? rowStart : rowEnd, neighbourKeys.Common ?? neighbourKeys.Scientific ?? string.Empty,
                member?.Taxon, _scanner.LineOf(neighbour.Cells[0].Whole.Start), before);
        }

        // The new row: the neighbour row's cells, attributes and line breaks, with the cell holding
        // its scientific name given the taxon's, the cell with its common name link the taxon's
        // common name, the status cell the taxon's status, and every other cell empty.
        private string NewTableRow(TableRow neighbour, (string? Scientific, string? Common) keys, ListTaxonRow taxon) {
            var sb = new StringBuilder();
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
                    var title = taxon.ListArticleTitle is { } t && !string.Equals(t, taxon.ScientificName, StringComparison.Ordinal) ? t + "|" : string.Empty;
                    value = ListScope.UnlinkLists($"''[[{title}{taxon.ScientificName}]]''");
                } else if (keys.Common is { } c && StatusUpdater.ArticleLinks(_scanner, cell.Content).Any(l => LinkLabel(_scanner.Original(l.Span)) == c)) {
                    // The article the list lines link: the taxon's article, else its scientific name.
                    var target = taxon.ListArticleTitle ?? taxon.ScientificName;
                    var title = !string.Equals(target, taxon.CommonNameEn, StringComparison.Ordinal) ? target + "|" : string.Empty;
                    value = taxon.CommonNameEn is { } name ? ListScope.UnlinkLists($"[[{title}{name}]]") : string.Empty;
                } else {
                    value = string.Empty;
                }
                var (start, end) = core.Length == 0 ? (cell.Content.Start, cell.Content.Start) : (core.Start, core.End);
                sb.Append(_text, at, start - at).Append(value);
                at = end;
            }
            sb.Append(_text, at, neighbour.Cells[^1].Whole.End - at);
            return sb.ToString();
        }
    }
}
