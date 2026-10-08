using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Update;

// Taxa now in another category taken out of a list, as the reader ticks them in the comparison's
// table of taxa now in a category other than the list's: their list lines (with the lines under
// them) and their {{Species table/row}} rows. A taxon is taken out only when every place the text
// names it can be: a taxon in a wikitable or in running text stays, and so does one whose line names
// a taxon that stays, has list lines under it that are not taken out, or defines a reference that
// other lines use.
public static partial class ListPlacement {
    private sealed partial class Placer {
        // The removal of each line taken out (every line of a removed block or row), by line number.
        private readonly Dictionary<int, TextRemoval> _removalOfLine = [];
        private readonly HashSet<WikiTemplate> _removedRows = [];

        public (List<RemovedTaxon> Removed, List<(ListScopeMember Member, KeptReason Reason)> Kept) Remove(IReadOnlyList<ListScopeMember> taxa) {
            var removed = new List<RemovedTaxon>();
            var kept = new List<(ListScopeMember, KeptReason)>();
            if (taxa.Count == 0) {
                return (removed, kept);
            }
            var ids = taxa.Select(t => t.Taxon.TaxonId).ToHashSet();
            var byLine = _members.Where(m => m.Taxon.InRelease).GroupBy(m => m.Line).ToDictionary(g => g.Key, g => g.ToList());
            foreach (var taxon in taxa) {
                var parts = new List<(TextRemoval Removal, int First, int Last, WikiTemplate? Row)>();
                KeptReason? problem = null;
                foreach (var line in _members.Where(m => m.Taxon.TaxonId == taxon.Taxon.TaxonId).GroupBy(m => m.Line)) {
                    var onLine = line.FirstOrDefault(m => m.Source == ListMemberSource.ListLine);
                    var inRow = line.FirstOrDefault(m => m.Source == ListMemberSource.SpeciesTableRow);
                    var (part, reason) = onLine is not null ? LineRemoval(onLine, ids, byLine)
                        : inRow is not null ? RowRemoval(inRow)
                        : (null, KeptReason.NotOnListLine);
                    if (part is null) {
                        problem = reason;
                        break;
                    }
                    parts.Add(part.Value);
                }
                if (problem is not null || parts.Count == 0) {
                    kept.Add((taxon, problem ?? KeptReason.NotOnListLine));
                    continue;
                }
                foreach (var (removal, first, last, row) in parts) {
                    for (var l = first; l <= last; l++) {
                        _removalOfLine.TryAdd(l, removal);
                    }
                    if (row is not null) {
                        _removedRows.Add(row);
                    }
                }
                removed.Add(new RemovedTaxon(taxon, [.. parts.Select(p => p.Removal).Distinct()]));
            }
            return (removed, kept);
        }

        // A list line and the lines under it (deeper lines and ":" notes). The line break goes with
        // the line; for the last line of a list layout template ("*[[Oxyura maccoa|Maccoa duck]]}}")
        // the line break before it goes instead, so that the "}}" stays at the end of the line before.
        private ((TextRemoval, int, int, WikiTemplate?)? Part, KeptReason Reason) LineRemoval(ListMember member, IReadOnlySet<long> ids,
            IReadOnlyDictionary<int, List<ListMember>> byLine) {
            var line = member.Line;
            var listStart = ListStart(_lines.Text(line));
            if (listStart < 0) {
                return (null, KeptReason.NotOnListLine);
            }
            var last = _lines.LastOfBlock(line);
            for (var l = line; l <= last; l++) {
                var named = byLine.GetValueOrDefault(l) ?? [];
                if (named.Any(o => !ids.Contains(o.Taxon.TaxonId))) {
                    return (null, KeptReason.SharesLine);
                }
                // A line under it that names no taxon taken out ("**Southwest Indian Ocean
                // subpopulation" under a species of another category in a list of NT taxa) is an
                // item of its own.
                if (l > line && ListStart(_lines.Text(l)) == 0 && named.Count == 0) {
                    return (null, KeptReason.SharesLine);
                }
            }
            var end = After(last);
            var endLine = _scanner.LineOf(end);
            var lineEnd = _lines.End(endLine);
            var closes = _text.AsSpan(end, Math.Max(0, lineEnd - end)).TrimStart().StartsWith("}}", StringComparison.Ordinal);
            var from = _lines.Start(line) + listStart;
            TextSpan span;
            if (closes) {
                span = listStart > 0 || line == 1 ? new TextSpan(from, end) : new TextSpan(_lines.End(line - 1), end);
            } else {
                span = new TextSpan(from, endLine < _lines.Count ? _lines.Start(endLine + 1) : _text.Length);
            }
            if (DefinesUsedReference(span)) {
                return (null, KeptReason.DefinesReference);
            }
            return ((new TextRemoval(span, new TextSpan(from, lineEnd)), line, endLine, null), default);
        }

        // A {{Species table/row}}, with its line break when it is alone on its lines.
        private ((TextRemoval, int, int, WikiTemplate?)? Part, KeptReason Reason) RowRemoval(ListMember member) {
            if (RowOf(member) is not { } found) {
                return (null, KeptReason.NotOnListLine);
            }
            var row = found.Table.Rows[found.Row];
            var firstLine = _scanner.LineOf(row.Span.Start);
            var lastLine = _scanner.LineOf(row.Span.End - 1);
            var start = row.Span.Start;
            var lineStart = _lines.Start(firstLine);
            if (string.IsNullOrWhiteSpace(_text[lineStart..start])) {
                start = lineStart;
            }
            var end = row.Span.End;
            var lineEnd = _lines.End(lastLine);
            if (end <= lineEnd && string.IsNullOrWhiteSpace(_text[end..lineEnd])) {
                end = lastLine < _lines.Count ? _lines.Start(lastLine + 1) : _text.Length;
            }
            var span = new TextSpan(start, end);
            if (DefinesUsedReference(span)) {
                return (null, KeptReason.DefinesReference);
            }
            return ((new TextRemoval(span, new TextSpan(start, row.Span.End)), firstLine, lastLine, row), default);
        }

        // Whether the span defines a named reference ("<ref name="IUCN">...</ref>") that the text
        // uses outside it ("<ref name="IUCN"/>").
        private bool DefinesUsedReference(TextSpan span) {
            var inside = _text[span.Start..span.End];
            foreach (Match definition in RefDefinition().Matches(inside)) {
                var name = Regex.Escape(definition.Groups["name"].Value.Trim());
                var use = new Regex($@"<ref\s+name\s*=\s*[""']?{name}[""']?\s*/?>", RegexOptions.IgnoreCase);
                if (use.Matches(_text).Any(u => u.Index < span.Start || u.Index >= span.End)) {
                    return true;
                }
            }
            return false;
        }
    }

    // A named reference with content: <ref name="x">, <ref name=x>; not <ref name="x"/>.
    [GeneratedRegex(@"<ref\s+name\s*=\s*[""']?(?<name>[^""'>/]+?)[""']?\s*>", RegexOptions.IgnoreCase)]
    private static partial Regex RefDefinition();
}
