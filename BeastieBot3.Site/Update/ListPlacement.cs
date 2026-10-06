using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// A missing taxon put into the text: its line, where it goes (Position in the text as it was
/// pasted), and the listed taxon it goes next to (Neighbour, on line NeighbourLine). Before: it goes
/// before the neighbour's line, not after it.
public sealed record PlacedTaxon(ListTaxonRow Taxon, string Line, int Position, StatusTaxon Neighbour, int NeighbourLine, bool Before);

/// Placed: the missing species put next to a listed species of the same genus. Unplaced: the
/// missing taxa that stay in the box to copy (a genus the list does not have, a subspecies or
/// variety, or a genus listed only in a table).
public sealed record ListPlacementResult(IReadOnlyList<PlacedTaxon> Placed, IReadOnlyList<ListTaxonRow> Unplaced);

/// What the comparison section of the update page shows: the comparison, the missing taxa put into
/// the wikitext (null when not asked for), and whether the reader asked for them.
public sealed record ListScopeView(ListScopeResult Scope, ListPlacementResult? Placement, bool AddMissing);

/// Puts the missing species of a list comparison (ListScope) into the list: each one on a new list
/// line next to a listed species of the same genus, in alphabetical order by scientific name when the
/// genus is written in that order, else after the genus's last species. The line copies the
/// neighbour's bullet markers and name style. Only list lines are used, never tables.
public static partial class ListPlacement {
    public static ListPlacementResult Place(string text, IReadOnlyList<ListMember> members, ListScopeResult scope, bool addIds, bool addYear) {
        var missing = scope.Missing ?? [];
        if (scope.Partial || missing.Count == 0) {
            return new ListPlacementResult([], missing);
        }
        var lines = new Lines(text);
        // The listed species on list lines, by genus (their lowest group), in text order.
        var byGenus = members
            .Where(m => m.OnListLine && m.Taxon is { InRelease: true, NodeId: not null, Kind: TaxonKinds.Species })
            .GroupBy(m => m.Taxon.NodeId!.Value)
            .ToDictionary(g => g.Key, g => g.GroupBy(m => m.Taxon.TaxonId).Select(t => t.First()).OrderBy(m => m.Line).ToList());

        var placed = new List<PlacedTaxon>();
        var unplaced = new List<ListTaxonRow>();
        foreach (var taxon in missing.OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)) {
            if (taxon.Kind != TaxonKinds.Species || !byGenus.TryGetValue(taxon.NodeId, out var mates) || mates.Count == 0) {
                unplaced.Add(taxon);
                continue;
            }
            var sorted = mates.Zip(mates.Skip(1)).All(p => Compare(p.First.Taxon.ScientificName, p.Second.Taxon.ScientificName) <= 0);
            ListMember neighbour;
            var before = false;
            if (sorted) {
                var previous = mates.LastOrDefault(m => Compare(m.Taxon.ScientificName, taxon.ScientificName) < 0);
                if (previous is null) {
                    neighbour = mates[0];
                    before = true;
                } else {
                    neighbour = previous;
                }
            } else {
                neighbour = mates[^1];
            }
            var line = NewLine(lines.Text(neighbour.Line), taxon, scope.Style, neighbour.HasStatusTemplate, addIds, addYear);
            var position = before ? lines.Start(neighbour.Line) : lines.EndOfBlock(neighbour.Line);
            placed.Add(new PlacedTaxon(taxon, line, position, neighbour.Taxon, neighbour.Line, before));
        }
        return new ListPlacementResult(placed, unplaced);
    }

    /// The insertions for StatusUpdater.TextWith. Several lines at one place keep the order of Placed,
    /// which is by scientific name.
    public static IEnumerable<(int Position, string Text)> Insertions(string text, ListPlacementResult result) {
        var eol = text.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
        return result.Placed.Select(p => (p.Position, p.Before ? p.Line + eol : eol + p.Line));
    }

    private static int Compare(string a, string b) => string.Compare(a, b, StringComparison.OrdinalIgnoreCase);

    // The new line: the neighbour's bullet markers and the space after them, the names in the
    // neighbour's style, and {{IUCN status}} when the neighbour has one.
    internal static string NewLine(string neighbour, ListTaxonRow taxon, SpeciesListStyle fallback, bool withStatus, bool addIds, bool addYear) {
        var prefix = Prefix().Match(neighbour);
        var style = StyleOf(neighbour[prefix.Length..]) ?? fallback;
        var line = SpeciesListLine.Format(GroupList.Entry(taxon), new SpeciesListLineOptions { Style = style, IncludeStatusTemplate = false });
        line = ListScope.UnlinkLists(line["* ".Length..]);
        if (withStatus && taxon.Category is not null) {
            line += " " + StatusTemplate(taxon, addIds, addYear);
        }
        return prefix.Value + line;
    }

    // Scientific name first when the line starts with italics, common name first when an italic name
    // comes later, common name only when the line has no italics.
    private static SpeciesListStyle? StyleOf(string afterMarkers) {
        var text = afterMarkers.TrimStart();
        if (text.StartsWith("''", StringComparison.Ordinal) || text.StartsWith("{{dagger}}''", StringComparison.OrdinalIgnoreCase)
            || text.StartsWith("†''", StringComparison.Ordinal)) {
            return SpeciesListStyle.ScientificNameFirst;
        }
        if (text.StartsWith("[[", StringComparison.Ordinal)) {
            return text.Contains("''", StringComparison.Ordinal) ? SpeciesListStyle.CommonNameFirst : SpeciesListStyle.CommonNameOnly;
        }
        return null;
    }

    // {{IUCN status|EN}}, with the ids and year when the reader asks for them, as the updater adds it.
    private static string StatusTemplate(ListTaxonRow taxon, bool addIds, bool addYear) {
        var code = GroupList.StatusCode(taxon);
        var ids = addIds && taxon.AssessmentId is { } a ? $"|{taxon.TaxonId}/{a}|1" : string.Empty;
        var year = addYear && code is not ("EX" or "EW") && taxon.YearPublished is { } y
            ? "|year=" + y.ToString(CultureInfo.InvariantCulture)
            : string.Empty;
        return $"{{{{IUCN status|{code}{ids}{year}}}}}";
    }

    [GeneratedRegex(@"^[*#]+ ?")]
    private static partial Regex Prefix();

    // The lines of the text, 1-based, with their ends before "\r\n" or "\n".
    private sealed class Lines {
        private readonly string _text;
        private readonly List<int> _starts = [0];

        public Lines(string text) {
            _text = text;
            for (var i = 0; i < text.Length; i++) {
                if (text[i] == '\n') {
                    _starts.Add(i + 1);
                }
            }
        }

        public int Start(int line) => _starts[Math.Clamp(line - 1, 0, _starts.Count - 1)];

        public int End(int line) {
            var next = line < _starts.Count ? _starts[line] - 1 : _text.Length;
            return next > 0 && next <= _text.Length && next - 1 >= Start(line) && _text[next - 1] == '\r' ? next - 1 : next;
        }

        public string Text(int line) => _text[Start(line)..End(line)];

        // The end of a line and of the lines after it that belong to it: lines with more bullet
        // markers (its subspecies) or starting with ":" (a note under it). A blank line, a line with
        // as many markers or fewer, or the end of a template ends it.
        public int EndOfBlock(int line) {
            var depth = Markers(Text(line));
            var last = line;
            for (var next = line + 1; next <= _starts.Count; next++) {
                var t = Text(next);
                if (t.Length == 0 || t.StartsWith("}}", StringComparison.Ordinal)) {
                    break;
                }
                if (Markers(t) > depth || (t[0] == ':' && depth > 0)) {
                    last = next;
                    continue;
                }
                break;
            }
            return End(last);
        }

        private static int Markers(string line) {
            var n = 0;
            while (n < line.Length && line[n] is '*' or '#' or ':' or ';') {
                n++;
            }
            return n;
        }
    }
}
