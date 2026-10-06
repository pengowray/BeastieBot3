using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// A missing taxon put into the text: its line, where it goes (Position in the text as it was
/// pasted), and the line it goes next to: NeighbourLine, which names NeighbourName (as written), the
/// taxon NeighbourTaxon when it is one the comparison found (null for a taxon IUCN does not have).
/// Before: it goes before that line, not after it.
public sealed record PlacedTaxon(ListTaxonRow Taxon, string Line, int Position, string NeighbourName, StatusTaxon? NeighbourTaxon,
    int NeighbourLine, bool Before);

/// Placed: the missing species put next to a listed species of the same genus. Unplaced: the
/// missing taxa that stay in the box to copy (a genus the list does not have, a subspecies or
/// variety, or a genus listed only in a table).
public sealed record ListPlacementResult(IReadOnlyList<PlacedTaxon> Placed, IReadOnlyList<ListTaxonRow> Unplaced);

/// What the comparison section of the update page shows: the comparison, the missing taxa put into
/// the wikitext (null when not asked for), and whether the reader asked for them.
public sealed record ListScopeView(ListScopeResult Scope, ListPlacementResult? Placement, bool AddMissing);

/// Puts the missing species of a list comparison (ListScope) into the list: each one on a new list
/// line next to a listed species of the same genus: before the first one whose name (as written)
/// sorts after it, when the genus is written in alphabetical order, else after the genus's last species. The line copies the
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
            .ToDictionary(g => g.Key, g => g.OrderBy(m => m.Line).ToList());

        var placed = new List<PlacedTaxon>();
        var unplaced = new List<ListTaxonRow>();
        foreach (var taxon in missing.OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)) {
            if (taxon.Kind != TaxonKinds.Species || !byGenus.TryGetValue(taxon.NodeId, out var mates) || mates.Count == 0) {
                unplaced.Add(taxon);
                continue;
            }
            // The genus's species lines: the lines with fewest markers. A deeper line naming a species
            // ("** ''Panthera leo'' subsp. ...") is under it.
            int DepthOf(ListMember m) {
                var text = lines.Text(m.Line);
                var start = ListStart(text);
                return start < 0 ? int.MaxValue : Markers(text[start..]);
            }
            var top = mates.Min(DepthOf);
            mates = [.. mates.Where(m => DepthOf(m) == top)];
            // The names as the list writes them: a synonym sorts where the list put it.
            var outOfOrder = mates.Zip(mates.Skip(1)).Count(p => Compare(p.First.Written, p.Second.Written) > 0);
            var sorted = outOfOrder <= (mates.Count - 1) / 10;
            // In a list in order (one in ten pairs may be out of order), after the last line before
            // the first species that sorts after it, counting the lines of species IUCN does not have
            // ("Carex gynandra" between "Carex grayi" and "Carex hallii"); else after the genus's last
            // species.
            var next = sorted ? mates.FirstOrDefault(m => Compare(m.Written, taxon.ScientificName) > 0) : null;
            var previous = sorted
                ? mates.LastOrDefault(m => m.Line < (next?.Line ?? int.MaxValue) && Compare(m.Written, taxon.ScientificName) < 0)
                : mates[^1];
            var anchor = previous ?? next!;
            var before = previous is null;
            // A list that starts on the line of its template ("{{columns-list|...|*[[Tiger]]") starts
            // at its first marker.
            var anchorText = lines.Text(anchor.Line);
            var listStart = ListStart(anchorText);
            if (listStart < 0) {
                unplaced.Add(taxon);
                continue;
            }
            var anchorLine = anchor.Line;
            var anchorName = anchor.Written;
            StatusTaxon? anchorTaxon = anchor.Taxon;
            if (sorted && previous is not null) {
                var depth = Markers(anchorText[listStart..]);
                for (var l = lines.LastOfBlock(anchorLine) + 1; l < (next?.Line ?? lines.Count + 1); l = lines.LastOfBlock(l) + 1) {
                    var lineText = lines.Text(l);
                    if (Markers(lineText) != depth || LeadingName().Match(lineText) is not { Success: true } leading
                        || Compare(leading.Groups["name"].Value, taxon.ScientificName) >= 0) {
                        break;
                    }
                    anchorLine = l;
                    anchorName = leading.Groups["name"].Value;
                    anchorTaxon = null;
                }
            }
            var line = NewLine(anchorText[listStart..], taxon, scope.Style, anchor.HasStatusTemplate, addIds, addYear);
            var position = before ? lines.Start(anchorLine) + listStart : lines.End(lines.LastOfBlock(anchorLine));
            placed.Add(new PlacedTaxon(taxon, line, position, anchorName, anchorTaxon, anchorLine, before));
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

    [GeneratedRegex(@"\|(?=[*#])")]
    private static partial Regex FirstMarker();

    // The first scientific name of a line in the scientific name first style:
    // "*''[[Carex gynandra]]'' <small>Schwein.</small>", "* †''Acer alaskense''".
    [GeneratedRegex(@"^[*#]+\s*(?:†|\{\{dagger\}\})?\s*'{2,5}(?:\[\[(?:[^|\]\n]*\|)?)?(?<name>\p{Lu}[\p{Ll}-]+ (?:× ?)?[\p{Ll}-]+)")]
    private static partial Regex LeadingName();

    // Where the list part of a line starts: at 0 for a line starting with "*" or "#", after the "|"
    // for "{{columns-list|...|*...", or -1.
    private static int ListStart(string line) =>
        line.Length > 0 && line[0] is '*' or '#' ? 0 : FirstMarker().Match(line) is { Success: true } m ? m.Index + 1 : -1;

    private static int Markers(string line) {
        var n = 0;
        while (n < line.Length && line[n] is '*' or '#' or ':' or ';') {
            n++;
        }
        return n;
    }

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

        public int Count => _starts.Count;

        // The last of a line and the lines after it that belong to it: lines with more bullet
        // markers (its subspecies) or starting with ":" (a note under it). A blank line, a line with
        // as many markers or fewer, or the end of a template ends it.
        public int LastOfBlock(int line) {
            var first = Text(line);
            var start = ListStart(first);
            var depth = Markers(start < 0 ? first : first[start..]);
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
            return last;
        }
    }
}
