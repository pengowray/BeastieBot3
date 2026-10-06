using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// A missing taxon put into the text: the text put in (a list line, a table row, or a whole
/// {{Species table}} for a genus), where it goes (Position in the text as it was pasted), and the
/// line it goes next to: NeighbourLine, which names NeighbourName (as written), the taxon
/// NeighbourTaxon when the comparison found one there. Before: it goes before that line, not after it.
/// The taxa of one new genus table share its text: the first carries it, the others have an empty Line.
public sealed record PlacedTaxon(ListTaxonRow Taxon, string Line, int Position, string NeighbourName, StatusTaxon? NeighbourTaxon,
    int NeighbourLine, bool Before);

/// Placed: the missing taxa put into the text. Unplaced: the missing taxa that stay in the box to copy.
public sealed record ListPlacementResult(IReadOnlyList<PlacedTaxon> Placed, IReadOnlyList<ListTaxonRow> Unplaced);

/// What the comparison section of the update page shows: the comparison, the missing taxa put into
/// the wikitext (null when not asked for), and whether the reader asked for them.
public sealed record ListScopeView(ListScopeResult Scope, ListPlacementResult? Placement, bool AddMissing);

/// The updater's options that change the text put in: ids and year in {{IUCN status}}, and {{cite Q}}
/// for the references of new {{Species table/row}} rows.
public sealed record ListPlacementOptions(bool AddIds = false, bool AddYear = false, bool CiteQ = false);

/// Puts the missing taxa of a list comparison (ListScope) into the list, where the list has a place
/// for them:
/// - a species with a species of its genus on a list line: a new list line among them;
/// - a subspecies or variety whose species is on a list line: a new line under the species;
/// - a species with a species of its genus in a {{Species table}}: a new {{Species table/row}};
/// - a species whose genus has no table, in a list of {{Species table}}s with a table of a genus of
///   its family: a new {{Species table}} for the genus;
/// - a species with a species of its genus in a row of a simple wikitable: a new row.
/// Among its neighbours a taxon goes in alphabetical order, by scientific name or by common name,
/// whichever order the list keeps (one pair in ten may be out of order); in a list in neither order,
/// after the last of them. New lines and rows copy their neighbour's form.
public static partial class ListPlacement {
    public static ListPlacementResult Place(string text, IReadOnlyList<ListMember> members, ListScopeResult scope,
        ListPlacementOptions options, IListScopeLookup lookup) {
        var missing = scope.Missing ?? [];
        if (scope.Partial || missing.Count == 0) {
            return new ListPlacementResult([], missing);
        }
        var placer = new Placer(text, members, scope, options, lookup);
        var placed = new List<PlacedTaxon>();
        var rest = new List<ListTaxonRow>();
        foreach (var taxon in missing.OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)) {
            if (placer.Place(taxon) is { } p) {
                placed.Add(p);
            } else {
                rest.Add(taxon);
            }
        }
        var tables = placer.NewGenusTables(rest);
        placed.AddRange(tables);
        var inTables = tables.Select(p => p.Taxon.TaxonId).ToHashSet();
        return new ListPlacementResult(placed, [.. rest.Where(t => !inTables.Contains(t.TaxonId))]);
    }

    /// The insertions for StatusUpdater.TextWith. Several texts at one place keep the order of
    /// Placed. A text written with "\n" gets the pasted text's line ends.
    public static IEnumerable<(int Position, string Text)> Insertions(string text, ListPlacementResult result) {
        var crlf = text.Contains("\r\n", StringComparison.Ordinal);
        return result.Placed.Where(p => p.Line.Length > 0).Select(p => {
            var inserted = p.Before ? p.Line + "\n" : "\n" + p.Line;
            return (p.Position, crlf ? inserted.Replace("\n", "\r\n", StringComparison.Ordinal) : inserted);
        });
    }

    private static int Compare(string a, string b) => string.Compare(a, b, StringComparison.OrdinalIgnoreCase);

    /// Where a key goes among keys in text order: the index of the first key that sorts after it,
    /// or Count to go after the last; null when the keys are not in order (more than one pair in ten
    /// out of order) or one is missing.
    internal static int? OrderedPlace(IReadOnlyList<string?> keys, string? key) {
        if (key is null || keys.Count == 0 || keys.Any(k => k is null)) {
            return null;
        }
        var outOfOrder = keys.Zip(keys.Skip(1)).Count(p => Compare(p.First!, p.Second!) > 0);
        if (outOfOrder > (keys.Count - 1) / 10) {
            return null;
        }
        for (var i = 0; i < keys.Count; i++) {
            if (Compare(keys[i]!, key) > 0) {
                return i;
            }
        }
        return keys.Count;
    }

    /// The order a list keeps among the neighbours of a new taxon: by scientific name, else by common
    /// name, else none. Index: the first neighbour that sorts after the new taxon (Count: after the
    /// last); with no order, Count.
    internal enum ListOrder { None, Scientific, Common }

    internal static (ListOrder Order, int Index) Order(IReadOnlyList<string?> scientific, string scientificKey,
        IReadOnlyList<string?> common, string? commonKey) =>
        OrderedPlace(scientific, scientificKey) is { } s ? (ListOrder.Scientific, s)
        : OrderedPlace(common, commonKey) is { } c ? (ListOrder.Common, c)
        : (ListOrder.None, scientific.Count);

    // {{IUCN status|EN}}, with the ids and year when the reader asks for them, as the updater adds it.
    private static string StatusTemplate(ListTaxonRow taxon, ListPlacementOptions options) {
        var code = GroupList.StatusCode(taxon);
        var ids = options.AddIds && taxon.AssessmentId is { } a ? $"|{taxon.TaxonId}/{a}|1" : string.Empty;
        var year = options.AddYear && code is not ("EX" or "EW") && taxon.YearPublished is { } y
            ? "|year=" + y.ToString(CultureInfo.InvariantCulture)
            : string.Empty;
        return $"{{{{IUCN status|{code}{ids}{year}}}}}";
    }

    private sealed partial class Placer {
        private readonly string _text;
        private readonly WikitextScanner _scanner;
        private readonly Lines _lines;
        private readonly IReadOnlyList<ListMember> _members;
        private readonly ListScopeResult _scope;
        private readonly ListPlacementOptions _options;
        private readonly IListScopeLookup _lookup;

        public Placer(string text, IReadOnlyList<ListMember> members, ListScopeResult scope, ListPlacementOptions options, IListScopeLookup lookup) {
            _text = text;
            _scanner = new WikitextScanner(text);
            _lines = new Lines(text);
            _members = members;
            _scope = scope;
            _options = options;
            _lookup = lookup;
        }

        private IEnumerable<ListMember> Listed(ListMemberSource source) =>
            _members.Where(m => m.Source == source && m.Taxon is { InRelease: true, NodeId: not null });

        public PlacedTaxon? Place(ListTaxonRow taxon) {
            if (taxon.Kind != TaxonKinds.Species) {
                return taxon.ParentTaxonId is { } parent ? UnderSpecies(taxon, parent) : null;
            }
            return OnListLine(taxon) ?? InSpeciesTable(taxon) ?? InTableRow(taxon);
        }

        // ------------------------------------------------------------ list lines

        private PlacedTaxon? OnListLine(ListTaxonRow taxon) {
            var mates = Listed(ListMemberSource.ListLine)
                .Where(m => m.Taxon.Kind == TaxonKinds.Species && m.Taxon.NodeId == taxon.NodeId).OrderBy(m => m.Line).ToList();
            if (mates.Count == 0) {
                return null;
            }
            // The genus's species lines: the lines with fewest markers. A deeper line naming a species
            // ("** ''Panthera leo'' subsp. ...") is under it.
            var top = mates.Min(DepthOf);
            mates = [.. mates.Where(m => DepthOf(m) == top).GroupBy(m => m.Line).Select(g => g.First())];
            var (order, index) = Order(
                [.. mates.Select(m => m.Written)], taxon.ScientificName,
                [.. mates.Select(m => CommonOnLine(ListPart(_lines.Text(m.Line))))], taxon.CommonNameEn);
            var before = index == 0 && order != ListOrder.None;
            var anchor = before ? mates[0] : mates[Math.Max(0, index - 1)];
            var anchorText = _lines.Text(anchor.Line);
            var listStart = ListStart(anchorText);
            if (listStart < 0) {
                return null;
            }
            var anchorLine = anchor.Line;
            var anchorName = anchor.Written;
            StatusTaxon? anchorTaxon = anchor.Taxon;
            // After its neighbour: also after the lines of species IUCN does not have that sort before
            // the new one ("Carex gynandra" between "Carex grayi" and "Carex hallii").
            if (!before && order != ListOrder.None) {
                var key = order == ListOrder.Scientific ? taxon.ScientificName : taxon.CommonNameEn!;
                var depth = Markers(anchorText[listStart..]);
                var stop = index < mates.Count ? mates[index].Line : _lines.Count + 1;
                for (var l = _lines.LastOfBlock(anchorLine) + 1; l < stop; l = _lines.LastOfBlock(l) + 1) {
                    var lineText = _lines.Text(l);
                    var name = order == ListOrder.Scientific
                        ? LeadingName().Match(lineText) is { Success: true } m ? m.Groups["name"].Value : null
                        : CommonOnLine(lineText);
                    if (Markers(lineText) != depth || name is null || Compare(name, key) >= 0) {
                        break;
                    }
                    anchorLine = l;
                    anchorName = name;
                    anchorTaxon = null;
                }
            }
            var line = NewLine(anchorText[listStart..], taxon, _scope.Style, anchor.HasStatusTemplate, _options);
            var position = before ? _lines.Start(anchorLine) + listStart : _lines.End(_lines.LastOfBlock(anchorLine));
            return new PlacedTaxon(taxon, line, position, anchorName, anchorTaxon, anchorLine, before);
        }

        private int DepthOf(ListMember m) {
            var text = _lines.Text(m.Line);
            var start = ListStart(text);
            return start < 0 ? int.MaxValue : Markers(text[start..]);
        }

        private static string ListPart(string line) => ListStart(line) is >= 0 and var i ? line[i..] : line;

        // A subspecies or variety under its species' line: after the lines already under it, one
        // marker deeper, with the genus and species abbreviated ("** ''P. l. persica''").
        private PlacedTaxon? UnderSpecies(ListTaxonRow taxon, long parentId) {
            var species = Listed(ListMemberSource.ListLine).Where(m => m.Taxon.TaxonId == parentId).OrderBy(m => m.Line).FirstOrDefault();
            if (species is null) {
                return null;
            }
            var speciesText = _lines.Text(species.Line);
            var listStart = ListStart(speciesText);
            if (listStart < 0) {
                return null;
            }
            var markers = Prefix().Match(speciesText[listStart..]).Value;
            var trimmed = markers.TrimEnd();
            var prefix = trimmed + trimmed[^1] + (markers.Length > trimmed.Length ? " " : string.Empty);
            var last = _lines.LastOfBlock(species.Line);
            // In the style of the lines already under the species ("**[[Western lowland gorilla]]"),
            // else with the genus and species abbreviated.
            var style = last > species.Line ? StyleOf(_lines.Text(species.Line + 1).TrimStart('*', '#', ' ')) : null;
            var entry = GroupList.Entry(taxon);
            var line = style is { } s
                ? SpeciesListLine.Format(entry, new SpeciesListLineOptions { Style = s, IncludeStatusTemplate = false })
                : SpeciesListLine.FormatInfraspecificUnderSpecies(entry, new SpeciesListLineOptions { Style = _scope.Style, IncludeStatusTemplate = false });
            line = prefix + ListScope.UnlinkLists(line["* ".Length..]);
            if (species.HasStatusTemplate && taxon.Category is not null) {
                line += " " + StatusTemplate(taxon, _options);
            }
            return new PlacedTaxon(taxon, line, _lines.End(last), species.Written, species.Taxon, species.Line, false);
        }
    }

    // The new line: the neighbour's bullet markers and the space after them, the names in the
    // neighbour's style, and {{IUCN status}} when the neighbour has one.
    internal static string NewLine(string neighbour, ListTaxonRow taxon, SpeciesListStyle fallback, bool withStatus, ListPlacementOptions options) {
        var prefix = Prefix().Match(neighbour);
        var style = StyleOf(neighbour[prefix.Length..]) ?? fallback;
        var line = SpeciesListLine.Format(GroupList.Entry(taxon), new SpeciesListLineOptions { Style = style, IncludeStatusTemplate = false });
        line = ListScope.UnlinkLists(line["* ".Length..]);
        if (withStatus && taxon.Category is not null) {
            line += " " + StatusTemplate(taxon, options);
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

    // The common name a list line starts with: the text of its first link, when the line starts with
    // a link that is not in italics ("*[[Lepilemur ruficaudatus|Red-tailed sportive lemur]]").
    private static string? CommonOnLine(string line) =>
        LeadingCommon().Match(line) is { Success: true } m ? m.Groups["label"].Success ? m.Groups["label"].Value : m.Groups["target"].Value : null;

    [GeneratedRegex(@"^[*#]+ ?")]
    private static partial Regex Prefix();

    [GeneratedRegex(@"\|(?=[*#])")]
    private static partial Regex FirstMarker();

    // The first scientific name of a line in the scientific name first style:
    // "*''[[Carex gynandra]]'' <small>Schwein.</small>", "* †''Acer alaskense''".
    [GeneratedRegex(@"^[*#]+\s*(?:†|\{\{dagger\}\})?\s*'{2,5}(?:\[\[(?:[^|\]\n]*\|)?)?(?<name>\p{Lu}[\p{Ll}-]+ (?:× ?)?[\p{Ll}-]+)")]
    private static partial Regex LeadingName();

    [GeneratedRegex(@"^[*#]+\s*\[\[(?<target>[^|\]\n]+)(?:\|(?<label>[^\]\n]+))?\]\]")]
    private static partial Regex LeadingCommon();

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
