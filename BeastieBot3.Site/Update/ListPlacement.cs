using System.Text.RegularExpressions;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// A missing taxon put into the text: the text put in (a list line, a table row, or a whole
/// {{Species table}} for a genus), where it goes (Position in the text as it was pasted), and the
/// line it goes next to: NeighbourLine, which names NeighbourName (as written), the taxon
/// NeighbourTaxon when the comparison found one there. Before: it goes before that line, not after it.
/// The taxa of one new genus table or new section share its text: the first carries it, the others
/// have an empty Line.
/// Heading: the heading of the section it goes in (its text, links removed), or null for a text
/// with no headings. NewHeading: the heading is a new one, put in with the taxon.
public sealed record PlacedTaxon(ListTaxonRow Taxon, string Line, int Position, string NeighbourName, StatusTaxon? NeighbourTaxon,
    int NeighbourLine, bool Before) {
    public string? Heading { get; init; }
    public bool NewHeading { get; init; }
}

/// Why a missing taxon was not put into the text.
public enum UnplacedReason {
    /// The text has no taxon of its genus and no heading of a group it is in.
    NoPlace,
    /// The species of its genus are in a wikitable that has rowspan or colspan, or no single header row.
    TableLayout,
    /// A subspecies or variety whose species is not on a list line.
    SpeciesNotOnLine,
    /// The species of its genus are in a {{Species table}} whose rows this page cannot copy.
    RowLayout,
    /// Its place is a line taken out in the same run.
    RemovedLine,
}

/// A listed taxon that is now in another category, taken out of the text: the parts of the text
/// removed (positions in the text as it was pasted) and the lines they were on.
public sealed record RemovedTaxon(ListScopeMember Member, IReadOnlyList<TextRemoval> Removals);

/// Span: the text removed. Owned: the text of the removed lines, inside which the status updater's
/// own edits are dropped (Span can start at the end of the line before, to take its line break).
public sealed record TextRemoval(TextSpan Span, TextSpan Owned);

/// Why a taxon now in another category was left in the text.
public enum KeptReason {
    /// It is named in a wikitable, a taxobox or running text, not on a list line or in a {{Species table/row}}.
    NotOnListLine,
    /// Its line names other taxa, or has list lines under it that are not taken out.
    SharesLine,
    /// Its line defines a reference that other lines use.
    DefinesReference,
}

/// Placed: the missing taxa put into the text. Unplaced: the missing taxa that stay in the box to
/// copy, with the reason for each in UnplacedReasons (by taxon id).
/// Removed and Kept: the taxa the reader asked to take out, and those left in with the reason.
/// RemovedHeadings: the sections taken out because no taxa were left in them (their heading text).
public sealed record ListPlacementResult(IReadOnlyList<PlacedTaxon> Placed, IReadOnlyList<ListTaxonRow> Unplaced) {
    public IReadOnlyDictionary<long, UnplacedReason> UnplacedReasons { get; init; } = new Dictionary<long, UnplacedReason>();
    public IReadOnlyList<RemovedTaxon> Removed { get; init; } = [];
    public IReadOnlyList<(ListScopeMember Member, KeptReason Reason)> Kept { get; init; } = [];
    public IReadOnlyList<string> RemovedHeadings { get; init; } = [];
    /// The removals of the taxa and of the empty sections, for StatusUpdater.TextWith.
    public IReadOnlyList<TextRemoval> Removals { get; init; } = [];
}

/// What the comparison section of the update page shows: the comparison, the missing taxa put into
/// the wikitext (null when not asked for), and whether the reader asked for them.
/// Removing: the taxa now in another category the reader chose to take out, null when not asked.
/// Forms: what the text lists its taxa in. TextKey: the key of the text, sent with the checkboxes.
public sealed record ListScopeView(ListScopeResult Scope, ListPlacementResult? Placement, bool AddMissing, bool ExtraSpecies = false,
    BeastieBot3.Shared.Wikitext.ListCategoryChoice? Categories = null,
    IReadOnlyDictionary<long, IReadOnlyList<EpbcListingRow>>? Epbc = null, bool CategoriesFromTitle = false,
    BeastieBot3.Shared.Wikitext.AreaNames? Areas = null) {
    public IReadOnlySet<long>? Removing { get; init; }
    public ListForms Forms { get; init; } = new(false, false, false, false);
    public string TextKey { get; init; } = string.Empty;

    /// The name of the area compared with, for a sentence ("the United States"), or null.
    public string? AreaName => Scope.Area is { } code ? Areas?.ByCode(code)?.SentenceName ?? code : null;

    /// The name of the area for a column heading ("United States"), or null.
    public string? AreaHeadingName => Scope.Area is { } code ? Areas?.ByCode(code)?.DisplayName ?? code : null;
}

/// A table of listed taxa in the comparison; Epbc: their EPBC Act listings, when they are shown.
/// Removing: the taxa ticked to take out, when the table has a checkbox for each; Kept: why a ticked
/// taxon stayed.
public sealed record ListScopeMembersView(IReadOnlyList<ListScopeMember> Members, IReadOnlyDictionary<long, IReadOnlyList<EpbcListingRow>>? Epbc) {
    public IReadOnlySet<long>? Removing { get; init; }
    public IReadOnlyDictionary<long, KeptReason> Kept { get; init; } = new Dictionary<long, KeptReason>();
}

/// The updater's options that change the text put in: ids and year in {{IUCN status}}, and {{cite Q}}
/// for the references of new {{Species table/row}} rows.
/// StatusOnLines: every new list line gets {{IUCN status}}, as StatusUpdateOptions.AddToListLines gives
/// the other lines one; otherwise a new line has one when its neighbour has.
/// TablesWithNewColumn: the tables (by the line of their header row) that get a status column in the
/// same run (StatusUpdateOptions.AddStatusColumns), with the 1-based column it goes after: a new row
/// gets a status cell there too.
/// AddMissing: put the missing taxa in. Remove: the taxa (ids) of ListScopeResult.OtherCategory to
/// take out of the text.
public sealed record ListPlacementOptions(bool AddIds = false, bool AddYear = false, bool CiteQ = false, bool StatusOnLines = false) {
    public IReadOnlyDictionary<int, int> TablesWithNewColumn { get; init; } = new Dictionary<int, int>();
    public bool AddMissing { get; init; } = true;
    public IReadOnlySet<long> Remove { get; init; } = new HashSet<long>();
}

/// Puts the missing taxa of a list comparison (ListScope) into the list, where the list has a place
/// for them:
/// - a species with a species of its genus on a list line: a new list line among them;
/// - a subspecies or variety whose species is on a list line: a new line under the species;
/// - a species with a species of its genus in a {{Species table}}: a new {{Species table/row}};
/// - a species whose genus has no table, in a list of {{Species table}}s with a table of a genus of
///   its family: a new {{Species table}} for the genus;
/// - a species with a species of its genus in a row of a simple wikitable: a new row;
/// - in a list of list lines under headings, any other species: a new line in the section of the
///   deepest group that holds it, or a new section for its group beside the sections of other groups
///   of the same rank (ListPlacement.Sections.cs).
/// Among its neighbours a taxon goes in alphabetical order, by scientific name or by common name,
/// whichever order the list keeps (one pair in ten may be out of order); in a list in neither order,
/// after the last of them. New lines and rows copy their neighbour's form.
/// It also takes out the taxa now in another category that the reader ticked (ListPlacement.Removal.cs),
/// and the sections left with no taxa.
public static partial class ListPlacement {
    public static ListPlacementResult Place(string text, IReadOnlyList<ListMember> members, ListScopeResult scope,
        ListPlacementOptions options, IListScopeLookup lookup) {
        var placer = new Placer(text, members, scope, options, lookup);
        var toRemove = scope.OtherCategory.Where(m => options.Remove.Contains(m.Taxon.TaxonId)).ToList();
        var (removed, kept) = placer.Remove(toRemove);

        // The species IUCN has not assessed (MissingExtra) go in the same way, with no status.
        IReadOnlyList<ListTaxonRow> missing = options.AddMissing && !scope.Partial ? [.. scope.Missing ?? [], .. scope.MissingExtra ?? []] : [];
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
        rest = [.. rest.Where(t => !inTables.Contains(t.TaxonId))];
        var sections = placer.NewSections(rest);
        placed.AddRange(sections);
        var inSections = sections.Select(p => p.Taxon.TaxonId).ToHashSet();
        rest = [.. rest.Where(t => !inSections.Contains(t.TaxonId))];

        var (emptied, headings) = placer.EmptiedSections(placed);
        List<TextRemoval> removals = [.. emptied, .. removed.SelectMany(r => r.Removals).Distinct()];
        // A text put in where the text is taken out would be lost: such a taxon stays in the box to copy.
        var cut = Merge(removals.Select(r => r.Span), text);
        var lost = placed.Where(p => p.Line.Length > 0 && cut.Any(r => r.Start < p.Position && p.Position < r.End))
            .Select(p => p.Position).ToHashSet();
        var reasons = rest.ToDictionary(t => t.TaxonId, placer.WhyNotPlaced);
        foreach (var p in placed.Where(p => lost.Contains(p.Position))) {
            rest.Add(p.Taxon);
            reasons[p.Taxon.TaxonId] = UnplacedReason.RemovedLine;
        }
        return new ListPlacementResult([.. placed.Where(p => !lost.Contains(p.Position)).Select(placer.WithHeading)], rest) {
            UnplacedReasons = reasons,
            Removed = removed,
            Kept = kept,
            RemovedHeadings = headings,
            Removals = removals,
        };
    }

    /// The spans in order, with overlapping ones joined: the line break before the last line of a list
    /// layout template is in the removal of that line and of the line before it when both go. A span
    /// that starts a line and ends at a "}}" on the same line (of a list layout template) also takes
    /// the line break before it, so that the "}}" stays at the end of the last line left.
    public static List<TextSpan> Merge(IEnumerable<TextSpan> spans, string text) {
        var merged = new List<TextSpan>();
        foreach (var span in spans.OrderBy(s => s.Start).ThenBy(s => s.End)) {
            if (merged.Count > 0 && span.Start < merged[^1].End) {
                merged[^1] = merged[^1] with { End = Math.Max(merged[^1].End, span.End) };
            } else {
                merged.Add(span);
            }
        }
        for (var i = 0; i < merged.Count; i++) {
            var (start, end) = (merged[i].Start, merged[i].End);
            if (start > 0 && text[start - 1] == '\n' && text[end - 1] != '\n' && text.AsSpan(end).TrimStart(" \t").StartsWith("}}", StringComparison.Ordinal)
                && (i == 0 || merged[i - 1].End < start - 1)) {
                var newline = start >= 2 && text[start - 2] == '\r' ? 2 : 1;
                merged[i] = new TextSpan(start - newline, end);
            }
        }
        return merged;
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
    private static string StatusTemplate(ListTaxonRow taxon, ListPlacementOptions options) =>
        StatusUpdater.NewStatusTemplate(GroupList.StatusCode(taxon), taxon.TaxonId, taxon.AssessmentId, taxon.YearPublished,
            options.AddIds, options.AddYear);

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

        // The listed members on lines that stay in the text.
        private IEnumerable<ListMember> Kept(ListMemberSource source) => Listed(source).Where(m => !_removalOfLine.ContainsKey(m.Line));

        public PlacedTaxon? Place(ListTaxonRow taxon) {
            if (taxon.Kind != TaxonKinds.Species) {
                return taxon.ParentTaxonId is { } parent ? UnderSpecies(taxon, parent) : null;
            }
            return OnListLine(taxon) ?? InSpeciesTable(taxon) ?? InTableRow(taxon) ?? InSection(taxon);
        }

        /// Why a taxon that no step put in was left out.
        public UnplacedReason WhyNotPlaced(ListTaxonRow taxon) =>
            taxon.Kind != TaxonKinds.Species ? UnplacedReason.SpeciesNotOnLine
            : Kept(ListMemberSource.TableRow).Any(m => m.Taxon.NodeId == taxon.NodeId) ? UnplacedReason.TableLayout
            : Kept(ListMemberSource.SpeciesTableRow).Any(m => m.Taxon.NodeId == taxon.NodeId) ? UnplacedReason.RowLayout
            : UnplacedReason.NoPlace;

        // ------------------------------------------------------------ list lines

        // The species lines of each genus and their keys, read once per genus.
        private Dictionary<int, LineGroup>? _genusLines;

        /// Lines among which a new line goes, with their scientific names (as written) and common
        /// names, in text order.
        private sealed record LineGroup(List<ListMember> Mates, List<string?> Scientific, List<string?> Common);

        // The lines with fewest markers among these members, one member per line. A deeper line naming
        // a species ("** ''Panthera leo'' subsp. ...") is under one of them.
        private LineGroup TopLines(IEnumerable<ListMember> members) {
            var lines = members.OrderBy(m => m.Line).ToList();
            var top = lines.Count == 0 ? 0 : lines.Min(DepthOf);
            List<ListMember> mates = [.. lines.Where(m => DepthOf(m) == top).GroupBy(m => m.Line).Select(l => l.First())];
            return new LineGroup(mates, [.. mates.Select(m => (string?)m.Written)], [.. mates.Select(m => CommonOnLine(ListPart(_lines.Text(m.Line))))]);
        }

        private PlacedTaxon? OnListLine(ListTaxonRow taxon) {
            _genusLines ??= Kept(ListMemberSource.ListLine).Where(m => m.Taxon.Kind == TaxonKinds.Species)
                .GroupBy(m => m.Taxon.NodeId!.Value)
                .ToDictionary(g => g.Key, TopLines);
            return _genusLines.TryGetValue(taxon.NodeId, out var genus) ? AmongLines(taxon, genus) : null;
        }

        // A new line among these lines, in the order they keep, in the form of its neighbour.
        private PlacedTaxon? AmongLines(ListTaxonRow taxon, LineGroup group) {
            var mates = group.Mates;
            if (mates.Count == 0) {
                return null;
            }
            var (order, index) = Order(group.Scientific, taxon.ScientificName, group.Common, taxon.CommonNameEn);
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
            // the new one ("Carex gynandra" between "Carex grayi" and "Carex hallii"), up to a line
            // taken out in this run.
            if (!before && order != ListOrder.None) {
                var key = order == ListOrder.Scientific ? taxon.ScientificName : taxon.CommonNameEn!;
                var depth = Markers(anchorText[listStart..]);
                var stop = index < mates.Count ? mates[index].Line : _lines.Count + 1;
                for (var l = _lines.LastOfBlock(anchorLine) + 1; l < stop && !_removalOfLine.ContainsKey(l); l = _lines.LastOfBlock(l) + 1) {
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
            var line = NewLine(anchorText[listStart..], taxon, _scope.Style, anchor.HasStatusTemplate || _options.StatusOnLines, _options,
                LinksScientificName(anchor));
            var position = before ? _lines.Start(anchorLine) + listStart : After(_lines.LastOfBlock(anchorLine));
            return new PlacedTaxon(taxon, line, position, anchorName, anchorTaxon, anchorLine, before);
        }

        // A line that links its scientific name with its common name as the label:
        // "*[[Anas bernieri|Bernier's teal]]".
        private bool LinksScientificName(ListMember member) =>
            LeadingCommon().Match(ListPart(_lines.Text(member.Line))) is { Success: true } m && m.Groups["label"].Success
            && string.Equals(m.Groups["target"].Value.Trim().Replace('_', ' '), member.Written, StringComparison.OrdinalIgnoreCase);

        // Where a new line goes after a list line: at its end, past a reference or template that runs
        // on to the next lines ("<ref>{{cite web\n |title=x}}</ref>"), and before the "}}" of a
        // list layout template that ends on the line ("* ''Panthera tigris''}}").
        private int After(int line) {
            var at = _lines.End(line);
            for (var guard = 0; guard < 100 && _scanner.ContainerAt(at, StatusUpdater.ListWrappers) is { } open; guard++) {
                at = _lines.End(_scanner.LineOf(open.End - 1));
            }
            var lineStart = _lines.Start(_scanner.LineOf(at));
            if (_scanner.OuterTemplateAt(lineStart) is { } wrapper && StatusUpdater.ListWrappers.Contains(wrapper.Name)
                && wrapper.Span.End <= at && wrapper.Span.End - 2 > lineStart) {
                at = wrapper.Span.End - 2;
                while (at > lineStart && _text[at - 1] is ' ' or '\t') {
                    at--;
                }
            }
            return at;
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
            var species = Kept(ListMemberSource.ListLine).Where(m => m.Taxon.TaxonId == parentId).OrderBy(m => m.Line).FirstOrDefault();
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
            // In the style of the lines already under the species ("**[[Western lowland gorilla]]"), else
            // of the species' line. The name is written in full, so that the next run finds the taxon
            // by it (an abbreviated "''P. i. stockleyi''" names no taxon on a line of its own).
            var style = (last > species.Line ? StyleOf(_lines.Text(species.Line + 1).TrimStart('*', '#', ' ')) : null)
                ?? StyleOf(speciesText[listStart..].TrimStart('*', '#', ' ')) ?? SpeciesListStyle.ScientificNameFirst;
            var line = SpeciesListLine.Format(GroupList.Entry(taxon), new SpeciesListLineOptions { Style = style, IncludeStatusTemplate = false });
            line = prefix + ListScope.UnlinkLists(line["* ".Length..]);
            if ((species.HasStatusTemplate || _options.StatusOnLines) && taxon.Category is not null) {
                line += " " + StatusTemplate(taxon, _options);
            }
            return new PlacedTaxon(taxon, line, After(last), species.Written, species.Taxon, species.Line, false);
        }
    }

    // The new line: the neighbour's bullet markers and the space after them, the names in the
    // neighbour's style, and {{IUCN status}} when the neighbour has one. linkScientific: the
    // neighbour links its scientific name with its common name as the label, and so does the new line.
    internal static string NewLine(string neighbour, ListTaxonRow taxon, SpeciesListStyle fallback, bool withStatus, ListPlacementOptions options,
        bool linkScientific = false) {
        var prefix = Prefix().Match(neighbour);
        var style = StyleOf(neighbour[prefix.Length..]) ?? fallback;
        var line = SpeciesListLine.Format(GroupList.Entry(taxon), new SpeciesListLineOptions { Style = style, IncludeStatusTemplate = false });
        line = linkScientific && style == SpeciesListStyle.CommonNameOnly && taxon.CommonNameEn is { } common
            ? $"[[{taxon.ScientificName}|{common}]]"
            : ListScope.UnlinkLists(line["* ".Length..]);
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
