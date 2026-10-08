using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Update;

// Missing species put under the headings of a list of list lines (List of endangered birds:
// "==[[Galliformes]]==", "===[[Accipitridae]]==="), when no species of their genus is listed. The
// sections and their groups are ListSections. A species goes among the list lines of the deepest
// section whose group holds it. When that section has sections for groups of one rank below it and
// none for the species' group of that rank, it goes in a section for the others ("Other Myomorpha
// species"), or among the section's own lines when they are of several groups of that rank, or in a
// new section for its group, in the form of the section next to it.
public static partial class ListPlacement {
    private sealed partial class Placer {
        // The new sections waiting for NewSections: the section they go in and their group, by taxon id.
        private readonly Dictionary<long, (ListSection Parent, GroupRow Group)> _pendingSections = [];

        // The members on list lines, those taken out in this run included: they still show what a
        // section lists.
        private List<ListMember>? _lineMembers;
        private List<ListMember> LineMembers => _lineMembers ??= [.. Listed(ListMemberSource.ListLine)];

        private ListSections? _sections;
        private ListSections Sections => _sections ??= new ListSections(_text, _scanner, _lines, LineMembers, _scope.Scope, _lookup);

        private IReadOnlyList<GroupRow> PathOf(int node) => Sections.PathOf(node);

        // Only a text whose taxa are mostly on list lines is placed by its headings: in a list of
        // tables, a bullet list in the introduction is not where species go.
        private bool ListedOnLines =>
            LineMembers.Count > 0 && LineMembers.Count * 2 >= _members.Count(m => m.Taxon.InRelease && m.Source != ListMemberSource.Other);

        private PlacedTaxon? InSection(ListTaxonRow taxon) {
            if (!ListedOnLines) {
                return null;
            }
            var path = PathOf(taxon.NodeId);
            var section = Sections.DeepestFor(path);
            var lines = Kept(ListMemberSource.ListLine).ToList();
            if (section.ChildRank is { } rank && path.FirstOrDefault(g => g.Rank == rank && Sections.Below(g, section.Group)) is { } group) {
                // Its group has no section. It goes in a section for the others ("Other Myomorpha
                // species"), or among the section's own lines when they are of several groups of
                // that rank; else in a new section for its group.
                if (Sections.OthersFor(section, path, lines) is { } others) {
                    section = others;
                } else if (!Sections.Mixed(ListSections.Own(section, lines), rank)) {
                    _pendingSections[taxon.TaxonId] = (section, group);
                    return null;
                }
            }
            var heading = section == Sections.Root ? null : section.Plain;
            var kept = (section.ChildRank is not null && !ListSections.IsOthers(section) ? ListSections.Own(section, lines) : Sections.Direct(section, lines)).ToList();
            if (kept.Count > 0) {
                return AmongLines(taxon, TopLines(kept)) is { } placed ? placed with { Heading = heading } : null;
            }
            // Every line of the section is taken out in this run: the new line goes in place of the first.
            var first = Sections.Direct(section, LineMembers).OrderBy(m => m.Line).FirstOrDefault();
            if (first is null || !_removalOfLine.TryGetValue(first.Line, out var removal)) {
                return null;
            }
            var text = _lines.Text(first.Line);
            var listStart = ListStart(text);
            var line = NewLine(text[listStart..], taxon, _scope.Style, first.HasStatusTemplate || _options.StatusOnLines, _options, LinksScientificName(first));
            var at = _lines.Start(first.Line) + listStart;
            var before = removal.Span.Start == at;
            return new PlacedTaxon(taxon, line, before ? at : removal.Span.Start, first.Written, null, first.Line, before) { Heading = heading };
        }

        // ------------------------------------------------------------ new sections

        // The sections that EmptiedSections will take out unless a new line goes in them.
        private HashSet<ListSection>? _emptying;

        /// The taxa InSection left for a new section: one new section for each group, with its taxa.
        public List<PlacedTaxon> NewSections(IReadOnlyList<ListTaxonRow> rest) {
            var placed = new List<PlacedTaxon>();
            // New sections that go in at one place keep the order of Placed: by group name.
            foreach (var group in rest.Where(t => _pendingSections.ContainsKey(t.TaxonId))
                         .GroupBy(t => (_pendingSections[t.TaxonId].Parent, _pendingSections[t.TaxonId].Group.NodeId))
                         .OrderBy(g => _pendingSections[g.First().TaxonId].Group.Name, StringComparer.OrdinalIgnoreCase)) {
                var (parent, newGroup) = _pendingSections[group.First().TaxonId];
                if (NewSection(parent, newGroup, [.. group]) is { } section) {
                    placed.AddRange(section);
                }
            }
            return placed;
        }

        private List<PlacedTaxon>? NewSection(ListSection parent, GroupRow group, List<ListTaxonRow> taxa) {
            _emptying ??= Emptying(null);
            var all = parent.Children.Where(c => c.Group is { } g && Sections.Below(g, parent.Group)).ToList();
            var siblings = all.Where(c => !_emptying.Contains(c)).ToList();
            if (siblings.Count == 0) {
                return null;
            }
            // Where: in alphabetical order, or IUCN's order, when the sections keep it; otherwise after
            // the last section of the group's nearest relatives (the groups sharing most of its path).
            ListSection anchor;
            bool before;
            var index = OrderedPlace([.. siblings.Select(c => (string?)c.Group!.Name)], group.Name)
                ?? (InOrder([.. siblings.Select(c => c.Group!.FirstPos)]) ? siblings.Count(c => c.Group!.FirstPos < group.FirstPos) : null);
            if (index is { } i) {
                before = i < siblings.Count;
                anchor = before ? siblings[i] : siblings[^1];
            } else {
                var path = PathOf(group.NodeId);
                int Shared(ListSection c) => PathOf(c.Group!.NodeId).Zip(path).TakeWhile(p => p.First.NodeId == p.Second.NodeId).Count();
                var nearest = siblings.Max(Shared);
                anchor = siblings.Last(c => Shared(c) == nearest);
                before = false;
            }
            var (text, ordered) = SectionText(anchor, group, taxa);
            if (text is null) {
                return null;
            }
            var gap = Sections.BlankBefore(anchor) ? "\n" : string.Empty;
            var position = before ? _lines.Start(anchor.HeadingLine) : _lines.End(Sections.LastContentLine(anchor));
            // Insertions puts a line break after a text that goes before its anchor, and before one that goes after.
            var line = before ? text + gap : gap + text;
            var heading = ListSections.PlainTitle(ListSections.NewTitle(anchor, group));
            return [.. ordered.Select((t, k) => new PlacedTaxon(t, k == 0 ? line : string.Empty, position, anchor.Plain, null, anchor.HeadingLine, before) {
                Heading = heading,
                NewHeading = true,
            })];
        }

        // Keys that rise from first to last, one pair in ten aside.
        private static bool InOrder(IReadOnlyList<int> keys) =>
            keys.Zip(keys.Skip(1)).Count(p => p.First >= p.Second) <= (keys.Count - 1) / 10;

        // The new section: a heading like the anchor's, the anchor's {{gray}} line with the group's
        // English name, and a line for each taxon in the form of the anchor's first line, in the
        // anchor's list layout template when it has one.
        private (string? Text, List<ListTaxonRow> Ordered) SectionText(ListSection anchor, GroupRow group, List<ListTaxonRow> taxa) {
            var members = LineMembers.Where(m => anchor.Holds(m.Line)).OrderBy(m => m.Line).ToList();
            if (members.Count == 0) {
                return (null, taxa);
            }
            var first = members[0];
            var firstText = _lines.Text(first.Line);
            var listStart = ListStart(firstText);
            if (listStart < 0) {
                return (null, taxa);
            }
            var keys = TopLines(Sections.Direct(anchor, LineMembers).Any() ? Sections.Direct(anchor, LineMembers) : members);
            var byCommon = OrderedPlace(keys.Scientific, string.Empty) is null && OrderedPlace(keys.Common, string.Empty) is not null
                && taxa.All(t => t.CommonNameEn is not null);
            List<ListTaxonRow> ordered = byCommon
                ? [.. taxa.OrderBy(t => t.CommonNameEn, StringComparer.OrdinalIgnoreCase)]
                : [.. taxa.OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)];
            var withStatus = first.HasStatusTemplate || _options.StatusOnLines;
            var links = LinksScientificName(first);
            var lines = ordered.Select(t => NewLine(firstText[listStart..], t, _scope.Style, withStatus, _options, links)).ToList();

            var sb = new StringBuilder(ListSections.HeadingLine(anchor, ListSections.NewTitle(anchor, group)));
            if (Sections.GrayLineFor(anchor, group) is { } gray) {
                sb.Append('\n').Append(gray);
            }
            sb.Append('\n');
            var lineStart = _lines.Start(first.Line);
            if (_scanner.OuterTemplateAt(lineStart + Math.Max(listStart, 1)) is { } wrapper && StatusUpdater.ListWrappers.Contains(wrapper.Name)
                && wrapper.Span.Start >= _lines.Start(anchor.HeadingLine)) {
                // "{{columns-list|colwidth=30em|" up to the first list line, and "}}" where the
                // anchor's template has it: on a line of its own or after the last list line.
                var wrapperLine = _scanner.LineOf(wrapper.Span.Start);
                var sameLine = wrapperLine == first.Line;
                var opening = sameLine ? _text[wrapper.Span.Start..(lineStart + listStart)] : _text[wrapper.Span.Start.._lines.End(wrapperLine)];
                var close = wrapper.Span.End - 3;
                while (close > wrapper.Span.Start && _text[close] is ' ' or '\t' or '\r') {
                    close--;
                }
                var closeOwnLine = _text[close] == '\n';
                sb.Append(opening).Append(sameLine ? string.Empty : "\n").Append(string.Join("\n", lines)).Append(closeOwnLine ? "\n}}" : "}}");
            } else {
                sb.Append(string.Join("\n", lines));
            }
            return (sb.ToString().Replace("\r", string.Empty, StringComparison.Ordinal), ordered);
        }

        // ------------------------------------------------------------ sections left with no taxa

        // The sections every taxon of which is taken out in this run, whose other text is only
        // templates ({{gray}}, an empty {{columns-list}}), comments and blank lines.
        // insertions: positions where text goes in; a section with one inside it keeps its heading.
        private HashSet<ListSection> Emptying(IReadOnlyCollection<int>? insertions) {
            var emptied = new HashSet<ListSection>();
            if (_removalOfLine.Count == 0) {
                return emptied;
            }
            void Visit(ListSection section) {
                foreach (var child in section.Children) {
                    Visit(child);
                }
                if (section == Sections.Root) {
                    return;
                }
                var members = _members.Where(m => m.Taxon.InRelease && section.Holds(m.Line)).ToList();
                if (members.Count == 0 || members.Any(m => !_removalOfLine.ContainsKey(m.Line))) {
                    return;
                }
                var span = Sections.Span(section);
                if (insertions?.Any(p => span.Start < p && p < span.End) == true) {
                    return;
                }
                var body = _text[_lines.Start(section.HeadingLine + 1)..span.End].ToCharArray();
                var offset = _lines.Start(section.HeadingLine + 1);
                IEnumerable<TextSpan> cut = [.. _removalOfLine.Values.Select(r => r.Span), .. section.Children.Where(emptied.Contains).Select(Sections.Span)];
                foreach (var c in cut) {
                    for (var i = Math.Max(c.Start, offset); i < Math.Min(c.End, span.End); i++) {
                        body[i - offset] = ' ';
                    }
                }
                var left = Comment().Replace(new string(body), string.Empty);
                for (string before; (before = left) != (left = InnerTemplate().Replace(left, string.Empty));) {
                }
                if (left.Trim().Length == 0) {
                    emptied.Add(section);
                }
            }
            Visit(Sections.Root);
            return emptied;
        }

        /// The sections and {{Species table}}s left with no taxa, with the headings of the sections.
        public (List<TextRemoval> Removals, List<string> Headings) EmptiedSections(IReadOnlyList<PlacedTaxon> placed) {
            var insertions = placed.Where(p => p.Line.Length > 0).Select(p => p.Position).ToList();
            var emptied = Emptying(insertions);
            // The outermost of them: a section inside one taken out goes with it.
            var outer = emptied.Where(s => !emptied.Contains(s.Parent!)).OrderBy(s => s.HeadingLine).ToList();
            List<TextRemoval> removals = [.. outer.Select(s => new TextRemoval(Sections.Span(s), Sections.Span(s)))];
            foreach (var table in GenusTables().Where(t => t.End is not null && t.Rows.Count > 0 && t.Rows.All(_removedRows.Contains))) {
                var span = new TextSpan(_lines.Start(_scanner.LineOf(table.Header.Span.Start)), table.End!.Span.End);
                var endLine = _scanner.LineOf(table.End.Span.End - 1);
                if (string.IsNullOrWhiteSpace(_text[table.End.Span.End.._lines.End(endLine)])) {
                    span = span with { End = endLine < _lines.Count ? _lines.Start(endLine + 1) : _text.Length };
                }
                if (!insertions.Any(p => span.Start < p && p < span.End) && !removals.Any(r => r.Span.Start <= span.Start && span.End <= r.Span.End)) {
                    removals.Add(new TextRemoval(span, span));
                }
            }
            return (removals, [.. outer.Select(s => s.Plain)]);
        }

        /// The placed taxon with the heading of the section its neighbour is in, when it has none.
        public PlacedTaxon WithHeading(PlacedTaxon placed) {
            if (placed.Heading is not null || _sections is null && !ListSections.Heading().IsMatch(_scanner.Masked)) {
                return placed;
            }
            var at = Sections.SectionOf(placed.NeighbourLine);
            return at == Sections.Root ? placed : placed with { Heading = at.Plain };
        }
    }

    [GeneratedRegex(@"<!--.*?-->", RegexOptions.Singleline)]
    private static partial Regex Comment();

    [GeneratedRegex(@"\{\{[^{}]*\}\}")]
    private static partial Regex InnerTemplate();
}
