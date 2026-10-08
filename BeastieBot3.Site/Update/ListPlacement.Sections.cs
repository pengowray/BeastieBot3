using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Update;

// Missing species put under the headings of a list of list lines (List of endangered birds:
// "==[[Galliformes]]==", "===[[Accipitridae]]==="), when no species of their genus is listed.
// Each section is given the group it lists: the group its heading names that holds at least half its
// taxa, else the deepest group that holds nearly all of them; sections whose groups are not one rank
// are raised to the rank most of them have (a family heading with one species is the family's, not
// the genus's). A species goes among the list lines of the deepest section whose group holds it. When
// that section has sections for groups of one rank below it and none for the species' group of that
// rank, it gets a new section for that group, in the form of the section next to it.
public static partial class ListPlacement {
    /// A heading of the text and the lines under it, up to the next heading of the same level or
    /// higher. The whole text is the section of level 1 with HeadingLine 0.
    private sealed class Section {
        public int Level { get; init; }
        public int HeadingLine { get; init; }
        /// The heading's text as written between the "="s, trimmed; RawTitle untrimmed.
        public string Title { get; init; } = string.Empty;
        public string RawTitle { get; init; } = string.Empty;
        public int LastLine { get; set; }
        public Section? Parent { get; init; }
        public List<Section> Children { get; } = [];
        /// The group the section lists; null for a section that names no taxa on list lines.
        public GroupRow? Group { get; set; }
        /// Group is a group the heading names.
        public bool Named { get; set; }
        /// The rank of the groups of the sections below it, when they have groups below its own.
        public string? ChildRank { get; set; }

        public bool Holds(int line) => line > HeadingLine && line <= LastLine;

        /// The heading as a reader sees it: links and formatting removed.
        public string Plain => PlainTitle(Title);
    }

    private static string PlainTitle(string title) =>
        StatusUpdater.LinkText(TemplateInTitle().Replace(title, string.Empty)).Replace("''", string.Empty, StringComparison.Ordinal).Trim();

    private sealed partial class Placer {
        private Section? _root;
        // The new sections waiting for NewSections: the section they go in and their group, by taxon id.
        private readonly Dictionary<long, (Section Parent, GroupRow Group)> _pendingSections = [];
        private readonly Dictionary<int, IReadOnlyList<GroupRow>> _paths = [];

        private IReadOnlyList<GroupRow> PathOf(int node) => _paths.TryGetValue(node, out var p) ? p : _paths[node] = _lookup.PathOf(node);

        // The members on list lines, those taken out in this run included: they still show what a
        // section lists.
        private List<ListMember>? _lineMembers;
        private List<ListMember> LineMembers => _lineMembers ??= [.. Listed(ListMemberSource.ListLine)];

        private Section Root() {
            if (_root is not null) {
                return _root;
            }
            _root = new Section { Level = 1, HeadingLine = 0, LastLine = _lines.Count, Group = _scope.Scope };
            var open = new Stack<Section>();
            open.Push(_root);
            foreach (Match m in SectionHeading().Matches(_scanner.Masked)) {
                var line = _scanner.LineOf(m.Index);
                var level = m.Groups["eq"].Length;
                while (open.Peek().Level >= level) {
                    open.Pop().LastLine = line - 1;
                }
                var title = m.Groups["title"];
                var raw = _text.Substring(title.Index, title.Length);
                var section = new Section { Level = level, HeadingLine = line, Title = raw.Trim(), RawTitle = raw, Parent = open.Peek(), LastLine = _lines.Count };
                open.Peek().Children.Add(section);
                open.Push(section);
            }
            GiveGroups(_root);
            return _root;
        }

        private bool Below(GroupRow group, GroupRow? above) =>
            above is null || (group.Depth > above.Depth && PathOf(group.NodeId).Any(g => g.NodeId == above.NodeId));

        // The groups of the sections below this one, then of theirs.
        private void GiveGroups(Section section) {
            foreach (var child in section.Children) {
                // A section for the others holds whatever has no section of its own: it is of the
                // group of the section it is in, whatever taxa it has now.
                (child.Group, child.Named) = IsOthers(child) ? (section.Group, false) : GroupOf(child);
            }
            var below = section.Children.Where(c => c.Group is { } g && Below(g, section.Group)).ToList();
            var named = below.Where(c => c.Named).ToList();
            section.ChildRank = (named.Count > 0 ? named : below).GroupBy(c => c.Group!.Rank).MaxBy(g => g.Count())?.Key;
            foreach (var child in below.Where(c => !c.Named && c.Group!.Rank != section.ChildRank)) {
                if (PathOf(child.Group!.NodeId).FirstOrDefault(g => g.Rank == section.ChildRank && Below(g, section.Group)) is { } raised) {
                    child.Group = raised;
                }
            }
            foreach (var child in section.Children) {
                GiveGroups(child);
            }
        }

        private (GroupRow? Group, bool Named) GroupOf(Section section) {
            var taxa = LineMembers.Where(m => section.Holds(m.Line)).Select(m => m.Taxon).DistinctBy(t => t.TaxonId).ToList();
            if (taxa.Count == 0) {
                return (null, false);
            }
            var holding = new Dictionary<int, int>();
            var groups = new Dictionary<int, GroupRow>();
            foreach (var taxon in taxa) {
                foreach (var g in PathOf(taxon.NodeId!.Value)) {
                    holding[g.NodeId] = holding.GetValueOrDefault(g.NodeId) + 1;
                    groups[g.NodeId] = g;
                }
            }
            var names = TitleNames(section.Title);
            var named = groups.Values.Where(g => holding[g.NodeId] * 2 >= taxa.Count && Names(g).Any(names.Contains))
                .OrderByDescending(g => holding[g.NodeId]).ThenByDescending(g => g.Depth).FirstOrDefault();
            if (named is not null) {
                return (named, true);
            }
            return (groups.Values.Where(g => holding[g.NodeId] >= ListScope.ScopeShare * taxa.Count && (!g.IsCol || holding[g.NodeId] == taxa.Count))
                .MaxBy(g => g.Depth), false);
        }

        private static IEnumerable<string> Names(GroupRow g) => new[] { g.Name, g.CommonNameEn, g.EnwikiTitle }.OfType<string>();

        // The names a heading may give its group by: the targets and labels of its links, its text,
        // its text without a bracket at the end and the words in the bracket ("Felidae (cats)"), and
        // each capitalised word ("Order Galliformes").
        private static HashSet<string> TitleNames(string title) {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (Match link in TitleLink().Matches(title)) {
                names.Add(link.Groups["target"].Value.Replace('_', ' ').Trim());
                if (link.Groups["label"].Success) {
                    names.Add(link.Groups["label"].Value.Trim());
                }
            }
            var plain = PlainTitle(title);
            names.Add(plain);
            if (EndBracket().Match(plain) is { Success: true } bracket) {
                names.Add(plain[..bracket.Index].Trim());
                names.Add(bracket.Groups["inner"].Value.Trim());
            }
            foreach (Match word in CapitalisedWord().Matches(plain)) {
                names.Add(word.Value);
            }
            return names;
        }

        // The members on the lines of the section that are not in a section of a group below its own.
        private IEnumerable<ListMember> Direct(Section section, IEnumerable<ListMember> members) =>
            members.Where(m => section.Holds(m.Line)
                && !section.Children.Any(c => c.Group is { } g && Below(g, section.Group) && c.Holds(m.Line)));

        // The section of the deepest group that holds the taxon, looking inside sections whose groups
        // are not below their parent's ("===[[Lemuroidea|Lemurs]]===" with lines of several families
        // and a "====[[Cheirogaleidae]]====" under it). Of sections of the same group, the outermost.
        private Section DeepestFor(IReadOnlyList<GroupRow> path) {
            var onPath = path.Select(g => g.NodeId).ToHashSet();
            Section best = Root();
            var depth = (best.Group?.Depth ?? -1, -best.Level);
            void Visit(Section section) {
                foreach (var child in section.Children) {
                    if (child.Group is { } g && onPath.Contains(g.NodeId) && (g.Depth, -child.Level).CompareTo(depth) > 0) {
                        best = child;
                        depth = (g.Depth, -child.Level);
                    }
                    Visit(child);
                }
            }
            Visit(best);
            return best;
        }

        // A section for the taxa of groups that have no section of their own: "Other Myomorpha species".
        private static bool IsOthers(Section section) =>
            OthersTitle().IsMatch(PlainTitle(section.Title));

        // The section for the others under this one ("====Other microbat species====" under
        // "==[[Chiroptera|Bats]]=="), of a group that holds the taxon, with lines: of the deepest group,
        // then the deepest heading.
        private Section? OthersFor(Section section, IReadOnlyList<GroupRow> path, List<ListMember> lines) {
            var onPath = path.Select(g => g.NodeId).ToHashSet();
            IEnumerable<Section> Under(Section s) => s.Children.SelectMany(c => (IEnumerable<Section>)[c, .. Under(c)]);
            return Under(section).Where(c => IsOthers(c) && (c.Group is null || onPath.Contains(c.Group.NodeId)) && Direct(c, lines).Any())
                .OrderByDescending(c => c.Group?.Depth ?? -1).ThenByDescending(c => c.Level).FirstOrDefault();
        }

        // The members on the section's own lines, outside every section under it.
        private IEnumerable<ListMember> Own(Section section, IEnumerable<ListMember> members) =>
            members.Where(m => section.Holds(m.Line) && !section.Children.Any(c => c.Holds(m.Line)));

        // Lines of taxa of two or more groups of the rank.
        private bool Mixed(IEnumerable<ListMember> members, string rank) =>
            members.Select(m => PathOf(m.Taxon.NodeId!.Value).FirstOrDefault(g => g.Rank == rank)?.NodeId).OfType<int>().Distinct().Skip(1).Any();

        // Only a text whose taxa are mostly on list lines is placed by its headings: in a list of
        // tables, a bullet list in the introduction is not where species go.
        private bool ListedOnLines =>
            LineMembers.Count > 0 && LineMembers.Count * 2 >= _members.Count(m => m.Taxon.InRelease && m.Source != ListMemberSource.Other);

        private PlacedTaxon? InSection(ListTaxonRow taxon) {
            if (!ListedOnLines) {
                return null;
            }
            var path = PathOf(taxon.NodeId);
            var section = DeepestFor(path);
            var lines = Kept(ListMemberSource.ListLine).ToList();
            if (section.ChildRank is { } rank && path.FirstOrDefault(g => g.Rank == rank && Below(g, section.Group)) is { } group) {
                // Its group has no section. It goes in a section for the others ("Other Myomorpha
                // species"), or among the section's own lines when they are of several groups of
                // that rank; else in a new section for its group.
                if (OthersFor(section, path, lines) is { } others) {
                    section = others;
                } else if (!Mixed(Own(section, lines), rank)) {
                    _pendingSections[taxon.TaxonId] = (section, group);
                    return null;
                }
            }
            var heading = section == _root ? null : section.Plain;
            var kept = (section.ChildRank is not null && !IsOthers(section) ? Own(section, lines) : Direct(section, lines)).ToList();
            if (kept.Count > 0) {
                return AmongLines(taxon, TopLines(kept)) is { } placed ? placed with { Heading = heading } : null;
            }
            // Every line of the section is taken out in this run: the new line goes in place of the first.
            var first = Direct(section, LineMembers).OrderBy(m => m.Line).FirstOrDefault();
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
        private HashSet<Section>? _emptying;

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

        private List<PlacedTaxon>? NewSection(Section parent, GroupRow group, List<ListTaxonRow> taxa) {
            _emptying ??= Emptying(null);
            var all = parent.Children.Where(c => c.Group is { } g && Below(g, parent.Group)).ToList();
            var siblings = all.Where(c => !_emptying.Contains(c)).ToList();
            if (siblings.Count == 0) {
                return null;
            }
            // Where: in alphabetical order, or IUCN's order, when the sections keep it; otherwise after
            // the last section of the group's nearest relatives (the groups sharing most of its path).
            Section anchor;
            bool before;
            var index = OrderedPlace([.. siblings.Select(c => (string?)c.Group!.Name)], group.Name)
                ?? (InOrder([.. siblings.Select(c => c.Group!.FirstPos)]) ? siblings.Count(c => c.Group!.FirstPos < group.FirstPos) : null);
            if (index is { } i) {
                before = i < siblings.Count;
                anchor = before ? siblings[i] : siblings[^1];
            } else {
                var path = PathOf(group.NodeId);
                int Shared(Section c) => PathOf(c.Group!.NodeId).Zip(path).TakeWhile(p => p.First.NodeId == p.Second.NodeId).Count();
                var nearest = siblings.Max(Shared);
                anchor = siblings.Last(c => Shared(c) == nearest);
                before = false;
            }
            var (text, ordered) = SectionText(anchor, group, taxa);
            if (text is null) {
                return null;
            }
            var gap = BlankBefore(anchor) ? "\n" : string.Empty;
            var position = before ? _lines.Start(anchor.HeadingLine) : _lines.End(LastContentLine(anchor));
            // Insertions puts a line break after a text that goes before its anchor, and before one that goes after.
            var line = before ? text + gap : gap + text;
            var heading = PlainTitle(NewTitle(anchor, group));
            return [.. ordered.Select((t, k) => new PlacedTaxon(t, k == 0 ? line : string.Empty, position, anchor.Plain, null, anchor.HeadingLine, before) {
                Heading = heading,
                NewHeading = true,
            })];
        }

        // Keys that rise from first to last, one pair in ten aside.
        private static bool InOrder(IReadOnlyList<int> keys) =>
            keys.Zip(keys.Skip(1)).Count(p => p.First >= p.Second) <= (keys.Count - 1) / 10;

        private bool BlankBefore(Section section) =>
            section.HeadingLine > 1 && _lines.Text(section.HeadingLine - 1).Trim().Length == 0;

        private int LastContentLine(Section section) {
            var line = section.LastLine;
            while (line > section.HeadingLine && _lines.Text(line).Trim().Length == 0) {
                line--;
            }
            return line;
        }

        // The new section: a heading like the anchor's, the anchor's {{gray}} line with the group's
        // English name, and a line for each taxon in the form of the anchor's first line, in the
        // anchor's list layout template when it has one.
        private (string? Text, List<ListTaxonRow> Ordered) SectionText(Section anchor, GroupRow group, List<ListTaxonRow> taxa) {
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
            var keys = TopLines(Direct(anchor, LineMembers).Any() ? Direct(anchor, LineMembers) : members);
            var byCommon = OrderedPlace(keys.Scientific, string.Empty) is null && OrderedPlace(keys.Common, string.Empty) is not null
                && taxa.All(t => t.CommonNameEn is not null);
            List<ListTaxonRow> ordered = byCommon
                ? [.. taxa.OrderBy(t => t.CommonNameEn, StringComparer.OrdinalIgnoreCase)]
                : [.. taxa.OrderBy(t => t.ScientificName, StringComparer.OrdinalIgnoreCase)];
            var withStatus = first.HasStatusTemplate || _options.StatusOnLines;
            var links = LinksScientificName(first);
            var lines = ordered.Select(t => NewLine(firstText[listStart..], t, _scope.Style, withStatus, _options, links)).ToList();

            var level = new string('=', anchor.Level);
            var pad = anchor.RawTitle.StartsWith(' ') ? " " : string.Empty;
            var sb = new StringBuilder().Append(level).Append(pad).Append(NewTitle(anchor, group)).Append(pad).Append(level);
            if (anchor.HeadingLine < _lines.Count && GrayLine().Match(_lines.Text(anchor.HeadingLine + 1)) is { Success: true } gray
                && group.CommonNameEn is { } english && !string.Equals(english, group.Name, StringComparison.OrdinalIgnoreCase)) {
                sb.Append('\n').Append("{{").Append(gray.Groups["name"].Value).Append('|').Append(Capitalised(english)).Append("}}");
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

        // The new heading's text: the group's scientific name, linked when the anchor heading has a
        // link ("[[Galliformes]]", "[[Hylobatidae|Gibbons]]"), after the anchor's rank word when it
        // has one ("Order"), with the group's English name in a bracket when the anchor has a bracket.
        // English names in the site are often not the plural form headings use ("Cuckooshrike"), so a
        // heading of English names ("Pigeons and doves") gets the scientific name.
        private static string NewTitle(Section anchor, GroupRow group) {
            var name = !anchor.Title.Contains("[[", StringComparison.Ordinal) ? group.Name
                : group.EnwikiTitle is { } title && !string.Equals(title, group.Name, StringComparison.Ordinal) ? $"[[{title}|{group.Name}]]"
                : $"[[{group.Name}]]";
            var plain = PlainTitle(anchor.Title);
            var rankWord = plain.Split(' ', 2)[0].TrimEnd(':');
            var prefix = anchor.Group is { } g && string.Equals(rankWord, g.Rank, StringComparison.OrdinalIgnoreCase)
                ? anchor.Title[..(anchor.Title.IndexOf(rankWord, StringComparison.OrdinalIgnoreCase) + rankWord.Length)] + (plain.Contains(':') ? ": " : " ")
                : string.Empty;
            var bracket = EndBracket().IsMatch(plain) && group.CommonNameEn is { } common ? $" ({common})" : string.Empty;
            return prefix + name + bracket;
        }

        private static string Capitalised(string text) => text.Length == 0 ? text : char.ToUpperInvariant(text[0]) + text[1..];

        // ------------------------------------------------------------ sections left with no taxa

        // The sections every taxon of which is taken out in this run, whose other text is only
        // templates ({{gray}}, an empty {{columns-list}}), comments and blank lines.
        // insertions: positions where text goes in; a section with one inside it keeps its heading.
        private HashSet<Section> Emptying(IReadOnlyCollection<int>? insertions) {
            var emptied = new HashSet<Section>();
            if (_removalOfLine.Count == 0) {
                return emptied;
            }
            void Visit(Section section) {
                foreach (var child in section.Children) {
                    Visit(child);
                }
                if (section == _root) {
                    return;
                }
                var members = _members.Where(m => m.Taxon.InRelease && section.Holds(m.Line)).ToList();
                if (members.Count == 0 || members.Any(m => !_removalOfLine.ContainsKey(m.Line))) {
                    return;
                }
                var span = SectionSpan(section);
                if (insertions?.Any(p => span.Start < p && p < span.End) == true) {
                    return;
                }
                var body = _text[_lines.Start(section.HeadingLine + 1)..span.End].ToCharArray();
                var offset = _lines.Start(section.HeadingLine + 1);
                IEnumerable<TextSpan> cut = [.. _removalOfLine.Values.Select(r => r.Span), .. section.Children.Where(emptied.Contains).Select(SectionSpan)];
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
            Visit(Root());
            return emptied;
        }

        // From the heading to the start of the next heading (or the end of the text).
        private TextSpan SectionSpan(Section section) =>
            new(_lines.Start(section.HeadingLine), section.LastLine < _lines.Count ? _lines.Start(section.LastLine + 1) : _text.Length);

        /// The sections and {{Species table}}s left with no taxa, with the headings of the sections.
        public (List<TextRemoval> Removals, List<string> Headings) EmptiedSections(IReadOnlyList<PlacedTaxon> placed) {
            var insertions = placed.Where(p => p.Line.Length > 0).Select(p => p.Position).ToList();
            var emptied = Emptying(insertions);
            // The outermost of them: a section inside one taken out goes with it.
            var outer = emptied.Where(s => !emptied.Contains(s.Parent!)).OrderBy(s => s.HeadingLine).ToList();
            List<TextRemoval> removals = [.. outer.Select(s => new TextRemoval(SectionSpan(s), SectionSpan(s)))];
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
            if (placed.Heading is not null || _root is null && SectionHeading().Matches(_scanner.Masked).Count == 0) {
                return placed;
            }
            var at = Root();
            while (at.Children.FirstOrDefault(c => c.Holds(placed.NeighbourLine)) is { } next) {
                at = next;
            }
            return at == _root ? placed : placed with { Heading = at.Plain };
        }
    }

    [GeneratedRegex(@"^(?<eq>={2,6})(?<title>[^=\n]+)\k<eq>[ \t\r]*$", RegexOptions.Multiline)]
    private static partial Regex SectionHeading();

    [GeneratedRegex(@"\{\{[^{}]*\}\}")]
    private static partial Regex TemplateInTitle();

    [GeneratedRegex(@"\[\[(?<target>[^|\]\n]+)(?:\|(?<label>[^\]\n]+))?\]\]")]
    private static partial Regex TitleLink();

    [GeneratedRegex(@"\s*\((?<inner>[^()]*)\)\s*$")]
    private static partial Regex EndBracket();

    [GeneratedRegex(@"\b\p{Lu}\p{Ll}{2,}\b")]
    private static partial Regex CapitalisedWord();

    [GeneratedRegex(@"^\{\{\s*(?<name>gr[ae]y)\s*\|[^{}\n]*\}\}\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex GrayLine();

    [GeneratedRegex(@"^(Other|Others|Miscellaneous|Unplaced)\b", RegexOptions.IgnoreCase)]
    private static partial Regex OthersTitle();

    [GeneratedRegex(@"<!--.*?-->", RegexOptions.Singleline)]
    private static partial Regex Comment();

    [GeneratedRegex(@"\{\{[^{}]*\}\}")]
    private static partial Regex InnerTemplate();
}
