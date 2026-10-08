using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// A heading of the text and the lines under it, up to the next heading of the same level or
/// higher. The whole text is the section of level 1 with HeadingLine 0.
internal sealed class ListSection {
    public int Level { get; init; }
    public int HeadingLine { get; init; }
    /// The heading's text as written between the "="s, trimmed; RawTitle untrimmed.
    public string Title { get; init; } = string.Empty;
    public string RawTitle { get; init; } = string.Empty;
    public int LastLine { get; set; }
    public ListSection? Parent { get; init; }
    public List<ListSection> Children { get; } = [];
    /// The group the section lists; null for a section that names no taxa on list lines.
    public GroupRow? Group { get; set; }
    /// Group is a group the heading names.
    public bool Named { get; set; }
    /// The rank of the groups of the sections below it, when they have groups below its own.
    public string? ChildRank { get; set; }

    public bool Holds(int line) => line > HeadingLine && line <= LastLine;

    /// The heading as a reader sees it: links and formatting removed.
    public string Plain => ListSections.PlainTitle(Title);

    /// The sections under this one, at any depth, in text order.
    public IEnumerable<ListSection> Descendants() => Children.SelectMany(c => (IEnumerable<ListSection>)[c, .. c.Descendants()]);
}

/// The headings of a list of list lines (List of endangered birds: "==[[Galliformes]]==",
/// "===[[Accipitridae]]===") and the group each section lists: the group its heading names that
/// holds at least half its taxa, else the deepest group that holds nearly all of them. Sections
/// beside each other whose groups are of different ranks are raised to the rank most of them have (a
/// family heading with one species is the family's, not the genus's; "== Landfowl ==" among order
/// headings is the order's). A section titled "Other ..." is of its parent's group. Used to put
/// missing species under their headings (ListPlacement) and to rebuild a list (ListRebuild).
internal sealed partial class ListSections {
    private readonly string _text;
    private readonly WikitextScanner _scanner;
    private readonly TextLines _lines;
    private readonly IReadOnlyList<ListMember> _lineMembers;
    private readonly IListScopeLookup _lookup;
    private readonly Dictionary<int, IReadOnlyList<GroupRow>> _paths = [];

    /// lineMembers: the listed taxa on list lines, those taken out in a run included (they still
    /// show what a section lists). scope: the group the whole text is compared with.
    public ListSections(string text, WikitextScanner scanner, TextLines lines, IReadOnlyList<ListMember> lineMembers, GroupRow scope,
        IListScopeLookup lookup) {
        _text = text;
        _scanner = scanner;
        _lines = lines;
        _lineMembers = lineMembers;
        _lookup = lookup;
        Root = new ListSection { Level = 1, HeadingLine = 0, LastLine = lines.Count, Group = scope };
        var open = new Stack<ListSection>();
        open.Push(Root);
        foreach (Match m in Heading().Matches(scanner.Masked)) {
            var line = scanner.LineOf(m.Index);
            var level = m.Groups["eq"].Length;
            while (open.Peek().Level >= level) {
                open.Pop().LastLine = line - 1;
            }
            var title = m.Groups["title"];
            var raw = text.Substring(title.Index, title.Length);
            var section = new ListSection { Level = level, HeadingLine = line, Title = raw.Trim(), RawTitle = raw, Parent = open.Peek(), LastLine = lines.Count };
            open.Peek().Children.Add(section);
            open.Push(section);
        }
        GiveGroups(Root);
    }

    public ListSection Root { get; }

    public IReadOnlyList<GroupRow> PathOf(int node) => _paths.TryGetValue(node, out var p) ? p : _paths[node] = _lookup.PathOf(node);

    /// Whether group is below the group above (any group is below null).
    public bool Below(GroupRow group, GroupRow? above) =>
        above is null || (group.Depth > above.Depth && PathOf(group.NodeId).Any(g => g.NodeId == above.NodeId));

    /// The sections under this one whose groups are below its group: the sections of its subgroups.
    public IEnumerable<ListSection> GroupChildren(ListSection section) =>
        section.Children.Where(c => c.Group is { } g && Below(g, section.Group));

    // The groups of the sections below this one, then of theirs.
    private void GiveGroups(ListSection section) {
        foreach (var child in section.Children) {
            // A section for the others holds whatever has no section of its own: it is of the
            // group of the section it is in, whatever taxa it has now.
            (child.Group, child.Named) = IsOthers(child) ? (section.Group, false) : GroupOf(child);
        }
        var below = GroupChildren(section).ToList();
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

    private (GroupRow? Group, bool Named) GroupOf(ListSection section) {
        var taxa = _lineMembers.Where(m => section.Holds(m.Line)).Select(m => m.Taxon).DistinctBy(t => t.TaxonId).ToList();
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

    /// The members on the lines of the section that are not in a section of a group below its own.
    public IEnumerable<ListMember> Direct(ListSection section, IEnumerable<ListMember> members) =>
        members.Where(m => section.Holds(m.Line) && !GroupChildren(section).Any(c => c.Holds(m.Line)));

    /// The members on the section's own lines, outside every section under it.
    public static IEnumerable<ListMember> Own(ListSection section, IEnumerable<ListMember> members) =>
        members.Where(m => section.Holds(m.Line) && !section.Children.Any(c => c.Holds(m.Line)));

    /// The section of the deepest group that holds the taxon, looking inside sections whose groups
    /// are not below their parent's ("===[[Lemuroidea|Lemurs]]===" with lines of several families
    /// and a "====[[Cheirogaleidae]]====" under it). Of sections of the same group, the outermost.
    public ListSection DeepestFor(IReadOnlyList<GroupRow> path) {
        var onPath = path.Select(g => g.NodeId).ToHashSet();
        var best = Root;
        var depth = (best.Group?.Depth ?? -1, -best.Level);
        foreach (var section in Root.Descendants()) {
            if (section.Group is { } g && onPath.Contains(g.NodeId) && (g.Depth, -section.Level).CompareTo(depth) > 0) {
                best = section;
                depth = (g.Depth, -section.Level);
            }
        }
        return best;
    }

    /// A section for the taxa of groups that have no section of their own: "Other Myomorpha species".
    public static bool IsOthers(ListSection section) =>
        OthersTitle().IsMatch(PlainTitle(section.Title));

    /// The section for the others under this one ("====Other microbat species====" under
    /// "==[[Chiroptera|Bats]]=="), of a group that holds the taxon, with lines: of the deepest group,
    /// then the deepest heading.
    public ListSection? OthersFor(ListSection section, IReadOnlyList<GroupRow> path, IReadOnlyList<ListMember> lines) {
        var onPath = path.Select(g => g.NodeId).ToHashSet();
        return section.Descendants().Where(c => IsOthers(c) && (c.Group is null || onPath.Contains(c.Group.NodeId)) && Direct(c, lines).Any())
            .OrderByDescending(c => c.Group?.Depth ?? -1).ThenByDescending(c => c.Level).FirstOrDefault();
    }

    /// Whether these lines are of taxa of two or more groups of the rank.
    public bool Mixed(IEnumerable<ListMember> members, string rank) =>
        members.Select(m => PathOf(m.Taxon.NodeId!.Value).FirstOrDefault(g => g.Rank == rank)?.NodeId).OfType<int>().Distinct().Skip(1).Any();

    /// From the heading to the start of the next heading (or the end of the text).
    public TextSpan Span(ListSection section) =>
        new(_lines.Start(section.HeadingLine), section.LastLine < _lines.Count ? _lines.Start(section.LastLine + 1) : _text.Length);

    /// Whether a blank line comes before the section's heading.
    public bool BlankBefore(ListSection section) =>
        section.HeadingLine > 1 && _lines.Text(section.HeadingLine - 1).Trim().Length == 0;

    /// The section's last line that is not blank.
    public int LastContentLine(ListSection section) {
        var line = section.LastLine;
        while (line > section.HeadingLine && _lines.Text(line).Trim().Length == 0) {
            line--;
        }
        return line;
    }

    /// The deepest section whose lines include the line (the whole text when no heading does).
    public ListSection SectionOf(int line) {
        var at = Root;
        while (at.Children.FirstOrDefault(c => c.Holds(line)) is { } next) {
            at = next;
        }
        return at;
    }

    /// The heading as a reader sees it: links, templates and italics removed.
    public static string PlainTitle(string title) =>
        StatusUpdater.LinkText(TemplateInTitle().Replace(title, string.Empty)).Replace("''", string.Empty, StringComparison.Ordinal).Trim();

    /// A heading for the group in the form of the anchor heading: the group's scientific name, linked
    /// when the anchor heading has a link ("[[Galliformes]]", "[[Hylobatidae|Gibbons]]"), after the
    /// anchor's rank word when it has one ("Order"), with the group's English name in a bracket when
    /// the anchor has a bracket. English names in the site are often not the plural form headings use
    /// ("Cuckooshrike"), so a heading of English names ("Pigeons and doves") gets the scientific name.
    public static string NewTitle(ListSection anchor, GroupRow group) {
        var name = !anchor.Title.Contains("[[", StringComparison.Ordinal) ? group.Name
            : group.EnwikiTitle is { } title && !string.Equals(title, group.Name, StringComparison.Ordinal) ? $"[[{title}|{group.Name}]]"
            : $"[[{group.Name}]]";
        var plain = PlainTitle(anchor.Title);
        var rankWord = plain.Split(' ', 2)[0].TrimEnd(':');
        var prefix = anchor.Group is { } g && string.Equals(rankWord, g.Rank, StringComparison.OrdinalIgnoreCase)
            ? anchor.Title[..(anchor.Title.IndexOf(rankWord, StringComparison.OrdinalIgnoreCase) + rankWord.Length)] + (plain.Contains(':') ? ": " : " ")
            : string.Empty;
        var bracket = EndBracket().IsMatch(plain) && GroupList.HasSentenceName(group) && group.CommonNameEn is { } common ? $" ({common})" : string.Empty;
        return prefix + name + bracket;
    }

    /// The whole heading line for a new section in the form of the anchor's: its level and the
    /// spaces inside its "="s.
    public static string HeadingLine(ListSection anchor, string title, int? level = null) {
        var marks = new string('=', level ?? anchor.Level);
        var pad = anchor.RawTitle.StartsWith(' ') ? " " : string.Empty;
        return marks + pad + title + pad + marks;
    }

    /// The "{{gray|English name}}" line after a heading, when the anchor heading has one and the group
    /// has an English name for a heading: one from the rules files, which give plurals ("Cuckoo-shrikes");
    /// a name from a Wikipedia article's title is singular ("Hummingbird").
    public string? GrayLineFor(ListSection anchor, GroupRow group) =>
        anchor.HeadingLine > 0 && anchor.HeadingLine < _lines.Count && GrayLine().Match(_lines.Text(anchor.HeadingLine + 1)) is { Success: true } gray
        && GroupList.HasSentenceName(group) && group.CommonNameEn is { } english && !string.Equals(english, group.Name, StringComparison.OrdinalIgnoreCase)
            ? "{{" + gray.Groups["name"].Value + "|" + Capitalised(english) + "}}"
            : null;

    /// Whether a line is a {{gray}} line.
    public static bool IsGrayLine(string line) => GrayLine().IsMatch(line);

    private static string Capitalised(string text) => text.Length == 0 ? text : char.ToUpperInvariant(text[0]) + text[1..];

    [GeneratedRegex(@"^(?<eq>={2,6})(?<title>[^=\n]+)\k<eq>[ \t\r]*$", RegexOptions.Multiline)]
    internal static partial Regex Heading();

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
}
