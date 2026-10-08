using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

public enum RebuildChange { Kept, Added, Moved }

/// A taxon in the rebuilt list. Heading: the section it is in (links removed), null for a text with
/// no headings; NewHeading: that section is new. OldHeading: for a moved taxon, the section it was in.
public sealed record RebuiltTaxon(ListTaxonRow Taxon, RebuildChange Change, string? Heading, bool NewHeading, string? OldHeading);

/// A section of the old text left out of the rebuilt list because no taxon of the list is in it now.
/// Text: what it had besides its heading, list lines, list layout templates and labels ("" for nothing).
public sealed record DroppedSection(string Heading, string Text);

/// Why a list was not rebuilt.
public enum RebuildRefusal {
    None,
    /// The text lists its taxa mostly in tables, not on list lines.
    NotLineList,
    /// The text lists too little of the group to be a list of it, and the reader did not ask to compare anyway.
    Partial,
    /// The list would have more lines than one Wikipedia page can hold (GroupList.MaxLines).
    TooLong,
}

/// Lines: the taxa in the rebuilt list, with what happened to each. Removed and Kept: the taxa now in
/// another category that the reader ticked, taken out or left in (ListPlacement.Remove).
/// DuplicateLines: lines of a taxon that the text lists twice, left out (the first line stays).
/// OtherLines: list lines that name no taxon of the list, kept in their sections.
public sealed record RebuildResult(string Text, IReadOnlyList<RebuiltTaxon> Taxa) {
    public RebuildRefusal Refusal { get; init; }
    /// How many lines the list would have (for TooLong).
    public int LineCount { get; init; }
    public IReadOnlyList<RemovedTaxon> Removed { get; init; } = [];
    public IReadOnlyList<(ListScopeMember Member, KeptReason Reason)> Kept { get; init; } = [];
    public IReadOnlyList<(StatusTaxon Taxon, int Line)> DuplicateLines { get; init; } = [];
    public IReadOnlyList<string> NewHeadings { get; init; } = [];
    public IReadOnlyList<DroppedSection> Dropped { get; init; } = [];
    public int OtherLines { get; init; }

    public static RebuildResult Refused(string text, RebuildRefusal refusal, int lineCount = 0) =>
        new(text, []) { Refusal = refusal, LineCount = lineCount };
}

/// The choices of a rebuild; null means as the wikitext has it.
/// HeadingRanks: the ranks of the headings, broad to narrow ("order", "family"), in place of the
/// text's own headings (an empty list: no headings). KeepWording: lines already in the list keep their
/// wording (false: every line is written anew, keeping only the lines under it). Style and Sort: of new
/// lines (of every line when KeepWording is off) and of the order of lines. IucnOrder: sections in
/// IUCN's order (alphabetical within each group) in place of the text's. Infra: whether subspecies and
/// varieties are listed, and how.
public sealed record RebuildOptions {
    public IReadOnlyList<string>? HeadingRanks { get; init; }
    public bool KeepWording { get; init; } = true;
    public BeastieBot3.Shared.Wikitext.SpeciesListStyle? Style { get; init; }
    public ListSort? Sort { get; init; }
    public bool IucnOrder { get; init; }
    public InfraMode? Infra { get; init; }
}

/// Rebuilds a list of list lines: every taxon the comparison counts in the group (ListScope.ComparedTaxa)
/// gets a line, and every line is in the section of its group, keeping the text's own arrangement.
/// A listed taxon stays in its section while the section's group holds it; a missing taxon, or one
/// IUCN has moved to another group, goes where ListPlacement would put it: the section of the deepest
/// group that holds it, a section for the others ("Other ... species"), the section's own lines when
/// they are of several groups, or a new section for its group in the form of the section beside it.
/// Each section keeps its heading as written and everything in it that is not a list line (its
/// {{gray}} line, text, images, {{main}}), and its list layout template ({{columns-list}}). Its lines
/// keep their wording, with the status updater's changes; new lines copy the form of the section's
/// lines. Lines are in the order the section keeps (scientific or common name), or the old ones in
/// their old order and the new ones after them. Lines that name no taxon of the list stay where they
/// are. A section with no taxon of the list left is dropped (the result lists it, with its text).
/// The text before and after the list sections, and sections with no taxa (See also, References), stay
/// as they are.
public static partial class ListRebuild {
    public static RebuildResult Rebuild(string text, StatusUpdater updater, IReadOnlyList<ListMember> members, ListScopeResult scope,
        ListPlacementOptions options, IListScopeLookup lookup, RebuildOptions? rebuild = null) {
        if (scope.Partial && scope.Missing is null) {
            return RebuildResult.Refused(text, RebuildRefusal.Partial);
        }
        return new Builder(text, updater, members, scope, options, lookup, rebuild ?? new RebuildOptions()).Build();
    }

    // An entry of a section's list: an old block of lines (a taxon's line with the lines under it, or
    // a line that names no taxon of the list), or a new line. Key: the scientific or common name it
    // sorts by; OldLine: its first line in the old text (int.MaxValue for a new line).
    // Moved: an old block from another section, which does not show the order this section keeps.
    private sealed record Entry(string Text, string? Scientific, string? Common, int OldLine, bool Infra, bool Moved = false);

    // A part of a section's lines: before any label, or after a label such as '''Subspecies'''.
    private sealed class Part {
        public string? Label { get; init; }
        public bool BlankBeforeLabel { get; init; }
        public List<(int First, int Last)> Blocks { get; } = [];
        public string? Opening { get; set; }
        public bool OpeningOnFirstLine { get; set; }
        public string? Closing { get; set; }
        public bool ClosingOnLastLine { get; set; }
        public List<Entry> Entries { get; } = [];
    }

    // The lines of a section outside the sections under it, sorted out.
    private sealed class Body {
        public List<TextSpan> Before { get; } = [];
        public List<TextSpan> After { get; } = [];
        public bool BlankBeforeList { get; set; }
        public bool BlankAfterList { get; set; }
        public bool SpeciesLabel { get; set; }
        public bool SpeciesLabelBlank { get; set; }
        public List<Part> Parts { get; } = [new Part()];
    }

    // A section to put in for a group the text has none for. A class, not a record: it is a key
    // while its Anchor is still being chosen.
    private sealed class NewSection(ListSection parent, GroupRow group) {
        public ListSection Parent { get; } = parent;
        public GroupRow Group { get; } = group;
        public ListSection? Anchor { get; set; }
        public bool Before { get; set; }
    }

    // A section of the rebuilt list: an old section (Old), whose heading and text it keeps, or a new
    // one for Group, in the form of Anchor. Level: its heading level (1 for the whole text).
    private sealed class OutNode {
        public ListSection? Old { get; init; }
        public GroupRow? Group { get; init; }
        public ListSection? Anchor { get; init; }
        public int Level { get; init; }
        public List<OutNode> Children { get; } = [];
        public List<Entry> Entries { get; } = [];
        public bool IsNew => Old is null;
    }

    private sealed class Builder {
        private readonly RebuildOptions _rebuild;
        private readonly string _text;
        private readonly StatusUpdater _updater;
        private readonly ListScopeResult _scope;
        private readonly ListPlacementOptions _options;
        private readonly IListScopeLookup _lookup;
        private readonly ListPlacement.Placer _placer;
        private readonly WikitextScanner _scanner;
        private readonly TextLines _lines;
        private readonly ListSections _sections;
        private readonly List<ListMember> _lineMembers;
        private readonly Dictionary<ListSection, Body> _bodies = [];

        public Builder(string text, StatusUpdater updater, IReadOnlyList<ListMember> members, ListScopeResult scope,
            ListPlacementOptions options, IListScopeLookup lookup, RebuildOptions rebuild) {
            _rebuild = rebuild;
            _text = text;
            _updater = updater;
            _scope = scope;
            _options = options;
            _lookup = lookup;
            _placer = new ListPlacement.Placer(text, members, scope, options, lookup);
            _scanner = new WikitextScanner(text);
            _lines = new TextLines(text);
            _lineMembers = _placer.LineMembers;
            _sections = _placer.Sections;
            _allMembers = members;
        }

        private readonly IReadOnlyList<ListMember> _allMembers;

        // A name as it sorts: without the hybrid sign or a dagger ("Carex × deamii" sorts as "Carex deamii").
        private static string SortKey(string name) =>
            Regex.Replace(name.Replace("×", " ", StringComparison.Ordinal).Replace("†", " ", StringComparison.Ordinal), @"\s+", " ").Trim();

        private static string? Key(string? name) => name is null ? null : SortKey(name);

        public RebuildResult Build() {
            var listedInRelease = _allMembers.Count(m => m.Taxon.InRelease && m.Source != ListMemberSource.Other);
            if (_lineMembers.Count == 0 || _lineMembers.Count * 2 < listedInRelease) {
                return RebuildResult.Refused(_text, RebuildRefusal.NotLineList);
            }
            var (removed, kept) = _placer.Remove([.. _scope.OtherCategory.Where(m => _options.Remove.Contains(m.Taxon.TaxonId))]);
            var removedLines = _placer.RemovalOfLine;
            var removedIds = removed.Select(r => r.Member.Taxon.TaxonId).ToHashSet();
            var stay = _scope.OtherCategory.Select(m => m.Taxon.TaxonId).Where(id => !removedIds.Contains(id)).ToHashSet();
            List<ListTaxonRow> target = [.. ListScope.ComparedTaxa(_scope, _lookup, stay, _rebuild.Infra is { } infraMode ? infraMode != InfraMode.None : null),
                .. _scope.MissingExtra ?? []];
            if (target.Count > GroupList.MaxLines) {
                return RebuildResult.Refused(_text, RebuildRefusal.TooLong, target.Count);
            }
            var targetIds = target.Select(t => t.TaxonId).ToHashSet();

            // The line of each taxon of the list: the first line that writes its own name, else its first
            // line. Another line that writes the same name and is a block of its own is a duplicate,
            // left out unless it defines a reference other lines use; a line that writes another name (a
            // synonym the list keeps as a species of its own) stays where it is.
            var lineOf = new Dictionary<long, int>();
            var duplicates = new List<(StatusTaxon Taxon, int Line)>();
            var dropLines = new HashSet<int>();
            var listed = _lineMembers.Where(m => targetIds.Contains(m.Taxon.TaxonId) && !removedLines.ContainsKey(m.Line)).OrderBy(m => m.Line).ToList();
            foreach (var group in listed.GroupBy(m => m.Taxon.TaxonId)) {
                var chosen = group.FirstOrDefault(m => SameName(m.Written, m.Taxon.ScientificName)) ?? group.First();
                lineOf[group.Key] = chosen.Line;
                foreach (var m in group.Where(m => m.Line != chosen.Line && SameName(m.Written, chosen.Written))) {
                    var outside = m.Line < chosen.Line || m.Line > _lines.LastOfBlock(chosen.Line);
                    if (outside && _lineMembers.Where(o => o.Line == m.Line).All(o => o.Taxon.TaxonId == m.Taxon.TaxonId)
                        && !_placer.DefinesUsedReference(BlockSpan(m.Line, _lines.LastOfBlock(m.Line))) && dropLines.Add(m.Line)) {
                        duplicates.Add((m.Taxon, m.Line));
                    }
                }
            }

            // Each section's body, and the block each list line is in.
            foreach (var section in All(_sections.Root)) {
                _bodies[section] = Read(section);
            }
            var blockOf = new Dictionary<int, int>();
            foreach (var (first, last) in _bodies.Values.SelectMany(b => b.Parts).SelectMany(p => p.Blocks)) {
                for (var l = first; l <= last; l++) {
                    blockOf[l] = first;
                }
            }
            // The taxon each old block is the line of: the first taxon of the list on its first line.
            var owner = new Dictionary<int, long>();
            foreach (var (id, line) in lineOf.OrderBy(p => p.Value)) {
                if (blockOf.GetValueOrDefault(line, line) == line) {
                    owner.TryAdd(line, id);
                }
            }

            // The taxa whose lines go with another taxon's line (a subspecies under its species, two taxa
            // on one line), or stay under a line that names no taxon of the list (Holder null).
            var under = new Dictionary<long, long?>();
            foreach (var taxon in target) {
                if (lineOf.TryGetValue(taxon.TaxonId, out var line)) {
                    var start = blockOf.GetValueOrDefault(line, line);
                    if (owner.TryGetValue(start, out var holder) && holder != taxon.TaxonId) {
                        under[taxon.TaxonId] = holder;
                    } else if (!owner.ContainsKey(start)) {
                        under[taxon.TaxonId] = null;
                    }
                }
            }

            // Old blocks that are neither a taxon's line, removed nor a duplicate name no taxon of the
            // list and stay in their section.
            var otherLines = 0;
            foreach (var (section, body) in _bodies) {
                foreach (var (part, index) in body.Parts.Select((p, i) => (p, i))) {
                    foreach (var (first, last) in part.Blocks) {
                        if (Enumerable.Range(first, last - first + 1).Any(removedLines.ContainsKey) || dropLines.Contains(first) || owner.ContainsKey(first)) {
                            continue;
                        }
                        part.Entries.Add(new Entry(BlockText(first, last), LeadingScientific(first), ListPlacement.CommonOnLine(_lines.Text(first)), first, index > 0));
                        if (section.Group is not null && section != _sections.Root) {
                            otherLines++;
                        }
                    }
                }
            }

            // Where each taxon goes, and the sections of the rebuilt list.
            var home = new Dictionary<long, OutNode>();
            var keptLines = _lineMembers.Where(m => !removedLines.ContainsKey(m.Line)).ToList();
            var (dropped, newHeadings) = _rebuild.HeadingRanks is { } ranks
                ? HomeByRanks(target, under, home, ranks)
                : HomeInText(target, under, home, lineOf, keptLines);

            // Each taxon's line in its section: its old lines, or a new line.
            var reports = new List<(ListTaxonRow Taxon, OutNode Node, int? OldLine)>();
            var underEntries = new Dictionary<long, List<string>>();
            var nested = _rebuild.Infra switch { InfraMode.UnderSpecies => true, InfraMode.Separate => false, _ => NestedInfra() };
            foreach (var taxon in target) {
                if (under.TryGetValue(taxon.TaxonId, out var holder)) {
                    var line = lineOf[taxon.TaxonId];
                    var node = holder is { } h && home.TryGetValue(h, out var hn) ? hn : NodeOfOld(_sections.SectionOf(line));
                    reports.Add((taxon, node, line));
                    continue;
                }
                var where = home[taxon.TaxonId];
                var infra = taxon.Kind != TaxonKinds.Species;
                if (lineOf.TryGetValue(taxon.TaxonId, out var old)) {
                    var member = _lineMembers.First(m => m.Line == old && m.Taxon.TaxonId == taxon.TaxonId);
                    // A line that names the taxon by its article's title ("[[Northern pig-tailed macaque]]")
                    // sorts by the taxon's scientific name.
                    var scientific = StatusUpdater.IsScientificNameShape(member.Written) ? member.Written : taxon.ScientificName;
                    var text = _rebuild.KeepWording ? BlockText(old, _lines.LastOfBlock(old)) : Rewritten(where, taxon, old);
                    where.Entries.Add(new Entry(text, scientific, ListPlacement.CommonOnLine(_lines.Text(old)) ?? taxon.CommonNameEn, old, infra,
                        Moved: where.Old != _sections.SectionOf(old)));
                } else if (infra && nested && taxon.ParentTaxonId is { } parent && lineOf.ContainsKey(parent)) {
                    // A new subspecies or variety goes under its species' line.
                    (underEntries.TryGetValue(parent, out var list) ? list : underEntries[parent] = []).Add(NewInfraLine(taxon, lineOf[parent]));
                } else {
                    where.Entries.Add(new Entry(NewLineFor(where, taxon), taxon.ScientificName, taxon.CommonNameEn, int.MaxValue, infra));
                }
                reports.Add((taxon, where, lineOf.TryGetValue(taxon.TaxonId, out var was) ? was : null));
            }
            _underEntries = underEntries;
            _lineOfOwner = owner;
            var rebuilt = reports.Select(r => Report(r.Taxon, r.Node, r.OldLine)).ToList();

            var output = Render(_root);
            var trailing = _text[_text.TrimEnd('\n', '\r').Length..];
            output = output.TrimEnd('\n', '\r') + trailing;
            if (_text.Contains("\r\n", StringComparison.Ordinal)) {
                output = output.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\n", "\r\n", StringComparison.Ordinal);
            }
            return new RebuildResult(output, rebuilt) {
                Removed = removed,
                Kept = kept,
                DuplicateLines = duplicates,
                NewHeadings = newHeadings,
                Dropped = dropped,
                OtherLines = otherLines,
                LineCount = target.Count,
            };
        }

        // The old section's place in the rebuilt list: its own node, else the whole text's.
        private readonly Dictionary<ListSection, OutNode> _nodeOfOld = [];
        private OutNode NodeOfOld(ListSection section) {
            for (ListSection? at = section; at is not null; at = at.Parent) {
                if (_nodeOfOld.TryGetValue(at, out var node)) {
                    return node;
                }
            }
            return _root;
        }

        // The text's own sections: a taxon stays in its section while the section's group holds it;
        // the others go where ListPlacement would put them, or in new sections. A section with no taxon
        // and no other line left is dropped.
        private (List<DroppedSection> Dropped, List<string> NewHeadings) HomeInText(List<ListTaxonRow> target, Dictionary<long, long?> under,
            Dictionary<long, OutNode> home, Dictionary<long, int> lineOf, List<ListMember> keptLines) {
            var where = new Dictionary<long, object>();
            var newSections = new Dictionary<(ListSection, int), NewSection>();
            foreach (var taxon in target.OrderBy(t => t.Kind == TaxonKinds.Species ? 0 : 1)) {
                if (under.ContainsKey(taxon.TaxonId)) {
                    continue;
                }
                if (lineOf.TryGetValue(taxon.TaxonId, out var line) && Holds(_sections.SectionOf(line), taxon)) {
                    where[taxon.TaxonId] = _sections.SectionOf(line);
                    continue;
                }
                if (taxon.Kind != TaxonKinds.Species && taxon.ParentTaxonId is { } parent && where.TryGetValue(parent, out var parentWhere)) {
                    where[taxon.TaxonId] = parentWhere;
                    continue;
                }
                var placed = Place(taxon, keptLines);
                if (placed is NewSection ns) {
                    var key = (ns.Parent, ns.Group.NodeId);
                    where[taxon.TaxonId] = newSections.TryGetValue(key, out var existing) ? existing : newSections[key] = ns;
                } else {
                    where[taxon.TaxonId] = placed;
                }
            }
            // The taxa under another line stay in that line's section.
            var occupied = new HashSet<ListSection>(where.Values.OfType<ListSection>());
            occupied.UnionWith(newSections.Values.Select(n => n.Parent));
            occupied.UnionWith(_bodies.Where(b => b.Value.Parts.Any(p => p.Entries.Count > 0)).Select(b => b.Key));
            foreach (var (id, _) in under) {
                occupied.Add(_sections.SectionOf(lineOf[id]));
            }
            var dropped = new List<DroppedSection>();
            var droppedSet = new HashSet<ListSection>();
            foreach (var section in _sections.Root.Descendants().Where(s => s.Group is not null)) {
                if (droppedSet.Contains(section.Parent!)) {
                    droppedSet.Add(section);
                } else if (!All(section).Any(occupied.Contains)) {
                    droppedSet.Add(section);
                    dropped.Add(new DroppedSection(section.Plain, OtherText(section, subsections: true)));
                }
            }
            foreach (var ns in newSections.Values) {
                PlaceSection(ns, droppedSet);
            }

            var newNodes = new Dictionary<NewSection, OutNode>();
            OutNode NewNode(NewSection ns) => newNodes[ns] = new OutNode {
                Group = ns.Group, Anchor = ns.Anchor, Level = ns.Anchor?.Level ?? Math.Min(6, ns.Parent.Level + 1),
            };
            OutNode Mirror(ListSection section) {
                var node = new OutNode { Old = section, Group = section.Group, Level = section.Level };
                _nodeOfOld[section] = node;
                var news = newSections.Values.Where(n => n.Parent == section).OrderBy(n => n.Group.Name, StringComparer.OrdinalIgnoreCase).ToList();
                foreach (var child in section.Children) {
                    node.Children.AddRange(news.Where(n => n.Anchor == child && n.Before).Select(NewNode));
                    if (!droppedSet.Contains(child)) {
                        node.Children.Add(Mirror(child));
                    }
                    node.Children.AddRange(news.Where(n => n.Anchor == child && !n.Before).Select(NewNode));
                }
                // A new section with no section beside it goes after the others.
                node.Children.AddRange(news.Where(n => n.Anchor is null || !section.Children.Contains(n.Anchor)).Select(NewNode));
                return node;
            }
            _root = Mirror(_sections.Root);
            if (_rebuild.IucnOrder) {
                IucnOrder(_root);
            }
            foreach (var (id, w) in where) {
                home[id] = w is NewSection ns ? newNodes[ns] : _nodeOfOld[(ListSection)w];
            }
            return (dropped, [.. newNodes.Values.Select(TitleOf)]);
        }

        // Headings for the ranks the reader chose, in IUCN's order. A heading keeps the heading line
        // and text of the text's section of its group, when it has one; the text's other sections of
        // groups are dropped, and their lines that name no taxon of the list go to the heading of the
        // nearest group. Sections of no group (See also, References) stay where they are.
        private (List<DroppedSection> Dropped, List<string> NewHeadings) HomeByRanks(List<ListTaxonRow> target, Dictionary<long, long?> under,
            Dictionary<long, OutNode> home, IReadOnlyList<string> ranks) {
            var root = _sections.Root;
            var groupSections = root.Descendants().Where(s => s.Group is not null).ToList();
            var oldByGroup = groupSections.Where(s => !ListSections.IsOthers(s)).GroupBy(s => s.Group!.NodeId).ToDictionary(g => g.Key, g => g.First());
            var top = _sections.GroupChildren(root).FirstOrDefault()?.Level ?? 2;
            var rootNode = new OutNode { Old = root, Group = root.Group, Level = 1 };
            _nodeOfOld[root] = rootNode;
            var byGroup = new Dictionary<int, OutNode>();
            var groupNodes = new List<OutNode>();
            foreach (var taxon in target.OrderBy(t => t.Kind == TaxonKinds.Species ? 0 : 1)) {
                if (under.ContainsKey(taxon.TaxonId)) {
                    continue;
                }
                if (taxon.Kind != TaxonKinds.Species && taxon.ParentTaxonId is { } parent && home.TryGetValue(parent, out var parentNode)) {
                    home[taxon.TaxonId] = parentNode;
                    continue;
                }
                var path = _sections.PathOf(taxon.NodeId);
                var at = rootNode;
                var level = top;
                foreach (var rank in ranks) {
                    if (path.FirstOrDefault(g => g.Rank == rank) is not { } group) {
                        continue;
                    }
                    if (!byGroup.TryGetValue(group.NodeId, out var node)) {
                        var old = oldByGroup.GetValueOrDefault(group.NodeId);
                        node = new OutNode {
                            Old = old, Group = group, Level = Math.Min(6, level),
                            Anchor = old is null ? groupSections.FirstOrDefault(s => s.Group!.Rank == group.Rank) ?? groupSections.FirstOrDefault() : null,
                        };
                        if (old is not null) {
                            _nodeOfOld[old] = node;
                        }
                        byGroup[group.NodeId] = node;
                        (at == rootNode ? groupNodes : at.Children).Add(node);
                    }
                    at = node;
                    level++;
                }
                home[taxon.TaxonId] = at;
            }
            var top_ = new OutNode { Level = 0 };
            top_.Children.AddRange(groupNodes);
            IucnOrder(top_);
            // The sections of no group before the first section of a group stay before the headings.
            var firstGroup = root.Children.FirstOrDefault(c => c.Group is not null);
            var before = firstGroup is null ? [] : root.Children.TakeWhile(c => c != firstGroup).Where(c => c.Group is null).ToList();
            rootNode.Children.AddRange(before.Select(s => _nodeOfOld[s] = new OutNode { Old = s, Level = s.Level }));
            rootNode.Children.AddRange(top_.Children);
            rootNode.Children.AddRange(root.Children.Where(c => c.Group is null && !before.Contains(c)).Select(s => _nodeOfOld[s] = new OutNode { Old = s, Level = s.Level }));
            _root = rootNode;

            var used = byGroup.Values.Select(n => n.Old).OfType<ListSection>().ToHashSet();
            var dropped = new List<DroppedSection>();
            foreach (var section in groupSections.Where(s => !used.Contains(s))) {
                if (!ListSections.IsOthers(section)) {
                    dropped.Add(new DroppedSection(section.Plain, OtherText(section, subsections: false)));
                }
                // Its lines that name no taxon of the list go to the heading of the nearest group.
                var nearest = _sections.PathOf(section.Group!.NodeId).Reverse().Select(g => byGroup.GetValueOrDefault(g.NodeId)).FirstOrDefault(n => n is not null) ?? rootNode;
                foreach (var part in _bodies[section].Parts) {
                    nearest.Entries.AddRange(part.Entries.Select(e => e with { Moved = true }));
                    part.Entries.Clear();
                }
            }
            return (dropped, [.. byGroup.Values.Where(n => n.IsNew).Select(TitleOf)]);
        }

        // The sections of groups in IUCN's order (alphabetical within each group), at every level;
        // other sections keep their places.
        private static void IucnOrder(OutNode node) {
            var slots = node.Children.Select((c, i) => (c, i)).Where(x => x.c.Group is { } g && g.NodeId != node.Group?.NodeId).ToList();
            var sorted = slots.Select(x => x.c).OrderBy(c => c.Group!.FirstPos).ToList();
            for (var k = 0; k < slots.Count; k++) {
                node.Children[slots[k].i] = sorted[k];
            }
            foreach (var child in node.Children) {
                IucnOrder(child);
            }
        }

        // A line written anew for a listed taxon, with the lines that were under its old line.
        private string Rewritten(OutNode node, ListTaxonRow taxon, int line) {
            var last = _lines.LastOfBlock(line);
            var first = NewLineFor(node, taxon);
            return last > line ? first + "\n" + BlockText(line + 1, last) : first;
        }

        private Dictionary<long, List<string>> _underEntries = [];
        private Dictionary<int, long> _lineOfOwner = [];
        private OutNode _root = new();

        // A taxon whose line is the line of another taxon of the list, or under it (Holder), or under
        // a line that names no taxon of the list (Holder null).
        private sealed record Under(long? Holder);

        private bool Holds(ListSection section, ListTaxonRow taxon) =>
            section.Group is null || _sections.PathOf(taxon.NodeId).Any(g => g.NodeId == section.Group.NodeId);

        // Where ListPlacement would put a taxon: in the section of the line of its genus that sorts just
        // before it (or of the first line of its genus), so that a species goes into the right
        // section of a list in sections by letter ("===C==="); with no line of its genus, in the
        // section of the deepest group that holds it, a section for the others, or a new section.
        private object Place(ListTaxonRow taxon, IReadOnlyList<ListMember> keptLines) {
            if (taxon.Kind == TaxonKinds.Species) {
                var mates = keptLines.Where(m => m.Taxon.NodeId == taxon.NodeId && m.Taxon.Kind == TaxonKinds.Species && m.Taxon.TaxonId != taxon.TaxonId
                    && Holds(_sections.SectionOf(m.Line), taxon)).OrderBy(m => m.Line).ToList();
                if (mates.Count > 0) {
                    var key = SortKey(taxon.ScientificName);
                    var before = mates.LastOrDefault(m => string.Compare(SortKey(m.Written), key, StringComparison.OrdinalIgnoreCase) <= 0) ?? mates[0];
                    return _sections.SectionOf(before.Line);
                }
            }
            var path = _sections.PathOf(taxon.NodeId);
            var section = _sections.DeepestFor(path);
            if (section.ChildRank is { } rank && path.FirstOrDefault(g => g.Rank == rank && _sections.Below(g, section.Group)) is { } group) {
                if (_sections.OthersFor(section, path, keptLines) is { } others) {
                    return others;
                }
                if (!_sections.Mixed(ListSections.Own(section, keptLines), rank)) {
                    return new NewSection(section, group);
                }
            }
            return section;
        }

        // A new section goes among the sections of its parent's subgroups that stay: in alphabetical
        // order or IUCN's when they keep it, else after the last section of its nearest relatives.
        private void PlaceSection(NewSection ns, HashSet<ListSection> dropped) {
            var all = _sections.GroupChildren(ns.Parent).ToList();
            var siblings = all.Where(c => !dropped.Contains(c)).ToList();
            if (siblings.Count == 0) {
                ns.Anchor = all.LastOrDefault();
                ns.Before = false;
                return;
            }
            var index = ListPlacement.OrderedPlace([.. siblings.Select(c => (string?)c.Group!.Name)], ns.Group.Name)
                ?? (Rising([.. siblings.Select(c => c.Group!.FirstPos)]) ? siblings.Count(c => c.Group!.FirstPos < ns.Group.FirstPos) : null);
            if (index is { } i) {
                ns.Before = i < siblings.Count;
                ns.Anchor = ns.Before ? siblings[i] : siblings[^1];
                return;
            }
            var path = _sections.PathOf(ns.Group.NodeId);
            int Shared(ListSection c) => _sections.PathOf(c.Group!.NodeId).Zip(path).TakeWhile(p => p.First.NodeId == p.Second.NodeId).Count();
            var nearest = siblings.Max(Shared);
            ns.Anchor = siblings.Last(c => Shared(c) == nearest);
            ns.Before = false;
        }

        private static bool Rising(IReadOnlyList<int> keys) =>
            keys.Zip(keys.Skip(1)).Count(p => p.First >= p.Second) <= (keys.Count - 1) / 10;

        // The heading of a section of the rebuilt list as a reader sees it.
        private static string TitleOf(OutNode node) =>
            node.Old is { } old ? old.Plain
            : node.Anchor is { } anchor ? ListSections.PlainTitle(ListSections.NewTitle(anchor, node.Group!))
            : GroupList.HeadingText(node.Group!);

        private RebuiltTaxon Report(ListTaxonRow taxon, OutNode node, int? oldLine) {
            var heading = node == _root ? null : TitleOf(node);
            if (oldLine is not { } line) {
                return new RebuiltTaxon(taxon, RebuildChange.Added, heading, node.IsNew, null);
            }
            var oldSection = _sections.SectionOf(line);
            // With headings by rank every section is rebuilt, so a taxon is not reported as moved.
            return node.Old == oldSection || _rebuild.HeadingRanks is not null
                ? new RebuiltTaxon(taxon, RebuildChange.Kept, heading, node.IsNew, null)
                : new RebuiltTaxon(taxon, RebuildChange.Moved, heading, node.IsNew, oldSection == _sections.Root ? null : oldSection.Plain);
        }

        private static IEnumerable<ListSection> All(ListSection section) => [section, .. section.Descendants()];

        private static bool SameName(string a, string b) => string.Equals(SortKey(a), SortKey(b), StringComparison.OrdinalIgnoreCase);

        // ------------------------------------------------------------ reading a section

        // The section's lines before its first subsection, sorted out into text before the list,
        // list blocks (by part), layout template lines, labels and text after the list.
        private Body Read(ListSection section) {
            var body = new Body();
            var first = section.HeadingLine + 1;
            var last = section.Children.Count > 0 ? section.Children[0].HeadingLine - 1 : section.LastLine;
            var part = body.Parts[0];
            var seenList = false;
            var pending = new List<int>();
            void FlushContent(List<TextSpan> into) {
                if (pending.Count > 0) {
                    into.Add(new TextSpan(_lines.Start(pending[0]), _lines.End(pending[^1])));
                    pending.Clear();
                }
            }
            for (var line = first; line <= last; line++) {
                var lineText = _lines.Text(line);
                var listStart = ListPlacement.ListStart(lineText);
                if (listStart >= 0 && !(listStart > 0 && StartsInsideNonWrapper(line))) {
                    if (!seenList) {
                        body.BlankBeforeList = pending.Count > 0 && _lines.Text(pending[^1]).Trim().Length == 0;
                        FlushContent(body.Before);
                        seenList = true;
                    } else {
                        // Text between lists goes after the list.
                        FlushContent(body.After);
                    }
                    var end = Math.Min(_lines.LastOfBlock(line), last);
                    if (part.Blocks.Count == 0) {
                        if (listStart > 0) {
                            part.Opening = lineText[..listStart];
                            part.OpeningOnFirstLine = true;
                        } else if (PreviousLine(line, first) is { } previous && WrapperOpening().IsMatch(_lines.Text(previous))) {
                            part.Opening ??= _lines.Text(previous).TrimEnd('\r');
                        }
                    }
                    part.Blocks.Add((line, end));
                    var contentEnd = _placer.After(end);
                    var rest = _text[contentEnd.._lines.End(_scanner.LineOf(contentEnd))].Trim();
                    if (rest.StartsWith("}}", StringComparison.Ordinal)) {
                        part.Closing = "}}";
                        part.ClosingOnLastLine = true;
                    }
                    line = Math.Max(end, _scanner.LineOf(contentEnd));
                    continue;
                }
                if (seenList && (WrapperOpening().IsMatch(lineText) || WrapperClosing().IsMatch(lineText))) {
                    if (WrapperClosing().IsMatch(lineText) && part.Blocks.Count > 0) {
                        part.Closing ??= lineText.Trim();
                    }
                    FlushContent(body.After);
                    continue;
                }
                if (!seenList && WrapperOpening().IsMatch(lineText) && NextListLine(line, last)) {
                    body.BlankBeforeList = pending.Count > 0 && _lines.Text(pending[^1]).Trim().Length == 0;
                    FlushContent(body.Before);
                    seenList = true;
                    continue;
                }
                if (Label().Match(lineText) is { Success: true } label) {
                    var blank = pending.Count > 0 && _lines.Text(pending[^1]).Trim().Length == 0;
                    if (!seenList) {
                        body.BlankBeforeList = blank;
                        FlushContent(body.Before);
                    } else {
                        FlushContent(body.After);
                    }
                    seenList = true;
                    if (label.Groups["text"].Value.Equals("Species", StringComparison.OrdinalIgnoreCase)) {
                        body.SpeciesLabel = true;
                        body.SpeciesLabelBlank = blank;
                    } else {
                        part = new Part { Label = lineText.Trim(), BlankBeforeLabel = blank };
                        body.Parts.Add(part);
                    }
                    continue;
                }
                pending.Add(line);
            }
            if (seenList) {
                body.BlankAfterList = pending.Count > 0 && _lines.Text(pending[0]).Trim().Length == 0;
                FlushContent(body.After);
            } else {
                FlushContent(body.Before);
            }
            return body;
        }

        // A list marker after a "|" that is not in a list layout template ("| * x" in a table).
        private bool StartsInsideNonWrapper(int line) =>
            _scanner.OuterTemplateAt(_lines.Start(line) + ListPlacement.ListStart(_lines.Text(line))) is not { } outer
            || !StatusUpdater.ListWrappers.Contains(outer.Name);

        private int? PreviousLine(int line, int first) {
            for (var l = line - 1; l >= first; l--) {
                if (_lines.Text(l).Trim().Length > 0) {
                    return l;
                }
            }
            return null;
        }

        private bool NextListLine(int line, int last) {
            for (var l = line + 1; l <= last; l++) {
                var t = _lines.Text(l);
                if (t.Trim().Length > 0) {
                    return ListPlacement.ListStart(t) >= 0;
                }
            }
            return false;
        }

        // The text of a block from its list markers to the end of its content (before a "}}" that
        // closes the list layout template), with the updater's changes.
        private string BlockText(int first, int last) => _updater.TextWithin(BlockSpan(first, last)).Replace("\r", string.Empty, StringComparison.Ordinal);

        private TextSpan BlockSpan(int first, int last) {
            var start = _lines.Start(first) + Math.Max(0, ListPlacement.ListStart(_lines.Text(first)));
            return new TextSpan(start, _placer.After(last));
        }

        private string? LeadingScientific(int line) =>
            ListPlacement.LeadingName().Match(ListPlacement.Placer.ListPart(_lines.Text(line))) is { Success: true } m ? m.Groups["name"].Value : null;

        // What a dropped section had besides its heading, list lines, layout templates, labels and
        // {{gray}} line; with subsections, theirs too.
        private string OtherText(ListSection section, bool subsections) {
            var lines = (subsections ? All(section) : [section]).SelectMany(s => _bodies[s].Before.Concat(_bodies[s].After))
                .SelectMany(span => _text[span.Start..span.End].Replace("\r", string.Empty, StringComparison.Ordinal).Split('\n'))
                .Where(t => t.Trim().Length > 0 && !ListSections.IsGrayLine(t));
            return string.Join("\n", lines);
        }

        // Whether the text writes subspecies and varieties under their species' lines.
        private bool? _nested;
        private bool NestedInfra() {
            if (_nested is { } known) {
                return known;
            }
            var infra = _lineMembers.Where(m => m.Taxon.Kind != TaxonKinds.Species).ToList();
            var under = infra.Count(m => _lineMembers.Any(s => s.Taxon.Kind == TaxonKinds.Species && s.Line < m.Line && m.Line <= _lines.LastOfBlock(s.Line)));
            return (_nested = infra.Count == 0 || under * 2 >= infra.Count).Value;
        }

        // ------------------------------------------------------------ new lines

        // A line for a new taxon in the form of a line of the section it goes in (or of the section
        // beside a new section, or of the text's first list line), or in the chosen style.
        private string NewLineFor(OutNode node, ListTaxonRow taxon) {
            var model = (node.Old ?? node.Anchor) is { } section ? ModelLine(section) : null;
            model ??= _lineMembers.OrderBy(m => m.Line).FirstOrDefault();
            if (model is null) {
                return ListScope.MissingLines([taxon], _rebuild.Style ?? _scope.Style);
            }
            var listPart = ListPlacement.Placer.ListPart(_lines.Text(model.Line));
            var withStatus = model.HasStatusTemplate || _options.StatusOnLines;
            if (_rebuild.Style is { } style) {
                // The markers of the model line; the names in the chosen style.
                return ListPlacement.NewLine(ListPlacement.Prefix().Match(listPart).Value, taxon, style, withStatus, _options);
            }
            return ListPlacement.NewLine(listPart, taxon, _scope.Style, withStatus, _options, _placer.LinksScientificName(model));
        }

        // The first species line of the section or of the sections under it.
        private ListMember? ModelLine(ListSection section) =>
            _lineMembers.Where(m => section.Holds(m.Line) && m.Taxon.Kind == TaxonKinds.Species).OrderBy(m => m.Line).FirstOrDefault();

        // A new subspecies or variety under its species' line, one marker deeper, in the style of the
        // species' line.
        private string NewInfraLine(ListTaxonRow taxon, int speciesLine) {
            var listPart = ListPlacement.Placer.ListPart(_lines.Text(speciesLine));
            var markers = ListPlacement.Prefix().Match(listPart).Value.TrimEnd();
            var line = ListPlacement.NewLine(listPart, taxon, _scope.Style, false, _options);
            return markers + line;
        }

        // ------------------------------------------------------------ writing

        private string Render(OutNode node) {
            if (node != _root && node.Old is { } old && old.Group is null && node.Entries.Count == 0
                && !All(old).Any(s => _lineMembers.Any(m => s.Holds(m.Line)))) {
                // A section with no list lines of taxa stays as it is.
                return _updater.TextWithin(_sections.Span(old)).TrimEnd('\n', '\r', ' ', '\t').Replace("\r", string.Empty, StringComparison.Ordinal);
            }
            var sb = new StringBuilder();
            if (node != _root) {
                sb.Append(HeadingLine(node));
                if (node.IsNew && node.Anchor is { } anchor && _sections.GrayLineFor(anchor, node.Group!) is { } gray) {
                    sb.Append('\n').Append(gray);
                }
            }
            AppendOwn(sb, node);
            foreach (var child in node.Children) {
                if (sb.Length > 0) {
                    sb.Append('\n');
                    if (Gap(child)) {
                        sb.Append('\n');
                    }
                }
                sb.Append(Render(child));
            }
            return sb.ToString();
        }

        // A blank line before a heading: as before that heading in the text, else as before most headings.
        private bool Gap(OutNode node) =>
            node.Old is { } old && _rebuild.HeadingRanks is null ? _sections.BlankBefore(old)
            : node.Anchor is { } anchor && _rebuild.HeadingRanks is null ? _sections.BlankBefore(anchor)
            : _mostHeadingsGap ??= _sections.Root.Descendants().Count(_sections.BlankBefore) * 2 >= _sections.Root.Descendants().Count();

        private bool? _mostHeadingsGap;

        // The heading line: the old one as written (at the node's level when that changed), or a new one
        // in the form of the anchor's, or "== Family Felidae ==" when the text has no headings.
        private string HeadingLine(OutNode node) {
            if (node.Old is { } old) {
                var line = _lines.Text(old.HeadingLine).TrimEnd('\r');
                return node.Level == old.Level ? line : new string('=', node.Level) + old.RawTitle + new string('=', node.Level);
            }
            if (node.Anchor is { } anchor) {
                return ListSections.HeadingLine(anchor, ListSections.NewTitle(anchor, node.Group!), node.Level);
            }
            var marks = new string('=', node.Level);
            return $"{marks} {GroupList.HeadingText(node.Group!)} {marks}";
        }

        // The section's own text: text before the list, the list by parts, text after the list.
        private void AppendOwn(StringBuilder sb, OutNode node) {
            var body = node.Old is { } old ? _bodies[old] : null;
            var parts = body?.Parts.Select(p => (Part: p, Entries: new List<Entry>(p.Entries))).ToList() ?? [(new Part(), new List<Entry>())];
            foreach (var entry in node.Entries) {
                var target = parts[0];
                if (entry.Infra) {
                    var infraPart = parts.Skip(1).FirstOrDefault(p => p.Part.Label is { } l && InfraLabel().IsMatch(l));
                    if (infraPart.Part is null && _rebuild.Infra == InfraMode.Separate) {
                        parts.Add(infraPart = (new Part { Label = "'''Subspecies'''" }, []));
                    }
                    target = infraPart.Part is not null ? infraPart : parts[0];
                }
                target.Entries.Add(entry);
            }
            var model = ModelPart(node);
            var listText = new StringBuilder();
            foreach (var (part, entries) in parts) {
                if (entries.Count == 0) {
                    continue;
                }
                if (part.Label is not null) {
                    if (listText.Length > 0) {
                        listText.Append('\n');
                        if (part.BlankBeforeLabel) {
                            listText.Append('\n');
                        }
                    }
                    listText.Append(part.Label).Append('\n');
                } else if (body?.SpeciesLabel == true) {
                    listText.Append("'''Species'''\n");
                } else if (listText.Length > 0) {
                    listText.Append('\n');
                }
                listText.Append(Wrap(part.Blocks.Count > 0 ? part : model, Sorted(entries)));
            }
            var pieces = new List<string>();
            var before = body is null ? string.Empty : string.Join("\n", body.Before.Select(Within)).Trim('\n');
            if (before.Length > 0) {
                pieces.Add(before + (body!.BlankBeforeList && listText.Length > 0 ? "\n" : string.Empty));
            }
            if (listText.Length > 0) {
                pieces.Add(listText.ToString());
            }
            var after = body is null ? string.Empty : string.Join("\n", body.After.Select(Within)).Trim('\n');
            if (after.Length > 0) {
                pieces.Add((body!.BlankAfterList ? "\n" : string.Empty) + after);
            }
            foreach (var piece in pieces) {
                if (sb.Length > 0) {
                    sb.Append('\n');
                }
                sb.Append(piece);
            }
        }

        // The list layout template new lines go in: of the section's first list; for a new section, of
        // the section its first old line came from, or of the anchor's section or a section under it;
        // else of the text's first list.
        private Part ModelPart(OutNode node) {
            var from = node.Entries.Where(e => e.OldLine != int.MaxValue).Select(e => _sections.SectionOf(e.OldLine)).FirstOrDefault();
            IEnumerable<ListSection> where = node.Old is { } old ? All(old)
                : [.. from is null ? [] : All(from), .. node.Anchor is { } anchor ? All(anchor) : []];
            return where.Concat(All(_sections.Root)).Select(s => _bodies[s].Parts.FirstOrDefault(p => p.Blocks.Count > 0)).FirstOrDefault(p => p is not null)
                ?? new Part();
        }

        private string Within(TextSpan span) => _updater.TextWithin(span).Replace("\r", string.Empty, StringComparison.Ordinal);

        // The entries in the order the old ones keep: by scientific name or by common name when they
        // are in that order, else the old ones in their old order and the others after them by
        // scientific name. When that arrangement is itself in order (many new lines after a few old
        // ones out of order), it is sorted, so that rebuilding the rebuilt list changes nothing. A line
        // with no name to sort by goes after the line it came after (first when it was first). With a
        // chosen order, every line is sorted. New subspecies and varieties go under their species' line.
        private List<string> Sorted(List<Entry> entries) {
            // The old lines of this section; in a new section, the old lines moved into it, which keep
            // the order of the sections they came from.
            var old = entries.Where(e => e.OldLine != int.MaxValue && !e.Moved).OrderBy(e => e.OldLine).ToList();
            if (old.Count == 0) {
                old = [.. entries.Where(e => e.OldLine != int.MaxValue).OrderBy(e => e.OldLine)];
            }
            static string? Scientific(Entry e) => Key(e.Scientific);
            static string? Common(Entry e) => Key(e.Common);
            static string? CommonFirst(Entry e) => Key(e.Common ?? e.Scientific);
            static bool InOrder(IEnumerable<Entry> list, Func<Entry, string?> key) =>
                ListPlacement.OrderedPlace([.. list.Select(key).Where(k => k is not null)], string.Empty) is not null;
            List<Entry> ByKey(Func<Entry, string?> key) {
                var ordered = entries.Where(e => key(e) is not null).OrderBy(key, StringComparer.OrdinalIgnoreCase).ThenBy(e => e.OldLine).ToList();
                foreach (var e in entries.Where(e => key(e) is null).OrderBy(e => e.OldLine)) {
                    var previous = old.LastOrDefault(o => o.OldLine < e.OldLine && ordered.Contains(o));
                    ordered.Insert(previous is null ? 0 : ordered.IndexOf(previous) + 1, e);
                }
                return ordered;
            }
            List<Entry> arranged;
            if (_rebuild.Sort is { } sort) {
                var byScientific = sort == ListSort.ScientificName
                    || (sort == ListSort.FirstName && (_rebuild.Style ?? _scope.Style) == BeastieBot3.Shared.Wikitext.SpeciesListStyle.ScientificNameFirst);
                arranged = ByKey(byScientific ? Scientific : CommonFirst);
            } else if (old.Count == 0 || InOrder(old, Scientific)) {
                arranged = ByKey(Scientific);
            } else if (InOrder(old, Common)) {
                arranged = ByKey(Common);
            } else {
                List<Entry> kept = [.. old, .. entries.Except(old).OrderBy(Scientific, StringComparer.OrdinalIgnoreCase)];
                arranged = InOrder(kept, Scientific) ? ByKey(Scientific) : InOrder(kept, Common) ? ByKey(Common) : kept;
            }
            return [.. arranged.Select(WithUnder)];
        }

        // A kept line with the new subspecies and varieties under it.
        private string WithUnder(Entry entry) =>
            entry.OldLine != int.MaxValue && _lineOfOwner.TryGetValue(entry.OldLine, out var id) && _underEntries.TryGetValue(id, out var under)
                ? entry.Text + "\n" + string.Join("\n", under)
                : entry.Text;

        // The lines in the part's list layout template, as the part (or the model part) writes it.
        private static string Wrap(Part part, List<string> lines) {
            var joined = string.Join("\n", lines);
            if (part.Opening is null) {
                return joined;
            }
            var opening = part.OpeningOnFirstLine ? part.Opening : part.Opening + "\n";
            var closing = part.Closing is null ? string.Empty : part.ClosingOnLastLine ? part.Closing : "\n" + part.Closing;
            return opening + joined + closing;
        }
    }

    // A line that opens a list layout template and has no list line on it: "{{columns-list|colwidth=30em|",
    // "{{div col|colwidth=22em}}".
    [GeneratedRegex(@"^\s*\{\{\s*(?:columns-list|col-list|collist|column-list|div col(?: list)?|multicol|plainlist|plain list|flatlist|flat list|refbegin)\b[^\n]*$", RegexOptions.IgnoreCase)]
    private static partial Regex WrapperOpening();

    // A line that closes one: "}}", "{{div col end}}".
    [GeneratedRegex(@"^\s*(?:\}\}|\{\{\s*(?:div col end|end div col|colend|col-end|multicol-end|endplainlist|endflatlist|refend)\s*\}\})\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex WrapperClosing();

    // A bold label before a list: '''Species''', '''Subspecies''', ;Subspecies.
    [GeneratedRegex(@"^\s*(?:'''|;\s*)(?<text>Species|Subspecies|Varieties|Subspecies and varieties|Subpopulations)(?:''')?\s*:?\s*$", RegexOptions.IgnoreCase)]
    private static partial Regex Label();

    [GeneratedRegex(@"Subspecies|Varieties", RegexOptions.IgnoreCase)]
    private static partial Regex InfraLabel();
}
