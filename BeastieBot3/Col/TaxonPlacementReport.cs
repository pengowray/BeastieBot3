using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using BeastieBot3.Taxonomy;

// Markdown report for `col build-placement --report`: which Catalogue of Life groups were placed
// between IUCN's ranks, with species counts, which candidate groups were left out and why. Built
// from the builder's diagnostics (PlacementBuildOutput) and the matching statistics.

namespace BeastieBot3.Col;

internal static class TaxonPlacementReport {
    // A taxon counts as split when a CoL group missed the vote but still had at least this share.
    private const double SplitShare = 0.2;

    public static string Build(PlacementRunResult result, string iucnDatabasePath, string colDatabasePath) {
        var output = result.Output ?? throw new ArgumentException("The report needs a build with diagnostics.", nameof(result));
        var matching = result.Matching ?? throw new ArgumentException("The report needs the matching statistics.", nameof(result));
        var options = output.Options;
        var sb = new StringBuilder();

        sb.AppendLine("# Catalogue of Life groups between IUCN ranks");
        sb.AppendLine();
        sb.AppendLine($"Built {Date(result.Source?.BuiltAtUtc ?? DateTime.UtcNow)} from:");
        sb.AppendLine();
        sb.AppendLine($"- IUCN database: `{iucnDatabasePath}`");
        sb.AppendLine($"- Catalogue of Life database: `{colDatabasePath}`");
        sb.AppendLine();
        sb.AppendLine("A Catalogue of Life (CoL) group, such as a suborder or a subfamily, is placed between two IUCN ranks for an IUCN order, family or genus when both of these hold:");
        sb.AppendLine();
        sb.AppendLine($"- **Vote:** at least {Percent(options.VoteThreshold)} of that IUCN taxon's species matched in CoL are in the group.");
        sb.AppendLine($"- **Containment:** at least {Percent(options.ContainmentThreshold)} of the group's matched species are under the same IUCN class (for a group between class and order), order (between order and family) or family (between family and genus).");
        sb.AppendLine();
        sb.AppendLine("Species that CoL does not match take the groups of their IUCN order, family or genus. A group whose CoL rank is not between the two IUCN ranks, such as CoL's order Cetacea inside IUCN's order Artiodactyla, is placed but shown in headings without its rank.");
        sb.AppendLine();

        WriteSummary(sb, output, matching);
        WriteTrees(sb, output, PlacementSpan.ClassToOrder);
        WriteTrees(sb, output, PlacementSpan.OrderToFamily);
        WriteFamilyToGenus(sb, output);
        WriteContainmentDrops(sb, output);
        WriteSplits(sb, output);
        return sb.ToString();
    }

    // ---- summary ----

    private static void WriteSummary(StringBuilder sb, PlacementBuildOutput output, PlacementMatchStats matching) {
        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine("| | Count |");
        sb.AppendLine("|---|---:|");
        foreach (var (label, value, indent) in MatchingRows(matching)) {
            sb.AppendLine($"| {(indent ? "&nbsp;&nbsp;" : "")}{label} | {value} |");
        }
        foreach (var span in Spans) {
            var anchors = output.Anchors.Where(a => a.Anchor.Span == span).ToList();
            sb.AppendLine($"| IUCN {AnchorNoun(span, plural: true)} with CoL groups {SpanPhrase(span)} | {N(anchors.Count(a => a.Kept.Count > 0))} of {N(anchors.Count)} |");
        }
        sb.AppendLine($"| CoL groups left out by the containment rule (see below) | {N(ContainmentDrops(output).Count)} |");
        sb.AppendLine($"| IUCN taxa whose species are split between CoL groups (see below) | {N(Splits(output).Count)} |");
        sb.AppendLine();
    }

    private static readonly (ColMatchKind Kind, string Label)[] FoundKinds = {
        (ColMatchKind.Accepted, "by an accepted CoL name"),
        (ColMatchKind.ProvisionallyAccepted, "by a provisionally accepted CoL name"),
        (ColMatchKind.Synonym, "through a CoL synonym"),
        (ColMatchKind.ScientificName, "by exact scientific name only"),
    };

    /// <summary>The matching summary rows (label, value, indented), shared by the report and the command's table.</summary>
    internal static IEnumerable<(string Label, string Value, bool Indent)> MatchingRows(PlacementMatchStats m) {
        string Share(int count) => $"{N(count)} ({Percent((double)count / Math.Max(1, m.Species), 1)})";
        yield return ("IUCN species (global, species-level assessments)", N(m.Species), false);
        yield return ("Found in CoL", Share(m.Found), false);
        foreach (var (kind, label) in FoundKinds) {
            yield return (label, N(m.ByKind.GetValueOrDefault(kind)), true);
        }
        yield return ("Not found in CoL", N(m.ByKind.GetValueOrDefault(ColMatchKind.NotFound)), false);
        yield return ("Found, but no CoL classification (parent row missing)", N(m.NoClassification), false);
        yield return ("Found, with a CoL classification (used to choose the CoL groups)", Share(m.Matched), false);
        yield return ("of which the classification is incomplete (a higher CoL row is missing)", N(m.CutShort), true);
    }

    // ---- class to order, order to family: one tree per IUCN parent ----

    private static void WriteTrees(StringBuilder sb, PlacementBuildOutput output, PlacementSpan span) {
        var parentNoun = span == PlacementSpan.ClassToOrder ? "class" : "order";
        var parentPlural = span == PlacementSpan.ClassToOrder ? "classes" : "orders";
        var childNoun = AnchorNoun(span, plural: true);
        sb.AppendLine(span == PlacementSpan.ClassToOrder
            ? "## Between class and order"
            : "## Between order and family");
        sb.AppendLine();
        sb.AppendLine($"For each IUCN {parentNoun}, the CoL groups placed {SpanPhrase(span)}, with the number of IUCN species in each group and the IUCN {childNoun} under each group.");
        sb.AppendLine();

        var parents = output.Anchors
            .Where(a => a.Anchor.Span == span)
            .GroupBy(a => ParentKey(a.Anchor))
            .Select(g => (Anchor: g.First().Anchor, Children: g.ToList()))
            .OrderBy(p => p.Anchor.Kingdom, StringComparer.Ordinal)
            .ThenBy(p => p.Anchor.ClassName, StringComparer.Ordinal)
            .ThenBy(p => span == PlacementSpan.OrderToFamily ? p.Anchor.OrderName : string.Empty, StringComparer.Ordinal)
            .ToList();

        var without = new List<string>();
        foreach (var (anchor, children) in parents) {
            var parentLabel = span == PlacementSpan.ClassToOrder
                ? Title(anchor.ClassName)
                : $"{Title(anchor.OrderName)} ({Title(anchor.ClassName)})";
            var species = children.Sum(c => c.Species);
            if (children.All(c => c.Kept.Count == 0)) {
                without.Add($"{parentLabel} {N(species)}");
                continue;
            }
            sb.AppendLine($"### {parentLabel}");
            sb.AppendLine();
            sb.AppendLine($"{N(species)} species in {N(children.Count)} {(children.Count == 1 ? AnchorNoun(span, plural: false) : childNoun)}.");
            sb.AppendLine();
            var root = new TreeNode(null, null);
            foreach (var child in children) {
                var at = root;
                foreach (var kept in child.Kept) {
                    at = at.Child(kept.ColId, kept.Node);
                    at.Species += child.Species;
                }
                at.Ends.Add((ChildLabel(child.Anchor), child.Species));
            }
            foreach (var node in root.Ordered()) {
                WriteTreeNode(sb, node, 0, childNoun);
            }
            if (root.Ends.Count > 0) {
                sb.AppendLine($"- {Capitalize(childNoun)} with no CoL group: {EndList(root.Ends)}");
            }
            sb.AppendLine();
        }

        if (without.Count > 0) {
            sb.AppendLine($"**IUCN {parentPlural} with no CoL group {SpanPhrase(span)}** ({N(without.Count)}, with species counts): {string.Join(", ", without)}.");
            sb.AppendLine();
        }
    }

    private static void WriteTreeNode(StringBuilder sb, TreeNode node, int depth, string childNoun) {
        var indent = new string(' ', depth * 2);
        var line = $"{indent}- **{node.Node!.Name}** ({RankLabel(node.Node)}): {N(node.Species)} species";
        if (node.Ends.Count > 0) {
            line += $". {Capitalize(childNoun)}: {EndList(node.Ends)}";
        }
        sb.AppendLine(line);
        foreach (var child in node.Ordered()) {
            WriteTreeNode(sb, child, depth + 1, childNoun);
        }
    }

    private static string EndList(List<(string Label, int Species)> ends) =>
        string.Join(", ", ends.OrderByDescending(e => e.Species).ThenBy(e => e.Label, StringComparer.Ordinal)
            .Select(e => $"{e.Label} {N(e.Species)}"));

    // ---- family to genus: one table row per IUCN family ----

    private static void WriteFamilyToGenus(StringBuilder sb, PlacementBuildOutput output) {
        sb.AppendLine("## Between family and genus");
        sb.AppendLine();
        sb.AppendLine("IUCN families with CoL groups (subfamilies, tribes and subtribes) between family and genus, largest first. \"Top-level groups\" are the broadest CoL groups used below the family, with the number of IUCN species in each.");
        sb.AppendLine();

        var families = output.Anchors
            .Where(a => a.Anchor.Span == PlacementSpan.FamilyToGenus)
            .GroupBy(a => ParentKey(a.Anchor))
            .Select(g => new {
                g.First().Anchor,
                Species = g.Sum(a => a.Species),
                Genera = g.Count(),
                Placed = g.Count(a => a.Kept.Count > 0),
                Top = g.Where(a => a.Kept.Count > 0)
                    .GroupBy(a => a.Kept[0].ColId)
                    .Select(t => (Name: t.First().Kept[0].Node.Name, Species: t.Sum(a => a.Species)))
                    .OrderByDescending(t => t.Species).ThenBy(t => t.Name, StringComparer.Ordinal)
                    .ToList(),
            })
            .ToList();

        sb.AppendLine("| IUCN family | IUCN order | Species | Genera | Genera with a CoL group | Top-level groups |");
        sb.AppendLine("|---|---|---:|---:|---:|---|");
        foreach (var f in families.Where(f => f.Placed > 0).OrderByDescending(f => f.Species).ThenBy(f => f.Anchor.FamilyName, StringComparer.Ordinal)) {
            const int shown = 12;
            var top = string.Join(", ", f.Top.Take(shown).Select(t => $"{t.Name} {N(t.Species)}"));
            if (f.Top.Count > shown) {
                top += $", and {N(f.Top.Count - shown)} more";
            }
            sb.AppendLine($"| {Title(f.Anchor.FamilyName)} | {Title(f.Anchor.OrderName)} | {N(f.Species)} | {N(f.Genera)} | {N(f.Placed)} | {top} |");
        }
        sb.AppendLine();

        var none = families.Where(f => f.Placed == 0).OrderByDescending(f => f.Species).ToList();
        sb.AppendLine($"**IUCN families with no CoL group between family and genus:** {N(none.Count)} families, {N(none.Sum(f => f.Species))} species. The 40 largest: "
            + string.Join(", ", none.Take(40).Select(f => $"{Title(f.Anchor.FamilyName)} {N(f.Species)}")) + ".");
        sb.AppendLine();
    }

    // ---- groups left out ----

    private sealed record ContainmentDrop(
        PlacementSpan Span, string Parent, string Name, string Rank, int Taxa, int TaxonSpecies, double Containment, int NodeSpecies, string? MainParent, int MainParentSpecies);

    private static List<ContainmentDrop> ContainmentDrops(PlacementBuildOutput output) =>
        output.Anchors
            .SelectMany(a => a.Dropped.Where(d => d.Reason == PlacementDropReason.NotContained).Select(d => (Anchor: a, Drop: d)))
            .GroupBy(x => (x.Anchor.Anchor.Span, Parent: ParentKey(x.Anchor.Anchor), x.Drop.ColId))
            .Select(g => {
                var first = g.First();
                return new ContainmentDrop(
                    g.Key.Span,
                    ParentLabel(first.Anchor.Anchor),
                    first.Drop.Name,
                    first.Drop.ColRank,
                    g.Count(),
                    g.Sum(x => x.Anchor.Species),
                    first.Drop.Containment,
                    first.Drop.NodeSpecies,
                    first.Drop.MainParent,
                    first.Drop.MainParentSpecies);
            })
            .OrderBy(d => d.Span)
            .ThenByDescending(d => d.TaxonSpecies)
            .ThenBy(d => d.Name, StringComparer.Ordinal)
            .ToList();

    private static void WriteContainmentDrops(StringBuilder sb, PlacementBuildOutput output) {
        var drops = ContainmentDrops(output);
        sb.AppendLine("## CoL groups left out by the containment rule");
        sb.AppendLine();
        sb.AppendLine($"These CoL groups won the vote for some IUCN taxa, but fewer than {Percent(output.Options.ContainmentThreshold)} of the group's matched species are under the IUCN parent named here, so the group is not placed there. \"IUCN taxa\" counts the IUCN {AnchorNoun(PlacementSpan.ClassToOrder, true)}, {AnchorNoun(PlacementSpan.OrderToFamily, true)} or {AnchorNoun(PlacementSpan.FamilyToGenus, true)} that lost the group.");
        sb.AppendLine();
        if (drops.Count == 0) {
            sb.AppendLine("None.");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("| Between | Under IUCN | CoL group | IUCN taxa | Their species | Group's species under this IUCN parent | Most of the group is under |");
        sb.AppendLine("|---|---|---|---:|---:|---:|---|");
        foreach (var d in drops) {
            var main = d.MainParent is null ? "" : $"{Title(d.MainParent)} ({N(d.MainParentSpecies)} of {N(d.NodeSpecies)})";
            sb.AppendLine($"| {SpanShort(d.Span)} | {Title(d.Parent)} | {d.Name} ({d.Rank}) | {N(d.Taxa)} | {N(d.TaxonSpecies)} | {Percent(d.Containment)} of {N(d.NodeSpecies)} | {main} |");
        }
        sb.AppendLine();
    }

    private sealed record Split(PlacementSpan Span, string Taxon, string Parent, int Voting, string Groups);

    private static List<Split> Splits(PlacementBuildOutput output) =>
        output.Anchors
            .Where(a => a.Dropped.Any(d => d.Reason == PlacementDropReason.BelowVote && d.Share >= SplitShare))
            .Select(a => new Split(
                a.Anchor.Span,
                ChildLabel(a.Anchor),
                ParentLabel(a.Anchor),
                a.Voting,
                string.Join(", ", a.Dropped
                    .Where(d => d.Reason == PlacementDropReason.BelowVote && d.Share >= 0.05)
                    .OrderByDescending(d => d.Share)
                    .Select(d => $"{d.Name} ({d.ColRank}) {Percent(d.Share)}"))))
            .OrderBy(s => s.Span)
            .ThenByDescending(s => s.Voting)
            .ThenBy(s => s.Taxon, StringComparer.Ordinal)
            .ToList();

    private static void WriteSplits(StringBuilder sb, PlacementBuildOutput output) {
        var splits = Splits(output);
        sb.AppendLine("## IUCN taxa split between CoL groups");
        sb.AppendLine();
        sb.AppendLine($"For these IUCN taxa, at least {Percent(SplitShare)} of the matched species are in a CoL group that fewer than {Percent(output.Options.VoteThreshold)} share, so that group is not placed. The groups listed are those with at least 5% of the matched species; the rest of the species are in other groups or in none.");
        sb.AppendLine();
        if (splits.Count == 0) {
            sb.AppendLine("None.");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("| Between | IUCN taxon | Under IUCN | Matched species | CoL groups and their share of the matched species |");
        sb.AppendLine("|---|---|---|---:|---|");
        foreach (var s in splits) {
            sb.AppendLine($"| {SpanShort(s.Span)} | {s.Taxon} | {Title(s.Parent)} | {N(s.Voting)} | {s.Groups} |");
        }
        sb.AppendLine();
    }

    // ---- labels ----

    private static readonly PlacementSpan[] Spans = { PlacementSpan.ClassToOrder, PlacementSpan.OrderToFamily, PlacementSpan.FamilyToGenus };

    internal static string SpanPhrase(PlacementSpan span) => span switch {
        PlacementSpan.ClassToOrder => "between class and order",
        PlacementSpan.OrderToFamily => "between order and family",
        PlacementSpan.FamilyToGenus => "between family and genus",
        _ => span.ToString(),
    };

    private static string SpanShort(PlacementSpan span) => span switch {
        PlacementSpan.ClassToOrder => "class and order",
        PlacementSpan.OrderToFamily => "order and family",
        PlacementSpan.FamilyToGenus => "family and genus",
        _ => span.ToString(),
    };

    internal static string AnchorNoun(PlacementSpan span, bool plural) => span switch {
        PlacementSpan.ClassToOrder => plural ? "orders" : "order",
        PlacementSpan.OrderToFamily => plural ? "families" : "family",
        PlacementSpan.FamilyToGenus => plural ? "genera" : "genus",
        _ => span.ToString(),
    };

    private static string ParentKey(PlacementAnchor a) => a.Span switch {
        PlacementSpan.ClassToOrder => $"{a.Kingdom}|{a.ClassName}",
        PlacementSpan.OrderToFamily => $"{a.Kingdom}|{a.ClassName}|{a.OrderName}",
        _ => $"{a.Kingdom}|{a.FamilyName}",
    };

    private static string ParentLabel(PlacementAnchor a) => a.Span switch {
        PlacementSpan.ClassToOrder => a.ClassName ?? "",
        PlacementSpan.OrderToFamily => a.OrderName ?? "",
        _ => a.FamilyName ?? "",
    };

    private static string ChildLabel(PlacementAnchor a) => a.Span switch {
        PlacementSpan.ClassToOrder => Title(a.OrderName),
        PlacementSpan.OrderToFamily => Title(a.FamilyName),
        _ => (a.GenusName ?? "").Trim(),
    };

    private static string RankLabel(PlacementNode node) =>
        node.ShowRank ? node.ColRank : $"CoL rank: {node.ColRank}, not shown in headings";

    /// <summary>IUCN stores higher ranks in upper case; the report writes them as names ("Artiodactyla").</summary>
    internal static string Title(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return "(none)";
        }
        var trimmed = name.Trim();
        return trimmed.Length == 1 ? trimmed.ToUpperInvariant() : char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    private static string Capitalize(string text) => text.Length == 0 ? text : char.ToUpperInvariant(text[0]) + text[1..];

    private static string N(int value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static string Percent(double share, int decimals = 0) =>
        (share * 100).ToString("F" + decimals, CultureInfo.InvariantCulture) + "%";

    private static string Date(DateTime utc) => utc.ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture);

    private sealed class TreeNode {
        private readonly Dictionary<string, TreeNode> _children = new(StringComparer.Ordinal);

        public TreeNode(string? colId, PlacementNode? node) {
            ColId = colId;
            Node = node;
        }

        public string? ColId { get; }
        public PlacementNode? Node { get; }
        public int Species { get; set; }
        public List<(string Label, int Species)> Ends { get; } = new();

        public TreeNode Child(string colId, PlacementNode node) {
            if (!_children.TryGetValue(colId, out var child)) {
                child = new TreeNode(colId, node);
                _children[colId] = child;
            }
            return child;
        }

        public IEnumerable<TreeNode> Ordered() =>
            _children.Values.OrderByDescending(c => c.Species).ThenBy(c => c.Node!.Name, StringComparer.Ordinal);
    }
}
