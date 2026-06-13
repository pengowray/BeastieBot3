using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static BeastieBot3.WikipediaLists.ProseFormat;

namespace BeastieBot3.WikipediaLists;

// Renders the presentation blocks of a parent (nested) list: the per-child status summary wikitable
// + its legend, the per-child count-sentence helpers, and the plain "Related lists" see-also block.
// Pure markup — no database, no tree rendering. Extracted from WikipediaListGenerator (R2 carve-up);
// the parent-section orchestration (which scans the breakdown and recurses into children) stays in
// the generator and calls into here. Import with `using static` to keep call sites terse.
internal static class ParentSummaryTableBuilder {
    // Soft highlight tints for the critically-endangered family columns (PE/PEW/CR) and the CR-total.
    private const string CrCellStyle = "style=\"background:#fce8e6\"|";
    private const string CrTotalStyle = "style=\"background:#f8d7da\"|";

    private static bool IsCrFamily(string code) =>
        code is "CR" or "CR(PE)" or "CR(PEW)";

    // Header tooltip text for a status entry (pure CR gets an explicit exclusion note).
    private static string StatusTooltip(ChartStatusEntry e) =>
        e.Code == "CR" ? "Critically Endangered (excluding possibly-extinct)" : e.Label;

    /// <summary>
    /// Build the per-child summary wikitable: a single sortable header row (so column sorting works),
    /// the full EX..DD breakdown that sums to Total, then a derived <em>CR total</em> column placed at
    /// the far right so it never disturbs the additive columns. PE/PEW/CR cells are tinted; a status-code
    /// legend (the pie-chart replacement) follows the table. Curated children first, then the remaining
    /// sub-taxa by descending total, with a pinned Total row.
    /// </summary>
    public static string BuildChildSummaryTable(
        Dictionary<string, IReadOnlyList<StatusCount>> breakdown,
        IReadOnlyList<string> orderedCuratedKeys,
        IReadOnlyDictionary<string, ChildListLink> linkByValue) {

        var entries = ChartStatusOrder.Entries;
        var sb = new StringBuilder();
        sb.AppendLine("{| class=\"wikitable sortable\"");

        // Single header row: abbr tooltips on every code, CR family tinted; Total, then CR total last.
        var header = new StringBuilder("! Group");
        foreach (var e in entries) {
            var hl = IsCrFamily(e.Code) ? CrCellStyle : "";
            header.Append($" !! {hl}{{{{abbr|{e.Code}|{StatusTooltip(e)}}}}}");
        }
        header.Append(" !! Total");
        header.Append($" !! {CrTotalStyle}{{{{abbr|CR total|All critically endangered, including CR(PE) and CR(PEW)}}}}");
        sb.AppendLine(header.ToString());

        var totals = new int[entries.Count];
        var grandTotal = 0;
        var grandCr = 0;
        var emitted = new HashSet<string>(StringComparer.Ordinal);

        void EmitRow(string key, IReadOnlyList<StatusCount> row) {
            sb.AppendLine("|-");
            var label = linkByValue.TryGetValue(key, out var link)
                ? $"[[{link.WikiTitle}|{link.DisplayName}]]"
                : ToTitleCase(key);
            var cells = new StringBuilder($"| {label}");
            var rowTotal = 0;
            var rowCr = 0;
            for (var i = 0; i < entries.Count; i++) {
                var c = row[i].Count;
                var hl = IsCrFamily(entries[i].Code) ? CrCellStyle : "";
                cells.Append($" || {hl}{c}");
                totals[i] += c;
                rowTotal += c;
                if (IsCrFamily(entries[i].Code)) rowCr += c;
            }
            cells.Append($" || {rowTotal}");
            cells.Append($" || {CrTotalStyle}{rowCr}");
            sb.AppendLine(cells.ToString());
            grandTotal += rowTotal;
            grandCr += rowCr;
        }

        foreach (var key in orderedCuratedKeys) {
            if (breakdown.TryGetValue(key, out var row) && emitted.Add(key)) EmitRow(key, row);
        }
        foreach (var kv in breakdown
            .Where(k => !emitted.Contains(k.Key))
            .OrderByDescending(k => k.Value.Sum(s => s.Count))
            .ThenBy(k => k.Key, StringComparer.Ordinal)) {
            EmitRow(kv.Key, kv.Value);
            emitted.Add(kv.Key);
        }

        sb.AppendLine("|- class=\"sortbottom\"");
        var totalCells = new StringBuilder("! Total");
        for (var i = 0; i < entries.Count; i++) {
            var hl = IsCrFamily(entries[i].Code) ? CrCellStyle : "";
            totalCells.Append($" !! {hl}{totals[i]}");
        }
        totalCells.Append($" !! {grandTotal}");
        totalCells.Append($" !! {CrTotalStyle}{grandCr}");
        sb.AppendLine(totalCells.ToString());
        sb.AppendLine("|}");
        sb.Append(BuildStatusLegend());
        return sb.ToString();
    }

    /// <summary>
    /// Reader-facing key to the status columns — the explanatory text that replaces the old pie charts.
    /// </summary>
    private static string BuildStatusLegend() {
        return
            "''Conservation status'' ([[IUCN Red List]]): " +
            "[[Extinct]] (EX), [[Extinct in the wild]] (EW), " +
            "[[Critically endangered]] (CR) — shown split into possibly extinct (PE), " +
            "possibly extinct in the wild (PEW), and other critically endangered; " +
            "[[Endangered species|Endangered]] (EN), [[Vulnerable species|Vulnerable]] (VU), " +
            "[[Near-threatened species|Near threatened]] (NT), [[Least-concern species|Least concern]] (LC), " +
            "[[Data deficient]] (DD). " +
            "The category columns (EX–DD) add up to ''Total''; the ''CR total'' column on the right " +
            "is the sum of CR, CR(PE) and CR(PEW).";
    }

    /// <summary>Sum a child's breakdown row over the codes this section covers (e.g. CR+CR(PE)+CR(PEW) for a CR list).</summary>
    public static int SectionStatusTotal(IReadOnlyList<StatusCount> row, IReadOnlyCollection<string> sectionStatusCodes) {
        var set = new HashSet<string>(sectionStatusCodes, StringComparer.OrdinalIgnoreCase);
        return row.Where(sc => set.Contains(sc.Code)).Sum(sc => sc.Count);
    }

    /// <summary>Count for one status code in a breakdown row (0 if absent).</summary>
    public static int RowCount(IReadOnlyList<StatusCount> row, string code) =>
        row.FirstOrDefault(sc => sc.Code == code)?.Count ?? 0;

    /// <summary>
    /// A trailing ", including N possibly extinct [and M possibly extinct in the wild]" clause for a
    /// child count sentence. Empty when both counts are zero; omits whichever of PE/PEW is zero.
    /// </summary>
    public static string PossiblyExtinctClause(int pe, int pew) {
        if (pe > 0 && pew > 0)
            return $", including {NewspaperNumber(pe)} ''possibly extinct'' and {NewspaperNumber(pew)} ''possibly extinct in the wild''";
        if (pe > 0)
            return $", including {NewspaperNumber(pe)} ''possibly extinct''";
        if (pew > 0)
            return $", including {NewspaperNumber(pew)} ''possibly extinct in the wild''";
        return string.Empty;
    }

    /// <summary>Plain "Related lists" bullet block for non-phylogenetic see-also cross-references.</summary>
    public static string BuildRelatedListsBlock(IReadOnlyList<ChildListLink> seeAlso) {
        var sb = new StringBuilder();
        sb.AppendLine("== Related lists ==");
        foreach (var link in seeAlso) {
            sb.AppendLine($"* [[{link.WikiTitle}|{link.DisplayName}]]");
        }
        return sb.ToString().TrimEnd();
    }
}
