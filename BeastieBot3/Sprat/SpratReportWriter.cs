using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BeastieBot3.Sprat;

// Renders the two Markdown reports that accompany the SPRAT Australia lists:
//   (a) the audit of taxonomy modernizations actually applied to the generated lists, and
//   (b) recommended fixes to the SPRAT/EPBC source data (obsolete orders left for review, non-standard
//       status values, and descriptive names that don't link), each surfaced during generation.

internal static class SpratReportWriter {
    /// <summary>Report (a): the audit of order modernizations applied across the lists.</summary>
    public static string BuildAppliedReport(ModernizationLog log, string datasetVersion, string generatedOn) {
        var sb = new StringBuilder();
        sb.AppendLine("# SPRAT Australia lists — taxonomy modernizations applied");
        sb.AppendLine();
        sb.AppendLine($"Generated {generatedOn} from SPRAT dataset `{datasetVersion}`.");
        sb.AppendLine();

        var changes = log.Changes;
        if (changes.Count == 0) {
            sb.AppendLine("No modernizations were applied.");
            return sb.ToString();
        }

        sb.AppendLine($"{changes.Count} change(s) were applied, rewriting obsolete or misspelled order names to "
            + "their modern form (see `rules/taxon-modernization.yml`). Order names drive the section headings, "
            + "so each change retitles a heading and regroups its taxa.");
        sb.AppendLine();
        sb.AppendLine("| SPRAT order | → Modern | Kind | Taxa | Lists |");
        sb.AppendLine("| --- | --- | --- | ---: | --- |");
        foreach (var g in changes
                     .GroupBy(c => (c.From, c.To, c.Kind))
                     .OrderBy(g => g.Key.From, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(g => g.Key.To, StringComparer.OrdinalIgnoreCase)) {
            var lists = string.Join(", ", g.Select(c => c.Group).Distinct().OrderBy(x => x, StringComparer.OrdinalIgnoreCase));
            sb.AppendLine($"| {g.Key.From} | {g.Key.To} | {g.Key.Kind} | {g.Count()} | {lists} |");
        }
        return sb.ToString();
    }

    /// <summary>Report (b): recommended SPRAT source-data corrections.</summary>
    public static string BuildRecommendationsReport(
        ModernizationLog log,
        IReadOnlyList<FlagOrder> flagOrders,
        IReadOnlyDictionary<string, int> orderCounts,
        IReadOnlyCollection<StatusFinding> statuses,
        IReadOnlyList<DescriptiveNameFinding> redlinks,
        string datasetVersion,
        string generatedOn) {
        var sb = new StringBuilder();
        sb.AppendLine("# SPRAT data — recommended source-data corrections");
        sb.AppendLine();
        sb.AppendLine($"Generated {generatedOn} from SPRAT dataset `{datasetVersion}`. These are suggested fixes to "
            + "the SPRAT/EPBC source data, surfaced while generating the Australia lists. Items in §1 are already "
            + "auto-corrected in the generated output (listed for traceability); §2–§4 are left for manual review.");
        sb.AppendLine();

        AppendAutoModernized(sb, log);
        AppendOrdersForReview(sb, flagOrders, orderCounts);
        AppendStatusValues(sb, statuses);
        AppendRedlinks(sb, redlinks);
        return sb.ToString();
    }

    private static void AppendAutoModernized(StringBuilder sb, ModernizationLog log) {
        sb.AppendLine("## 1. Order names auto-modernized in the lists");
        sb.AppendLine();
        if (log.Changes.Count == 0) {
            sb.AppendLine("_None._");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("Corrected automatically in the generated lists; still worth fixing at source. "
            + "\"Fixed elsewhere?\" = modern authorities already use the corrected name (so the SPRAT value is a "
            + "data artefact) vs. a spelling still carried by the official listing (—  = not yet determined).");
        sb.AppendLine();
        sb.AppendLine("| SPRAT (CSV) | → Modern | Kind | Taxa | Fixed elsewhere? | Note |");
        sb.AppendLine("| --- | --- | --- | ---: | :---: | --- |");
        foreach (var g in log.Changes
                     .GroupBy(c => (c.From, c.To))
                     .OrderBy(g => g.Key.From, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(g => g.Key.To, StringComparer.OrdinalIgnoreCase)) {
            var first = g.First();
            var fixedElsewhere = first.FixedElsewhere switch { true => "yes", false => "no", _ => "—" };
            sb.AppendLine($"| {g.Key.From} | {g.Key.To} | {first.Kind} | {g.Count()} | {fixedElsewhere} | {first.Note ?? ""} |");
        }
        sb.AppendLine();
    }

    private static void AppendOrdersForReview(
        StringBuilder sb, IReadOnlyList<FlagOrder> flagOrders, IReadOnlyDictionary<string, int> orderCounts) {
        sb.AppendLine("## 2. Order names needing review (not auto-changed)");
        sb.AppendLine();
        // Only flag orders that actually appear in the generated data.
        var present = flagOrders
            .Where(f => orderCounts.ContainsKey(f.Order))
            .OrderBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (present.Count == 0) {
            sb.AppendLine("_None present in the current dataset._");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("Obsolete or non-standard order names left unchanged pending a judgement call "
            + "(see `flag_orders` in `rules/taxon-modernization.yml`).");
        sb.AppendLine();
        sb.AppendLine("| SPRAT (CSV) | Suggested | Taxa | Note |");
        sb.AppendLine("| --- | --- | ---: | --- |");
        foreach (var f in present) {
            sb.AppendLine($"| {f.Order} | {f.Suggest ?? ""} | {orderCounts[f.Order]} | {f.Note ?? ""} |");
        }
        sb.AppendLine();
    }

    private static void AppendStatusValues(StringBuilder sb, IReadOnlyCollection<StatusFinding> statuses) {
        sb.AppendLine("## 3. Non-standard status values");
        sb.AppendLine();
        if (statuses.Count == 0) {
            sb.AppendLine("_None — every status cell mapped to a recognised code._");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("Cells that aren't one of the recognised codes (CR/EN/VU/NT/Rare/…) pass through verbatim. "
            + "Verify against the listing instrument and add a code mapping or a citation.");
        sb.AppendLine();
        sb.AppendLine("| System | Value | Example taxon |");
        sb.AppendLine("| --- | --- | --- |");
        foreach (var s in statuses
                     .OrderBy(s => s.System, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(s => s.Value, StringComparer.OrdinalIgnoreCase)) {
            sb.AppendLine($"| {s.System} | {s.Value} | _{s.ExampleTaxon}_ |");
        }
        sb.AppendLine();
    }

    private static void AppendRedlinks(StringBuilder sb, IReadOnlyList<DescriptiveNameFinding> redlinks) {
        sb.AppendLine("## 4. Descriptive non-trinomial names");
        sb.AppendLine();
        var distinct = redlinks
            .GroupBy(r => r.ScientificName, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .OrderBy(r => r.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (distinct.Count == 0) {
            sb.AppendLine("_None._");
            sb.AppendLine();
            return;
        }
        sb.AppendLine("Informal EPBC names that aren't proper trinomials (e.g. a descriptive \"… subspecies\" phrase). "
            + "The list now links them to the binomial and shows the descriptive words as a distinguishing qualifier; "
            + "a proper trinomial at source would resolve them cleanly.");
        sb.AppendLine();
        sb.AppendLine("| SPRAT scientific name | Linked as | List |");
        sb.AppendLine("| --- | --- | --- |");
        foreach (var r in distinct) {
            sb.AppendLine($"| {r.ScientificName} | {r.SuggestedLink} | {r.Group} |");
        }
        sb.AppendLine();
    }
}
