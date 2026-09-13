using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

// The dry run's Markdown summary: what the plan would change on Wikidata, how sure each pairing is,
// and what still has to exist on Wikidata first. The detail (every pair, sample edit payloads) sits
// beside it as CSV and JSON Lines, named in the summary.

namespace BeastieBot3.WikidataEdits;

internal static class WikidataIucnPlanReport {
    private static readonly PlanCategory[] Categories = {
        PlanCategory.StatusChanged, PlanCategory.StatusAdded, PlanCategory.ReferencesOnly,
        PlanCategory.NoChange, PlanCategory.UnmappedCategory,
    };

    private static readonly Dictionary<PlanCategory, string> CategoryNames = new() {
        [PlanCategory.StatusChanged] = "Status changed",
        [PlanCategory.StatusAdded] = "No status on item",
        [PlanCategory.ReferencesOnly] = "Status agrees, references to add",
        [PlanCategory.NoChange] = "Nothing to change",
        [PlanCategory.UnmappedCategory] = "No Wikidata value for the IUCN category",
    };

    private static readonly Dictionary<ConfidenceTier, string> TierNames = new() {
        [ConfidenceTier.A] = "A: IUCN id on item, all checks pass",
        [ConfidenceTier.B] = "B: IUCN id on item, name or rank differs",
        [ConfidenceTier.C] = "C: found by exact name, or id is ambiguous",
        [ConfidenceTier.D] = "D: weak or contradictory link",
    };

    public static string Write(WikidataIucnPlanTally t, WikidataIucnEditConfig config, DateTime generatedUtc,
        string csvFile, string samplesFile, string assessmentSamplesFile) {
        var sb = new StringBuilder();
        string N(long n) => n.ToString("n0", CultureInfo.InvariantCulture);

        sb.AppendLine($"# Wikidata IUCN status dry run: Red List {config.Release}");
        sb.AppendLine();
        sb.AppendLine($"Generated {generatedUtc:yyyy-MM-dd HH:mm} UTC. Nothing was sent to Wikidata.");
        sb.AppendLine();
        sb.AppendLine(config.EditionItemOrNull is { } edition
            ? $"- Release item: {edition}"
            : $"- Release item: **not created yet**. Edits cite a placeholder until `edition_item` is set in rules/wikidata/iucn-status.yml.");
        if (t.OldestItemDownloadUtc is { } oldest && t.NewestItemDownloadUtc is { } newest) {
            sb.AppendLine($"- Wikidata items downloaded between {oldest:yyyy-MM-dd} and {newest:yyyy-MM-dd}. Edits are checked against those revisions, so refresh old items before applying.");
        }
        sb.AppendLine();

        sb.AppendLine("## Coverage");
        sb.AppendLine();
        sb.AppendLine("| | Taxa |");
        sb.AppendLine("|---|---:|");
        sb.AppendLine($"| Species and subspecies with a global assessment | {N(t.TaxaWithGlobalAssessment)} |");
        sb.AppendLine($"| Linked to a Wikidata item | {N(t.TaxaWithGlobalAssessment - t.TaxaWithNoItem)} |");
        sb.AppendLine($"| Linked to more than one item | {N(t.TaxaWithSeveralItems)} |");
        sb.AppendLine($"| No Wikidata item found | {N(t.TaxaWithNoItem)} |");
        sb.AppendLine($"| No global assessment (regional only), left out | {N(t.TaxaWithoutGlobalAssessment)} |");
        sb.AppendLine();
        if (t.ItemsLinkedButNotDownloaded > 0 || t.LinksToTaxaOutsideRelease > 0) {
            sb.AppendLine($"Also left out: {N(t.ItemsLinkedButNotDownloaded)} links to items not downloaded yet, {N(t.LinksToTaxaOutsideRelease)} links to IUCN ids not in {config.Release}.");
            sb.AppendLine();
        }

        sb.AppendLine("## Planned changes by confidence");
        sb.AppendLine();
        sb.AppendLine("Tiers A and B would be edited. C and D are listed for review and never edited without a decision.");
        sb.AppendLine();
        sb.Append("| Tier |");
        foreach (var c in Categories) sb.Append($" {CategoryNames[c]} |");
        sb.AppendLine(" Total |");
        sb.Append("|---|");
        foreach (var _ in Categories) sb.Append("---:|");
        sb.AppendLine("---:|");
        foreach (var tier in Enum.GetValues<ConfidenceTier>()) {
            sb.Append($"| {TierNames[tier]} |");
            long total = 0;
            foreach (var c in Categories) {
                var n = t.Count(tier, c);
                total += n;
                sb.Append($" {N(n)} |");
            }
            sb.AppendLine($" {N(total)} |");
        }
        sb.AppendLine();

        sb.AppendLine("## Edits in detail");
        sb.AppendLine();
        sb.AppendLine($"- IUCN taxon id (P627) to add: {N(t.TaxonIdsToAdd)}");
        sb.AppendLine($"- Renumbered IUCN ids to mark deprecated: {N(t.TaxonIdsToDeprecate)}");
        sb.AppendLine($"- Pairs where the two rank variants differ (status changed): {N(t.VariantsDiffer)}");
        sb.AppendLine($"- Assessment items to create: {N(t.AssessmentItemsToCreate)}; existing assessment items reused: {N(t.AssessmentItemsReused)}");
        if (t.UnmappedCodes.Count > 0) {
            sb.AppendLine($"- IUCN categories with no Wikidata value, left unchanged: {string.Join(", ", t.UnmappedCodes.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key} ({N(kv.Value)})"))}");
        }
        sb.AppendLine();

        sb.AppendLine("## Why pairs need review");
        sb.AppendLine();
        sb.AppendLine("| Flag | Pairs |");
        sb.AppendLine("|---|---:|");
        foreach (var (flag, n) in t.Flags.OrderByDescending(kv => kv.Value)) {
            sb.AppendLine($"| {flag} | {N(n)} |");
        }
        sb.AppendLine();

        sb.AppendLine("## How links were made");
        sb.AppendLine();
        sb.AppendLine("| Source | Pairs |");
        sb.AppendLine("|---|---:|");
        foreach (var (source, n) in t.LinkSources.OrderByDescending(kv => kv.Value)) {
            sb.AppendLine($"| {source} | {N(n)} |");
        }
        sb.AppendLine();

        sb.AppendLine("## Files");
        sb.AppendLine();
        sb.AppendLine($"- `{Path.GetFileName(csvFile)}`: every pair with its tier, flags and planned actions");
        sb.AppendLine($"- `{Path.GetFileName(samplesFile)}`: sample edits as wbeditentity JSON, a few per tier, category and rank variant");
        sb.AppendLine($"- `{Path.GetFileName(assessmentSamplesFile)}`: sample new assessment items");
        return sb.ToString();
    }
}
