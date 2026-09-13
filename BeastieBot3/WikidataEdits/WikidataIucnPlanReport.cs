using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

// The dry run's Markdown summary: what the plan would change on Wikidata, how sure each pairing is,
// and what still has to exist on Wikidata first. The detail (every pair, sample edit payloads) sits
// beside it as CSV and JSON Lines, named in the summary.
//
// Order: the answer first (what would be edited, how sure, what is still needed), then coverage,
// the tier-by-category table, the flags that explain the tiers, and the files.

namespace BeastieBot3.WikidataEdits;

internal static class WikidataIucnPlanReport {
    private static readonly PlanCategory[] Categories = {
        PlanCategory.StatusChanged, PlanCategory.StatusAdded, PlanCategory.ReferencesOnly,
        PlanCategory.NoChange, PlanCategory.UnmappedCategory,
    };

    private static readonly PlanCategory[] EditCategories = {
        PlanCategory.StatusChanged, PlanCategory.StatusAdded, PlanCategory.ReferencesOnly,
    };

    private static readonly ConfidenceTier[] EditedTiers = { ConfidenceTier.A, ConfidenceTier.B };
    private static readonly ConfidenceTier[] ReviewTiers = { ConfidenceTier.C, ConfidenceTier.D };

    private static readonly Dictionary<PlanCategory, string> CategoryNames = new() {
        [PlanCategory.StatusChanged] = "Status changed",
        [PlanCategory.StatusAdded] = "Status added (item had none)",
        [PlanCategory.ReferencesOnly] = "References added, status already agrees",
        [PlanCategory.NoChange] = "Nothing to change",
        [PlanCategory.UnmappedCategory] = "Left unchanged: IUCN category has no P141 value",
    };

    // Tier definitions, shown as a legend above the table. Keep in step with TaxonLinkClassifier.
    private static readonly Dictionary<ConfidenceTier, string> TierNames = new() {
        [ConfidenceTier.A] = "**A**, would be edited: IUCN taxon id (P627) on this item and no other; name and rank agree.",
        [ConfidenceTier.B] = "**B**, would be edited, flagged: P627 on this item and no other, but the name differs or the item has no rank.",
        [ConfidenceTier.C] = "**C**, a person confirms the match first: found by exact name search with no P627 on the item, or the id is on several items.",
        [ConfidenceTier.D] = "**D**, review only: found by synonym, label or cached name; rank does not match; conflicting or deprecated ids; item linked to several taxa.",
    };

    // Readable labels for the flag codes the tally stores (LinkFlag names). Whether a flag lowers the
    // tier or is information only mirrors the two halves of the LinkFlag enum; see TaxonLinkClassifier.
    private static readonly Dictionary<string, (string Label, bool LowersTier)> FlagLabels = new(StringComparer.Ordinal) {
        [nameof(LinkFlag.NameDiffers)] = ("Item's taxon name (P225) differs from the IUCN name", true),
        [nameof(LinkFlag.TaxonIdDeprecated)] = ("The item's IUCN id for this taxon is at deprecated rank", true),
        [nameof(LinkFlag.RankMissing)] = ("Item has no taxon rank (P105)", true),
        [nameof(LinkFlag.RankMismatch)] = ("Item's taxon rank is not the assessed rank (species vs subspecies)", true),
        [nameof(LinkFlag.NotTaxonItem)] = ("Item is not a taxon (its instance of is something else)", true),
        [nameof(LinkFlag.TaxonIdOnSeveralItems)] = ("This IUCN id is on more than one item", true),
        [nameof(LinkFlag.ItemHasSeveralTaxonIds)] = ("Item has more than one IUCN id, this one included", true),
        [nameof(LinkFlag.ItemHasOtherCurrentTaxonId)] = ("Item has another IUCN id that is current in this release", true),
        [nameof(LinkFlag.ItemLinkedToSeveralTaxa)] = ("Item is linked to more than one IUCN taxon", true),
        [nameof(LinkFlag.FoundByNameSearch)] = ("Found by exact name search; no IUCN id on the item", true),
        [nameof(LinkFlag.FoundBySynonym)] = ("Found by exact match on a synonym", true),
        [nameof(LinkFlag.FoundByCachedName)] = ("Name matched an item already in the local cache, without searching Wikidata", true),
        [nameof(LinkFlag.FoundByLabel)] = ("Found by English label only", true),
        [nameof(LinkFlag.ItemHasRenumberedTaxonId)] = ("Item also has an IUCN id not in this release (renumbered; the plan marks it deprecated)", false),
        [nameof(LinkFlag.ReferenceCitesOtherTaxonId)] = ("A reference on the item's status cites a different IUCN id", false),
        [nameof(LinkFlag.PossiblyExtinct)] = ("Assessment is CR possibly extinct (PE or PEW); written as CR", false),
        [nameof(LinkFlag.LegacyCategory)] = ("IUCN 2.3 category (LR/nt, LR/lc or LR/cd)", false),
    };

    // Readable labels for the link source codes (LinkSource names).
    private static readonly Dictionary<string, string> LinkSourceLabels = new(StringComparer.Ordinal) {
        [nameof(LinkSource.P627Claim)] = "IUCN taxon id (P627) on the item",
        [nameof(LinkSource.SearchTaxonName)] = "Search: exact match on the taxon name (P225)",
        [nameof(LinkSource.SearchTaxonNameSynonym)] = "Search: exact match on a synonym (IUCN or Catalogue of Life)",
        [nameof(LinkSource.CachedName)] = "Name matched an item already in the local cache, no search",
        [nameof(LinkSource.Label)] = "Search: English label",
    };

    public static string Write(WikidataIucnPlanTally t, WikidataIucnEditConfig config, DateTime generatedUtc,
        string csvFile, string samplesFile, string assessmentSamplesFile) {
        var sb = new StringBuilder();
        string N(long n) => n.ToString("n0", CultureInfo.InvariantCulture);
        long Sum(IEnumerable<ConfidenceTier> tiers, IEnumerable<PlanCategory> categories) =>
            tiers.Sum(tier => categories.Sum(c => t.Count(tier, c)));

        sb.AppendLine($"# Wikidata IUCN status dry run: Red List {config.Release}");
        sb.AppendLine();
        sb.AppendLine($"Generated {generatedUtc:yyyy-MM-dd HH:mm} UTC from the local caches. Nothing was sent to Wikidata.");
        sb.AppendLine();

        // ---- The answer first
        sb.AppendLine("## Summary");
        sb.AppendLine();
        var edited = Sum(EditedTiers, EditCategories);
        var review = Sum(ReviewTiers, EditCategories);
        sb.AppendLine($"- **Would be edited (tiers A and B): {N(edited)} pairs.** " +
            $"{N(Sum(EditedTiers, new[] { PlanCategory.StatusChanged }))} status changed, " +
            $"{N(Sum(EditedTiers, new[] { PlanCategory.StatusAdded }))} status added, " +
            $"{N(Sum(EditedTiers, new[] { PlanCategory.ReferencesOnly }))} references added to a status that already agrees.");
        sb.AppendLine($"- **Need a person to confirm the match first (tiers C and D): {N(review)} pairs.**");
        sb.AppendLine($"- Nothing to change: {N(Sum(Enum.GetValues<ConfidenceTier>(), new[] { PlanCategory.NoChange }))} pairs. " +
            "Left unchanged because the IUCN category has no P141 value: " +
            (t.UnmappedCodes.Count > 0
                ? $"{string.Join(", ", t.UnmappedCodes.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key} {N(kv.Value)}"))} pairs."
                : "0 pairs."));
        if (t.ReviewConfirmed > 0 || t.ReviewRejected > 0) {
            sb.AppendLine($"- Review decisions applied: {N(t.ReviewConfirmed)} confirmed, {N(t.ReviewRejected)} rejected (rejected pairs are left out of the plan).");
        }
        sb.AppendLine();

        sb.AppendLine("## Needed before editing");
        sb.AppendLine();
        sb.AppendLine(config.EditionItemOrNull is { } edition
            ? $"- Release item for {config.Release}: {edition}."
            : $"- Release item for {config.Release}: **not on Wikidata yet**. Edits cite a placeholder until `edition_item` is set in rules/wikidata/iucn-status.yml.");
        sb.AppendLine($"- Assessment items: {N(t.AssessmentItemsToCreate)} would be created, {N(t.AssessmentItemsReused)} existing items reused.");
        if (t.OldestItemDownloadUtc is { } oldest && t.NewestItemDownloadUtc is { } newest) {
            sb.AppendLine($"- Item copies: downloaded between {oldest:yyyy-MM-dd} and {newest:yyyy-MM-dd}; {N(t.StaleItems)} linked items were over {WikidataIucnPlanTally.StaleAfterDays} days old at the dry run. " +
                "Wikidata refuses an edit to an item that changed after the copy it was planned on, so re-download old items before applying.");
        }
        sb.AppendLine();

        // ---- Coverage
        sb.AppendLine("## Coverage");
        sb.AppendLine();
        sb.AppendLine($"| IUCN {config.Release} | Taxa |");
        sb.AppendLine("|---|---:|");
        sb.AppendLine($"| Species and subspecies with a global assessment | {N(t.TaxaWithGlobalAssessment)} |");
        sb.AppendLine($"| Linked to a Wikidata item | {N(t.TaxaWithGlobalAssessment - t.TaxaWithNoItem)} |");
        sb.AppendLine($"| Of those, linked to more than one item | {N(t.TaxaWithSeveralItems)} |");
        sb.AppendLine($"| No Wikidata item found | {N(t.TaxaWithNoItem)} |");
        sb.AppendLine($"| Left out: regional assessments only, no global one | {N(t.TaxaWithoutGlobalAssessment)} |");
        sb.AppendLine($"| Left out: varieties and subpopulations | {N(t.TaxaNotEligible)} |");
        sb.AppendLine();
        if (t.ItemsLinkedButNotDownloaded > 0 || t.LinksToTaxaOutsideRelease > 0) {
            sb.AppendLine($"Links skipped: {N(t.LinksToTaxaOutsideRelease)} point at IUCN ids not in {config.Release}; {N(t.ItemsLinkedButNotDownloaded)} point at items not downloaded yet.");
            sb.AppendLine();
        }

        // ---- Tier by category
        sb.AppendLine("## Planned changes by confidence tier");
        sb.AppendLine();
        sb.AppendLine("A pair is one IUCN taxon matched to one Wikidata item. Its tier says how sure the match is:");
        sb.AppendLine();
        foreach (var tier in Enum.GetValues<ConfidenceTier>()) sb.AppendLine($"- {TierNames[tier]}");
        sb.AppendLine();
        sb.Append("| Pairs |");
        foreach (var tier in Enum.GetValues<ConfidenceTier>()) sb.Append($" {tier} |");
        sb.AppendLine(" All tiers |");
        sb.Append("|---|");
        foreach (var _ in Enum.GetValues<ConfidenceTier>()) sb.Append("---:|");
        sb.AppendLine("---:|");
        foreach (var c in Categories) {
            sb.Append($"| {CategoryNames[c]} |");
            long total = 0;
            foreach (var tier in Enum.GetValues<ConfidenceTier>()) {
                var n = t.Count(tier, c);
                total += n;
                sb.Append($" {N(n)} |");
            }
            sb.AppendLine($" {N(total)} |");
        }
        sb.Append("| All pairs |");
        long grand = 0;
        foreach (var tier in Enum.GetValues<ConfidenceTier>()) {
            var n = Sum(new[] { tier }, Categories);
            grand += n;
            sb.Append($" {N(n)} |");
        }
        sb.AppendLine($" {N(grand)} |");
        sb.AppendLine();

        // ---- The rest of the edit
        sb.AppendLine("## Other planned changes");
        sb.AppendLine();
        sb.AppendLine("Counts cover every tier. Like the rest of a pair's edit, those in tiers C and D wait for a person to confirm the match; most P627 additions are tier C pairs found by name search.");
        sb.AppendLine();
        sb.AppendLine($"- IUCN taxon id (P627) added to the item: {N(t.TaxonIdsToAdd)} pairs");
        sb.AppendLine($"- Renumbered IUCN id on the item marked deprecated (reason: withdrawn identifier value): {N(t.TaxonIdsToDeprecate)} pairs");
        sb.AppendLine($"- Pairs where the two rank variants plan different edits: {N(t.VariantsDiffer)}. Only a changed status differs: `preferred` adds a new preferred-rank statement and keeps the old one at normal rank; `replace` overwrites the current statement's value and references.");
        sb.AppendLine();

        // ---- Flags
        sb.AppendLine("## Flags on pairs");
        sb.AppendLine();
        sb.AppendLine("A pair can carry several flags. A flag that lowers the tier is why the pair is below A; the rest are information only. The code is what the CSV's `flags` column holds.");
        sb.AppendLine();
        sb.AppendLine("| Flag | Pairs | Effect | Code |");
        sb.AppendLine("|---|---:|---|---|");
        foreach (var (flag, n) in t.Flags.OrderByDescending(kv => kv.Value)) {
            var (label, lowers) = FlagLabels.TryGetValue(flag, out var f) ? f : (flag, false);
            sb.AppendLine($"| {label} | {N(n)} | {(lowers ? "lowers the tier" : "information only")} | `{flag}` |");
        }
        sb.AppendLine();

        // ---- Link sources
        sb.AppendLine("## How each pair was linked");
        sb.AppendLine();
        sb.AppendLine("The strongest link found for the pair. The code is what the CSV's `link_source` column holds.");
        sb.AppendLine();
        sb.AppendLine("| Link | Pairs | Code |");
        sb.AppendLine("|---|---:|---|");
        foreach (var (source, n) in t.LinkSources.OrderByDescending(kv => kv.Value)) {
            var label = LinkSourceLabels.TryGetValue(source, out var l) ? l : source;
            sb.AppendLine($"| {label} | {N(n)} | `{source}` |");
        }
        sb.AppendLine();

        // ---- Files
        sb.AppendLine("## Files");
        sb.AppendLine();
        sb.AppendLine($"- `{Path.GetFileName(csvFile)}`: every pair, with its tier, flags, category and the planned actions for each rank variant");
        sb.AppendLine($"- `{Path.GetFileName(samplesFile)}`: sample edits as wbeditentity JSON, a few per tier, category and rank variant");
        sb.AppendLine($"- `{Path.GetFileName(assessmentSamplesFile)}`: sample new assessment items as wbeditentity JSON");
        return sb.ToString();
    }
}
