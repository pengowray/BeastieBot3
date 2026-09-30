using System;
using System.Collections.Generic;
using System.Linq;

// Running counts for one dry run, filled pair by pair and saved with the run so the report and the
// flow page read the same numbers. Plain dictionaries keyed by enum names, so the JSON in plan_runs
// stays readable.

namespace BeastieBot3.WikidataEdits;

internal sealed class WikidataIucnPlanTally {
    /// The --limit value when the run stopped there, so the pair counts cover only that many
    /// linked items. Null when the run read every item, including a --limit run that never reached
    /// its limit.
    public int? StoppedAtLimit { get; set; }

    // IUCN side
    public long TaxaWithGlobalAssessment { get; set; }
    public long TaxaWithoutGlobalAssessment { get; set; }
    public long TaxaNotEligible { get; set; }
    public long TaxaWithNoItem { get; set; }
    public long TaxaWithSeveralItems { get; set; }

    // Wikidata side
    public long ItemsRead { get; set; }
    public long ItemsLinkedButNotDownloaded { get; set; }
    public long LinksToTaxaOutsideRelease { get; set; }
    public DateTime? OldestItemDownloadUtc { get; set; }
    public DateTime? NewestItemDownloadUtc { get; set; }
    /// Linked items whose copy was more than StaleAfterDays old when the plan ran: their revision
    /// has likely moved on, so an edit built on it would be refused.
    public long StaleItems { get; set; }
    public const int StaleAfterDays = 30;

    // Pairs
    public long Pairs { get; set; }
    public Dictionary<string, Dictionary<string, long>> TierByCategory { get; } = new();
    public Dictionary<string, long> Flags { get; } = new();
    public Dictionary<string, long> LinkSources { get; } = new();
    public Dictionary<string, long> UnmappedCodes { get; } = new();
    public long VariantsDiffer { get; set; }
    public long AssessmentItemsToCreate { get; set; }
    public long AssessmentItemsReused { get; set; }
    public long TaxonIdsToAdd { get; set; }
    public long TaxonIdsToDeprecate { get; set; }
    public long ReviewConfirmed { get; set; }
    public long ReviewRejected { get; set; }

    public void Add(PlanPairRow row) {
        Pairs++;
        Bump(Row(TierByCategory, row.Tier.ToString()), row.Category.ToString());
        Bump(LinkSources, row.Source.ToString());
        foreach (var flag in row.Flags) Bump(Flags, flag.ToString());
        if (row.Category == PlanCategory.UnmappedCategory) Bump(UnmappedCodes, row.IucnCode);
        if (row.VariantsDiffer) VariantsDiffer++;
        if (row.CreatesAssessmentItem) AssessmentItemsToCreate++;
        else if (!row.AssessmentRef.StartsWith("CREATE:", StringComparison.Ordinal) && row.Category != PlanCategory.UnmappedCategory) AssessmentItemsReused++;
        var first = row.Actions.Values.FirstOrDefault() ?? Array.Empty<PlannedAction>();
        if (first.Any(a => a is AddTaxonIdClaim)) TaxonIdsToAdd++;
        if (first.Any(a => a is DeprecateTaxonIdClaim)) TaxonIdsToDeprecate++;
    }

    public void SeeItemDownload(DateTime? downloadedAtUtc, DateTime planStartedUtc) {
        if (downloadedAtUtc is not { } at) return;
        if (planStartedUtc - at > TimeSpan.FromDays(StaleAfterDays)) StaleItems++;
        if (OldestItemDownloadUtc is null || at < OldestItemDownloadUtc) OldestItemDownloadUtc = at;
        if (NewestItemDownloadUtc is null || at > NewestItemDownloadUtc) NewestItemDownloadUtc = at;
    }

    public long Count(ConfidenceTier tier, PlanCategory category) =>
        TierByCategory.TryGetValue(tier.ToString(), out var row) && row.TryGetValue(category.ToString(), out var n) ? n : 0;

    private static Dictionary<string, long> Row(Dictionary<string, Dictionary<string, long>> table, string key) {
        if (!table.TryGetValue(key, out var row)) table[key] = row = new Dictionary<string, long>();
        return row;
    }

    private static void Bump(Dictionary<string, long> counts, string key) =>
        counts[key] = counts.TryGetValue(key, out var n) ? n + 1 : 1;
}
