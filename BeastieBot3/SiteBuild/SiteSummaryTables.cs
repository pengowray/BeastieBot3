using BeastieBot3.Iucn.SummaryTables;

// Links the rows of IUCN's summary tables (`iucn summary-tables`) to the site's taxa and assessments,
// for `site build-db`:
//
//   Table 7 row (species, previous and new category, reason, version): the taxon is found by its
//   scientific name, else by an IUCN synonym (StatusListNameIndex, in the kingdom of the row's group
//   when the group names one). The change belongs to the taxon's global assessment that has the new
//   category and was published in the version's year (else the year after, for an assessment IUCN
//   amended and published again); of several, the first whose previous global assessment has another
//   category. Its reason (G, N or E) goes on that assessment. When several tables list the change,
//   the table latest in rules/iucn-summary-tables.yml wins.
//
//   Possibly Extinct: a Table 9 row says the species was CR(PE) or CR(PEW) on the Red List of the
//   table's release, and gives the year of its first such assessment. Both the global assessment
//   current at that release (the last one published in or before the release's year) and the CR
//   assessment published in that first year are listed. A Table 7 row printing "CR(PE)" lists the
//   new assessment, or for the previous category, the previous one. Only CR assessments are listed.
//
// Categories are compared as Table 7 counts them: LR/nt is NT and LR/lc is LC.

namespace BeastieBot3.SiteBuild;

/// One global assessment of a taxon, for the linking.
internal sealed record SiteHistoryEntry(long AssessmentId, string Category, bool PossiblyExtinct, bool PossiblyExtinctInTheWild,
    int? YearPublished, string? AssessmentDate);

/// A row of the site's summary_table.
internal sealed record SiteSummaryTable(long Id, int Table, string Release, string Url, string? LastUpdated);

/// A row of the site's category_change.
internal sealed record SiteCategoryChange(long AssessmentId, long TaxonId, string Reason, long? PreviousAssessmentId,
    string? OldCategory, string? NewCategory, string? RedListVersion, long SummaryTableId);

/// A row of the site's possibly_extinct_listing.
internal sealed record SitePossiblyExtinctListing(long AssessmentId, string Tag, long TaxonId, string FirstRelease,
    string LastRelease, string Tables, long SummaryTableId);

/// What the linking found, with counts for the build summary.
internal sealed class SiteSummaryTablesResult {
    public List<SiteSummaryTable> Tables { get; } = new();
    public List<SiteCategoryChange> Changes { get; } = new();
    public List<SitePossiblyExtinctListing> Listings { get; } = new();

    public int ChangeRows;
    public int ChangeRowsWithoutReason;
    public int ChangeRowsNoTaxon;
    public int ChangeRowsBySynonym;
    public int ChangeRowsNoAssessment;
    public int ChangeRowsLinked;
    public int ChangeRowsYearAfter;
    public int ChangeRowsPreviousDiffers;
    /// Assessments whose tables give the change different reasons; the latest table's is kept.
    public int ReasonConflicts;
    public int ListingRows;
    public int ListingRowsNoTaxon;
    public int ListingRowsNoAssessment;
    /// Listed assessments whose own record has no PE or PEW tag.
    public int ListedWithoutTag;
}

internal static class SiteSummaryTables {
    public static SiteSummaryTablesResult Link(
        IReadOnlyCollection<SummaryTableSource> sources,
        IReadOnlyList<StoredCategoryChange> changes,
        IReadOnlyList<StoredPossiblyExtinct> listings,
        StatusListNameIndex names,
        IReadOnlyDictionary<long, List<SiteHistoryEntry>> globalHistory) {
        var result = new SiteSummaryTablesResult();
        foreach (var source in sources.OrderBy(s => s.Table).ThenBy(s => s.Priority)) {
            result.Tables.Add(new SiteSummaryTable(source.Id, source.Table, source.Release, source.Url, source.LastUpdated));
        }

        var evidence = new Dictionary<(long AssessmentId, string Tag), ListingEvidence>();
        var chosen = new Dictionary<long, (SiteCategoryChange Change, int Priority)>();
        foreach (var stored in changes) {
            result.ChangeRows++;
            var row = stored.Row;
            var reason = row.Reason;
            var version = row.Version ?? stored.Source.Release;
            var year = SummaryTableValues.VersionYear(version);
            var newCategory = row.New;
            if (reason is null || year is null || newCategory.Category is null) {
                result.ChangeRowsWithoutReason++;
                continue;
            }
            var (taxonId, bySynonym) = FindTaxon(names, row.Group, row.ScientificName);
            if (taxonId is null || !globalHistory.TryGetValue(taxonId.Value, out var history)) {
                result.ChangeRowsNoTaxon++;
                continue;
            }
            if (bySynonym) result.ChangeRowsBySynonym++;
            var (index, yearAfter) = FindChange(history, newCategory.Category, year.Value);
            if (index is null) {
                result.ChangeRowsNoAssessment++;
                continue;
            }
            result.ChangeRowsLinked++;
            if (yearAfter) result.ChangeRowsYearAfter++;
            var assessment = history[index.Value];
            var previous = index.Value > 0 ? history[index.Value - 1] : null;
            if (previous is null || row.Old.Category is null || Family(previous.Category) != Family(row.Old.Category)) {
                result.ChangeRowsPreviousDiffers++;
            }
            if (newCategory.Tag is { } newTag) {
                AddEvidence(evidence, assessment, newTag, taxonId.Value, stored.Source, "7");
            }
            if (row.Old.Tag is { } oldTag && previous is not null && Family(previous.Category) == "CR") {
                AddEvidence(evidence, previous, oldTag, taxonId.Value, stored.Source, "7");
            }
            var change = new SiteCategoryChange(assessment.AssessmentId, taxonId.Value, reason, previous?.AssessmentId,
                Compact(row.OldCategoryText), Compact(row.NewCategoryText), version, stored.Source.Id);
            if (chosen.TryGetValue(assessment.AssessmentId, out var existing)) {
                if (existing.Change.Reason != reason) result.ReasonConflicts++;
                if (stored.Source.Priority < existing.Priority) continue;
            }
            chosen[assessment.AssessmentId] = (change, stored.Source.Priority);
        }
        result.Changes.AddRange(chosen.Values.Select(c => c.Change).OrderBy(c => c.AssessmentId));

        foreach (var stored in listings) {
            result.ListingRows++;
            var row = stored.Row;
            var tag = row.Category.Tag;
            var releaseYear = SummaryTableValues.VersionYear(stored.Source.Release);
            if (tag is null || releaseYear is null) {
                result.ListingRowsNoAssessment++;
                continue;
            }
            var (taxonId, _) = FindTaxon(names, row.Group, row.ScientificName);
            if (taxonId is null || !globalHistory.TryGetValue(taxonId.Value, out var history)) {
                result.ListingRowsNoTaxon++;
                continue;
            }
            var current = history.LastOrDefault(a => a.YearPublished is { } y && y <= releaseYear.Value);
            var linked = false;
            if (current is not null && Family(current.Category) == "CR") {
                AddEvidence(evidence, current, tag, taxonId.Value, stored.Source, "9");
                linked = true;
            }
            if (row.YearAssessed is { } first
                && history.FirstOrDefault(a => a.YearPublished == first && Family(a.Category) == "CR") is { } firstAssessment) {
                AddEvidence(evidence, firstAssessment, tag, taxonId.Value, stored.Source, "9");
                linked = true;
            }
            if (!linked) result.ListingRowsNoAssessment++;
        }
        foreach (var ((assessmentId, tag), e) in evidence.OrderBy(e => e.Key.AssessmentId).ThenBy(e => e.Key.Tag, StringComparer.Ordinal)) {
            result.Listings.Add(new SitePossiblyExtinctListing(assessmentId, tag, e.TaxonId, e.First.Release, e.Last.Release,
                string.Join(' ', e.Tables.Order(StringComparer.Ordinal)), e.First.Id));
            if (!e.HasTag) result.ListedWithoutTag++;
        }
        return result;
    }

    private sealed class ListingEvidence {
        public required long TaxonId;
        public required SummaryTableSource First;
        public required SummaryTableSource Last;
        public required bool HasTag;
        public readonly SortedSet<string> Tables = new(StringComparer.Ordinal);
    }

    private static void AddEvidence(Dictionary<(long, string), ListingEvidence> evidence, SiteHistoryEntry assessment, string tag,
        long taxonId, SummaryTableSource source, string table) {
        var key = (assessment.AssessmentId, tag);
        var hasTag = tag == "PE" ? assessment.PossiblyExtinct : assessment.PossiblyExtinctInTheWild;
        if (!evidence.TryGetValue(key, out var e)) {
            evidence[key] = e = new ListingEvidence { TaxonId = taxonId, First = source, Last = source, HasTag = hasTag };
        }
        if (ReleaseKey(source.Release).CompareTo(ReleaseKey(e.First.Release)) < 0) e.First = source;
        if (ReleaseKey(source.Release).CompareTo(ReleaseKey(e.Last.Release)) > 0) e.Last = source;
        e.Tables.Add(table);
    }

    /// The taxon of a row's name: by scientific name, in the group's kingdom when it names one, then
    /// in any kingdom; then by IUCN synonym the same way.
    internal static (long? TaxonId, bool BySynonym) FindTaxon(StatusListNameIndex names, string? group, string scientificName) {
        var kingdom = SummaryTableValues.KingdomOfGroup(group);
        var name = scientificName.Trim();
        var taxon = (kingdom is null ? null : names.Find(kingdom, name)) ?? names.Find(null, name);
        if (taxon is not null) return (taxon.TaxonId, false);
        taxon = (kingdom is null ? null : names.FindByIucnSynonym(kingdom, name)) ?? names.FindByIucnSynonym(null, name);
        return (taxon?.TaxonId, taxon is not null);
    }

    /// The index in history (oldest first) of the assessment that brought the new category in that
    /// year, else in the year after; null when there is none.
    internal static (int? Index, bool YearAfter) FindChange(IReadOnlyList<SiteHistoryEntry> history, string newCategory, int year) {
        foreach (var (candidateYear, yearAfter) in new[] { (year, false), (year + 1, true) }) {
            int? first = null;
            for (var i = 0; i < history.Count; i++) {
                var a = history[i];
                if (a.YearPublished != candidateYear || Family(a.Category) != Family(newCategory)) continue;
                first ??= i;
                if (i == 0 || Family(history[i - 1].Category) != Family(newCategory)) return (i, yearAfter);
            }
            if (first is not null) return (first, yearAfter);
        }
        return (null, false);
    }

    /// A category as Table 7 counts it: LR/nt as NT, LR/lc as LC.
    internal static string Family(string category) => category.Trim() switch {
        "LR/nt" => "NT",
        "LR/lc" => "LC",
        var c => c,
    };

    private static string? Compact(string? text) => text?.Replace(" ", "", StringComparison.Ordinal);

    private static (int Year, int Number) ReleaseKey(string release) => IucnSummaryTablesCommand.ReleaseKey(release);

    /// The global assessments of each taxon, oldest first (year published, then date, then id).
    public static Dictionary<long, List<SiteHistoryEntry>> SortHistory(IEnumerable<(long TaxonId, SiteHistoryEntry Entry)> rows) {
        var byTaxon = new Dictionary<long, List<SiteHistoryEntry>>();
        foreach (var (taxonId, entry) in rows) {
            if (!byTaxon.TryGetValue(taxonId, out var list)) byTaxon[taxonId] = list = new List<SiteHistoryEntry>();
            list.Add(entry);
        }
        foreach (var list in byTaxon.Values) {
            list.Sort((a, b) => (a.YearPublished ?? 0).CompareTo(b.YearPublished ?? 0) is var y and not 0 ? y
                : string.CompareOrdinal(a.AssessmentDate, b.AssessmentDate) is var d and not 0 ? d
                : a.AssessmentId.CompareTo(b.AssessmentId));
        }
        return byTaxon;
    }
}
