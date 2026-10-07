using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

// One page of `wikipedia report-species-lists`: runs the public site's /update code on the page's
// wikitext with its default options (StatusUpdater, then ListScope with the group it finds) and
// counts what an editor could change. Nothing is written back; the updated text is thrown away.

namespace BeastieBot3.Wikipedia;

/// What an update of one status changes.
internal enum SpeciesListChangeKind {
    /// The code on the page is a different category from the latest assessment's.
    Category,
    /// The code on the page is NA or RE, categories of regional assessments only: the page may give
    /// regional statuses (the European Red List's, say), which the global category does not replace.
    RegionalCode,
    /// CR on one side and CR(PE) or CR(PEW) on the other, or CR(PE) and CR(PEW).
    PossiblyExtinct,
    /// Same category; a {{Species table/row}} whose population trend (direction) is empty or differs.
    Trend,
    /// Same category; the assessment ids or year of an {{IUCN status}} are of an older assessment.
    Assessment,
    /// Not changed: the {{IUCN status}}'s taxon id is of another taxon than its row or line names, or
    /// was copied to rows that name other taxa (StatusNoteKind.IdUsedForOtherTaxa).
    IdOfAnotherTaxon,
}

/// A status on the page that the /update page would change. WrittenCode: the code on the page (null
/// when it could not be read); IucnCode: the latest global assessment's code.
internal sealed record SpeciesListChange(int Line, StatusItemKind Kind, string ScientificName, long TaxonId,
    string? WrittenCode, string IucnCode, int? IucnYear, SpeciesListChangeKind Change, string Before, string After);

/// A taxon of the page's group that the page does not list.
internal sealed record SpeciesListMissing(string ScientificName, long TaxonId, string Kind, string IucnCode);

/// What one page holds and what could be updated on it.
internal sealed record SpeciesListPageResult(
    string Title,
    long? RevisionId,
    DateTime? DownloadedAt,
    int Bytes,
    int TaxaListed,
    int Statuses,
    int CategoryChanged,
    int RegionalCodes,
    int PossiblyExtinctChanged,
    int TrendChanged,
    int NewerAssessment,
    int UpToDate,
    int NotMatched,
    int IdOfAnotherTaxon,
    int NameNotFound,
    int NameAmbiguous,
    int TaxaWithoutStatus,
    int ListLinesWithoutStatus,
    int TablesWithoutStatus,
    int OlderCitations,
    int PopulationDiffers,
    int NotChecked,
    string? GroupRank,
    string? GroupName,
    string? GroupCategories,
    bool GroupPartial,
    int? MissingFromGroup,
    int InOtherCategory,
    int OutsideGroup,
    int ListedTwice,
    IReadOnlyList<SpeciesListChange> Changes,
    IReadOnlyList<SpeciesListMissing> Missing) {
    /// Statuses that can be updated.
    public int Outdated => CategoryChanged + PossiblyExtinctChanged + TrendChanged + NewerAssessment;
}

internal static class SpeciesListSurvey {
    /// The kinds of finding that are a status of a listed taxon (not a taxobox, a citation, or an addition).
    private static readonly HashSet<StatusItemKind> StatusKinds = [
        StatusItemKind.StatusTemplate, StatusItemKind.TableCell, StatusItemKind.ListLine, StatusItemKind.SpeciesTableRow,
    ];

    /// Checks one page. The categories it is compared on are the ones its title names ("List of
    /// threatened birds of Brazil": CR, EN and VU), as on the status update page. A page whose title
    /// does not start "List of" (a genus or family article) is taken to list its whole group, so
    /// ListScope does not guess that it lists only threatened taxa.
    public static SpeciesListPageResult Check(StoredPageText page, IStatusLookup statuses, IListScopeLookup groups, DateOnly today) {
        var result = new StatusUpdater(statuses, today).Update(page.Wikitext);
        var members = result.Members ?? [];
        var scope = ListScope.Check(members, groups, new ListScopeOptions {
            GuessCategories = WikipediaFetchSpeciesListsCommand.IsListTitle(page.Title),
            Categories = ListCategories.FromTitle(page.Title),
        });

        var written = new Dictionary<(int Line, long TaxonId), string?>();
        foreach (var member in members) {
            written.TryAdd((member.Line, member.Taxon.TaxonId), member.WrittenCode);
        }

        var changes = new List<SpeciesListChange>();
        int statusCount = 0, upToDate = 0, notMatched = 0, nameNotFound = 0, nameAmbiguous = 0, idOfAnother = 0;
        foreach (var finding in result.Findings.Where(f => StatusKinds.Contains(f.Kind))) {
            statusCount++;
            switch (finding.Outcome) {
                case StatusOutcome.Current:
                    upToDate++;
                    break;
                case StatusOutcome.NotUpdated when finding.Notes.FirstOrDefault(n => n.Kind is StatusNoteKind.IdOfAnotherTaxon or StatusNoteKind.IdUsedForOtherTaxa) is { } wrongId:
                    idOfAnother++;
                    changes.Add(new SpeciesListChange(finding.Line, finding.Kind, finding.Taxon?.ScientificName ?? wrongId.Detail ?? "",
                        wrongId.Id ?? 0, WrittenCode: null, IucnCode: "", IucnYear: null, SpeciesListChangeKind.IdOfAnotherTaxon,
                        OneLine(finding.Before), After: ""));
                    break;
                case StatusOutcome.NotUpdated:
                    notMatched++;
                    if (finding.Notes.Any(n => n.Kind is StatusNoteKind.NameNotFound or StatusNoteKind.NoName or StatusNoteKind.TaxonNotFound)) {
                        nameNotFound++;
                    } else if (finding.Notes.Any(n => n.Kind == StatusNoteKind.NameAmbiguous)) {
                        nameAmbiguous++;
                    }
                    break;
                case StatusOutcome.Updated when finding.Taxon is { LatestGlobal: { } latest } taxon:
                    var iucnCode = IucnStatusTemplate.ToTemplateCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
                    var writtenCode = written.TryGetValue((finding.Line, taxon.TaxonId), out var w) ? w : null;
                    var change = Classify(writtenCode, iucnCode, finding.Kind);
                    changes.Add(new SpeciesListChange(finding.Line, finding.Kind, taxon.ScientificName, taxon.TaxonId,
                        writtenCode, iucnCode, latest.YearPublished, change, OneLine(finding.Before), OneLine(finding.After ?? "")));
                    break;
            }
        }

        var missing = scope is { Partial: false, Missing: { } m }
            ? m.Select(t => new SpeciesListMissing(t.ScientificName, t.TaxonId, t.Kind, Site.Lists.GroupList.StatusCode(t))).ToList()
            : [];

        return new SpeciesListPageResult(
            page.Title,
            page.RevisionId,
            page.DownloadedAt,
            page.Wikitext.Length,
            TaxaListed: members.Where(mb => mb.Taxon.InRelease).Select(mb => mb.Taxon.TaxonId).Distinct().Count(),
            Statuses: statusCount,
            CategoryChanged: changes.Count(c => c.Change == SpeciesListChangeKind.Category),
            RegionalCodes: changes.Count(c => c.Change == SpeciesListChangeKind.RegionalCode),
            PossiblyExtinctChanged: changes.Count(c => c.Change == SpeciesListChangeKind.PossiblyExtinct),
            TrendChanged: changes.Count(c => c.Change == SpeciesListChangeKind.Trend),
            NewerAssessment: changes.Count(c => c.Change == SpeciesListChangeKind.Assessment),
            UpToDate: upToDate,
            NotMatched: notMatched,
            IdOfAnotherTaxon: idOfAnother,
            NameNotFound: nameNotFound,
            NameAmbiguous: nameAmbiguous,
            TaxaWithoutStatus: members.Where(mb => mb.Taxon.InRelease && mb.WrittenCode is null && !mb.HasStatusTemplate)
                .Select(mb => mb.Taxon.TaxonId).Distinct().Count(),
            ListLinesWithoutStatus: result.ListLinesWithoutStatus,
            TablesWithoutStatus: result.TablesWithoutStatus,
            OlderCitations: result.CountNotes(StatusNoteKind.CitationOlder),
            PopulationDiffers: result.Populations.Count,
            NotChecked: result.NotChecked,
            GroupRank: scope?.Scope.Rank,
            GroupName: scope?.Scope.Name,
            GroupCategories: scope?.Categories is { } c ? string.Join(" ", c.Order(StringComparer.Ordinal)) : null,
            GroupPartial: scope?.Partial ?? false,
            MissingFromGroup: scope is { Partial: false } ? scope.MissingTotal : null,
            InOtherCategory: scope?.OtherCategory.Count ?? 0,
            OutsideGroup: scope?.Outside.Count ?? 0,
            ListedTwice: scope?.Duplicates.Count ?? 0,
            Changes: changes,
            Missing: missing);
    }

    /// What an update changes, from the code on the page (null when it could not be read), the
    /// latest assessment's code and the kind of item.
    internal static SpeciesListChangeKind Classify(string? writtenCode, string iucnCode, StatusItemKind kind) {
        if (writtenCode is not null && Category(writtenCode) is "NA" or "RE") {
            return SpeciesListChangeKind.RegionalCode;
        }
        if (writtenCode is null || !SameCategory(writtenCode, iucnCode)) {
            return SpeciesListChangeKind.Category;
        }
        if (Category(iucnCode) == "CR" && !string.Equals(writtenCode.Trim(), iucnCode, StringComparison.OrdinalIgnoreCase)) {
            return SpeciesListChangeKind.PossiblyExtinct;
        }
        return kind == StatusItemKind.SpeciesTableRow ? SpeciesListChangeKind.Trend : SpeciesListChangeKind.Assessment;
    }

    // CR(PE) and CR(PEW) are CR; LR/lc is LC, and LR/nt and LR/cd are NT (the updater writes the
    // old code for an old assessment, but the page's code names the same category).
    private static bool SameCategory(string written, string iucn) =>
        string.Equals(Category(written), Category(iucn), StringComparison.Ordinal);

    private static string Category(string code) {
        var c = code.Trim().ToUpperInvariant();
        return c switch {
            "CR(PE)" or "CR(PEW)" => "CR",
            "LR/LC" => "LC",
            "LR/NT" or "LR/CD" => "NT",
            _ => c,
        };
    }

    private static string OneLine(string text) {
        var flat = text.ReplaceLineEndings(" ");
        return flat.Length <= 200 ? flat : flat[..200] + "…";
    }
}
