using BeastieBot3.Site.Data;

// Puts a group's list together from the sources the reader picked (ListSourceOptions): IUCN's taxa
// and the species from the Catalogue of Life and Wikidata that IUCN does not have. Pure, so the
// rules are pinned by tests.
//
// - An IUCN species is listed when IUCN is picked, or when it has a CoL usage and CoL is picked, or
//   a Wikidata item and Wikidata is picked. Subspecies, varieties and subpopulations only come from
//   IUCN. An extra species is listed when one of its sources is picked.
// - Each entry's name comes from the most preferred picked source that has it (IUCN's taxa can have
//   another name in CoL or Wikidata: taxon_source_name).
// - Overlaps (extra_overlap) between two entries:
//     likely: the entry whose best picked source is less preferred is left out, and a notice says
//       so. With the same best source (CoL and Wikidata entries both preferred through Wikidata, say)
//       neither is left out and the notice names both.
//     possible: both stay, with a notice.
//   An entry outside the group (another genus, say) counts when one of its sources is picked: it
//   can make an entry in the list be left out, but is never listed itself, and a likely duplicate
//   that is outside the group and less preferred gets no notice here. An overlap with an entry none
//   of whose sources is picked is ignored.
// - Line counts before reading the rows (GroupModel) are therefore an upper bound.

namespace BeastieBot3.Site.Lists;

/// One side of a notice: an IUCN taxon (TaxonId set) or an extra species.
public sealed record NoticeEntry(long RowId, string ScientificName, ListSource Source, string? ColId, string? WikidataQid,
    long? TaxonId, bool InGroup);

/// A notice beside the list. LeftOut: Entry was left out as likely the same species as Kept.
/// Otherwise the two may be the same species and both are listed (when in the group).
public sealed record ListNotice(NoticeEntry Entry, NoticeEntry Kept, string Reason, bool LeftOut);

public sealed record ListSourceMergeResult(
    IReadOnlyList<ListTaxonRow> Rows,
    IReadOnlyList<ListNotice> Notices,
    IReadOnlyDictionary<long, NoticeEntry> Entries) {
    public static readonly ListSourceMergeResult Empty = new([], [], new Dictionary<long, NoticeEntry>());
}

public static class ListSourceMerge {
    public static ListSourceMergeResult Merge(
        IReadOnlyList<ListTaxonRow> iucnRows,
        IReadOnlyDictionary<long, IucnSourceInfo> iucnInfo,
        IReadOnlyList<ExtraSpeciesRow> extras,
        IReadOnlyList<ExtraOverlapRow> overlaps,
        IReadOnlyDictionary<long, OverlapTaxon> outsideTaxa,
        IReadOnlyDictionary<int, ExtraSpeciesRow> outsideExtras,
        ListSourceOptions options) {
        var entries = new Dictionary<long, NoticeEntry>();
        var listed = new List<(ListTaxonRow Row, bool Extra)>();

        foreach (var row in iucnRows) {
            var info = iucnInfo.GetValueOrDefault(row.TaxonId);
            var sources = IucnSources(row, info);
            if (Best(sources, options) is not { } best) {
                continue;
            }
            var name = best switch {
                ListSource.Col when info?.ColName is { } colName => colName,
                ListSource.Wikidata when info?.WikidataName is { } wikidataName => wikidataName,
                _ => row.ScientificName,
            };
            var shown = name == row.ScientificName ? row : Renamed(row, name);
            listed.Add((shown, false));
            entries[row.TaxonId] = new NoticeEntry(row.TaxonId, name, best, info?.ColId, info?.WikidataQid, row.TaxonId, true);
        }
        foreach (var extra in extras) {
            if (Best(ExtraSources(extra), options) is not { } best) {
                continue;
            }
            var name = ExtraName(extra, best);
            listed.Add((ExtraRow(extra, name), true));
            entries[extra.RowId] = new NoticeEntry(extra.RowId, name, best, extra.ColId, extra.WikidataQid, null, true);
        }

        // Entries named by an overlap that are not in the list.
        NoticeEntry? Find(long? taxonId, int? extraId) {
            if (taxonId is { } t) {
                if (entries.TryGetValue(t, out var inList)) {
                    return inList;
                }
                if (outsideTaxa.TryGetValue(t, out var outside)
                    && Best(OutsideSources(outside), options) is { } best) {
                    return new NoticeEntry(t, outside.ScientificName, best, outside.ColId, outside.WikidataQid, t, false);
                }
                return null;
            }
            if (extraId is { } e) {
                if (entries.TryGetValue(-e, out var inList)) {
                    return inList;
                }
                if (outsideExtras.TryGetValue(e, out var outside) && Best(ExtraSources(outside), options) is { } best) {
                    return new NoticeEntry(-e, ExtraName(outside, best), best, outside.ColId, outside.WikidataQid, null, false);
                }
            }
            return null;
        }

        var notices = new List<ListNotice>();
        var leftOut = new HashSet<long>();
        var seen = new HashSet<(long, long)>();
        foreach (var overlap in overlaps) {
            var a = Find(null, overlap.ExtraId);
            var b = Find(overlap.TaxonId, overlap.OtherExtraId);
            if (a is null || b is null || !a.InGroup && !b.InGroup) {
                continue;
            }
            if (!seen.Add((Math.Min(a.RowId, b.RowId), Math.Max(a.RowId, b.RowId)))) {
                continue;
            }
            var rankA = options.Rank(a.Source);
            var rankB = options.Rank(b.Source);
            if (overlap.Likely && rankA != rankB) {
                var (loser, winner) = rankA > rankB ? (a, b) : (b, a);
                // A less preferred entry outside the group is left out of its own group's list, and
                // has nothing to do with this one.
                if (loser.InGroup) {
                    leftOut.Add(loser.RowId);
                    notices.Add(new ListNotice(loser, winner, overlap.Reason, LeftOut: true));
                }
                continue;
            }
            // The entry in the list first; for two in the list, the less preferred first.
            var (first, second) = !b.InGroup || (a.InGroup && rankA >= rankB) ? (a, b) : (b, a);
            notices.Add(new ListNotice(first, second, overlap.Reason, LeftOut: false));
        }

        // In tree order: an extra species comes right after the taxon whose tree_pos is its sort
        // position, before the taxa after it. GroupList sorts by TreePos with a stable sort, which
        // keeps that order for equal positions.
        var rows = new List<ListTaxonRow>();
        var extrasInOrder = listed.Where(l => l.Extra && !leftOut.Contains(l.Row.TaxonId)).Select(l => l.Row).ToList();
        var next = 0;
        foreach (var (row, _) in listed.Where(l => !l.Extra && !leftOut.Contains(l.Row.TaxonId))) {
            while (next < extrasInOrder.Count && extrasInOrder[next].TreePos < row.TreePos) {
                rows.Add(extrasInOrder[next++]);
            }
            rows.Add(row);
        }
        rows.AddRange(extrasInOrder.Skip(next));

        notices.Sort((x, y) => (y.LeftOut ? 1 : 0).CompareTo(x.LeftOut ? 1 : 0) is var byKind and not 0
            ? byKind
            : StringComparer.OrdinalIgnoreCase.Compare(x.Entry.ScientificName, y.Entry.ScientificName));
        return new ListSourceMergeResult(rows, notices, entries);
    }

    /// The most preferred source the reader picked among the given ones; null when none is picked.
    public static ListSource? Best(IEnumerable<ListSource> sources, ListSourceOptions options) {
        var set = sources.ToHashSet();
        foreach (var source in options.Order) {
            if (set.Contains(source) && options.Enabled.Contains(source)) {
                return source;
            }
        }
        return null;
    }

    private static IEnumerable<ListSource> IucnSources(ListTaxonRow row, IucnSourceInfo? info) {
        yield return ListSource.Iucn;
        if (row.Kind == TaxonKinds.Species && info is not null) {
            if (info.ColId is not null) {
                yield return ListSource.Col;
            }
            if (info.WikidataQid is not null) {
                yield return ListSource.Wikidata;
            }
        }
    }

    private static IEnumerable<ListSource> OutsideSources(OverlapTaxon taxon) {
        yield return ListSource.Iucn;
        if (taxon.ColId is not null) {
            yield return ListSource.Col;
        }
        if (taxon.WikidataQid is not null) {
            yield return ListSource.Wikidata;
        }
    }

    private static IEnumerable<ListSource> ExtraSources(ExtraSpeciesRow extra) {
        if (extra.InCol) {
            yield return ListSource.Col;
        }
        if (extra.InWikidata) {
            yield return ListSource.Wikidata;
        }
    }

    private static string ExtraName(ExtraSpeciesRow extra, ListSource best) =>
        best == ListSource.Wikidata && extra.WikidataName is { } wikidataName ? wikidataName : extra.ScientificName;

    private static ListTaxonRow Renamed(ListTaxonRow row, string name) {
        var parts = name.Split(' ', 2);
        return row with {
            ScientificName = name,
            Genus = parts[0],
            SpeciesEpithet = parts.Length > 1 ? parts[1] : row.SpeciesEpithet,
        };
    }

    private static ListTaxonRow ExtraRow(ExtraSpeciesRow extra, string name) {
        var parts = name.Split(' ', 2);
        return new ListTaxonRow(
            extra.RowId, name, TaxonKinds.Species, extra.Kingdom, parts[0], parts.Length > 1 ? parts[1] : extra.Epithet,
            null, null, null, extra.CommonNameEn, extra.EnwikiTitle, null, null, extra.NodeId, extra.SortPos,
            null, null, false, false, null);
    }
}
