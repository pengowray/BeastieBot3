// Arranges the notices about possible duplicates (ListSourceMerge) for the panel under a group's
// list (_ListNotices): two sections, entries left out of the list and pairs that both stay; in each,
// one group per reason; in each reason, runs of pairs between the same two genera (the old names in
// one genus of species that are now in another, after a genus change) collapsed into one row. Pure,
// so the grouping is pinned by tests.

namespace BeastieBot3.Site.Lists;

/// Where an entry of a notice is with these options: in the list, left out of it as a likely
/// duplicate, or outside the group (listed on another group's page, if anywhere).
public enum NoticeState {
    InList,
    LeftOut,
    OutsideGroup,
}

/// One entry of a pair, with its state in this list.
public sealed record NoticeSide(NoticeEntry Entry, NoticeState State) {
    /// The first word of the name.
    public string Genus => Entry.ScientificName.Split(' ', 2)[0];
}

/// A pair of entries. In the left-out section, First is the entry left out and Second the entry it
/// is likely the same species as. In the other section, First is an entry in the list.
public sealed record NoticePair(NoticeSide First, NoticeSide Second);

/// Pairs with the same reason between one genus and another, from the same sources, in the same states.
public sealed record NoticeRun(string Id, IReadOnlyList<NoticePair> Pairs) {
    public NoticeSide FirstSample => Pairs[0].First;
    public NoticeSide SecondSample => Pairs[0].Second;
}

/// The pairs of one reason: runs (largest first), then the other pairs by name.
public sealed record NoticeReasonGroup(string Id, string Reason, IReadOnlyList<NoticeRun> Runs, IReadOnlyList<NoticePair> Pairs) {
    public int PairCount => Runs.Sum(r => r.Pairs.Count) + Pairs.Count;
    /// Rows the group shows while every run is collapsed.
    public int RowCount => Runs.Count + Pairs.Count;
}

public sealed record NoticeSection(IReadOnlyList<NoticeReasonGroup> Groups) {
    public int PairCount => Groups.Sum(g => g.PairCount);
    /// Distinct first entries: in the left-out section, the entries left out.
    public int EntryCount => Groups.SelectMany(g => g.Runs.SelectMany(r => r.Pairs).Concat(g.Pairs)).Select(p => p.First.Entry.RowId).Distinct().Count();
}

public sealed record ListNoticeGroups(NoticeSection LeftOut, NoticeSection Kept) {
    /// The fewest pairs collapsed into one row.
    public const int MinRun = 3;
    /// A reason group with at most this many rows starts open.
    public const int MaxOpenRows = 10;

    /// The reasons in the order the panel lists them: likely duplicates (synonyms, then gender
    /// endings), then possible ones.
    public static readonly string[] ReasonOrder =
        ["iucn-synonym", "col-synonym", "wikidata-synonym", "gender-ending", "spelling", "other-genus"];

    public bool IsEmpty => LeftOut.PairCount == 0 && Kept.PairCount == 0;

    public static ListNoticeGroups Build(ListSourceMergeResult result, int minRun = MinRun) {
        var inList = result.Rows.Select(r => r.TaxonId).ToHashSet();
        NoticeSide Side(NoticeEntry entry) => new(entry,
            inList.Contains(entry.RowId) ? NoticeState.InList : entry.InGroup ? NoticeState.LeftOut : NoticeState.OutsideGroup);

        NoticeSection Section(bool leftOut, string prefix) {
            var groups = result.Notices
                .Where(n => n.LeftOut == leftOut)
                .Select(n => (n.Reason, Pair: new NoticePair(Side(n.Entry), Side(n.Kept))))
                .Where(x => leftOut || BothStay(x.Pair))
                .Select(x => (x.Reason, Pair: leftOut || x.Pair.First.State == NoticeState.InList ? x.Pair : new NoticePair(x.Pair.Second, x.Pair.First)))
                .GroupBy(x => x.Reason, x => x.Pair)
                .OrderBy(g => Array.IndexOf(ReasonOrder, g.Key) is var i and >= 0 ? i : int.MaxValue)
                .ThenBy(g => g.Key, StringComparer.Ordinal)
                .Select(g => Group(prefix, g.Key, g.ToList(), minRun))
                .ToList();
            return new NoticeSection(groups);
        }
        return new ListNoticeGroups(Section(true, "out"), Section(false, "kept"));
    }

    // A pair of the second section: one entry in the list, and the other in the list too or outside
    // the group. A pair with an entry that another pair left out is not shown there: that entry is in
    // the first section with the reason it was left out, and the pair puts no duplicate in the list.
    private static bool BothStay(NoticePair pair) =>
        pair.First.State != NoticeState.LeftOut && pair.Second.State != NoticeState.LeftOut
        && (pair.First.State == NoticeState.InList || pair.Second.State == NoticeState.InList);

    private static NoticeReasonGroup Group(string prefix, string reason, IReadOnlyList<NoticePair> pairs, int minRun) {
        var groupId = Id("dup", prefix, reason);
        var runs = new List<NoticeRun>();
        var single = new List<NoticePair>();
        foreach (var bucket in pairs.GroupBy(p => (
                     p.First.Genus, p.First.Entry.Source, p.First.State,
                     p.Second.Genus, p.Second.Entry.Source, p.Second.State))) {
            var list = bucket.OrderBy(p => p.First.Entry.ScientificName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(p => p.Second.Entry.ScientificName, StringComparer.OrdinalIgnoreCase).ToList();
            var key = bucket.Key;
            if (list.Count >= minRun && !string.Equals(key.Item1, key.Item4, StringComparison.Ordinal)) {
                runs.Add(new NoticeRun(Id(groupId, key.Item1, key.Item2.ToString(), key.Item3.ToString(),
                    key.Item4, key.Item5.ToString(), key.Item6.ToString()), list));
            } else {
                single.AddRange(list);
            }
        }
        runs = runs.OrderByDescending(r => r.Pairs.Count)
            .ThenBy(r => r.FirstSample.Genus, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.SecondSample.Genus, StringComparer.OrdinalIgnoreCase).ToList();
        single = single.OrderBy(p => p.First.Entry.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(p => p.Second.Entry.ScientificName, StringComparer.OrdinalIgnoreCase).ToList();
        return new NoticeReasonGroup(groupId, reason, runs, single);
    }

    // An element id from the parts: lower case, with every run of other characters as one hyphen.
    private static string Id(params string[] parts) {
        var text = string.Join("-", parts).ToLowerInvariant();
        var chars = text.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '-').ToArray();
        return string.Join("-", new string(chars).Split('-', StringSplitOptions.RemoveEmptyEntries));
    }
}
