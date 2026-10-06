using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Tests;

// How the possible-duplicates panel arranges the notices (ListNoticeGroups): sections, reason groups,
// runs between two genera, and the state of each entry.
public sealed class ListNoticeGroupsTests {
    private static ListTaxonRow Row(long id) =>
        new(id, "X y", TaxonKinds.Species, "ANIMALIA", "X", "y", null, null, null, null, null, null, null, 7, 1, null, null, false, false, null);

    private static NoticeEntry Entry(long id, string name, ListSource source, bool inGroup = true) =>
        new(id, name, source, null, null, source == ListSource.Iucn ? id : null, inGroup);

    private static ListSourceMergeResult Result(IEnumerable<long> listed, params ListNotice[] notices) =>
        new(listed.Select(Row).ToList(), notices, new Dictionary<long, NoticeEntry>());

    // An old Rana name on Wikidata left out as a CoL synonym of an IUCN species in another genus.
    private static ListNotice OldName(int i, string genus) =>
        new(Entry(-i, "Rana e" + i, ListSource.Wikidata), Entry(1000 + i, genus + " e" + i, ListSource.Iucn, inGroup: false), "col-synonym", LeftOut: true);

    [Fact]
    public void PairsBetweenTwoGeneraBecomeOneRunWhenThereAreThreeOrMore() {
        var notices = Enumerable.Range(1, 4).Select(i => OldName(i, "Lithobates"))
            .Concat(Enumerable.Range(5, 2).Select(i => OldName(i, "Pelophylax")))
            .ToArray();

        var groups = ListNoticeGroups.Build(Result([], notices));

        var group = Assert.Single(groups.LeftOut.Groups);
        Assert.Equal("col-synonym", group.Reason);
        var run = Assert.Single(group.Runs);
        Assert.Equal(4, run.Pairs.Count);
        Assert.Equal(("Rana", "Lithobates"), (run.FirstSample.Genus, run.SecondSample.Genus));
        Assert.Equal(NoticeState.OutsideGroup, run.SecondSample.State);
        Assert.Equal(["Pelophylax e5", "Pelophylax e6"], group.Pairs.Select(p => p.Second.Entry.ScientificName));
        Assert.Equal(6, group.PairCount);
        Assert.Equal(3, group.RowCount);
        Assert.Equal(6, groups.LeftOut.EntryCount);
        Assert.Equal("dup-out-col-synonym-rana-wikidata-leftout-lithobates-iucn-outsidegroup", run.Id);
    }

    [Fact]
    public void PairsInOneGenusAreNeverARun() {
        var notices = Enumerable.Range(1, 4)
            .Select(i => new ListNotice(Entry(-i, "Rana a" + i, ListSource.Wikidata), Entry(i, "Rana b" + i, ListSource.Iucn), "spelling", LeftOut: false))
            .ToArray();

        var group = Assert.Single(ListNoticeGroups.Build(Result([-1, -2, -3, -4, 1, 2, 3, 4], notices)).Kept.Groups);

        Assert.Empty(group.Runs);
        Assert.Equal(4, group.Pairs.Count);
    }

    [Fact]
    public void LeftOutEntriesAreCountedOnceAndReasonsFollowTheFixedOrder() {
        var left = Entry(-1, "Rana adenopleura", ListSource.Wikidata);
        var notices = new[] {
            new ListNotice(left, Entry(1, "Nidirana adenopleura", ListSource.Iucn, false), "col-synonym", true),
            new ListNotice(left, Entry(2, "Nidirana hainanensis", ListSource.Iucn, false), "col-synonym", true),
            new ListNotice(Entry(-2, "Rana bufo", ListSource.Wikidata), Entry(3, "Bufo bufo", ListSource.Iucn, false), "iucn-synonym", true),
        };

        var section = ListNoticeGroups.Build(Result([], notices)).LeftOut;

        Assert.Equal(["iucn-synonym", "col-synonym"], section.Groups.Select(g => g.Reason));
        Assert.Equal(3, section.PairCount);
        Assert.Equal(2, section.EntryCount);
    }

    [Fact]
    public void BothKeptHoldsOnlyPairsWithNoEntryLeftOutWithTheEntryInTheListFirst() {
        var leftOut = Entry(-1, "Rana catesbeiana", ListSource.Wikidata);
        var notices = new[] {
            new ListNotice(leftOut, Entry(1, "Aquarana catesbeianus", ListSource.Iucn, false), "col-synonym", true),
            // Another pair of the entry left out: not shown with the pairs whose entries both stay.
            new ListNotice(leftOut, Entry(-2, "Rana catesbiana", ListSource.Col), "spelling", false),
            // The entry in the list comes second here, so the panel swaps it to the first column.
            new ListNotice(Entry(5, "Rana other", ListSource.Iucn, false), Entry(-3, "Rana longicus", ListSource.Col), "other-genus", false),
        };

        var groups = ListNoticeGroups.Build(Result([-2, -3], notices));

        var pair = Assert.Single(Assert.Single(groups.Kept.Groups).Pairs);
        Assert.Equal("Rana longicus", pair.First.Entry.ScientificName);
        Assert.Equal(NoticeState.InList, pair.First.State);
        Assert.Equal(NoticeState.OutsideGroup, pair.Second.State);
        Assert.Equal(1, groups.Kept.PairCount);
    }

    [Fact]
    public void StateComesFromTheListedRows() {
        // Entry B is in the group and named by a notice as kept, but another notice left it out.
        var notices = new[] {
            new ListNotice(Entry(-1, "Rana a", ListSource.Wikidata), Entry(-2, "Rana b", ListSource.Col), "gender-ending", true),
            new ListNotice(Entry(-2, "Rana b", ListSource.Col), Entry(3, "Rana c", ListSource.Iucn), "gender-ending", true),
        };

        var pairs = ListNoticeGroups.Build(Result([3], notices)).LeftOut.Groups.Single().Pairs;

        Assert.Equal(NoticeState.LeftOut, pairs.Single(p => p.First.Entry.RowId == -1).Second.State);
        Assert.Equal(NoticeState.InList, pairs.Single(p => p.First.Entry.RowId == -2).Second.State);
    }

    [Fact]
    public void NoNoticesIsEmpty() {
        Assert.True(ListNoticeGroups.Build(ListSourceMergeResult.Empty).IsEmpty);
    }
}
