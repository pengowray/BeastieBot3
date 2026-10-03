using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

// CombinedHistory.Build: which ids a combined table has, their order and colours, and when there is
// no combined table.
public sealed class CombinedHistoryTests {
    private static TaxonRow Taxon(long id, string name, bool inRelease = true) =>
        new(id, name, "species", "ANIMALIA", null, null, null, null, null, null, null, null, null, null, null, null, null, inRelease);

    private static AssessmentRow Global(long id, long taxonId, int year) =>
        new(id, taxonId, "Global", false, "LC", false, false, null, "3.1", year, $"{year}-01-01", null, null);

    private static AssessmentRow Europe(long id, long taxonId, int year) =>
        new(id, taxonId, "Europe", false, "LC", false, false, null, "3.1", year, $"{year}-01-01", null, null);

    private static CombinedHistory? Build(TaxonRow page, IReadOnlyList<TaxonLinkRow> linked, Dictionary<long, AssessmentRow[]> assessments,
        params long[] withNotes) =>
        CombinedHistory.Build(page, linked,
            id => assessments.TryGetValue(id, out var rows) ? rows.OrderByDescending(a => a.YearPublished).ToList() : [],
            ids => ids.ToDictionary(id => id, id => (bool?)withNotes.Contains(id)));

    [Fact]
    public void OldIdWithTwoLinks_ListsTheTaxaInTheReleaseFirst_EachWithItsOwnColour() {
        var old = Taxon(41758, "Platanista gangetica", inRelease: false);
        var linked = new[] {
            new TaxonLinkRow(Taxon(41756, "Platanista gangetica"), TaxonLinkKinds.SameName),
            new TaxonLinkRow(Taxon(41757, "Platanista minor"), TaxonLinkKinds.IucnSynonym),
        };
        var combined = Build(old, linked, new() {
            [41758] = [Global(1, 41758, 2012), Global(2, 41758, 1996)],
            [41756] = [Global(3, 41756, 2022)],
            [41757] = [Global(4, 41757, 2022), Europe(5, 41757, 2020)],
        }, withNotes: [3, 1])!;

        Assert.Equal(new long[] { 41756, 41757, 41758 }, combined.Ids.Select(i => i.TaxonId));
        Assert.Equal(new int?[] { 1, 2, 3 }, combined.Ids.Select(i => i.Tint));
        Assert.Equal(41758, combined.ThisPage.TaxonId);
        Assert.Equal(new string?[] { TaxonLinkKinds.SameName, TaxonLinkKinds.IucnSynonym, null }, combined.Ids.Select(i => i.LinkKind));
        Assert.True(combined.ShowNames);
        // Newest first, regional rows left out, counted per id.
        Assert.Equal(new long[] { 4, 3, 1, 2 }.Order(), combined.Rows.Select(r => r.Assessment.AssessmentId).Order());
        Assert.Equal(new[] { 2022, 2022, 2012, 1996 }, combined.Rows.Select(r => r.Assessment.YearPublished!.Value));
        Assert.Equal(1, combined.Ids[1].RegionalCount);
        Assert.Equal((1996, 2012), (combined.ThisPage.FirstYear, combined.ThisPage.LastYear));
        // The notes of each id's newest global assessment, taxa in the release first.
        Assert.Equal(new long[] { 41756, 41758 }, combined.WithNotes.Select(i => i.TaxonId));
    }

    [Fact]
    public void SameNamePair_HasNoNameColumn() {
        var combined = Build(Taxon(2790, "Bettongia penicillata"),
            [new TaxonLinkRow(Taxon(2785, "Bettongia penicillata", inRelease: false), TaxonLinkKinds.SameName)],
            new() { [2790] = [Global(1, 2790, 2026)], [2785] = [Global(2, 2785, 2025)] })!;
        Assert.False(combined.ShowNames);
        Assert.Equal(new long[] { 2790, 2785 }, combined.Ids.Select(i => i.TaxonId));
        Assert.Empty(combined.WithNotes);
    }

    // The table is only worth showing when it adds another id's global assessments.
    [Fact]
    public void NoCombinedTable_WhenNoLinkedIdHasAGlobalAssessment() {
        var page = Taxon(215033857, "Pupilla muscorum");
        var linked = new[] { new TaxonLinkRow(Taxon(156831, "Pupilla bigranata", inRelease: false), TaxonLinkKinds.IucnSynonym) };
        Assert.Null(Build(page, linked, new() { [215033857] = [Global(1, 215033857, 2017)], [156831] = [Europe(2, 156831, 2011)] }));
        Assert.Null(Build(page, [], new() { [215033857] = [Global(1, 215033857, 2017)] }));

        // From the old id's page the linked taxon has a global assessment, so there is a table; the
        // page's own id has no rows in it.
        var fromOld = Build(linked[0].Taxon, [new TaxonLinkRow(page, TaxonLinkKinds.IucnSynonym)],
            new() { [215033857] = [Global(1, 215033857, 2017)], [156831] = [Europe(2, 156831, 2011)] })!;
        Assert.Empty(fromOld.ThisPage.Global);
        Assert.Null(fromOld.ThisPage.FirstYear);
        Assert.Single(fromOld.Rows);
    }

    // More ids than colours: no id gets a colour, so no colour stands for two ids.
    [Fact]
    public void MoreIdsThanColours_GivesNoColours() {
        var page = Taxon(292912196, "Bythiospeum acicula");
        var linked = Enumerable.Range(1, CombinedHistory.TintCount)
            .Select(i => new TaxonLinkRow(Taxon(1000 + i, $"Bythiospeum old{i}", inRelease: false), TaxonLinkKinds.IucnSynonym))
            .ToList();
        var assessments = linked.ToDictionary(l => l.Taxon.TaxonId, l => new[] { Global(l.Taxon.TaxonId * 10, l.Taxon.TaxonId, 2000) });
        assessments[292912196] = [Global(1, 292912196, 2026)];

        var combined = Build(page, linked, assessments)!;
        Assert.Equal(CombinedHistory.TintCount + 1, combined.Ids.Count);
        Assert.All(combined.Ids, i => Assert.Null(i.Tint));
        Assert.All(combined.Ids, i => Assert.Null(i.TintClass));

        var six = Build(page, linked.Take(CombinedHistory.TintCount - 1).ToList(), assessments)!;
        Assert.Equal(Enumerable.Range(1, CombinedHistory.TintCount).Select(i => (int?)i), six.Ids.Select(i => i.Tint));
    }
}
