using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

// The status update page's table of the wikitext's taxa by category, beside the compared group's.
public sealed class StatusCountTableTests {
    // Animalia > Mammalia > Felidae > Panthera, Felis; Mammalia > Tachyglossidae > Zaglossus.
    private static FakeScopeLookup Tree() => new FakeScopeLookup()
        .Group(1, null, "kingdom", "Animalia")
        .Group(2, 1, "class", "Mammalia")
        .Group(3, 2, "family", "Felidae")
        .Group(4, 3, "genus", "Panthera")
        .Group(5, 3, "genus", "Felis")
        .Group(8, 2, "family", "Tachyglossidae")
        .Group(9, 8, "genus", "Zaglossus");

    private static List<ListMember> Members(FakeScopeLookup lookup, params long[] ids) =>
        [.. ids.Select((id, i) => new ListMember(lookup.Taxon(id), i + 1, lookup.Taxon(id).ScientificName, null))];

    [Fact]
    public void The_table_counts_what_the_comparison_counts() {
        var lookup = Tree().Species(4, 100, 3, "EN").Species(5, 200, 3).Species(5, 300, 1, "CR", pe: true).Infra(210, 200, "minor");
        // 101 is listed twice; 210 is a subspecies.
        var result = ListScope.Check(Members(lookup, 100, 101, 101, 102, 200, 201, 300, 210), lookup)!;
        var table = StatusCountTable.For(result);

        Assert.True(table.HasIucn);
        Assert.True(table.ShowInfra);
        Assert.Equal(["CR(PE)", "EN", "LC"], table.Rows.Select(r => r.Code));
        Assert.Equal(new StatusCountRow("CR(PE)", 1, 0, 1, 0), table.Rows[0]);
        Assert.Equal(new StatusCountRow("EN", 3, 0, 3, 0), table.Rows[1]);
        Assert.Equal(new StatusCountRow("LC", 2, 1, 3, 1), table.Rows[2]);
        Assert.Equal((result.Species, result.SpeciesInScope), (table.Total.Species, table.Total.IucnSpecies));
        Assert.Equal((6, 1, 7, 1), (table.Total.Species, table.Total.Infra, table.Total.IucnSpecies, table.Total.IucnInfra));
    }

    [Fact]
    public void Taxa_outside_the_group_are_left_out() {
        var lookup = Tree().Species(4, 100, 10).Species(5, 200, 10).Species(9, 900, 1, "CR");
        var result = ListScope.Check(Members(lookup, [.. Enumerable.Range(100, 10).Concat(Enumerable.Range(200, 10)).Select(i => (long)i), 900]), lookup)!;
        Assert.Equal("Felidae", result.Scope.Name);
        var table = StatusCountTable.For(result);
        Assert.Equal(new StatusCountRow("LC", 20, 0, 20, 0), Assert.Single(table.Rows));
        Assert.False(table.ShowInfra);
    }

    [Fact]
    public void Without_a_comparison_every_listed_taxon_counts_once() {
        var lookup = Tree().Species(4, 100, 2, "VU").Species(9, 900, 1, "CR");
        var subpopulation = new StatusTaxon(950, "Zaglossus x subpopulation", true, null, null, TaxonKinds.Subpopulation, 9);
        var oldId = new StatusTaxon(960, "Panthera old", false, 100, null, TaxonKinds.Species, 4);
        List<ListMember> members = [.. Members(lookup, 100, 100, 101, 900),
            new ListMember(subpopulation, 5, "x", null), new ListMember(oldId, 6, "y", null)];
        var table = StatusCountTable.For(members);
        Assert.False(table.HasIucn);
        Assert.Equal(["CR", "VU"], table.Rows.Select(r => r.Code));
        Assert.Equal(3, table.Total.Species);
    }

    [Fact]
    public void Codes_follow_the_list_sections_with_the_old_codes_beside_their_category() {
        var table = StatusCountTable.Build([], [
            new GroupCategoryCount("LR/lc", 1, 0, 0), new GroupCategoryCount("LC", 5, 0, 0), new GroupCategoryCount("LR/nt", 1, 0, 0),
            new GroupCategoryCount("NT", 2, 0, 0), new GroupCategoryCount("EX", 1, 0, 0), new GroupCategoryCount("CR(PEW)", 1, 0, 0),
            new GroupCategoryCount("CR", 3, 0, 0), new GroupCategoryCount("DD", 4, 0, 0),
        ]);
        Assert.Equal(["EX", "CR", "CR(PEW)", "LR/nt", "NT", "LC", "LR/lc", "DD"], table.Rows.Select(r => r.Code));
    }
}
