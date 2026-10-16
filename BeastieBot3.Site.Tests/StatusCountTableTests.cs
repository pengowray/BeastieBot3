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
        Assert.Equal(new StatusCountRow("CR(PE)", 1, 0, 1, 0) { MissingSpecies = 0, MissingInfra = 0 }, table.Rows[0]);
        Assert.Equal(new StatusCountRow("EN", 3, 0, 3, 0) { MissingSpecies = 0, MissingInfra = 0 }, table.Rows[1]);
        Assert.Equal(new StatusCountRow("LC", 2, 1, 3, 1) { MissingSpecies = 1, MissingInfra = 0 }, table.Rows[2]);
        Assert.Equal((result.Species, result.SpeciesInScope), (table.Total.Species, table.Total.IucnSpecies));
        Assert.Equal((6, 1, 7, 1), (table.Total.Species, table.Total.Infra, table.Total.IucnSpecies, table.Total.IucnInfra));
    }

    [Fact]
    public void Taxa_outside_the_group_are_left_out() {
        var lookup = Tree().Species(4, 100, 10).Species(5, 200, 10).Species(9, 900, 1, "CR");
        var result = ListScope.Check(Members(lookup, [.. Enumerable.Range(100, 10).Concat(Enumerable.Range(200, 10)).Select(i => (long)i), 900]), lookup)!;
        Assert.Equal("Felidae", result.Scope.Name);
        var table = StatusCountTable.For(result);
        Assert.Equal(new StatusCountRow("LC", 20, 0, 20, 0) { MissingSpecies = 0 }, Assert.Single(table.Rows));
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

    [Fact]
    public void The_missing_columns_are_the_differences_and_the_list_names_them() {
        var lookup = Tree().Species(4, 100, 3, "EN").Species(5, 200, 3).Species(5, 300, 1, "CR", pe: true);
        var result = ListScope.Check(Members(lookup, 100, 101, 102, 200, 201), lookup)!;
        var table = StatusCountTable.For(result);

        Assert.True(table.ShowMissing);
        Assert.Equal([("CR(PE)", 1), ("EN", 0), ("LC", 1)], table.Rows.Select(r => (r.Code, r.MissingSpecies ?? -1)));
        Assert.Equal(2, table.Total.MissingSpecies);
        Assert.Equal(["CR(PE)", "LC"], table.Missing.Select(m => m.Code));
        Assert.Equal([300L], table.Missing[0].Species.Select(t => t.TaxonId));
        Assert.Equal([202L], table.Missing[1].Species.Select(t => t.TaxonId));
        Assert.Equal("missing-CR-PE", StatusCountTable.AnchorOf("CR(PE)"));
        Assert.Equal("missing-LR-nt", StatusCountTable.AnchorOf("LR/nt"));
    }

    [Fact]
    public void Categories_and_kinds_the_comparison_leaves_out_have_no_missing_count() {
        var lookup = Tree().Species(4, 100, 6, "CR").Species(5, 200, 3).Species(5, 300, 1, "CR").Infra(110, 100, "minor");
        var result = ListScope.Check(Members(lookup, 100, 101, 102, 103, 104, 300), lookup,
            new ListScopeOptions { Categories = BeastieBot3.Shared.Wikitext.ListCategories.Find("cr") })!;
        var table = StatusCountTable.For(result);

        Assert.Equal(1, table.Rows.Single(r => r.Code == "CR").MissingSpecies);
        Assert.Null(table.Rows.Single(r => r.Code == "LC").MissingSpecies);
        // One subspecies in the group and none listed: subspecies are not compared.
        Assert.Null(table.Rows.Single(r => r.Code == "CR").MissingInfra);
        Assert.True(table.MissingCategoriesLeftOut);
        Assert.True(table.MissingInfraLeftOut);
        Assert.Equal("The comparison with Family Felidae includes only CR species, because the wikitext lists fewer than half of the group's "
            + "subspecies and varieties, so every other cell under \u201cMissing from the wikitext\u201d has a dash.",
            BeastieBot3.Site.Display.UpdateText.CountsDashHelp(result, table));
    }

    [Fact]
    public void A_list_of_part_of_the_group_has_no_missing_columns() {
        var lookup = Tree().Species(4, 100, 20).Species(5, 200, 20);
        var result = ListScope.Check(Members(lookup, 100, 101, 102, 103, 104, 200, 201, 202, 203, 204), lookup)!;
        Assert.True(result.Partial);
        var table = StatusCountTable.For(result);
        Assert.False(table.ShowMissing);
        Assert.Empty(table.Missing);
    }
}
