using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

// Taxa now in another category taken out of {{Species table}}s and wikitables (ListPlacement.Removal.cs).
public sealed class ListRemovalTests {
    // Felidae: Panthera 100-103, Felis 200-205; vu: the taxa that are VU, the others LC.
    private static (FakeScopeLookup Tree, FakeStatusLookup Statuses) Cats(params long[] vu) {
        var tree = new FakeScopeLookup().Group(1, null, "kingdom", "Animalia").Group(3, 1, "family", "Felidae")
            .Group(4, 3, "genus", "Panthera").Group(5, 3, "genus", "Felis");
        var statuses = new FakeStatusLookup();
        foreach (var (node, genus, ids) in new[] { (4, "Panthera", new long[] { 100, 101, 102, 103 }), (5, "Felis", new long[] { 200, 201, 202, 203, 204, 205 }) }) {
            foreach (var id in ids) {
                var category = vu.Contains(id) ? "VU" : "LC";
                tree.Species(node, id, 1, category).Common(id, $"Cat {id}");
                statuses.Taxon(id, $"{genus} {FakeScopeLookup.Epithet(id)}", category, 2020, id * 10, node: node);
            }
        }
        return (tree, statuses);
    }

    // A list of LC taxa.
    private static (string Text, ListPlacementResult Placement) Run(string text, long[] vu, long[] remove) {
        var (tree, statuses) = Cats(vu);
        var updater = new StatusUpdater(statuses, new DateOnly(2026, 10, 8));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree, new ListScopeOptions { Categories = ListCategories.Find("LC") })!;
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions { Remove = remove.ToHashSet() }, tree);
        return (updater.TextWith(ListPlacement.Insertions(text, placement), placement.Removals), placement);
    }

    private static string Row(long id, string genus) =>
        $"{{{{Species table/row\n|name=[[Cat {id}]] |binomial={genus[0]}. {FakeScopeLookup.Epithet(id)}\n|authority-name=X |authority-year=1900\n"
        + "|iucn-status=LC |population=Unknown\n|direction={{decrease|Population declining}}\n}}";

    private static string Table(string genus, params long[] ids) =>
        $"{{{{Species table |no-note=y |genus=[[{genus}]] |species-count=four}}}}\n{string.Concat(ids.Select(id => Row(id, genus) + "\n"))}{{{{Species table/end}}}}\n";

    [Fact]
    public void ARowIsTakenOutWithItsLineBreak() {
        var text = Table("Panthera", 100, 101, 102, 103) + "\n" + Table("Felis", 200, 201, 202, 203, 204, 205);
        var (result, placement) = Run(text, vu: [101], remove: [101]);
        Assert.Equal(Table("Panthera", 100, 102, 103) + "\n" + Table("Felis", 200, 201, 202, 203, 204, 205), result);
        Assert.Single(placement.Removed);
    }

    [Fact]
    public void ATableWithNoRowsLeftIsTakenOut() {
        var text = Table("Panthera", 100, 101, 102, 103) + "\n" + Table("Felis", 200, 201, 202, 203, 204, 205);
        var (result, placement) = Run(text, vu: [100, 101, 102, 103], remove: [100, 101, 102, 103]);
        Assert.Equal("\n" + Table("Felis", 200, 201, 202, 203, 204, 205), result);
        Assert.Equal(4, placement.Removed.Count);
    }

    [Fact]
    public void AMissingSpeciesTakesThePlaceOfTheRowsTakenOut() {
        // Panthera spbaa (100) is VU and taken out; 101 to 103 are LC and missing: they go in its table.
        var text = Table("Panthera", 100) + "\n" + Table("Felis", 200, 201, 202, 203, 204, 205);
        var (tree, statuses) = Cats(100);
        var updater = new StatusUpdater(statuses, new DateOnly(2026, 10, 8));
        var update = updater.Update(text);
        var scope = ListScope.Check(update.Members!, tree, new ListScopeOptions { Categories = ListCategories.Find("LC") })!;
        var placement = ListPlacement.Place(text, update.Members!, scope, new ListPlacementOptions { Remove = new HashSet<long> { 100 } }, tree);
        var result = updater.TextWith(ListPlacement.Insertions(text, placement), placement.Removals);
        Assert.Equal(3, placement.Placed.Count);
        Assert.DoesNotContain("P. spbaa", result);
        Assert.Contains("{{Species table |no-note=y |genus=[[Panthera]]", result);
        Assert.Contains("|binomial=P. spbab", result);
        Assert.Contains("|binomial=P. spbad", result);
        Assert.Contains("{{Species table/end}}\n\n{{Species table |no-note=y |genus=[[Felis]]", result);
    }

    [Fact]
    public void ATaxonInAWikitableStays() {
        var text = "{| class=\"wikitable\"\n! Name !! Scientific name !! Status\n"
            + string.Concat(new long[] { 100, 101, 102, 103 }.Select(id => $"|-\n| [[Cat {id}]] || ''[[Panthera {FakeScopeLookup.Epithet(id)}]]'' || LC\n"))
            + "|}\n" + string.Concat(new long[] { 200, 201, 202, 203, 204, 205 }.Select(id => $"* ''Felis {FakeScopeLookup.Epithet(id)}''\n"));
        var (result, placement) = Run(text, vu: [101], remove: [101]);
        Assert.Contains("Panthera spbab", result);
        Assert.Equal(KeptReason.NotOnListLine, Assert.Single(placement.Kept).Reason);
        Assert.Empty(placement.Removed);
    }
}
