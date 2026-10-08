using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

// Missing species put under the headings of a list (ListPlacement.Sections.cs), and taxa now in
// another category taken out (ListPlacement.Removal.cs), as in List of endangered birds.
public sealed class ListPlacementSectionTests {
    // Galliformes: Phasianidae (Pavo 100-102, Lophura 110-111), Cracidae (Crax 120-121);
    // Anseriformes: Anatidae (Anas 130-132); Columbiformes: Columbidae (Columba 140-141). All EN.
    // vu: the taxa that are VU, the others EN.
    internal static FakeScopeLookup Birds(params long[] vu) {
        var tree = new FakeScopeLookup()
            .Group(1, null, "kingdom", "Animalia")
            .Group(2, 1, "class", "Aves")
            .Group(10, 2, "order", "Galliformes", common: "landfowl")
            .Group(11, 10, "family", "Phasianidae")
            .Group(20, 11, "genus", "Pavo")
            .Group(21, 11, "genus", "Lophura")
            .Group(12, 10, "family", "Cracidae", common: "curassows")
            .Group(22, 12, "genus", "Crax")
            .Group(13, 2, "order", "Anseriformes")
            .Group(14, 13, "family", "Anatidae")
            .Group(23, 14, "genus", "Anas")
            .Group(15, 2, "order", "Columbiformes", common: "pigeons and doves")
            .Group(16, 15, "family", "Columbidae")
            .Group(24, 16, "genus", "Columba");
        foreach (var (id, _, node) in Taxa) {
            tree.Species(node, id, 1, vu.Contains(id) ? "VU" : "EN");
        }
        foreach (var id in new long[] { 100, 101, 102, 110, 111, 120, 121, 130, 131, 132, 140, 141 }) {
            tree.Common(id, $"Bird {id}");
        }
        return tree;
    }

    private static readonly (long Id, string Genus, int Node)[] Taxa = [
        (100, "Pavo", 20), (101, "Pavo", 20), (102, "Pavo", 20), (110, "Lophura", 21), (111, "Lophura", 21),
        (120, "Crax", 22), (121, "Crax", 22), (130, "Anas", 23), (131, "Anas", 23), (132, "Anas", 23), (140, "Columba", 24), (141, "Columba", 24)];

    // now: taxa whose latest category is VU, not EN.
    internal static FakeStatusLookup Statuses(params long[] now) {
        var lookup = new FakeStatusLookup();
        foreach (var (id, genus, node) in Taxa) {
            lookup.Taxon(id, $"{genus} {FakeScopeLookup.Epithet(id)}", now.Contains(id) ? "VU" : "EN", 2020, id * 10, node: node);
        }
        return lookup;
    }

    internal static string Name(long id) => $"{Taxa.Single(t => t.Id == id).Genus} {FakeScopeLookup.Epithet(id)}";

    // A line of the list: "*[[Pavo spbaa|Bird 100]]".
    internal static string Line(long id) => $"*[[{Name(id)}|Bird {id}]]";

    // Compared with class Aves, EN only; remove: the taxa to take out.
    private static (string Text, ListPlacementResult Placement, ListScopeResult Scope) Run(string text, long[]? now = null, long[]? remove = null,
        bool addMissing = true, FakeScopeLookup? tree = null) {
        tree ??= Birds();
        var updater = new StatusUpdater(Statuses(now ?? []), new DateOnly(2026, 10, 8));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree, new ListScopeOptions("class/Aves") { Categories = ListCategories.Find("EN") })!;
        var placement = ListPlacement.Place(text, result.Members!, scope,
            new ListPlacementOptions { AddMissing = addMissing, Remove = (remove ?? []).ToHashSet() }, tree);
        return (updater.TextWith(ListPlacement.Insertions(text, placement), placement.Removals), placement, scope);
    }

    internal static string Section(string title, params long[] ids) =>
        $"==[[{title}]]==\n{{{{columns-list|colwidth=30em|\n{string.Join("\n", ids.Select(Line))}\n}}}}\n\n";

    internal const string End = "== See also ==\n* [[List of birds]]\n";

    [Fact]
    public void ASpeciesWithNoGenusMateGoesAmongTheLinesOfItsOrder() {
        // Lophura is not in the text; its order's section is.
        var text = Section("Galliformes", 100, 101, 102, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, _) = Run(text);
        Assert.Contains($"{Line(102)}\n{Line(110)}\n{Line(111)}\n{Line(120)}", result);
        Assert.Equal([110L, 111L], placement.Placed.Select(p => p.Taxon.TaxonId).Order());
        Assert.All(placement.Placed, p => Assert.Equal("Galliformes", p.Heading));
        Assert.All(placement.Placed, p => Assert.False(p.NewHeading));
    }

    [Fact]
    public void AMissingOrderGetsANewSectionInTheFormOfTheOthers() {
        // The orders are in neither alphabetical nor IUCN's order, so the new one goes after the last.
        var text = "==[[Galliformes]]==\n{{gray|Landfowl}}\n{{columns-list|colwidth=30em|\n" + string.Join("\n", new long[] { 100, 101, 102, 110, 111, 120, 121 }.Select(Line))
            + "\n}}\n\n==[[Anseriformes]]==\n{{gray|Ducks}}\n{{columns-list|colwidth=30em|\n" + string.Join("\n", new long[] { 130, 131, 132 }.Select(Line))
            + "\n}}\n\n" + End;
        var (result, placement, _) = Run(text);
        Assert.Contains($"{Line(132)}\n}}}}\n\n==[[Columbiformes]]==\n{{{{gray|Pigeons and doves}}}}\n{{{{columns-list|colwidth=30em|\n{Line(140)}\n{Line(141)}\n}}}}\n\n== See also ==", result);
        Assert.Equal(2, placement.Placed.Count);
        Assert.All(placement.Placed, p => Assert.True(p.NewHeading));
        Assert.All(placement.Placed, p => Assert.Equal("Columbiformes", p.Heading));
        Assert.Equal("Anseriformes", placement.Placed[0].NeighbourName);
    }

    [Fact]
    public void ANewSectionGoesInAlphabeticalOrderWhenTheSectionsKeepIt() {
        var text = Section("Anseriformes", 130, 131, 132) + Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + End;
        var (result, _, _) = Run(text);
        Assert.Contains($"==[[Columbiformes]]==\n{{{{columns-list|colwidth=30em|\n{Line(140)}\n{Line(141)}\n}}}}\n\n==[[Galliformes]]==", result);
    }

    [Fact]
    public void AMissingFamilyGetsASectionUnderItsOrder() {
        var text = "==[[Galliformes]]==\n===[[Phasianidae]]===\n" + string.Join("\n", new long[] { 100, 101, 102, 110, 111 }.Select(Line)) + "\n\n"
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, _) = Run(text);
        // Cracidae sorts before Phasianidae, the only family heading.
        Assert.Contains($"==[[Galliformes]]==\n===[[Cracidae]]===\n{Line(120)}\n{Line(121)}\n===[[Phasianidae]]===", result);
        Assert.All(placement.Placed, p => Assert.Equal("Cracidae", p.Heading));
    }

    [Fact]
    public void AHeadingThatDoesNotNameItsGroupIsReadFromItsTaxa() {
        var text = "== Landfowl ==\n" + string.Join("\n", new long[] { 100, 101, 110 }.Select(Line)) + "\n\n"
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        // "Landfowl" names no group. Its lines are all Phasianidae, but the other sections are of
        // orders, so it is the section of order Galliformes and takes the Cracidae too. Its lines are
        // in no order, so they go after the last.
        var (result, placement, _) = Run(text, tree: Birds());
        Assert.Contains($"{Line(101)}\n{Line(102)}\n{Line(110)}\n{Line(120)}\n{Line(121)}\n{Line(111)}\n", result);
        Assert.All(placement.Placed, p => Assert.Equal("Landfowl", p.Heading));
    }

    [Fact]
    public void AGroupWithNoSectionGoesInTheSectionForTheOthers() {
        // Columbiformes has no section; "Other bird species" takes it, not a new heading.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + "==Other bird species==\n" + Line(130) + "\n" + Line(131) + "\n" + Line(132)
            + "\n\n" + End;
        var (result, placement, _) = Run(text);
        Assert.Contains($"{Line(132)}\n{Line(140)}\n{Line(141)}\n\n== See also ==", result);
        Assert.All(placement.Placed, p => Assert.Equal("Other bird species", p.Heading));
        Assert.All(placement.Placed, p => Assert.False(p.NewHeading));
    }

    [Fact]
    public void ATaxonNowInAnotherCategoryIsTakenOut() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, scope) = Run(text, now: [101], remove: [101]);
        Assert.Contains(scope.OtherCategory, m => m.Taxon.TaxonId == 101);
        Assert.DoesNotContain(Name(101), result);
        Assert.Contains($"{Line(100)}\n{Line(102)}", result);
        Assert.Equal(101, Assert.Single(placement.Removed).Member.Taxon.TaxonId);
        // Not asked for: the text stays.
        var (unchanged, none, _) = Run(text, now: [101]);
        Assert.Contains(Line(101), unchanged);
        Assert.Empty(none.Removed);
    }

    [Fact]
    public void TheFirstAndLastLinesOfAColumnsListAreTakenOut() {
        var text = "==[[Anseriformes]]==\n{{columns-list|colwidth=30em|" + Line(130) + "\n" + Line(131) + "\n" + Line(132) + "}}\n\n"
            + Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Columbiformes", 140, 141) + End;
        var (first, _, _) = Run(text, now: [130], remove: [130], addMissing: false);
        Assert.Contains("{{columns-list|colwidth=30em|" + Line(131) + "\n" + Line(132) + "}}", first);
        var (last, _, _) = Run(text, now: [132], remove: [132], addMissing: false);
        Assert.Contains("{{columns-list|colwidth=30em|" + Line(130) + "\n" + Line(131) + "}}", last);
        var (both, _, _) = Run(text, now: [131, 132], remove: [131, 132], addMissing: false);
        Assert.Contains("{{columns-list|colwidth=30em|" + Line(130) + "}}", both);
    }

    [Fact]
    public void ASectionLeftWithNoTaxaIsTakenOut() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + "==[[Columbiformes]]==\n{{gray|Pigeons}}\n{{columns-list|colwidth=30em|\n"
            + Line(140) + "\n" + Line(141) + "\n}}\n\n" + Section("Anseriformes", 130, 131, 132) + End;
        var (result, placement, _) = Run(text, now: [140, 141], remove: [140, 141]);
        Assert.DoesNotContain("Columbiformes", result);
        Assert.Contains($"{Line(121)}\n}}}}\n\n==[[Anseriformes]]==", result);
        Assert.Equal(["Columbiformes"], placement.RemovedHeadings);
    }

    [Fact]
    public void ANewLineNextToALineTakenOutIsKept() {
        // Pavo spbac (102) is missing; its neighbour Pavo spbab (101) is now VU and taken out.
        var text = Section("Galliformes", 100, 101, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, _) = Run(text, now: [101], remove: [101]);
        Assert.Contains($"{Line(100)}\n{Line(102)}\n{Line(110)}", result);
        Assert.Single(placement.Placed);
        Assert.Empty(placement.Unplaced);
    }

    [Fact]
    public void ASectionWhoseTaxaAreAllTakenOutTakesTheNewOnes() {
        // Columba spbea (140) is taken out; Columba spbeb (141) is missing and goes in its place.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140) + End;
        var (result, placement, _) = Run(text, now: [140], remove: [140]);
        Assert.Contains($"==[[Columbiformes]]==\n{{{{columns-list|colwidth=30em|\n{Line(141)}\n}}}}", result);
        Assert.DoesNotContain(Name(140), result);
        Assert.Empty(placement.RemovedHeadings);
    }

    [Fact]
    public void ALineWithAReferenceOtherLinesUseStays() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(101), Line(101) + "<ref name=\"a\">x</ref>")
            .Replace(Line(102), Line(102) + "<ref name=\"a\"/>") + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, _) = Run(text, now: [101], remove: [101]);
        Assert.Contains(Line(101), result);
        var kept = Assert.Single(placement.Kept);
        Assert.Equal(KeptReason.DefinesReference, kept.Reason);
    }

    [Fact]
    public void ALineWithItemsUnderItStays() {
        // The subpopulations under the species are items of the list too.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(101), Line(101) + "\n**Southwest subpopulation")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var (result, placement, _) = Run(text, now: [101], remove: [101]);
        Assert.Contains(Line(101) + "\n**Southwest subpopulation", result);
        Assert.Equal(KeptReason.SharesLine, Assert.Single(placement.Kept).Reason);
    }

    [Fact]
    public void ALineWithATaxonThatStaysUnderItStays() {
        var tree = Birds().Infra(500, 100, "alpha");
        var statuses = Statuses(100).Taxon(500, "Pavo spbaa ssp. alpha", "EN", 2020, 5000, node: 20, kind: TaxonKinds.Subspecies);
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(100), Line(100) + "\n**''Pavo spbaa alpha''")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var updater = new StatusUpdater(statuses, new DateOnly(2026, 10, 8));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree, new ListScopeOptions("class/Aves") { Categories = ListCategories.Find("EN") })!;
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions { Remove = new HashSet<long> { 100 } }, tree);
        Assert.Equal(KeptReason.SharesLine, Assert.Single(placement.Kept).Reason);
        Assert.Contains(Line(100), updater.TextWith(ListPlacement.Insertions(text, placement), placement.Removals));
    }
}
