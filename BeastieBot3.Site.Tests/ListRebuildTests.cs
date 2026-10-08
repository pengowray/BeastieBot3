using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;
using static BeastieBot3.Site.Tests.ListPlacementSectionTests;

namespace BeastieBot3.Site.Tests;

// Rebuilding a list of list lines (ListRebuild), on the birds of ListPlacementSectionTests (all EN
// unless now says VU), compared with class Aves, EN only.
public sealed class ListRebuildTests {
    private static RebuildResult Run(string text, long[]? now = null, long[]? remove = null, FakeStatusLookup? statuses = null,
        RebuildOptions? options = null, FakeScopeLookup? tree = null) {
        tree ??= Birds(now ?? []);
        var updater = new StatusUpdater(statuses ?? Statuses(now ?? []), new DateOnly(2026, 10, 8));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree, new ListScopeOptions("class/Aves") { Categories = ListCategories.Find("EN") })!;
        return ListRebuild.Rebuild(text, updater, result.Members!, scope, new ListPlacementOptions { Remove = (remove ?? []).ToHashSet() }, tree, options);
    }

    private const string Lead = "{{Short description|none}}\nThe IUCN lists these birds as endangered.\n\n";

    [Fact]
    public void RebuildingTheRebuiltListChangesNothing() {
        var text = Lead + Section("Galliformes", 100, 102, 120, 121, 101) + Section("Anseriformes", 130, 131, 132) + End;
        var first = Run(text, now: [101], remove: [101]);
        Assert.Equal(RebuildRefusal.None, first.Refusal);
        var second = Run(first.Text, now: [101], remove: [101]);
        Assert.Equal(first.Text, second.Text);
        Assert.All(second.Taxa, t => Assert.Equal(RebuildChange.Kept, t.Change));
    }

    [Fact]
    public void MissingTaxaGoInTheirSectionsAndANewOneForANewOrder() {
        var text = Lead + Section("Galliformes", 100, 101, 102, 120, 121) + Section("Anseriformes", 130, 131, 132) + End;
        var result = Run(text);
        Assert.Equal(Lead + $"==[[Galliformes]]==\n{{{{columns-list|colwidth=30em|\n{Line(100)}\n{Line(101)}\n{Line(102)}\n{Line(110)}\n{Line(111)}\n{Line(120)}\n{Line(121)}\n}}}}\n\n"
            + $"==[[Anseriformes]]==\n{{{{columns-list|colwidth=30em|\n{Line(130)}\n{Line(131)}\n{Line(132)}\n}}}}\n\n"
            + $"==[[Columbiformes]]==\n{{{{columns-list|colwidth=30em|\n{Line(140)}\n{Line(141)}\n}}}}\n\n" + End, result.Text);
        Assert.Equal(["Columbiformes"], result.NewHeadings);
        Assert.Equal(4, result.Taxa.Count(t => t.Change == RebuildChange.Added));
    }

    [Fact]
    public void ATaxonInTheWrongSectionMoves() {
        // Crax spbca (120) is in Galliformes, listed under Anseriformes.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 121) + Section("Anseriformes", 130, 120, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        // The fixture's lines are in order of common name ("Bird 111", "Bird 120", "Bird 121").
        Assert.Contains($"{Line(111)}\n{Line(120)}\n{Line(121)}", result.Text);
        Assert.Contains($"{Line(130)}\n{Line(131)}\n{Line(132)}", result.Text);
        var moved = Assert.Single(result.Taxa, t => t.Change == RebuildChange.Moved);
        Assert.Equal(120, moved.Taxon.TaxonId);
        Assert.Equal("Anseriformes", moved.OldHeading);
        Assert.Equal("Galliformes", moved.Heading);
    }

    [Fact]
    public void ASectionKeepsItsHeadingTextAndImages() {
        var text = "==[[Galliformes|Landfowl]]==\n{{gray|Landfowl}}\n[[File:Pavo.jpg|thumb|A peafowl]]\nLandfowl are heavy birds.\n\n"
            + string.Join("\n", new long[] { 101, 100, 102, 110, 111, 120, 121 }.Select(Line)) + "\n\nMost are hunted.\n\n"
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        Assert.StartsWith("==[[Galliformes|Landfowl]]==\n{{gray|Landfowl}}\n[[File:Pavo.jpg|thumb|A peafowl]]\nLandfowl are heavy birds.\n\n"
            + string.Join("\n", new long[] { 101, 100, 102, 110, 111, 120, 121 }.Select(Line)) + "\n\nMost are hunted.\n\n==[[Anseriformes]]==", result.Text);
    }

    [Fact]
    public void ASectionWithNoTaxaLeftIsDroppedWithItsText() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + "==[[Columbiformes]]==\n{{gray|Pigeons}}\nPigeons are doves.\n" + Line(140) + "\n" + Line(141)
            + "\n\n" + Section("Anseriformes", 130, 131, 132) + End;
        var result = Run(text, now: [140, 141], remove: [140, 141]);
        Assert.DoesNotContain("Columbiformes", result.Text);
        var dropped = Assert.Single(result.Dropped);
        Assert.Equal("Columbiformes", dropped.Heading);
        Assert.Equal("Pigeons are doves.", dropped.Text);
        Assert.Equal(2, result.Removed.Count);
    }

    [Fact]
    public void ADuplicateLineIsLeftOut() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121, 100) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        Assert.Single(result.Text.Split('\n'), l => l == Line(100));
        Assert.Equal(100, Assert.Single(result.DuplicateLines).Taxon.TaxonId);
    }

    [Fact]
    public void ALineUnderASynonymIsNotADuplicate() {
        // "Pavo cristatus" is a synonym of Pavo spbaa (100): the list keeps it as a species of its own.
        var statuses = Statuses().Synonym(100, "Pavo cristatus");
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(100), "*''[[Pavo cristatus]]''\n" + Line(100))
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, statuses: statuses);
        Assert.Contains("*''[[Pavo cristatus]]''", result.Text);
        Assert.Contains(Line(100), result.Text);
        Assert.Empty(result.DuplicateLines);
    }

    [Fact]
    public void ALineOfAnArticleTitleSortsByTheScientificName() {
        // "[[Bird 101]]" names Pavo spbab (101) by its article's title. By the taxa's scientific names
        // the lines are in order, so the missing Crax and Lophura go in alphabetical place.
        var statuses = Statuses().Article(101, "Bird 101");
        var lines = new[] { "*[[Crax spbca|A]]", "*[[Pavo spbaa|Z]]", "*[[Bird 101]]", "*[[Pavo spbac|Y]]" };
        var text = "==[[Galliformes]]==\n" + string.Join("\n", lines) + "\n\n" + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, statuses: statuses);
        Assert.Contains($"*[[Crax spbca|A]]\n{Line(121)}\n{Line(110)}\n{Line(111)}\n*[[Pavo spbaa|Z]]\n*[[Bird 101]]\n*[[Pavo spbac|Y]]", result.Text);
    }

    [Fact]
    public void ALineWithItemsUnderItStaysWithThem() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(101), Line(101) + "\n**Southwest subpopulation")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, now: [101], remove: [101]);
        Assert.Contains(Line(101) + "\n**Southwest subpopulation", result.Text);
        Assert.Equal(KeptReason.HasLinesUnder, Assert.Single(result.Kept).Reason);
    }

    [Fact]
    public void AStatusOnAKeptLineIsUpdated() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(102), Line(102) + " {{IUCN status|LC}}")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        Assert.Contains(Line(102) + " {{IUCN status|EN}}", result.Text);
    }

    [Fact]
    public void ALineThatNamesNoTaxonOfTheListStays() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(110), Line(110) + "\n*''Lophura imaginaria'' (not assessed)")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        Assert.Contains("*''Lophura imaginaria'' (not assessed)\n" + Line(111), result.Text);
        Assert.Equal(1, result.OtherLines);
    }

    [Fact]
    public void AListWithNoHeadingsIsSorted() {
        var text = Lead + string.Join("\n", new long[] { 100, 102, 110, 120, 121, 130, 131, 132, 140, 141 }.Select(Line)) + "\n\n" + End;
        var result = Run(text);
        Assert.Contains($"{Line(100)}\n{Line(101)}\n{Line(102)}\n{Line(110)}\n{Line(111)}\n{Line(120)}", result.Text);
        Assert.StartsWith(Lead, result.Text);
        Assert.EndsWith(End, result.Text);
    }

    [Fact]
    public void SectionsAfterTheListStayAsTheyAre() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141)
            + "== See also ==\n*  [[List of birds]]\n\n== References ==\n{{Reflist}}\n\n[[Category:Birds]]\n";
        var result = Run(text);
        Assert.Equal(text, result.Text);
    }
    // ------------------------------------------------------------ options

    private static string Flat => Lead + string.Join("\n", new long[] { 100, 101, 102, 110, 111, 120, 121, 130, 131, 132, 140, 141 }.Select(Line)) + "\n\n" + End;

    [Fact]
    public void AListWithNoHeadingsGetsHeadingsForTheChosenRanks() {
        var result = Run(Flat, options: new RebuildOptions { HeadingRanks = ["order", "family"] });
        // In IUCN's order: Anseriformes, Columbiformes, Galliformes (with Cracidae before Phasianidae).
        Assert.Equal(Lead + $"== Order Anseriformes ==\n\n=== Family Anatidae ===\n{Line(130)}\n{Line(131)}\n{Line(132)}\n\n"
            + $"== Order Columbiformes ==\n\n=== Family Columbidae ===\n{Line(140)}\n{Line(141)}\n\n"
            + $"== Order Galliformes ==\n\n=== Family Cracidae ===\n{Line(120)}\n{Line(121)}\n\n"
            + $"=== Family Phasianidae ===\n{Line(100)}\n{Line(101)}\n{Line(102)}\n{Line(110)}\n{Line(111)}\n\n" + End, result.Text);
        Assert.Equal(7, result.NewHeadings.Count);
        // Rebuilding again with the same ranks changes nothing.
        Assert.Equal(result.Text, Run(result.Text, options: new RebuildOptions { HeadingRanks = ["order", "family"] }).Text);
    }

    [Fact]
    public void HeadingsByRankKeepTheTextsHeadingsAndText() {
        var text = "==[[Galliformes|Landfowl]]==\n{{gray|Landfowl}}\nLandfowl are heavy birds.\n" + string.Join("\n", new long[] { 100, 101, 102, 110, 111, 120, 121 }.Select(Line))
            + "\n\n" + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, options: new RebuildOptions { HeadingRanks = ["order", "family"] });
        Assert.Contains("==[[Galliformes|Landfowl]]==\n{{gray|Landfowl}}\nLandfowl are heavy birds.\n\n===[[Cracidae]]===\n{{gray|Curassows}}\n", result.Text);
        Assert.Contains("===[[Phasianidae]]===\n" + Line(100), result.Text);
        Assert.Contains("==[[Anseriformes]]==\n\n===[[Anatidae]]===\n{{columns-list|colwidth=30em|\n" + Line(130), result.Text);
        Assert.EndsWith(End, result.Text);
    }

    [Fact]
    public void ADroppedHeadingsLinesGoToTheNearestHeading() {
        // A line that names no taxon of the list stays with its order's heading, or goes under the
        // family heading of its order when only families have headings.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121).Replace(Line(110), Line(110) + "\n*''Lophura imaginaria''")
            + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, options: new RebuildOptions { HeadingRanks = ["order"] });
        Assert.Contains("*''Lophura imaginaria''", result.Text);
        Assert.Empty(result.Dropped);
        var families = Run(text, options: new RebuildOptions { HeadingRanks = ["family"] });
        Assert.Contains("*''Lophura imaginaria''", families.Text);
        Assert.Equal(["Galliformes", "Anseriformes", "Columbiformes"], families.Dropped.Select(d => d.Heading));
    }

    [Fact]
    public void LinesWrittenAnewInTheChosenStyle() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, options: new RebuildOptions { KeepWording = false, Style = SpeciesListStyle.ScientificNameFirst });
        Assert.DoesNotContain(Line(100), result.Text);
        Assert.Contains("*''[[Pavo spbaa]]''", result.Text);
    }

    [Fact]
    public void LinesInTheChosenOrder() {
        // The fixture's lines are in order of common name; sorted by scientific name, Crax comes first.
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, options: new RebuildOptions { Sort = ListSort.ScientificName });
        Assert.Contains($"{{{{columns-list|colwidth=30em|\n{Line(120)}\n{Line(121)}\n{Line(110)}\n{Line(111)}\n{Line(100)}", result.Text);
    }

    [Fact]
    public void SectionsInIucnOrder() {
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + End;
        var result = Run(text, options: new RebuildOptions { IucnOrder = true });
        var a = result.Text.IndexOf("Anseriformes", StringComparison.Ordinal);
        var c = result.Text.IndexOf("Columbiformes", StringComparison.Ordinal);
        var g = result.Text.IndexOf("Galliformes", StringComparison.Ordinal);
        Assert.True(a < c && c < g, result.Text);
    }

    [Fact]
    public void SubspeciesInAPartOfTheirOwn() {
        var tree = Birds().Infra(500, 100, "alpha");
        var statuses = Statuses().Taxon(500, "Pavo spbaa ssp. alpha", "EN", 2020, 5000, node: 20, kind: TaxonKinds.Subspecies);
        var text = Section("Galliformes", 100, 101, 102, 110, 111, 120, 121) + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text, statuses: statuses, tree: tree, options: new RebuildOptions { Infra = InfraMode.Separate });
        Assert.Contains("'''Subspecies'''\n{{columns-list|colwidth=30em|\n*", result.Text);
        Assert.Contains("Pavo spbaa alpha", result.Text);
    }

    [Fact]
    public void AMissingSpeciesGoesInTheLetterSectionOfItsGenus() {
        // Sections by letter of the epithet; Pavo spbab (101) is missing and goes under its neighbours' letter.
        var text = "==Species==\n===A===\n" + Line(100) + "\n\n===C===\n" + Line(102) + "\n" + Line(110) + "\n" + Line(111) + "\n" + Line(120)
            + "\n" + Line(121) + "\n\n" + Section("Anseriformes", 130, 131, 132) + Section("Columbiformes", 140, 141) + End;
        var result = Run(text);
        Assert.Contains("===A===\n" + Line(100) + "\n" + Line(101) + "\n\n===C===", result.Text);
    }
}
