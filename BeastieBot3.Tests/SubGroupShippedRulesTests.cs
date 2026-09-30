using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Checks the shipped rules (rules/wikipedia-lists.yml and its sibling files, copied to the test output
// at build time), so a rules edit that leaves a parent page unable to link one of its sub-groups fails
// here instead of showing up only as a warning line during generate-lists.
public class SubGroupShippedRulesTests {
    private static readonly Lazy<WikipediaListConfig> Config = new(() =>
        new WikipediaListDefinitionLoader().Load(Path.Combine(AppContext.BaseDirectory, "rules", "wikipedia-lists.yml")));

    [Fact]
    public void NoListHasASubGroupWarning() {
        var config = Config.Value;
        Assert.NotEmpty(config.Lists);
        var warnings = config.Lists
            .SelectMany(l => ChildLinkReport.ForList(l.Id, config.ChildLinkNotes)
                .Where(m => m.Severity == ChildLinkReport.Severity.Warning)
                .Select(m => $"{l.Id}: {m.Text}"))
            .ToList();
        Assert.Empty(warnings);
    }

    [Theory]
    [InlineData("plants-threatened", new[] { "magnoliopsida-threatened", "liliopsida-threatened", "conifers-all-status", "cycads-all-status" })]
    [InlineData("plants-lc", new[] { "magnoliopsida-lc", "liliopsida-lc", "conifers-all-status", "cycads-all-status" })]
    public void PlantsParentPagesLinkTheirSubGroupLists(string listId, string[] expected) {
        Assert.Equal(expected, SubListIds(listId));
    }

    // No sub-group of plants has a list for these presets, and an all-status list alone does not make
    // a page a parent page.
    [Theory]
    [InlineData("plants-nt")]
    [InlineData("plants-dd")]
    [InlineData("plants-extinct-combined")]
    public void OtherPlantsListsAreOrdinaryLists(string listId) {
        Assert.Empty(SubListIds(listId));
    }

    // bryopsida is named "Mosses" but covers class Bryopsida only. As a sub-group of plants it put a
    // wrong moss count on the plants pages (96 threatened, of 112 threatened mosses in 2026-1).
    [Fact]
    public void BryopsidaIsNotASubGroupOfPlants() {
        Assert.DoesNotContain(Config.Value.ChildLinkNotes, n =>
            string.Equals(n.ParentGroup, "plants", StringComparison.OrdinalIgnoreCase)
            && string.Equals(n.ChildGroup, "bryopsida", StringComparison.OrdinalIgnoreCase));
    }

    // Every fish and invertebrates list is a parent page that links each sub-group's list for its own
    // preset (no all-status stand-ins, nothing left unlinked).
    [Theory]
    [InlineData("fish", 2)]
    [InlineData("invertebrates", 6)]
    public void FishAndInvertebratesListsLinkEverySubGroup(string group, int subGroupCount) {
        var config = Config.Value;
        var lists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.NotEmpty(lists);
        foreach (var list in lists) {
            var notes = config.ChildLinkNotes
                .Where(n => n.ParentListId == list.Id && n.Kind == GroupingKind.Phylogenetic)
                .ToList();
            Assert.Equal(subGroupCount, notes.Count);
            Assert.All(notes, n => Assert.Equal(ChildLinkOutcome.Linked, n.Outcome));
            Assert.Equal(notes.Select(n => $"{n.ChildGroup}-{list.Preset}"), list.SubLists.Select(s => s.Id));
        }
    }

    private static IEnumerable<string> SubListIds(string listId) =>
        Config.Value.Lists.Single(l => l.Id == listId).SubLists.Select(s => s.Id);
}
