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
    [InlineData("plants-threatened", new[] { "magnoliopsida-threatened", "liliopsida-threatened", "conifers-all-status", "cycads-all-status", "ferns-all-status", "mosses-all-status" })]
    [InlineData("plants-lc", new[] { "magnoliopsida-lc", "liliopsida-lc", "conifers-all-status", "cycads-all-status", "ferns-all-status", "mosses-all-status" })]
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

    // mosses covers all of phylum Bryophyta (seven classes in IUCN), not class Bryopsida only, and is a
    // sub-group of plants. The plants pages give it one table row and one section.
    [Fact]
    public void MossesAreTheWholePhylum() {
        var config = Config.Value;
        var mosses = config.Lists.Single(l => l.Id == "mosses-all-status");
        Assert.Contains(mosses.Filters, f =>
            string.Equals(f.Rank, "phylum", StringComparison.OrdinalIgnoreCase)
            && string.Equals(f.Value, "BRYOPHYTA", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(mosses.Filters, f => string.Equals(f.Rank, "class", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(config.Lists, l => string.Equals(l.TaxaGroup, "bryopsida", StringComparison.OrdinalIgnoreCase));
    }

    // Every fish, invertebrates and mammals list (mammals-ew aside) is a parent page that links each
    // sub-group's list for its own preset, or the all-status list of a sub-group that has only that.
    [Theory]
    [InlineData("fish", new[] { "ray-finned-fishes", "sharks-rays" }, new string[0])]
    [InlineData("invertebrates",
        new[] { "insects", "gastropods", "bivalves", "crustaceans", "corals", "arachnids" },
        new[] { "cephalopods", "sea-cucumbers", "millipedes" })]
    [InlineData("mammals", new[] { "bats", "rodents", "primates" }, new string[0])]
    public void ParentListsLinkEverySubGroup(string group, string[] presetChildren, string[] allStatusChildren) {
        var config = Config.Value;
        var lists = config.Lists
            .Where(l => string.Equals(l.TaxaGroup, group, StringComparison.OrdinalIgnoreCase) && l.Id != "mammals-ew")
            .ToList();
        Assert.NotEmpty(lists);
        foreach (var list in lists) {
            var notes = config.ChildLinkNotes
                .Where(n => n.ParentListId == list.Id && n.Kind == GroupingKind.Phylogenetic)
                .ToList();
            Assert.Equal(presetChildren.Length + allStatusChildren.Length, notes.Count);
            Assert.All(notes, n => Assert.Equal(
                allStatusChildren.Contains(n.ChildGroup) ? ChildLinkOutcome.LinkedAllStatus : ChildLinkOutcome.Linked,
                n.Outcome));
            var expected = presetChildren.Select(c => $"{c}-{list.Preset}")
                .Concat(allStatusChildren.Select(c => $"{c}-all-status"));
            Assert.Equal(expected, list.SubLists.Select(s => s.Id));
        }
    }

    // None of bats, rodents and primates has an extinct in the wild species, so they have no ew list
    // and mammals-ew is an ordinary list.
    [Fact]
    public void MammalsExtinctInTheWildIsAnOrdinaryList() {
        Assert.Empty(SubListIds("mammals-ew"));
    }

    private static IEnumerable<string> SubListIds(string listId) =>
        Config.Value.Lists.Single(l => l.Id == listId).SubLists.Select(s => s.Id);
}
