using System.Text.Json;
using BeastieBot3.Web.Endpoints;
using BeastieBot3.WikipediaLists;
using Microsoft.AspNetCore.Http;

namespace BeastieBot3.Tests;

// Pins which list a parent list links for each of its sub-groups (children: in taxa-groups.yml), the
// all-status stand-in and when it applies, and the messages generate-lists and the Taxa grouping page
// show for a sub-group a parent page cannot link.
public class SubGroupLinkTests {
    private static readonly HashSet<string> Ids = new(StringComparer.OrdinalIgnoreCase) {
        "dicots-threatened", "dicots-lc", "conifers-all-status",
    };

    [Fact]
    public void ResolveChildLink_PresetListWins() {
        var (outcome, id) = WikipediaListDefinitionLoader.ResolveChildLink("dicots", "lc", Ids, allowAllStatus: true);
        Assert.Equal(ChildLinkOutcome.Linked, outcome);
        Assert.Equal("dicots-lc", id);
    }

    [Fact]
    public void ResolveChildLink_AllStatusStandsInWhenAllowed() {
        var (outcome, id) = WikipediaListDefinitionLoader.ResolveChildLink("conifers", "threatened", Ids, allowAllStatus: true);
        Assert.Equal(ChildLinkOutcome.LinkedAllStatus, outcome);
        Assert.Equal("conifers-all-status", id);
    }

    [Fact]
    public void ResolveChildLink_NoStandInWhenNotAllowed() {
        var (outcome, id) = WikipediaListDefinitionLoader.ResolveChildLink("conifers", "nt", Ids, allowAllStatus: false);
        Assert.Equal(ChildLinkOutcome.NoList, outcome);
        Assert.Null(id);
    }

    [Fact]
    public void ResolveChildLink_NoListAtAll() {
        var (outcome, id) = WikipediaListDefinitionLoader.ResolveChildLink("dicots", "nt", Ids, allowAllStatus: true);
        Assert.Equal(ChildLinkOutcome.NoList, outcome);
        Assert.Null(id);
    }

    // ---- the loader over a small rules directory ----

    [Fact]
    public void Load_ParentPageLinksPresetListsThenAllStatusStandIns() {
        var config = LoadRules();
        Assert.Equal(
            new[] { "dicots-threatened", "monocots-threatened", "conifers-all-status" },
            SubListIds(config, "plants-threatened"));
    }

    [Fact]
    public void Load_ParentPageSkipsSubGroupWithNoListAndRecordsIt() {
        var config = LoadRules();
        Assert.Equal(new[] { "dicots-lc", "conifers-all-status" }, SubListIds(config, "plants-lc"));
        var note = Assert.Single(config.ChildLinkNotes, n => n.ParentListId == "plants-lc" && n.ChildGroup == "monocots");
        Assert.Equal(ChildLinkOutcome.NoList, note.Outcome);
        Assert.True(note.ChildHasLists);
    }

    // No sub-group has an nt list, so the all-status stand-in must not turn plants-nt into a parent page.
    [Fact]
    public void Load_AllStatusAloneDoesNotMakeAParentPage() {
        var config = LoadRules();
        Assert.Empty(SubListIds(config, "plants-nt"));
        var conifers = Assert.Single(config.ChildLinkNotes, n => n.ParentListId == "plants-nt" && n.ChildGroup == "conifers");
        Assert.Equal(ChildLinkOutcome.NoList, conifers.Outcome);
    }

    [Fact]
    public void Load_SeeAlsoOnAnOrdinaryListUsesAllStatusStandIn() {
        var config = LoadRules();
        var list = config.Lists.Single(l => l.Id == "trees-nt");
        Assert.Empty(list.SubLists);
        Assert.Equal(new[] { "conifers-all-status" }, list.SeeAlso.Select(s => s.Id));
    }

    [Fact]
    public void Load_UnknownSubGroupIsRecorded() {
        var config = LoadRules();
        Assert.Contains(config.ChildLinkNotes, n =>
            n.ParentListId == "fungi-threatened" && n.ChildGroup == "lichens" && n.Outcome == ChildLinkOutcome.UnknownGroup);
    }

    // ---- messages ----

    [Fact]
    public void ForList_ParentPage_ListsLinksAndWarnsAboutMissingList() {
        var messages = ChildLinkReport.ForList("plants-lc", LoadRules().ChildLinkNotes);
        Assert.Equal(2, messages.Count);
        Assert.Equal(ChildLinkReport.Severity.Info, messages[0].Severity);
        Assert.Equal("Sub-group lists linked: dicots-lc, conifers-all-status", messages[0].Text);
        Assert.Equal(ChildLinkReport.Severity.Warning, messages[1].Severity);
        Assert.Equal("No list monocots-lc, so species in sub-group monocots (Monocotyledons) are listed on plants-lc itself.", messages[1].Text);
        Assert.Equal("To create monocots-lc, add lc to the monocots entry in wikipedia-lists.yml.", messages[1].Hint);
    }

    [Fact]
    public void ForList_OrdinaryListWithSubGroups_IsOneInfoLine() {
        var message = Assert.Single(ChildLinkReport.ForList("plants-nt", LoadRules().ChildLinkNotes));
        Assert.Equal(ChildLinkReport.Severity.Info, message.Severity);
        Assert.Equal("No sub-group of plants (dicots, monocots, conifers) has a list for preset nt, so plants-nt has no summary table and no links to sub-group lists.", message.Text);
    }

    [Fact]
    public void ForList_ListWithoutSubGroups_HasNoMessages() {
        Assert.Empty(ChildLinkReport.ForList("dicots-lc", LoadRules().ChildLinkNotes));
    }

    [Fact]
    public void ForList_SubGroupWithNoListsAtAll_SuggestsAnEntry() {
        var warning = ChildLinkReport.ForList("fungi-threatened", LoadRules().ChildLinkNotes)
            .Single(m => m.Text.StartsWith("No list mushrooms-threatened", StringComparison.Ordinal));
        Assert.Equal("To create mushrooms-threatened, add an entry for mushrooms to wikipedia-lists.yml.", warning.Hint);
    }

    [Fact]
    public void WarningsForGroup_CollectsParentPageWarnings() {
        var config = LoadRules();
        var warnings = ChildLinkReport.WarningsForGroup("plants", new[] { "dicots", "monocots", "conifers" }, config);
        Assert.Equal(new[] {
            "No list monocots-lc, so species in sub-group monocots (Monocotyledons) are listed on plants-lc itself. To create monocots-lc, add lc to the monocots entry in wikipedia-lists.yml.",
        }, warnings);
    }

    [Fact]
    public void WarningsForGroup_NoListLinksAnySubGroup() {
        var config = LoadRules();
        var warnings = ChildLinkReport.WarningsForGroup("trees", new[] { "dicots" }, config);
        Assert.Contains("No trees list links to a sub-group list, because no sub-group of trees has a list for any of these presets: nt.", warnings);
    }

    [Fact]
    public void WarningsForGroup_GroupWithoutLists() {
        var config = LoadRules();
        var warnings = ChildLinkReport.WarningsForGroup("shrubs", new[] { "dicots" }, config);
        Assert.Equal(new[] { "shrubs has no lists in wikipedia-lists.yml, so no list links to its sub-groups." }, warnings);
    }

    // The Taxa grouping page's "Save sub-groups" answer carries the same warnings, read from the draft.
    [Fact]
    public void SaveChildren_ReturnsSubGroupLinkWarnings() {
        WithRules(dir => {
            var result = TaxaGroupingEndpoints.SaveChildren(
                new TaxaGroupingEndpoints.ChildrenRequest { Group = "plants", Add = Array.Empty<string>(), Remove = Array.Empty<string>() },
                Path.Combine(dir, "taxa-groups.yml"));
            var body = JsonSerializer.SerializeToElement(((IValueHttpResult)result).Value);
            var warning = Assert.Single(body.GetProperty("warnings").EnumerateArray());
            Assert.StartsWith("No list monocots-lc", warning.GetString());
        });
    }

    private static IEnumerable<string> SubListIds(WikipediaListConfig config, string listId) =>
        config.Lists.Single(l => l.Id == listId).SubLists.Select(s => s.Id);

    private static WikipediaListConfig LoadRules() {
        WikipediaListConfig? config = null;
        WithRules(dir => config = new WikipediaListDefinitionLoader().Load(Path.Combine(dir, "wikipedia-lists.yml")));
        return config!;
    }

    private static void WithRules(Action<string> test) {
        var dir = Directory.CreateTempSubdirectory("bb3-subgroups-");
        try {
            File.WriteAllText(Path.Combine(dir.FullName, "taxa-groups.yml"), GroupsYaml);
            File.WriteAllText(Path.Combine(dir.FullName, "list-presets.yml"), PresetsYaml);
            File.WriteAllText(Path.Combine(dir.FullName, "wikipedia-lists.yml"), ListsYaml);
            test(dir.FullName);
        } finally {
            dir.Delete(recursive: true);
        }
    }

    private const string GroupsYaml = """
        groups:
          plants:
            name: "Plants"
            filters:
              - rank: kingdom
                value: Plantae
            children: [dicots, monocots, conifers]
          dicots:
            name: "Dicots"
            filters:
              - rank: class
                value: Magnoliopsida
          monocots:
            name: "Monocotyledons"
            filters:
              - rank: class
                value: Liliopsida
          conifers:
            name: "Conifers"
            filters:
              - rank: class
                value: Pinopsida
          trees:
            name: "Trees"
            filters:
              - rank: kingdom
                value: Plantae
            children: [dicots]
            see_also: [conifers]
          shrubs:
            name: "Shrubs"
            children: [dicots]
          mushrooms:
            name: "Mushrooms"
          fungi:
            name: "Fungi"
            children: [dicots, mushrooms, lichens]
        """;

    private const string PresetsYaml = """
        presets:
          threatened:
            title_template: "List of threatened {taxa_name_lower}"
            output_template: "List_of_threatened_{taxa_slug}.wikitext"
            sections:
              - key: threatened
                heading: "Threatened"
                statuses:
                  - code: VU
          lc:
            title_template: "List of least concern {taxa_name_lower}"
            output_template: "List_of_least_concern_{taxa_slug}.wikitext"
            sections:
              - key: lc
                heading: "Least concern"
                statuses:
                  - code: LC
          nt:
            title_template: "List of near threatened {taxa_name_lower}"
            output_template: "List_of_near_threatened_{taxa_slug}.wikitext"
            sections:
              - key: nt
                heading: "Near threatened"
                statuses:
                  - code: NT
          all-status:
            title_template: "List of {taxa_name} by conservation status"
            output_template: "List_of_{taxa_name}_by_conservation_status.wikitext"
            sections:
              - key: lc
                heading: "Least concern"
                statuses:
                  - code: LC
        """;

    private const string ListsYaml = """
        lists:
          - taxa_group: plants
            presets: [threatened, lc, nt]
          - taxa_group: dicots
            presets: [threatened, lc]
          - taxa_group: monocots
            presets: [threatened]
          - taxa_group: conifers
            category_split: all-status
          - taxa_group: trees
            presets: [nt]
          - taxa_group: fungi
            presets: [threatened]
        """;
}
