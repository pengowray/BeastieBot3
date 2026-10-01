using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// A taxa group's sub_groups block (taxa-groups.yml) stands in for one full group block, one children:
// entry and one wikipedia-lists.yml entry per sub-group. TaxaSubGroups.Expand turns it into exactly
// those, for the loader and the Taxa grouping page alike.
public class TaxaSubGroupsTests {
    [Fact]
    public void Expand_MakesAGroupPerEntryWithTheParentsFiltersPlusTheRank() {
        var expanded = TaxaSubGroups.Expand(Groups());

        var eels = expanded.Groups["anguilliformes"];
        Assert.Equal("Eels", eels.Name);
        Assert.Equal("eel", eels.Adjective);
        Assert.False(eels.KeepNameCase);
        Assert.Equal(
            new[] { ("class", "Actinopterygii"), ("order", "Anguilliformes") },
            eels.Filters!.Select(f => (f.Rank, f.Value)));
        Assert.Same(expanded.Groups["fishes"].Display, eels.Display);
        Assert.Same(expanded.Groups["fishes"].SizeBudget, eels.SizeBudget);
        Assert.Equal("fishes", expanded.GeneratedBy["anguilliformes"]);
        Assert.False(expanded.GeneratedBy.ContainsKey("sharks"));
    }

    [Fact]
    public void Expand_NameDefaultsToTheValueAndAdjectiveToTheName() {
        var perch = TaxaSubGroups.Expand(Groups()).Groups["perciformes"];
        Assert.Equal("Perciformes", perch.Name);
        Assert.Equal("Perciformes", perch.Adjective);
    }

    [Fact]
    public void Expand_KeepNameCaseAppliesToTheWholeBlock() {
        var groups = TaxaSubGroups.Expand(Groups()).Groups;
        Assert.True(groups["fabales"].KeepNameCase);
        Assert.False(groups["perciformes"].KeepNameCase);
    }

    [Fact]
    public void Expand_AppendsSubGroupsToExplicitChildren() {
        var fishes = TaxaSubGroups.Expand(Groups()).Groups["fishes"];
        Assert.Equal(new[] { "sharks", "anguilliformes", "perciformes" }, fishes.Children);
    }

    [Fact]
    public void Expand_GivesEverySubGroupTheBlocksPresets() {
        var lists = TaxaSubGroups.Expand(Groups()).Lists;
        Assert.Equal(new[] { "anguilliformes", "perciformes", "fabales" }, lists.Select(l => l.Group));
        Assert.All(lists.Take(2), l => Assert.Equal(new[] { "lc", "dd" }, l.Presets));
        Assert.Equal(new[] { "lc" }, lists[2].Presets);
    }

    [Fact]
    public void Expand_SkipsAnIdThatIsAlreadyAGroup() {
        var groups = Groups();
        groups["perciformes"] = new TaxaGroupDefinition { Name = "Hand-written perch group" };
        var expanded = TaxaSubGroups.Expand(groups);
        Assert.Equal("Hand-written perch group", expanded.Groups["perciformes"].Name);
        Assert.Contains(expanded.Warnings, w => w.Contains("'perciformes'"));
        Assert.DoesNotContain(expanded.Lists, l => l.Group == "perciformes");
    }

    [Fact]
    public void InsertListEntries_PutsSubGroupListsAfterTheParentsEntry() {
        var lists = new List<WikipediaListDefinitionRaw> {
            new() { TaxaGroup = "birds" },
            new() { TaxaGroup = "fishes" },
            new() { TaxaGroup = "insects" },
        };
        var generated = new[] {
            new GeneratedListEntry("fishes", "anguilliformes", new[] { "lc" }),
            new GeneratedListEntry("fishes", "perciformes", new[] { "lc" }),
            new GeneratedListEntry("plants", "fabales", new[] { "lc" }),
        };
        Assert.Equal(
            new[] { "birds", "fishes", "anguilliformes", "perciformes", "insects", "fabales" },
            TaxaSubGroups.InsertListEntries(lists, generated).Select(l => l.TaxaGroup));
    }

    [Fact]
    public void Loader_LinksTheSubGroupListsFromTheParentPage() {
        var config = LoadRules();
        var parent = config.Lists.Single(l => l.Id == "fishes-lc");
        Assert.Equal(new[] { "sharks-lc", "anguilliformes-lc", "perciformes-lc" }, parent.SubLists.Select(s => s.Id));
        Assert.Equal("List of least concern eels", config.Lists.Single(l => l.Id == "anguilliformes-lc").Title);
        Assert.Equal("List of least concern perciformes", config.Lists.Single(l => l.Id == "perciformes-lc").Title);
        var fabales = config.Lists.Single(l => l.Id == "fabales-lc");
        Assert.Equal("List of least concern Fabales", fabales.Title);
        Assert.Equal("List_of_least_concern_Fabales.wikitext", fabales.OutputFile);
        Assert.DoesNotContain(config.ChildLinkNotes, n => n.Outcome == ChildLinkOutcome.NoList && n.ParentListId == "fishes-lc");
    }

    private static Dictionary<string, TaxaGroupDefinition> Groups() {
        var deserializer = new YamlDotNet.Serialization.DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(YamlDotNet.Serialization.NamingConventions.UnderscoredNamingConvention.Instance)
            .Build();
        return deserializer.Deserialize<TaxaGroupsFile>(GroupsYaml).Groups;
    }

    private static WikipediaListConfig LoadRules() {
        var dir = Directory.CreateTempSubdirectory("bb3-subgroups-block-");
        try {
            File.WriteAllText(Path.Combine(dir.FullName, "taxa-groups.yml"), GroupsYaml);
            File.WriteAllText(Path.Combine(dir.FullName, "list-presets.yml"), PresetsYaml);
            File.WriteAllText(Path.Combine(dir.FullName, "wikipedia-lists.yml"), ListsYaml);
            return new WikipediaListDefinitionLoader().Load(Path.Combine(dir.FullName, "wikipedia-lists.yml"));
        } finally {
            dir.Delete(recursive: true);
        }
    }

    private const string GroupsYaml = """
        groups:
          fishes:
            name: "Fishes"
            display:
              listing_style: ScientificNameFocus
            size_budget:
              max_entries: 3600
            filters:
              - rank: class
                value: Actinopterygii
            children: [sharks]
            sub_groups:
              rank: order
              presets: [lc, dd]
              groups:
                Anguilliformes: { name: "Eels", adjective: "eel" }
                Perciformes: {}
          plants:
            name: "Plants"
            filters:
              - rank: kingdom
                value: Plantae
            sub_groups:
              rank: order
              presets: [lc]
              keep_name_case: true
              groups:
                Fabales: {}
          sharks:
            name: "Sharks"
            filters:
              - rank: class
                value: Chondrichthyes
        """;

    private const string PresetsYaml = """
        presets:
          lc:
            title_template: "List of least concern {taxa_name_lower}"
            output_template: "List_of_least_concern_{taxa_slug}.wikitext"
            sections:
              - key: lc
                heading: "Least concern"
                statuses:
                  - code: LC
          dd:
            title_template: "List of data deficient {taxa_name_lower}"
            output_template: "List_of_data_deficient_{taxa_slug}.wikitext"
            sections:
              - key: dd
                heading: "Data deficient"
                statuses:
                  - code: DD
        """;

    private const string ListsYaml = """
        lists:
          - taxa_group: fishes
            presets: [lc]
          - taxa_group: sharks
            presets: [lc]
          - taxa_group: plants
            presets: [lc]
        """;
}
