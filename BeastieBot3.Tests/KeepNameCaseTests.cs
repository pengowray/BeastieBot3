using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// A taxa group named by a scientific name (an order such as Malpighiales) sets keep_name_case, so its
// list title and file name keep the capital letter. wikipedia post-drafts takes the article title
// from the file name, so a lower-case file name would post "List of threatened malpighiales".
public class KeepNameCaseTests {
    [Fact]
    public void KeepNameCase_KeepsCapitalInTitleAndFileName() {
        var list = Load().Lists.Single(l => l.Id == "malpighiales-threatened");
        Assert.Equal("List of threatened Malpighiales", list.Title);
        Assert.Equal("List_of_threatened_Malpighiales.wikitext", list.OutputFile);
    }

    [Fact]
    public void WithoutKeepNameCase_TitleAndFileNameAreLowerCase() {
        var list = Load().Lists.Single(l => l.Id == "ray-finned-fishes-threatened");
        Assert.Equal("List of threatened ray-finned fishes", list.Title);
        Assert.Equal("List_of_threatened_ray_finned_fishes.wikitext", list.OutputFile);
    }

    [Fact]
    public void ParentLinksTheSubGroupListByItsCapitalisedTitle() {
        var parent = Load().Lists.Single(l => l.Id == "dicots-threatened");
        var link = Assert.Single(parent.SubLists);
        Assert.Equal("List of threatened Malpighiales", link.WikiTitle);
        Assert.Equal("Malpighiales", link.DisplayName);
    }

    private static WikipediaListConfig Load() {
        var dir = Directory.CreateTempSubdirectory("bb3-keepcase-");
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
          dicots:
            name: "Dicotyledons"
            filters:
              - rank: class
                value: Magnoliopsida
            children: [malpighiales]
          malpighiales:
            name: "Malpighiales"
            keep_name_case: true
            filters:
              - rank: class
                value: Magnoliopsida
              - rank: order
                value: Malpighiales
          ray-finned-fishes:
            name: "Ray-finned fishes"
            filters:
              - rank: class
                value: Actinopterygii
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
        """;

    private const string ListsYaml = """
        lists:
          - taxa_group: dicots
            presets: [threatened]
          - taxa_group: malpighiales
            presets: [threatened]
          - taxa_group: ray-finned-fishes
            presets: [threatened]
        """;
}
