using System;
using System.IO;
using System.Linq;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins that the list loader reads the tree settings: the shipped intermediate_groups defaults, and a
// list's own auto_split: and intermediate_groups: blocks (auto_split was once dropped silently).
public sealed class TreeConfigLoaderTests : IDisposable {
    private readonly string _dir = Directory.CreateTempSubdirectory("tree-config-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Fact]
    public void ShippedDefaults_TurnOnIntermediateGroups() {
        var config = new WikipediaListDefinitionLoader().Load(Path.Combine(AppContext.BaseDirectory, "rules", "wikipedia-lists.yml"));

        var groups = config.Defaults.IntermediateGroups;
        Assert.NotNull(groups);
        Assert.True(groups!.Enabled);
        Assert.Equal(30, groups.MinItems);
        Assert.Equal(6, groups.MinAnchors);
        Assert.Equal(12, groups.MaxGroups);
        Assert.Equal(0.85, groups.MaxDominance);
        Assert.Equal(5, groups.MinGroupSize);
        Assert.False(groups.LookThroughDominant);
    }

    [Fact]
    public void ListLevelBlocks_AreRead() {
        var path = Path.Combine(_dir, "wikipedia-lists.yml");
        File.WriteAllText(path, """
            defaults:
              auto_split:
                threshold: 30
            lists:
              - id: plain
                title: Plain
              - id: tuned
                title: Tuned
                auto_split:
                  enabled: false
                intermediate_groups:
                  min_items: 50
            """);

        var config = new WikipediaListDefinitionLoader().Load(path);

        var plain = config.Lists.Single(l => l.Id == "plain");
        var tuned = config.Lists.Single(l => l.Id == "tuned");
        Assert.Null(plain.AutoSplit);
        Assert.False(tuned.AutoSplit!.Enabled);
        Assert.Equal(50, tuned.IntermediateGroups!.MinItems);
        Assert.Same(tuned.AutoSplit, TaxonGroupingHelper.ResolveAutoSplitConfig(tuned, config.Defaults));
        Assert.Same(config.Defaults.AutoSplit, TaxonGroupingHelper.ResolveAutoSplitConfig(plain, config.Defaults));
    }
}
