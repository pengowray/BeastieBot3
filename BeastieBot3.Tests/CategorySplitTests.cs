using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins the category_split tuning shorthand -> preset fan-out expansion.
public class CategorySplitTests {
    [Fact]
    public void CombinedThreatened_DropsSeparateCrEnVu() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { TaxaGroup = "plants", CategorySplit = "combined-threatened" });
        Assert.Equal(new[] { "threatened", "nt", "dd", "lc", "ew", "ex" }, presets);
    }

    [Fact]
    public void Merged_FoldsExtinctionIntoOneExtinctCombinedPage() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { TaxaGroup = "plants", CategorySplit = "merged" });
        Assert.Equal(new[] { "extinct-combined", "threatened", "nt", "dd", "lc" }, presets);
    }

    [Fact]
    public void Separate_IsOnePagePerCategory() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { CategorySplit = "separate" });
        Assert.Equal(new[] { "cr", "en", "vu", "nt", "dd", "lc", "ew", "ex" }, presets);
    }

    [Fact]
    public void AllStatus_IsSinglePage() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { CategorySplit = "all-status" });
        Assert.Equal(new[] { "all-status" }, presets);
    }

    [Fact]
    public void CategorySplit_OverridesExplicitPresets() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { Presets = new() { "cr", "en" }, CategorySplit = "all-status" });
        Assert.Equal(new[] { "all-status" }, presets);
    }

    [Fact]
    public void NoCategorySplit_KeepsExplicitPresets() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { Presets = new() { "threatened", "cr", "en" } });
        Assert.Equal(new[] { "threatened", "cr", "en" }, presets);
    }

    [Fact]
    public void UnknownValue_FallsBackToExplicitPresets() {
        var presets = WikipediaListDefinitionLoader.ResolveEffectivePresets(
            new WikipediaListDefinitionRaw { Presets = new() { "cr" }, CategorySplit = "bogus" });
        Assert.Equal(new[] { "cr" }, presets);
    }
}
