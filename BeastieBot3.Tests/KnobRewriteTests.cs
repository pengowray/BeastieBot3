using BeastieBot3.Web.Endpoints;

namespace BeastieBot3.Tests;

// Pins the conservative YAML line-surgery behind the web "tuning knobs": size_budget on a group in
// taxa-groups.yml, and category_split on a list entry in wikipedia-lists.yml. These rewrite the user's
// config files in place, so the exact emitted shape — and leaving every other line untouched — matters.
public class KnobRewriteTests {
    // ---- TryReplaceGroupKeyFlow (size_budget on a group) ----

    private const string GroupsYaml =
        "groups:\n" +
        "  mammals:\n" +
        "    name: mammals\n" +
        "    filters:\n" +
        "      - rank: class\n" +
        "        value: MAMMALIA\n" +
        "  birds:\n" +
        "    name: birds\n";

    [Fact]
    public void GroupKeyFlow_InsertsWhenAbsent() {
        var ok = TaxaGroupingEndpoints.TryReplaceGroupKeyFlow(
            GroupsYaml, "mammals", "size_budget", "{ max_entries: 8000 }", out var updated, out var err);
        Assert.True(ok, err);
        Assert.Contains("    size_budget: { max_entries: 8000 }", updated);
        Assert.Contains("  birds:", updated); // sibling group untouched
    }

    [Fact]
    public void GroupKeyFlow_ReplacesExistingFlow() {
        var yaml = GroupsYaml.Replace("    name: mammals\n",
            "    name: mammals\n    size_budget: { max_entries: 100 }\n");
        var ok = TaxaGroupingEndpoints.TryReplaceGroupKeyFlow(
            yaml, "mammals", "size_budget", "{ max_entries: 9000 }", out var updated, out var err);
        Assert.True(ok, err);
        Assert.Contains("max_entries: 9000", updated);
        Assert.DoesNotContain("max_entries: 100", updated);
    }

    [Fact]
    public void GroupKeyFlow_ReplacesExistingBlock() {
        var yaml = GroupsYaml.Replace("    name: mammals\n",
            "    name: mammals\n    size_budget:\n      max_entries: 100\n");
        var ok = TaxaGroupingEndpoints.TryReplaceGroupKeyFlow(
            yaml, "mammals", "size_budget", "{ max_entries: 9000 }", out var updated, out var err);
        Assert.True(ok, err);
        Assert.Contains("    size_budget: { max_entries: 9000 }", updated);
        Assert.DoesNotContain("      max_entries:", updated); // nested block line removed
    }

    [Fact]
    public void GroupKeyFlow_UnknownGroup_Fails() {
        var ok = TaxaGroupingEndpoints.TryReplaceGroupKeyFlow(
            GroupsYaml, "nonexistent", "size_budget", "{ max_entries: 1 }", out _, out var err);
        Assert.False(ok);
        Assert.Contains("not found", err);
    }

    // ---- TrySetListCategorySplit (category_split on a list entry) ----

    private const string ListsYaml =
        "lists:\n" +
        "  - taxa_group: mammals\n" +
        "    presets: [threatened, cr, en]\n" +
        "  - taxa_group: birds\n" +
        "    presets: [cr]\n";

    [Fact]
    public void CategorySplit_InsertsWhenAbsent() {
        var ok = TaxaGroupingEndpoints.TrySetListCategorySplit(
            ListsYaml, "mammals", "separate", out var updated, out var err);
        Assert.True(ok, err);
        Assert.Contains("    category_split: separate", updated);
        var birdsIdx = updated.IndexOf("taxa_group: birds");
        Assert.DoesNotContain("category_split", updated.Substring(birdsIdx)); // birds entry untouched
    }

    [Fact]
    public void CategorySplit_ReplacesExisting() {
        var yaml = ListsYaml.Replace("    presets: [threatened, cr, en]\n",
            "    presets: [threatened, cr, en]\n    category_split: separate\n");
        var ok = TaxaGroupingEndpoints.TrySetListCategorySplit(
            yaml, "mammals", "all-status", out var updated, out var err);
        Assert.True(ok, err);
        Assert.Contains("category_split: all-status", updated);
        Assert.DoesNotContain("category_split: separate", updated);
    }

    [Fact]
    public void CategorySplit_NullRemovesOverride() {
        var yaml = ListsYaml.Replace("    presets: [threatened, cr, en]\n",
            "    presets: [threatened, cr, en]\n    category_split: separate\n");
        var ok = TaxaGroupingEndpoints.TrySetListCategorySplit(
            yaml, "mammals", null, out var updated, out var err);
        Assert.True(ok, err);
        Assert.DoesNotContain("category_split", updated);
        Assert.Contains("presets: [threatened, cr, en]", updated); // presets preserved
    }

    [Fact]
    public void CategorySplit_UnknownGroup_Fails() {
        var ok = TaxaGroupingEndpoints.TrySetListCategorySplit(
            ListsYaml, "nonexistent", "separate", out _, out var err);
        Assert.False(ok);
        Assert.Contains("not found", err);
    }
}
