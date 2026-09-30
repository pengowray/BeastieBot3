using System.Text.Json;
using BeastieBot3.Web.Endpoints;
using Microsoft.AspNetCore.Http;

namespace BeastieBot3.Tests;

// Pins "Save sub-groups" on the Taxa grouping page: it adds the ticked rows' groups to the parent's
// children: list in the draft taxa-groups.yml and removes the unticked rows' groups, and keeps every
// other existing child. It used to replace the whole list with the ticked rows, which silently dropped
// sub-groups that have no row in the counts table (a system-filter group, or one at another rank).
public class ChildrenRewriteTests {
    private const string GroupsYaml =
        "groups:\n" +
        "  mammals:\n" +
        "    name: mammals\n" +
        "    filters:\n" +
        "      - rank: class\n" +
        "        value: MAMMALIA\n" +
        "    children: [bats, rodents, aquatic-mammals]\n" +
        "  bats:\n" +
        "    name: bats\n" +
        "    filters:\n" +
        "      - rank: order\n" +
        "        value: CHIROPTERA\n" +
        "  rodents:\n" +
        "    name: rodents\n" +
        "    filters:\n" +
        "      - rank: order\n" +
        "        value: RODENTIA\n" +
        "  primates:\n" +
        "    name: primates\n" +
        "    filters:\n" +
        "      - rank: order\n" +
        "        value: PRIMATES\n" +
        "  aquatic-mammals:\n" +
        "    name: aquatic mammals\n" +
        "    filters:\n" +
        "      - rank: class\n" +
        "        value: MAMMALIA\n" +
        "      - system: Marine\n" +
        "    children: [rodents]\n";

    // ---- MergeChildren ----

    [Fact]
    public void Merge_KeepsChildrenNamedInNeitherList() {
        var merged = TaxaGroupingEndpoints.MergeChildren(
            new[] { "bats", "rodents", "aquatic-mammals" }, add: new[] { "primates" }, remove: new[] { "rodents" });
        Assert.Equal(new[] { "bats", "aquatic-mammals", "primates" }, merged);
    }

    [Fact]
    public void Merge_AddAlreadyPresent_KeepsOrderAndDoesNotDuplicate() {
        var merged = TaxaGroupingEndpoints.MergeChildren(
            new[] { "bats", "rodents" }, add: new[] { "rodents", "bats" }, remove: Array.Empty<string>());
        Assert.Equal(new[] { "bats", "rodents" }, merged);
    }

    [Fact]
    public void Merge_NameInBothLists_IsAdded() {
        // Two rows can map to one group; if either is ticked, the group stays a sub-group.
        var merged = TaxaGroupingEndpoints.MergeChildren(
            new[] { "bats" }, add: new[] { "rodents" }, remove: new[] { "rodents", "bats" });
        Assert.Equal(new[] { "rodents" }, merged);
    }

    [Fact]
    public void Merge_NoExistingChildren_AppendsInRequestOrder() {
        var merged = TaxaGroupingEndpoints.MergeChildren(
            null, add: new[] { " rodents ", "bats", "" }, remove: Array.Empty<string>());
        Assert.Equal(new[] { "rodents", "bats" }, merged);
    }

    // ---- TryUpdateChildren (parse, merge, rewrite, round-trip) ----

    [Fact]
    public void Update_KeepsSubGroupWithNoRowInTheTable() {
        // The counts table at rank order shows rows for bats, rodents and primates; aquatic-mammals
        // (a system-filter group) has no row, so it is in neither list and must survive the save.
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "mammals",
            add: new[] { "bats", "primates" }, remove: new[] { "rodents" },
            out var updated, out var children, out var changed, out var err);
        Assert.True(ok, err);
        Assert.True(changed);
        Assert.Equal(new[] { "bats", "aquatic-mammals", "primates" }, children);
        Assert.Contains("    children: [bats, aquatic-mammals, primates]\n", updated);
        Assert.Contains("    children: [rodents]\n", updated); // aquatic-mammals' own children untouched
    }

    [Fact]
    public void Update_NoChange_LeavesTextAsItWas() {
        var yaml = GroupsYaml.Replace("    children: [bats, rodents, aquatic-mammals]\n",
            "    children:\n      - bats\n      - rodents\n");
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(yaml, "mammals",
            add: new[] { "bats" }, remove: new[] { "primates" },
            out var updated, out var children, out var changed, out var err);
        Assert.True(ok, err);
        Assert.False(changed);
        Assert.Equal(new[] { "bats", "rodents" }, children);
        Assert.Equal(yaml, updated); // the block-style list is not restyled by a no-op save
    }

    [Fact]
    public void Update_BlockStyleChildren_RewrittenAsFlow() {
        var yaml = GroupsYaml.Replace("    children: [bats, rodents, aquatic-mammals]\n",
            "    children:\n      - bats\n      - aquatic-mammals\n");
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(yaml, "mammals",
            add: new[] { "rodents" }, remove: Array.Empty<string>(),
            out var updated, out var children, out var changed, out var err);
        Assert.True(ok, err);
        Assert.True(changed);
        Assert.Equal(new[] { "bats", "aquatic-mammals", "rodents" }, children);
        Assert.Contains("    children: [bats, aquatic-mammals, rodents]\n", updated);
        Assert.DoesNotContain("      - bats", updated);
    }

    [Fact]
    public void Update_InsertsChildrenLineWhenAbsent() {
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "primates",
            add: new[] { "bats" }, remove: Array.Empty<string>(),
            out var updated, out var children, out var changed, out var err);
        Assert.True(ok, err);
        Assert.True(changed);
        Assert.Equal(new[] { "bats" }, children);
        Assert.Contains("  primates:\n    children: [bats]\n", updated);
    }

    [Fact]
    public void Update_RemovingLastChild_DropsTheLine() {
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "aquatic-mammals",
            add: Array.Empty<string>(), remove: new[] { "rodents" },
            out var updated, out var children, out var changed, out var err);
        Assert.True(ok, err);
        Assert.True(changed);
        Assert.Empty(children);
        Assert.DoesNotContain("    children: [rodents]", updated);
        Assert.Contains("    children: [bats, rodents, aquatic-mammals]", updated); // mammals untouched
    }

    [Fact]
    public void Update_SelfAsChild_Fails() {
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "mammals",
            add: new[] { "mammals" }, remove: Array.Empty<string>(),
            out var updated, out _, out var changed, out var err);
        Assert.False(ok);
        Assert.False(changed);
        Assert.Equal(GroupsYaml, updated);
        Assert.Contains("itself", err);
    }

    [Fact]
    public void Update_UnknownGroupInAdd_Fails() {
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "mammals",
            add: new[] { "whales" }, remove: Array.Empty<string>(),
            out var updated, out _, out _, out var err);
        Assert.False(ok);
        Assert.Equal(GroupsYaml, updated);
        Assert.Contains("whales", err);
    }

    [Fact]
    public void Update_UnknownParent_Fails() {
        var ok = TaxaGroupingEndpoints.TryUpdateChildren(GroupsYaml, "birds",
            add: new[] { "bats" }, remove: Array.Empty<string>(),
            out _, out _, out _, out var err);
        Assert.False(ok);
        Assert.Contains("not found", err);
    }

    // ---- SaveChildren (the POST handler, against a draft file on disk) ----

    // A page loaded before the request shape changed posts {group, children}, which binds to
    // Add = Remove = null. That must be refused, not answered with a 200 that saved nothing.
    [Fact]
    public void Save_OldRequestShape_IsRefusedAndDraftUnchanged() {
        WithDraft(file => {
            var result = TaxaGroupingEndpoints.SaveChildren(
                new TaxaGroupingEndpoints.ChildrenRequest { Group = "mammals" }, file);
            Assert.Equal(400, StatusOf(result));
            Assert.Contains("out of date", Body(result).GetProperty("error").GetString());
            Assert.Equal(GroupsYaml, File.ReadAllText(file));
        });
    }

    // The page sends two empty arrays when the table has no tickboxes (e.g. the Parent at its own rank):
    // a valid save that changes nothing.
    [Fact]
    public void Save_EmptyAddAndRemove_IsNoOpSave() {
        WithDraft(file => {
            var result = TaxaGroupingEndpoints.SaveChildren(
                new TaxaGroupingEndpoints.ChildrenRequest { Group = "mammals", Add = Array.Empty<string>(), Remove = Array.Empty<string>() }, file);
            Assert.Equal(200, StatusOf(result) ?? 200);
            Assert.False(Body(result).GetProperty("changed").GetBoolean());
            Assert.Equal(GroupsYaml, File.ReadAllText(file));
        });
    }

    [Fact]
    public void Save_TickedAndUntickedRows_WritesDraft() {
        WithDraft(file => {
            var result = TaxaGroupingEndpoints.SaveChildren(
                new TaxaGroupingEndpoints.ChildrenRequest { Group = "mammals", Add = new[] { "primates" }, Remove = new[] { "rodents" } }, file);
            Assert.Equal(200, StatusOf(result) ?? 200);
            Assert.True(Body(result).GetProperty("changed").GetBoolean());
            Assert.Contains("    children: [bats, aquatic-mammals, primates]\n", File.ReadAllText(file));
        });
    }

    private static void WithDraft(Action<string> test) {
        var dir = Directory.CreateTempSubdirectory("bb3-children-");
        try {
            var file = Path.Combine(dir.FullName, "taxa-groups.yml");
            File.WriteAllText(file, GroupsYaml);
            test(file);
        } finally {
            dir.Delete(recursive: true);
        }
    }

    private static int? StatusOf(IResult result) => ((IStatusCodeHttpResult)result).StatusCode;

    private static JsonElement Body(IResult result) =>
        JsonSerializer.SerializeToElement(((IValueHttpResult)result).Value);
}
