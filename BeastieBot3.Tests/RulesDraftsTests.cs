using BeastieBot3.Web.Endpoints;

namespace BeastieBot3.Tests;

// The web Rules editor keeps a draft only for a file with unapplied edits, reads every other file from
// rules/, and refuses to let a draft undo a newer change to rules/ without an explicit force.
public class RulesDraftsTests : IDisposable {
    private readonly string _root = Directory.CreateTempSubdirectory("bb3-drafts-").FullName;
    private string Source => Path.Combine(_root, "rules");
    private string Draft => Path.Combine(_root, "rules-draft");
    private RulesDrafts Drafts => new(Source, Draft);

    public RulesDraftsTests() {
        Directory.CreateDirectory(Path.Combine(Source, "wikipedia", "templates"));
        File.WriteAllText(Path.Combine(Source, "taxa-groups.yml"), "groups: {}\n");
        File.WriteAllText(Path.Combine(Source, "wikipedia", "templates", "status.mustache"), "As of <? year ?>\r\n");
    }

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public void WithoutADraft_ReadsTheRulesFile() {
        Assert.False(Drafts.HasDraft("taxa-groups.yml"));
        Assert.Equal("groups: {}\n", Drafts.ReadText("taxa-groups.yml"));
        Assert.False(Directory.Exists(Draft));
    }

    [Fact]
    public void Write_KeepsTheEditAsADraftAndLeavesRulesAlone() {
        Assert.True(Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n"));
        Assert.True(Drafts.HasDraft("taxa-groups.yml"));
        Assert.Equal("groups: { a: {} }\n", Drafts.ReadText("taxa-groups.yml"));
        Assert.Equal("groups: {}\n", File.ReadAllText(Path.Combine(Source, "taxa-groups.yml")));
        Assert.Equal(RulesDrafts.DraftStatus.Modified, Drafts.StatusOf("taxa-groups.yml"));
    }

    [Fact]
    public void Write_SameTextAsRules_KeepsNoDraft() {
        Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n");
        // Saving the rules/ text again, with other line endings, is not an edit.
        Assert.False(Drafts.Write("taxa-groups.yml", "groups: {}\r\n"));
        Assert.False(Drafts.HasDraft("taxa-groups.yml"));
        Assert.Empty(Drafts.DraftFiles());
    }

    [Fact]
    public void Apply_CopiesTheDraftToRulesAndDeletesIt() {
        Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n");
        Assert.Null(Drafts.Apply("taxa-groups.yml", force: false));
        Assert.Equal("groups: { a: {} }\n", File.ReadAllText(Path.Combine(Source, "taxa-groups.yml")));
        Assert.False(Drafts.HasDraft("taxa-groups.yml"));
        Assert.Empty(Directory.EnumerateFiles(Draft, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public void RulesChangedAfterTheDraftStarted_ApplyNeedsForce() {
        Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n");
        File.WriteAllText(Path.Combine(Source, "taxa-groups.yml"), "groups: { b: {} }\n"); // e.g. a git pull
        Assert.Equal(RulesDrafts.DraftStatus.SourceChanged, Drafts.StatusOf("taxa-groups.yml"));
        Assert.NotNull(Drafts.Apply("taxa-groups.yml", force: false));
        Assert.Equal("groups: { b: {} }\n", File.ReadAllText(Path.Combine(Source, "taxa-groups.yml")));

        Assert.Null(Drafts.Apply("taxa-groups.yml", force: true));
        Assert.Equal("groups: { a: {} }\n", File.ReadAllText(Path.Combine(Source, "taxa-groups.yml")));
    }

    // The draft folder used to be a full copy of rules/. A copy that matches rules/ holds no edits and
    // is deleted; one that differs may be an old copy, so applying it needs force.
    [Fact]
    public void OldFullCopy_MatchingFilesAreDeletedAndOthersNeedForce() {
        Directory.CreateDirectory(Path.Combine(Draft, "wikipedia", "templates"));
        File.WriteAllText(Path.Combine(Draft, "wikipedia", "templates", "status.mustache"), "As of <? year ?>\n");
        File.WriteAllText(Path.Combine(Draft, "taxa-groups.yml"), "groups: { old: {} }\n");

        Assert.Equal(new[] { "taxa-groups.yml" }, Drafts.DraftFiles());
        Assert.False(File.Exists(Path.Combine(Draft, "wikipedia", "templates", "status.mustache")));
        Assert.Equal(RulesDrafts.DraftStatus.UnknownBase, Drafts.StatusOf("taxa-groups.yml"));
        Assert.NotNull(Drafts.Apply("taxa-groups.yml", force: false));
    }

    [Fact]
    public void Discard_DeletesTheDraft() {
        Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n");
        Drafts.Discard("taxa-groups.yml");
        Assert.False(Drafts.HasDraft("taxa-groups.yml"));
        Assert.Equal("groups: {}\n", Drafts.ReadText("taxa-groups.yml"));
    }

    [Fact]
    public void MaterializeMerged_IsRulesWithTheDraftsOnTop() {
        Drafts.Write("taxa-groups.yml", "groups: { a: {} }\n");
        var merged = Drafts.MaterializeMerged();
        try {
            Assert.Equal("groups: { a: {} }\n", File.ReadAllText(Path.Combine(merged, "taxa-groups.yml")));
            Assert.True(File.Exists(Path.Combine(merged, "wikipedia", "templates", "status.mustache")));
            Assert.False(File.Exists(Path.Combine(merged, ".bases.json")));
        } finally {
            Directory.Delete(merged, recursive: true);
        }
    }

    [Fact]
    public void PathOutsideTheRulesFolder_IsRefused() {
        Assert.Null(Drafts.EffectivePath("../outside.yml"));
        Assert.Throws<ArgumentException>(() => Drafts.Write("../outside.yml", "x"));
    }
}
