using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Wikipedia;
using Xunit;

namespace BeastieBot3.Tests;

public sealed class OldDraftPlannerTests {
    private const string Base = "User:Beastie Bot/Draft 2026";

    private static string Draft(string article) =>
        DraftPageBuilder.BuildDraft("Some list text.", article, Base, new System.DateTime(2026, 10, 2));

    [Fact]
    public void CaseOnlyRename_IsMoved_OtherOldDraftsAreBlanked_OthersLeftAlone() {
        var subpages = new[] {
            new SubpageInfo($"{Base}/List of Fungi by conservation status", false),
            new SubpageInfo($"{Base}/List of grasses", false),
            new SubpageInfo($"{Base}/Notes", false),
            new SubpageInfo($"{Base}/List of Mosses by conservation status", true),
            new SubpageInfo($"{Base}/List of critically endangered mammals", false),
        };
        var texts = new Dictionary<string, string?> {
            [$"{Base}/List of Fungi by conservation status"] = Draft("List of Fungi by conservation status"),
            [$"{Base}/List of grasses"] = Draft("List of grasses"),
            [$"{Base}/Notes"] = "Things to check before posting.",
        };
        var current = new[] { $"{Base}/List of fungi by conservation status", $"{Base}/List of critically endangered mammals" };

        var steps = OldDraftPlanner.Plan(subpages, texts, current, _ => false, DraftPageBuilder.RetiredDraftText(Base));

        Assert.Equal(3, steps.Count);
        var move = Assert.Single(steps, s => s.Action == OldDraftAction.Move);
        Assert.Equal($"{Base}/List of fungi by conservation status", move.NewTitle);
        Assert.Equal($"{Base}/List of grasses", Assert.Single(steps, s => s.Action == OldDraftAction.Blank).Title);
        Assert.Equal($"{Base}/Notes", Assert.Single(steps, s => s.Action == OldDraftAction.LeaveAlone).Title);
    }

    [Fact]
    public void RenameWhoseNewPageExists_IsBlanked_AndABlankedPageIsLeftAsItIs() {
        var subpages = new[] {
            new SubpageInfo($"{Base}/List of Fungi by conservation status", false),
            new SubpageInfo($"{Base}/List of grasses", false),
        };
        var texts = new Dictionary<string, string?> {
            [$"{Base}/List of Fungi by conservation status"] = Draft("List of Fungi by conservation status"),
            [$"{Base}/List of grasses"] = DraftPageBuilder.RetiredDraftText(Base),
        };
        var current = new[] { $"{Base}/List of fungi by conservation status" };

        var steps = OldDraftPlanner.Plan(subpages, texts, current, _ => true, DraftPageBuilder.RetiredDraftText(Base));

        Assert.Equal(OldDraftAction.Blank, Assert.Single(steps).Action);
    }
}
