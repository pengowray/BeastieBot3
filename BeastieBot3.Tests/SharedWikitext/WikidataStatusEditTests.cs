using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins the QuickStatements batch the public site offers to bring a taxon item's IUCN conservation
// status (P141) up to the latest global assessment: the value mapping, the reference, the two
// choices for a changed status, and the cases where no batch can be made. Item and taxon ids are
// real cases from the caches of 3 October 2026; the statement GUIDs are made up.
public class WikidataStatusEditTests {
    private const string Tab = "\t";

    private static string L(params string[] columns) => string.Join(Tab, columns);

    // A statement with one reference that cites IUCN (stated in an edition of the Red List).
    private static WikidataStatusStatement S(string guid, string value, string rank = "normal", params string[] statedIn) =>
        new($"Q309238${guid}", value, rank, statedIn, TaxonIds: [], References: 1, CitesIucn: true);

    // A statement whose references cite another source (references > 0), or that has none.
    private static WikidataStatusStatement Other(string guid, string value, string rank = "normal", int references = 1) =>
        new($"Q309238${guid}", value, rank, [], TaxonIds: [], References: references, CitesIucn: false);

    // A statement with a reference that has this IUCN taxon ID (P627).
    private static WikidataStatusStatement Cited(string guid, string value, string taxonId, string rank = "normal") =>
        new($"Q309238${guid}", value, rank, ["Q115962546"], TaxonIds: [taxonId], References: 1, CitesIucn: true);

    private const string G1 = "11111111-1111-1111-1111-111111111111";
    private const string G2 = "22222222-2222-2222-2222-222222222222";
    private const string G3 = "33333333-3333-3333-3333-333333333333";

    private static readonly DateOnly Downloaded = new(2026, 8, 13);

    // Taxon 2725 (EN in 2026-1); its item Q309238 says critically endangered.
    private static StatusEditRequest Request(string category, params WikidataStatusStatement[] statements) => new() {
        TaxonItemQid = "Q309238",
        Statements = statements,
        Category = category,
        TaxonId = 2725,
        AssessmentId = 9461,
        Retrieved = Downloaded,
    };

    private const string Sources = "S627\t\"2725\"\tS854\t\"https://www.iucnredlist.org/species/2725/9461\"\tS813\t+2026-08-13T00:00:00Z/11";

    // ------------------------------------------------------------ value mapping

    [Theory]
    [InlineData("EX", "Q237350")]
    [InlineData("EW", "Q239509")]
    [InlineData("CR", "Q219127")]
    [InlineData("EN", "Q96377276")]
    [InlineData("VU", "Q278113")]
    [InlineData("NT", "Q719675")]
    [InlineData("LC", "Q211005")]
    [InlineData("DD", "Q3245245")]
    [InlineData("LR/nt", "Q719675")]
    [InlineData("LR/lc", "Q211005")]
    public void TargetQid_MapsTheCategory(string category, string qid) {
        Assert.Equal(qid, WikidataStatusEdit.TargetQid(category));
    }

    [Theory]
    [InlineData("LR/cd")]
    [InlineData("nt")]   // Not Threatened, pre-1994: not Near Threatened
    [InlineData("V")]
    [InlineData("CR(PE)")]
    [InlineData("")]
    [InlineData(null)]
    public void TargetQid_NoValue(string? category) {
        Assert.Null(WikidataStatusEdit.TargetQid(category));
    }

    [Fact]
    public void SharedValues_AreTheDryRunsValues() {
        Assert.Equal(WikidataIucnStatusValues.AllowedValues, WikidataStatusValues.AllowedValues);
        Assert.Equal(WikidataIucnStatusValues.CriticallyEndangeredQid, WikidataStatusValues.CriticallyEndangeredQid);
        Assert.Equal("critically endangered", WikidataStatusValues.Describe("Q219127")!.LabelEn);
    }

    // ------------------------------------------------------------ outcomes

    [Fact]
    public void Differs_Replace_AddsTheNewValueThenRemovesTheOld() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q219127")));

        Assert.Equal(StatusEditOutcome.Differs, plan.Outcome);
        Assert.True(plan.AddsValue);
        Assert.True(plan.AddsReference);
        Assert.Equal(new[] {
            L("Q309238", "P141", "Q96377276", Sources),
            L("-STATEMENT", "Q309238$" + G1),
        }, plan.Commands);
        Assert.Equal("Q309238$" + G1, Assert.Single(plan.Removes).Id);
        Assert.Empty(plan.RankSteps);
    }

    [Fact]
    public void Differs_Keep_RemovesNothingAndListsTheRanksToSet() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q219127")), StatusEditChoice.Keep);

        Assert.Equal(new[] { L("Q309238", "P141", "Q96377276", Sources) }, plan.Commands);
        Assert.Empty(plan.Removes);
        var step = Assert.Single(plan.RankSteps);
        Assert.Null(step.StatementId);   // the statement the batch adds
        Assert.Equal(("Q96377276", "preferred"), (step.Value, step.Rank));
    }

    [Fact]
    public void Differs_PreferredAndHistory_ReplaceRemovesOnlyThePreferredOne() {
        // Taxon 76109592 (VU): its item Q81849610 has near threatened (preferred, 2025.2) and
        // critically endangered (normal, 2021.2), which is history kept under the preferred one.
        IReadOnlyList<WikidataStatusStatement> statements = [S(G1, "Q219127", "normal", "Q108765945"), S(G2, "Q719675", "preferred", "Q136547248")];
        var plan = WikidataStatusEdit.Plan(Request("VU", [.. statements]));

        Assert.Equal(StatusEditOutcome.Differs, plan.Outcome);
        Assert.Equal(new[] { "Q719675", "Q219127" }, plan.Current.Select(s => s.Value));   // preferred first
        Assert.Equal(new[] { "Q309238$" + G2 }, plan.Removes.Select(s => s.Id));
        Assert.Equal(2, plan.Commands.Count);
        Assert.StartsWith("Q309238\tP141\tQ278113\t", plan.Commands[0]);
        // The history statement stays at normal rank, so the new one has to be preferred.
        Assert.Equal(new StatusRankStep(null, "Q278113", "preferred"), Assert.Single(plan.RankSteps));
        Assert.Equal(StatusEditChoice.Keep, WikidataStatusEdit.RecommendedChoice(statements));
    }

    [Fact]
    public void Differs_TwoNormalValues_ReplaceRemovesBoth() {
        // With no preferred statement both are best-ranked, so both are the current status.
        IReadOnlyList<WikidataStatusStatement> statements = [S(G1, "Q219127"), S(G2, "Q719675")];
        var plan = WikidataStatusEdit.Plan(Request("VU", [.. statements]));

        Assert.Equal(new[] { "Q309238$" + G1, "Q309238$" + G2 }, plan.Removes.Select(s => s.Id));
        Assert.Equal(3, plan.Commands.Count);
        Assert.Empty(plan.RankSteps);
        Assert.Equal(StatusEditChoice.Replace, WikidataStatusEdit.RecommendedChoice(statements));
    }

    [Fact]
    public void Differs_Keep_TheOldPreferredValueIsSetToNormal() {
        var plan = WikidataStatusEdit.Plan(Request("VU", S(G1, "Q219127"), S(G2, "Q719675", "preferred")), StatusEditChoice.Keep);

        Assert.Equal(new[] {
            new StatusRankStep(null, "Q278113", "preferred"),
            new StatusRankStep("Q309238$" + G2, "Q719675", "normal"),
        }, plan.RankSteps);
    }

    [Fact]
    public void Differs_TheValueIsThereAtNormalRank_ReplaceReferencesItAndRemovesThePreferredOne() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q96377276"), S(G2, "Q219127", "preferred")));

        Assert.Equal(StatusEditOutcome.Differs, plan.Outcome);
        Assert.False(plan.AddsValue);
        Assert.True(plan.AddsReference);
        Assert.Equal(new[] {
            L("Q309238", "P141", "Q96377276", Sources),
            L("-STATEMENT", "Q309238$" + G2),
        }, plan.Commands);
    }

    [Fact]
    public void Differs_Keep_TheValueIsThereAtNormalRank_OnlyRanksAreLeft() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q96377276"), S(G2, "Q219127", "preferred")), StatusEditChoice.Keep);

        Assert.Equal(new[] {
            new StatusRankStep("Q309238$" + G1, "Q96377276", "preferred"),
            new StatusRankStep("Q309238$" + G2, "Q219127", "normal"),
        }, plan.RankSteps);
    }

    [Fact]
    public void Agrees_AddsTheReferenceToTheExistingStatement() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q96377276", "normal", "Q115962546")));

        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.False(plan.AddsValue);
        Assert.True(plan.AddsReference);
        Assert.Empty(plan.Removes);
        // QuickStatements finds the statement by its value and adds the reference to it.
        Assert.Equal(new[] { L("Q309238", "P141", "Q96377276", Sources) }, plan.Commands);
    }

    [Fact]
    public void Agrees_PreferredValueWithOlderNormalOnes_NothingIsRemoved() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q96377276", "preferred"), S(G2, "Q278113")));

        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.Empty(plan.Removes);
        Assert.Single(plan.Commands);
    }

    [Fact]
    public void Agrees_AndCitesTheAssessmentItem_NothingToDo() {
        var plan = WikidataStatusEdit.Plan(Request("CR", S(G1, "Q219127", "normal", "Q61440620")) with { AssessmentItemQid = "Q61440620" });

        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.True(plan.AlreadyCited);
        Assert.False(plan.AddsReference);
        Assert.Empty(plan.Commands);
    }

    [Fact]
    public void Agrees_AReferenceWithThisTaxonId_IsAlreadyCited() {
        // Any date: the cache has no reference URL or retrieved date to compare, and a second
        // reference that differs only in its date would be added after every download.
        var plan = WikidataStatusEdit.Plan(Request("EN", Cited(G1, "Q96377276", "2725")));
        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.True(plan.AlreadyCited);
        Assert.Empty(plan.Commands);

        // Another taxon's id does not count.
        var other = WikidataStatusEdit.Plan(Request("EN", Cited(G1, "Q96377276", "999")));
        Assert.False(other.AlreadyCited);
        Assert.Single(other.Commands);
    }

    [Fact]
    public void OtherSource_IsNeitherCountedNorRemoved() {
        // Taxon 118263605 (EN): its item Q6782925 has endangered with an IUCN reference and
        // critically endangered whose only reference is a national red book (ISBN).
        var plan = WikidataStatusEdit.Plan(Request("EN", Cited(G1, "Q96377276", "2725"), Other(G2, "Q219127")));

        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.Empty(plan.Commands);
        Assert.Equal("Q309238$" + G2, Assert.Single(plan.Others).Id);

        // When the IUCN statement differs, Replace removes it and leaves the other one, which then
        // competes with the new value.
        var differs = WikidataStatusEdit.Plan(Request("VU", Cited(G1, "Q96377276", "2725"), Other(G2, "Q219127")));
        Assert.Equal(StatusEditOutcome.Differs, differs.Outcome);
        Assert.Equal(new[] { "Q309238$" + G1 }, differs.Removes.Select(s => s.Id));
        Assert.Equal(new StatusRankStep(null, "Q278113", "preferred"), Assert.Single(differs.RankSteps));
    }

    [Fact]
    public void Missing_OnlyStatementsWithNoIucnReference() {
        // The same value with no reference: QuickStatements adds the reference to that statement.
        var same = WikidataStatusEdit.Plan(Request("LC", Other(G1, "Q211005", references: 0)));
        Assert.Equal(StatusEditOutcome.Missing, same.Outcome);
        Assert.False(same.AddsValue);
        Assert.True(same.AddsReference);
        Assert.Single(same.Commands);
        Assert.Single(same.Others);

        // Another value: the IUCN value is added and the other statement stays.
        var other = WikidataStatusEdit.Plan(Request("EN", Other(G1, "Q219127", references: 0)));
        Assert.Equal(StatusEditOutcome.Missing, other.Outcome);
        Assert.True(other.AddsValue);
        Assert.Empty(other.Removes);
        Assert.Equal(new[] { L("Q309238", "P141", "Q96377276", Sources) }, other.Commands);
    }

    [Fact]
    public void AssessmentItem_IsStatedInTheReference() {
        // Taxon 8 (CR), item Q4661053; the assessment's item is Q61440620.
        var plan = WikidataStatusEdit.Plan(Request("CR", S(G1, "Q219127")) with { AssessmentItemQid = "Q61440620" });

        Assert.Equal(L("Q309238", "P141", "Q219127", "S248", "Q61440620", Sources), Assert.Single(plan.Commands));
        Assert.Equal("Q61440620", plan.Reference!.StatedIn);
    }

    [Fact]
    public void Missing_AddsTheValue() {
        // Taxon 31852 (VU): its item Q210858 has no P141.
        var plan = WikidataStatusEdit.Plan(Request("VU"));

        Assert.Equal(StatusEditOutcome.Missing, plan.Outcome);
        Assert.True(plan.AddsValue);
        Assert.Equal(new[] { L("Q309238", "P141", "Q278113", Sources) }, plan.Commands);
    }

    [Fact]
    public void Missing_OnlyDeprecatedStatementsWithOtherValues() {
        var plan = WikidataStatusEdit.Plan(Request("VU", S(G1, "Q219127", "deprecated")));

        Assert.Equal(StatusEditOutcome.Missing, plan.Outcome);
        Assert.Empty(plan.Current);
        Assert.Empty(plan.Removes);
        Assert.Single(plan.Commands);
    }

    [Fact]
    public void LowerRiskNearThreatened_IsWrittenAsNearThreatened() {
        var plan = WikidataStatusEdit.Plan(Request("LR/nt", S(G1, "Q719675")));
        Assert.Equal(StatusEditOutcome.Agrees, plan.Outcome);
        Assert.Equal("Q719675", plan.TargetQid);
    }

    [Fact]
    public void ConservationDependent_NoValueAndNoCommands() {
        // Taxon 2117 (LR/cd): its item Q4560564 says least concern.
        var plan = WikidataStatusEdit.Plan(Request("LR/cd", S(G1, "Q211005")));

        Assert.Equal(StatusEditOutcome.NoValue, plan.Outcome);
        Assert.Null(plan.TargetQid);
        Assert.Equal("Q211005", Assert.Single(plan.Current).Value);
        Assert.Empty(plan.Commands);
    }

    [Fact]
    public void DeprecatedStatementWithTheValue_IsBlocked() {
        // QuickStatements would find the deprecated statement by its value and put the reference on it.
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q219127"), S(G2, "Q96377276", "deprecated")));

        Assert.Equal(StatusEditOutcome.Blocked, plan.Outcome);
        Assert.Equal(StatusEditBlock.DeprecatedStatementHasTheValue, plan.Block);
        Assert.Empty(plan.Commands);
        Assert.Single(plan.Current);
    }

    [Fact]
    public void TwoStatementsWithTheValue_IsBlocked() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q96377276"), S(G2, "Q96377276"), S(G3, "Q219127", "preferred")));

        Assert.Equal(StatusEditOutcome.Blocked, plan.Outcome);
        Assert.Equal(StatusEditBlock.SeveralStatementsHaveTheValue, plan.Block);
        Assert.Empty(plan.Commands);
    }

    [Fact]
    public void NoRetrievedDate_LeavesP813Out() {
        var plan = WikidataStatusEdit.Plan(Request("VU") with { Retrieved = null });
        Assert.Equal(L("Q309238", "P141", "Q278113", "S627", "\"2725\"", "S854", "\"https://www.iucnredlist.org/species/2725/9461\""),
            Assert.Single(plan.Commands));
    }

    [Fact]
    public void MalformedIds_Throw() {
        Assert.Throws<ArgumentException>(() => WikidataStatusEdit.Plan(Request("VU") with { TaxonItemQid = "P141" }));
        Assert.Throws<ArgumentException>(() => WikidataStatusEdit.Plan(Request("EN", new WikidataStatusStatement("Q309238$bad\tP31", "Q219127", "normal", CitesIucn: true))));
    }

    [Fact]
    public void LowerCaseEntityInAStatementId_IsKept() {
        // Older statements have ids such as "q140$B12A2FD5-692F-4D9A-8FC7-144AA45A16F8".
        Assert.Equal("-STATEMENT\tq140$B12A2FD5-692F-4D9A-8FC7-144AA45A16F8",
            WikidataStatusEdit.RemoveStatementCommand("q140$B12A2FD5-692F-4D9A-8FC7-144AA45A16F8"));
    }

    [Fact]
    public void Link_EncodesTheStatementIdAndKeepsEveryCommand() {
        var plan = WikidataStatusEdit.Plan(Request("EN", S(G1, "Q219127")));
        var url = WikidataCitation.QuickStatementsUrl(plan.Commands);
        var data = Uri.UnescapeDataString(url[WikidataCitation.QuickStatementsBase.Length..]);

        Assert.Equal(string.Join("||", plan.Commands.Select(c => c.Replace('\t', '|'))), data);
        Assert.Contains("%24", url);   // "$" in the statement id
        Assert.True(WikidataCitation.QuickStatementsUrlFits(url));
    }

    [Fact]
    public void Statements_JsonRoundTrip() {
        IReadOnlyList<WikidataStatusStatement> statements = [
            new("Q309238$" + G1, "Q219127", "preferred", ["Q115962546", "Q136547248"], ["2725"], 2, true),
        ];
        var json = WikidataStatusStatement.ListToJson(statements);
        Assert.Contains("\"statedIn\":[\"Q115962546\",\"Q136547248\"]", json);
        Assert.Contains("\"taxonIds\":[\"2725\"],\"references\":2,\"citesIucn\":true", json);
        Assert.DoesNotContain("IsDeprecated", json);
        Assert.DoesNotContain("IsPreferred", json);
        var back = WikidataStatusStatement.ListFromJson(json)!;
        Assert.Equal(statements[0].Id, back[0].Id);
        Assert.Equal(statements[0].StatedIn, back[0].StatedIn);
        Assert.Equal(statements[0].TaxonIds, back[0].TaxonIds);
        Assert.Equal((2, true), (back[0].References, back[0].CitesIucn));
        Assert.Null(WikidataStatusStatement.ListFromJson(null));
    }
}
