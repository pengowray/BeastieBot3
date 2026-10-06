using BeastieBot3.Web.Flows;
using BeastieBot3.Wikidata;

namespace BeastieBot3.Tests;

// `wikidata sweep-taxa`: the result parser, the page cut, what a run does, and the public-site
// workflow light for it.
public sealed class WikidataTaxonSweepTests {
    private static string Binding(long qid, string name, string? rank = null, string? parent = null, string? col = null,
        string? iucn = null, string? enwiki = null, string? label = null, string? inst = null) {
        var parts = new List<string> {
            $$"""
            "qid":{"type":"literal","value":"{{qid}}"}
            """,
            $$"""
            "name":{"type":"literal","value":"{{name}}"}
            """,
        };
        void AddUri(string key, string? value) {
            if (value is not null) parts.Add($$"""
                "{{key}}":{"type":"uri","value":"{{value}}"}
                """);
        }
        void AddLiteral(string key, string? value) {
            if (value is not null) parts.Add($$"""
                "{{key}}":{"type":"literal","value":"{{value}}"}
                """);
        }
        AddUri("rank", rank is null ? null : "http://www.wikidata.org/entity/" + rank);
        AddUri("parent", parent is null ? null : "http://www.wikidata.org/entity/" + parent);
        AddLiteral("col", col);
        AddLiteral("iucn", iucn);
        AddUri("enwiki", enwiki);
        AddLiteral("label", label);
        AddUri("inst", inst is null ? null : "http://www.wikidata.org/entity/" + inst);
        return "{" + string.Join(",", parts) + "}";
    }

    private static string Result(params string[] bindings) =>
        """{"head":{"vars":[]},"results":{"bindings":[""" + string.Join(",", bindings) + "]}}";

    [Fact]
    public void ParseGroupsTheRowsOfAnItem() {
        var page = WikidataTaxonSweep.Parse(Result(
            Binding(140, "Panthera leo", "Q7432", "Q127960", "4CGXP", "15951", "https://en.wikipedia.org/wiki/Lion", "lion"),
            Binding(140, "Panthera leo", "Q7432", "Q2", "4CGXP", "15951", "https://en.wikipedia.org/wiki/Lion", "lion"),
            Binding(536, "Betta imbellis", "Q7432", label: "Peaceful betta", enwiki: "https://en.wikipedia.org/wiki/Betta_imbellis"),
            Binding(900, "Panthera spelaea", "Q7432", label: "Panthera spelaea", inst: "Q23038290")));

        Assert.Equal(4, page.RowCount);
        Assert.Equal(new long[] { 140, 536, 900 }, page.Items.Select(i => i.Qid));
        var lion = page.Items[0];
        Assert.Equal("Panthera leo", lion.TaxonName);
        Assert.Equal(7432, lion.RankQid);
        Assert.Equal(new long[] { 2, 127960 }, lion.ParentQids);
        Assert.Equal(new[] { "4CGXP" }, lion.ColIds);
        Assert.Equal(new[] { "15951" }, lion.IucnTaxonIds);
        Assert.Equal("Lion", lion.EnwikiTitle);
        Assert.Equal("lion", lion.LabelEn);
        Assert.Equal("Betta imbellis", page.Items[1].EnwikiTitle);
        // A label that is the taxon name is not kept.
        Assert.Null(page.Items[2].LabelEn);
        Assert.Equal(new long[] { 23038290 }, page.Items[2].InstanceOf);
    }

    [Fact]
    public void AFullPageLeavesItsLastItemForTheNextPage() {
        var page = WikidataTaxonSweep.Parse(Result(
            Binding(1, "Aus bus"), Binding(2, "Aus cus", parent: "Q10"), Binding(2, "Aus cus", parent: "Q11")));

        Assert.Equal(new long[] { 1 }, WikidataTaxonSweep.TakeComplete(page, limit: 3).Select(i => i.Qid));
        Assert.Equal(new long[] { 1, 2 }, WikidataTaxonSweep.TakeComplete(page, limit: 4).Select(i => i.Qid));
        // One item with more rows than a page is kept as it is.
        var single = WikidataTaxonSweep.Parse(Result(Binding(2, "Aus cus", parent: "Q10"), Binding(2, "Aus cus", parent: "Q11")));
        Assert.Single(WikidataTaxonSweep.TakeComplete(single, limit: 2));
    }

    [Theory]
    [InlineData("https://en.wikipedia.org/wiki/Polar_bear", "Polar bear")]
    [InlineData("https://en.wikipedia.org/wiki/Fran%C3%A7ois%27_langur", "François' langur")]
    [InlineData("https://de.wikipedia.org/wiki/Eisb%C3%A4r", null)]
    public void EnwikiTitles(string url, string? title) =>
        Assert.Equal(title, WikidataTaxonSweep.EnwikiTitle(url));

    private static WikidataTaxonSweepState State(DateTime? started = null, DateTime? completed = null) =>
        new(Cursor: 0, PassStartedUtc: started, LastCompletedUtc: completed, Rows: 0, RowsSeenThisPass: 0, Species: 0, LastTotal: null);

    [Fact]
    public void PlanStartsContinuesOrWaits() {
        var now = new DateTime(2026, 10, 6, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(WikidataSweepPlan.Action.StartPass, WikidataSweepPlan.Decide(State(), false, null, now));
        Assert.Equal(WikidataSweepPlan.Action.Continue, WikidataSweepPlan.Decide(State(started: now.AddHours(-1)), false, null, now));
        Assert.Equal(WikidataSweepPlan.Action.UpToDate, WikidataSweepPlan.Decide(State(completed: now.AddDays(-40)), false, null, now));
        Assert.Equal(WikidataSweepPlan.Action.StartPass, WikidataSweepPlan.Decide(State(completed: now.AddDays(-40)), false, 30, now));
        Assert.Equal(WikidataSweepPlan.Action.UpToDate, WikidataSweepPlan.Decide(State(completed: now.AddDays(-10)), false, 30, now));
        Assert.Equal(WikidataSweepPlan.Action.StartPass, WikidataSweepPlan.Decide(State(started: now, completed: now), true, null, now));
    }

    [Fact]
    public void WorkflowLightForTheSweep() {
        var now = new DateTime(2026, 10, 6, 0, 0, 0, DateTimeKind.Utc);
        PublicSiteState S(DateTime? started = null, DateTime? completed = null, bool cache = true) => new() {
            WikidataCacheExists = cache, SweepPassStartedUtc = started, SweepCompletedUtc = completed, SweepCursor = 1234, ReadAtUtc = now,
        };

        Assert.Equal("todo", PublicSiteProbes.SweepStep(S(cache: false)).Status);
        Assert.Equal("todo", PublicSiteProbes.SweepStep(S()).Status);
        var running = PublicSiteProbes.SweepStep(S(started: now.AddHours(-1)));
        Assert.Equal("backlog", running.Status);
        Assert.Contains("Q1234", running.Detail);
        Assert.Equal("ok", PublicSiteProbes.SweepStep(S(completed: now.AddDays(-3))).Status);
        Assert.Equal("todo", PublicSiteProbes.SweepStep(S(completed: now.AddDays(-45))).Status);
    }
}
