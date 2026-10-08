using System.Net;
using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.SiteBuild;
using BeastieBot3.Wikipedia;

namespace BeastieBot3.Tests;

// Merging a batched action API answer split over several responses (WikipediaBatchResponse), and
// the order `wikipedia fetch-group-titles` asks for titles in (GroupTitlePlanner).
public class WikipediaBatchResponseTests {
    private static IReadOnlyDictionary<string, string> Add(WikipediaBatchResponse batch, string json) {
        using var document = JsonDocument.Parse(json);
        return batch.Add(document.RootElement);
    }

    [Fact]
    public void PagesAreMergedAcrossContinuationsAndFollowedThroughRedirects() {
        var batch = new WikipediaBatchResponse();
        var next = Add(batch, """
            {"continue":{"clcontinue":"123|B","continue":"||"},
             "query":{"normalized":[{"from":"pteropodidae","to":"Pteropodidae"}],
                      "redirects":[{"from":"Pteropodidae","to":"Megabat"}],
                      "pages":[{"pageid":123,"ns":0,"title":"Megabat","categories":[{"ns":14,"title":"Category:A"}],
                                "revisions":[{"revid":9,"slots":{"main":{"content":"{{Automatic taxobox|taxon=Pteropodidae}}"}}}]},
                               {"ns":0,"title":"Nosuchgenus","missing":true}]}}
            """);
        Assert.Equal("123|B", next["clcontinue"]);
        var done = Add(batch, """
            {"query":{"redirects":[{"from":"Pteropodidae","to":"Megabat"}],
                      "pages":[{"pageid":123,"ns":0,"title":"Megabat","categories":[{"ns":14,"title":"Category:B"},{"ns":14,"title":"Category:Disambiguation pages"}]}]}}
            """);
        Assert.Empty(done);

        var results = batch.PageResults(["pteropodidae", "Nosuchgenus"], HttpStatusCode.OK, 100);

        var megabat = results["pteropodidae"];
        Assert.True(megabat.Exists);
        Assert.Equal("Megabat", megabat.CanonicalTitle);
        Assert.Equal(new[] { "A", "B", "Disambiguation pages" }, megabat.Categories);
        Assert.True(megabat.IsDisambiguation);
        Assert.Equal(9, megabat.RevisionId);
        Assert.Contains("Pteropodidae", megabat.Wikitext);
        Assert.Equal(new WikipediaRedirectStep("Pteropodidae", "Megabat"), Assert.Single(megabat.Redirects));
        Assert.False(results["Nosuchgenus"].Exists);
    }

    [Fact]
    public void IncomingRedirectsKeepTheirSections() {
        var batch = new WikipediaBatchResponse();
        Add(batch, """
            {"continue":{"rdcontinue":"5","continue":"||"},
             "query":{"pages":[{"pageid":1,"ns":0,"title":"Megabat","redirects":[{"pageid":2,"ns":0,"title":"Fruit bat"}]}]}}
            """);
        Add(batch, """
            {"query":{"pages":[{"pageid":1,"ns":0,"title":"Megabat","redirects":[{"pageid":3,"ns":0,"title":"Dobsoniini","fragment":"List of genera"}]}]}}
            """);

        var result = batch.IncomingRedirects(["Megabat"])["Megabat"];

        Assert.True(result.Exists);
        Assert.Equal(new[] { new WikipediaIncomingRedirect("Fruit bat", null), new WikipediaIncomingRedirect("Dobsoniini", "List of genera") },
            result.Redirects);
    }

    [Fact]
    public void PlannerPutsFamiliesBeforeTribesBeforeGenera() {
        var kingdom = new SiteTreeNode { Rank = "kingdom", Name = "Animalia", Source = GroupSources.Iucn, ShowRank = true, Kingdom = "ANIMALIA" };
        var family = new SiteTreeNode { Rank = "family", Name = "Pteropodidae", Source = GroupSources.Iucn, ShowRank = true, Kingdom = "ANIMALIA", Parent = kingdom, Depth = 5 };
        var tribe = new SiteTreeNode { Rank = "tribe", Name = "Epomophorini", Source = GroupSources.Col, ShowRank = true, Kingdom = "ANIMALIA", Parent = family, Depth = 6 };
        var genus = new SiteTreeNode { Rank = "genus", Name = "Epomophorus", Source = GroupSources.Iucn, ShowRank = true, Kingdom = "ANIMALIA", Parent = tribe, Depth = 7 };
        var suborder = new SiteTreeNode { Rank = "suborder", Name = "Yinpterochiroptera", Source = GroupSources.Col, ShowRank = true, Kingdom = "ANIMALIA", Parent = kingdom, Depth = 4 };

        var titles = GroupTitlePlanner.Plan([genus, tribe, family, suborder, kingdom], node => node == family ? "Megabat" : null);

        Assert.Equal(new[] { "Animalia", "Yinpterochiroptera", "Megabat", "Pteropodidae", "Epomophorini", "Epomophorus" },
            titles.Select(t => t.Title));
        Assert.Equal(new[] { 0, 0, 0, 0, 1, 2 }, titles.Select(t => t.Band));
    }
}
