using System.Net;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

/// The "{{cite Q}} citation from Wikidata" part of the wikitext section. The {{cite Q}} wikitext and
/// the QuickStatements commands come from WikidataCitation in BeastieBot3.Shared; these tests pin
/// what the site decides (which part is shown, the item link, the order) and that a failing
/// renderer leaves the page working.
public sealed class WikidataSectionTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static string Part(string html) {
        var start = Html.IndexOf(html, "<section class=\"wikidata-cite\" id=\"wikidata-cite\"");
        Assert.True(start > 0, "the page has the {{cite Q}} part");
        return html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
    }

    [Fact]
    public async Task AssessmentWithAnItem() {
        var response = await _client.GetAsync($"/species/{FixtureDb.Tiger}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var part = Part(html);
        Assert.Contains("<h3 id=\"wikidata-cite-heading\">{{cite Q}} citation from Wikidata</h3>", part);
        Assert.Contains($"<p class=\"wikidata-item\">Wikidata item for this assessment: <a href=\"https://www.wikidata.org/wiki/{FixtureDb.TigerLatestItem}\">{FixtureDb.TigerLatestItem}</a></p>", part);
        Assert.DoesNotContain("No Wikidata item found", part);
        Assert.DoesNotContain("QuickStatements commands to create the item", part);
    }

    [Fact]
    public async Task AssessmentWithoutAnItem() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        var part = Part(html);
        Assert.Contains("<p class=\"wikidata-item\">No Wikidata item found for this assessment.</p>", part);
        Assert.DoesNotContain("wikidata.org/wiki/Q", part);
        Assert.DoesNotContain("{{cite Q}} citation</label>", part);
    }

    [Fact]
    public async Task ItemWithoutCitationParts() {
        // The West African lion's assessment has no citation, so there is no options form, but its
        // item can still be cited.
        var response = await _client.GetAsync($"/species/{FixtureDb.WestAfricanLion}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.DoesNotContain("options-form", html);
        Assert.Contains($"<a href=\"https://www.wikidata.org/wiki/{FixtureDb.WestAfricanLionLatestItem}\">", Part(html));
        Assert.DoesNotContain("id=\"wikidata-commands\"", html);
    }

    [Fact]
    public async Task PartComesLastInTheWikitextSection() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var form = Html.IndexOf(html, "<form class=\"options-form\"");
        var part = Html.IndexOf(html, "id=\"wikidata-cite\"");
        var history = Html.IndexOf(html, "<section class=\"names\"");
        Assert.True(form > 0 && part > form && history > part);
        // Inside the wikitext section: the section's closing tag comes after the part's.
        var partEnd = html.IndexOf("</section>", part, StringComparison.Ordinal);
        var sectionEnd = html.IndexOf("</section>", partEnd + 1, StringComparison.Ordinal);
        Assert.True(sectionEnd > partEnd && sectionEnd < history);
    }

    [Fact]
    public async Task AboutPageSaysWhatThePartDoes() {
        var text = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("a {{cite Q}} citation of the assessment's Wikidata item, or QuickStatements commands to create that item", text);
        Assert.Contains("Citing through Wikidata.", text);
        Assert.Contains("QuickStatements makes the edits with your own Wikidata account. This site never edits Wikidata.", text);
        Assert.Contains("don't use it in an article whose citations mostly give authors as “Last, First” or in Vancouver style.", text);
        Assert.Contains("Wikidata items of taxa and of assessments", text);
    }

    [Fact]
    public async Task EveryAssessmentShownHasThePart() {
        foreach (var url in new[] {
                     $"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}",
                     $"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear1988Nt}",
                     $"/species/{FixtureDb.HouseSparrow}?assessment={FixtureDb.HouseSparrowEurope}",
                     $"/species/{FixtureDb.RegionalOnly}",
                 }) {
            var response = await _client.GetAsync(url);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains("No Wikidata item found for this assessment.", Part(await response.Content.ReadAsStringAsync()));
        }
    }
}

/// A site database whose stored assessment item model cannot be read: the page offers no
/// QuickStatements commands and still works.
public sealed class UnreadableItemModelTests(UnreadableItemModelSiteFactory factory) : IClassFixture<UnreadableItemModelSiteFactory> {
    [Fact]
    public async Task NoCommandsAndNoError() {
        var client = factory.Client();
        var response = await client.GetAsync($"/species/{FixtureDb.PolarBear}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("No Wikidata item found for this assessment.", html);
        Assert.DoesNotContain("id=\"wikidata-commands\"", html);
        Assert.DoesNotContain("QuickStatements", html);

        var tiger = await client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains($"wikidata.org/wiki/{FixtureDb.TigerLatestItem}", tiger);
        Assert.DoesNotContain("id=\"wikidata-commands\"", tiger);
    }
}

public sealed class UnreadableItemModelSiteFactory : SiteFactory {
    private static readonly Lazy<string> Db = new(() => FixtureDb.Create("bad-item-model", wikidataItemModelJson: "{ not json"));
    protected override string DatabasePath => Db.Value;
}

public sealed class WikidataCiteUnitTests {
    private static AssessmentRow Row(string? item, string? properties = null) =>
        new(10, 1, "Global", true, "VU", false, false, null, "3.1", 2015, null, null, null, null, item, properties);

    private static readonly IucnCitationParts Parts = new() {
        TaxonId = 22823, AssessmentId = 14871490, Year = 2015, ScientificName = "Ursus maritimus",
        Doi = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en",
    };

    [Theory]
    [InlineData("Q123", "Q123")]
    [InlineData(" q123 ", "Q123")]
    [InlineData("Q0123", null)]
    [InlineData("P31", null)]
    [InlineData("Q12x", null)]
    [InlineData("https://www.wikidata.org/wiki/Q1", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void ItemIds(string? text, string? expected) => Assert.Equal(expected, WikidataCite.ItemId(text));

    [Fact]
    public void PropertiesAreReadFromTheSpaceSeparatedList() {
        Assert.Equal(new HashSet<string> { "P31", "P356", "P2093" }, WikidataCite.Properties(" P31  p356\tP2093 "));
        Assert.Empty(WikidataCite.Properties(null));
    }

    [Fact]
    public void StatementsAddedAreNamedInOrder() {
        string[] commands = [
            "CREATE",
            "LAST\tLen\t\"Ursus maritimus. The IUCN Red List of Threatened Species 2015: e.T22823A14871490\"",
            "LAST\tDen\t\"IUCN Red List assessment of Ursus maritimus\"",
            "Q5\tP123\tQ48268",
            "Q5\tP2093\t\"Wiig, Ø.\"\tP1545\t\"1\"",
            "Q5\tP2093\t\"Amstrup, S.\"\tP1545\t\"2\"",
            "Q5\tP356\t\"10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.EN\"",
            "Q5\tP9999\tQ1",
        ];
        Assert.Equal(["label (en)", "description (en)", "publisher (P123)", "author name string (P2093)", "DOI (P356)", "P9999"],
            WikidataCite.StatementsAdded(commands));
    }

    [Fact]
    public void SearchIsByDoiWhenThereIsOne() {
        Assert.Equal("https://www.wikidata.org/w/index.php?title=Special:Search&search=haswbstatement%3AP356%3D10.2305%2FIUCN.UK.2015-4.RLTS.T22823A14871490.EN",
            WikidataCite.SearchUrl(Parts));
        Assert.Equal("https://www.wikidata.org/w/index.php?title=Special:Search&search=%22e.T22823A14871490%22",
            WikidataCite.SearchUrl(Parts with { Doi = null }));
    }

    [Fact]
    public void NoItemAndNoCitationGivesOnlyTheLine() {
        var calls = 0;
        var view = WikidataCite.Build(Row(null), null, "Q33609", new WikidataItemModel(), new CiteQOptions(), (_, _) => calls++);
        Assert.Null(view.ItemQid);
        Assert.Null(view.Commands);
        Assert.Null(view.SearchUrl);
        Assert.Equal(0, calls);
    }

    [Fact]
    public void NoModelGivesNoCommands() {
        var calls = 0;
        var view = WikidataCite.Build(Row(null), Parts, "Q33609", model: null, new CiteQOptions(), (_, _) => calls++);
        Assert.Null(view.Commands);
        Assert.Null(view.QuickStatementsUrl);
        Assert.Equal(0, calls);
    }

    [Fact]
    public void AnItemIdThatIsNotOneIsIgnored() {
        var view = WikidataCite.Build(Row("not an item"), null, null, new WikidataItemModel(), new CiteQOptions());
        Assert.Null(view.ItemQid);
    }

    [Fact]
    public void ARendererThatThrowsLeavesOutOnlyItsBox() {
        // Whether WikidataCitation works or throws, the view has the item, and anything that failed
        // is reported, not thrown.
        var failed = new List<string>();
        var view = WikidataCite.Build(Row(" q900000001 ", "P31"), Parts, "Q33609", new WikidataItemModel(),
            new CiteQOptions { WrapInRef = true, RefName = "iucn" }, (what, _) => failed.Add(what));
        Assert.Equal("Q900000001", view.ItemQid);
        Assert.Equal("https://www.wikidata.org/wiki/Q900000001", view.ItemUrl);
        Assert.Equal(failed.Contains("CiteQ"), view.CiteQ is null);
        Assert.Null(view.SearchUrl);
    }

    [Fact]
    public void QuickStatementsBoxesHaveTheirOwnCopyName() {
        Assert.Equal("Copy {{cite Q}} wikitext", new WikitextBox("a", "b", "{{cite Q}}", "c", 2).CopyAccessibleName);
        Assert.Equal("Copy QuickStatements commands",
            new WikitextBox("a", "b", "QuickStatements", "c", 2, CopyName: SiteText.CopyQuickStatements).CopyAccessibleName);
    }

    [Fact]
    public void PropertyLabels() {
        Assert.Equal("DOI (P356)", SiteText.WikidataPropertyLabel("P356"));
        Assert.Equal("P123456", SiteText.WikidataPropertyLabel("P123456"));
        Assert.Equal("label (en)", SiteText.WikidataTermLabel('L', "en"));
        Assert.Equal("sitelink (enwiki)", SiteText.WikidataTermLabel('S', "enwiki"));
    }

    [Fact]
    public void FixtureHasTheDefaultModel() =>
        Assert.Equal(new WikidataItemModel(), WikidataItemModel.FromJson(new WikidataItemModel().ToJson()));

    [Fact]
    public void SchemaHasTheColumnsTheSiteReads() {
        Assert.Contains("wikidata_item_qid", SiteDbSchema.Ddl);
        Assert.Contains("wikidata_item_properties", SiteDbSchema.Ddl);
    }
}
