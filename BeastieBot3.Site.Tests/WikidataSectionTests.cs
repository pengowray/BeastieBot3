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
        var response = await _client.GetAsync($"/species/{FixtureDb.Tiger}/wikidata");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var part = Part(html);
        Assert.Contains("<h3 id=\"wikidata-cite-heading\">Wikidata item of the assessment</h3>", part);
        Assert.Contains($"<p class=\"wikidata-item\">Wikidata item for this assessment: <a href=\"https://www.wikidata.org/wiki/{FixtureDb.TigerLatestItem}\">{FixtureDb.TigerLatestItem}</a></p>", part);
        Assert.DoesNotContain("No Wikidata item found", part);
        Assert.DoesNotContain("QuickStatements commands to create the item", part);
    }

    [Fact]
    public async Task AssessmentWithoutAnItem() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikidata");
        var part = Part(html);
        Assert.Contains("<p class=\"wikidata-item\">No Wikidata item found for this assessment.</p>", part);
        Assert.DoesNotContain("wikidata.org/wiki/Q", part);
        Assert.DoesNotContain("{{cite Q}} citation</label>", part);
    }

    // Two items state the leopard's IUCN taxon ID, so the create commands leave out main subject
    // (P921), and one line says why.
    [Fact]
    public async Task TaxonIdOnSeveralItems_CreateCommandsLeaveOutMainSubject() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Leopard}/wikidata");
        var box = Html.Textarea(html, WikidataCite.CommandsBoxId)!;
        Assert.StartsWith("CREATE", box);
        Assert.Contains("LAST\tP1433\tQ32059", box);
        Assert.DoesNotContain("P921", box);
        Assert.Contains(SiteText.MainSubjectSeveralItems(2, FixtureDb.Leopard), Html.Text(Part(html)));
    }

    // The cassowary's item states its IUCN taxon ID only at deprecated rank, so the add commands
    // leave out main subject (P921), which the assessment item lacks.
    [Fact]
    public async Task TaxonIdAtDeprecatedRank_AddCommandsLeaveOutMainSubject() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}/wikidata");
        var box = Html.Textarea(html, WikidataCite.CommandsBoxId)!;
        Assert.Equal($"{FixtureDb.CassowaryLatestItem}\tP577\t+2016-00-00T00:00:00Z/9", box);
        var text = Html.Text(Part(html));
        Assert.Contains(SiteText.MainSubjectDeprecated(FixtureDb.CassowaryItem, FixtureDb.Cassowary), text);
        Assert.Contains(SiteText.MissingStatements("publication date (P577)"), text);
    }

    // One item states the taxon's IUCN taxon ID: main subject (P921) is written, with no line.
    [Fact]
    public async Task OneItemWithTheTaxonId_CommandsHaveMainSubject() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikidata");
        Assert.Contains("LAST\tP921\tQ33609", Html.Textarea(html, WikidataCite.CommandsBoxId)!);
        Assert.DoesNotContain("wikidata-main-subject", Part(html));
    }

    [Fact]
    public void TaxonItemDoubt_FromTheTaxonsLinks() {
        static TaxonRow Taxon(string? source = "p627", bool deprecated = false, string? others = null) =>
            new(1, "Panthera pardus", TaxonKinds.Species, "ANIMALIA", null, null, null, null, null, null, null, null, null, null, "Q35694", null, null,
                WikidataQidSource: source, WikidataP627Deprecated: deprecated, WikidataOtherItems: others);
        Assert.Null(TaxonItemDoubt.Of(Taxon()));
        Assert.Null(TaxonItemDoubt.Of(null));
        Assert.Null(TaxonItemDoubt.Of(Taxon(source: "name-match", deprecated: true)));
        Assert.Equal(new TaxonItemDoubt(TaxonItemDoubtKind.TaxonIdDeprecated, 1, "Q35694", 1), TaxonItemDoubt.Of(Taxon(deprecated: true)));
        var others = WikidataOtherTaxonItem.ListToJson([new WikidataOtherTaxonItem("Q1"), new WikidataOtherTaxonItem("Q2", true)]);
        Assert.Equal(new TaxonItemDoubt(TaxonItemDoubtKind.SeveralItems, 1, "Q35694", 3), TaxonItemDoubt.Of(Taxon(deprecated: true, others: others)));
    }

    [Fact]
    public async Task ItemWithoutCitationParts() {
        // The West African lion's assessment has no citation, so there is no options form, but its
        // item can still be cited.
        var response = await _client.GetAsync($"/species/{FixtureDb.WestAfricanLion}/wikidata");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.DoesNotContain("options-form", html);
        Assert.Contains($"<a href=\"https://www.wikidata.org/wiki/{FixtureDb.WestAfricanLionLatestItem}\">", Part(html));
        Assert.DoesNotContain("id=\"wikidata-commands\"", html);
    }

    [Fact]
    public async Task TheWikidataPageHasTheItemThenTheStatusThenTheTables_AndNoForm() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikidata");
        var part = Html.IndexOf(html, "id=\"wikidata-cite\"");
        var status = Html.IndexOf(html, "id=\"wikidata-status\"");
        var history = Html.IndexOf(html, "<section class=\"history\"");
        Assert.True(part > 0 && status > part && history > status);
        Assert.DoesNotContain("<form class=\"options-form\"", html);
        // The Wikidata page is the Wikidata choice of the Citations tab.
        Assert.Contains($"<a href=\"/species/{FixtureDb.Tiger}/wikitext\" aria-current=\"page\">Citations</a>", html);
        Assert.Contains("<h2 id=\"wikitext-heading\">Cite for Wikidata</h2>", html);
        Assert.Contains("<span aria-current=\"page\">Wikidata</span>", html);
        Assert.Contains($"<a href=\"/species/{FixtureDb.Tiger}/wikitext#wikitext\" lang=\"en\" hreflang=\"en\">English</a>", html);
        Assert.Contains("<title>Panthera tigris (Tiger): Citations for Wikidata | Species Check</title>", html);
        Assert.Contains($"<link rel=\"canonical\" href=\"http://localhost/species/{FixtureDb.Tiger}/wikidata\">", html);
        Assert.Contains(">Show Wikidata item</a>", await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikidata"));
    }

    [Fact]
    public async Task TheWikipediaPageHasNoWikidataCommandsUnlessAsked_AndCiteQByDefault() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikitext");
        Assert.DoesNotContain("id=\"wikidata-cite\"", html);
        Assert.DoesNotContain("id=\"wikidata-status\"", html);
        Assert.StartsWith("<ref", Html.Textarea(html, WikidataCite.CiteQBoxId));
        Assert.NotNull(Html.Textarea(html, "wikitext-cite"));
        // Wikidata is the first choice of the row, with the assessment only.
        Assert.Contains($"<a href=\"/species/{FixtureDb.Tiger}/wikidata#wikitext\">Wikidata</a>", html);

        var citeIucn = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikitext?cite=iucn");
        Assert.Null(Html.Textarea(citeIucn, WikidataCite.CiteQBoxId));
        Assert.Contains("id=\"wikidata-cite\"", await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikitext?cite=new"));
    }

    [Fact]
    public async Task AboutPageSaysWhatThePartDoes() {
        var text = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("the Wikidata choice has the assessment's Wikidata item, or QuickStatements commands to create that item", text);
        Assert.Contains("Citing through Wikidata.", text);
        Assert.Contains("QuickStatements makes the edits with your own Wikidata account. This site never edits Wikidata.", text);
        Assert.Contains("don't use it in an article whose citations mostly give authors as “Last, First” or in Vancouver style.", text);
        Assert.Contains("Wikidata items of taxa and of assessments", text);
    }

    [Fact]
    public async Task EveryAssessmentShownHasThePart() {
        foreach (var url in new[] {
                     $"/species/{FixtureDb.PolarBear}/wikidata?assessment={FixtureDb.PolarBear2008}",
                     $"/species/{FixtureDb.PolarBear}/wikidata?assessment={FixtureDb.PolarBear1988Nt}",
                     $"/species/{FixtureDb.HouseSparrow}/wikidata?assessment={FixtureDb.HouseSparrowEurope}",
                     $"/species/{FixtureDb.RegionalOnly}/wikidata",
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
        var response = await client.GetAsync($"/species/{FixtureDb.PolarBear}/wikidata");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("No Wikidata item found for this assessment.", html);
        Assert.DoesNotContain("id=\"wikidata-commands\"", html);
        Assert.DoesNotContain("QuickStatements commands to create the item", html);
        // The status commands follow no item model, so they are still offered.
        Assert.Contains("id=\"wikidata-status-commands\"", html);

        var tiger = await client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikidata");
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

    // Taxon 30321's 1998 assessment: Crossref registered "Apollonias barbujana ssp. ceballosi", and
    // IUCN's citation has "subsp.". The names are the same, so there is no name note; the create
    // commands keep Crossref's form, which is the registered title.
    [Fact]
    public void RegisteredNameThatDiffersOnlyInTheRankMarker_NoNameNote() {
        var parts = new IucnCitationParts {
            TaxonId = 30321, AssessmentId = 9535286, Year = 1998, ScientificName = "Apollonias barbujana subsp. ceballosi",
            RegisteredName = "Apollonias barbujana ssp. ceballosi", Doi = "10.2305/IUCN.UK.1998.RLTS.T30321A9535286.en",
        };
        var view = WikidataCite.Build(Row(null), parts, "Q30252628", new WikidataItemModel(), new CiteQOptions());
        Assert.Equal(new TitleName("Apollonias barbujana ssp. ceballosi", TitleNameSource.Crossref), view.Name);
        Assert.False(view.ShowNameNote);
        Assert.Contains("LAST\tP1476\ten:\"Apollonias barbujana ssp. ceballosi\"", view.Commands!.Text);

        // A different name still gets the note.
        Assert.True(WikidataCite.Build(Row(null), parts with { RegisteredName = "Apollonias ceballosi" }, "Q30252628",
            new WikidataItemModel(), new CiteQOptions()).ShowNameNote);
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
    public void NoAddCommandsWhenTheItemsPropertiesAreNotKnown() {
        var calls = new List<string>();
        var view = WikidataCite.Build(Row("Q900000001", properties: null), Parts, "Q33609", new WikidataItemModel(), new CiteQOptions(),
            (what, _) => calls.Add(what));
        Assert.Equal("Q900000001", view.ItemQid);
        Assert.Null(view.Commands);
        Assert.Empty(view.AddedStatements);
        Assert.DoesNotContain("AddMissingCommands", calls);
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

    // Q56226968's assessment with no item and no Crossref title: IUCN's citation name is internal,
    // so there are no create commands, and the line says why.
    [Fact]
    public void OnlyAnInternalName_NoCreateCommands() {
        var parts = new IucnCitationParts { TaxonId = 22694346, AssessmentId = 39183818, Year = 2012, ScientificName = "Larus glaucoides_old" };
        var view = WikidataCite.Build(Row(null), parts, null, new WikidataItemModel(), new CiteQOptions());
        Assert.True(view.NoUsableName);
        Assert.Null(view.Commands);
        Assert.Equal("No commands: the only name this site has for this assessment is IUCN's citation name, Larus glaucoides_old, "
            + "which IUCN uses as an internal name for a taxon it has replaced.", SiteText.NoUsableName(view.CitationName!));
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
        Assert.Contains("wikidata_item_titles", SiteDbSchema.Ddl);
        Assert.Contains("wikidata_item_label_en", SiteDbSchema.Ddl);
        Assert.Contains("wikidata_p141", SiteDbSchema.Ddl);
        Assert.Contains("wikidata_item_downloaded", SiteDbSchema.Ddl);
    }

    [Fact]
    public void StatementsAdded_SkipsRemovals() {
        string[] commands = [
            "Q5\tP1476\ten:\"Ursus maritimus\"",
            "-Q5\tP1476\ten:\"Ursus maritimus: Wiig, Ø.\"",
            "-STATEMENT\tQ5$1A2B3C4D-0000-4000-8000-000000000001",
        ];
        Assert.Equal(["title (P1476)"], WikidataCite.StatementsAdded(commands));
    }

    [Fact]
    public void ItemPropertiesKeepTheLabelToken() {
        // "Len" (the item has an English label) must survive parsing; upper-casing it made every
        // item's label look missing, so the add batch set the label again.
        var present = BeastieBot3.Site.Pages.WikidataCite.Properties("P31 P1476 P1433 P577 P356 Len");
        Assert.Contains(BeastieBot3.Shared.Wikitext.WikidataCitation.EnglishLabelToken, present);
        Assert.Contains("p356", present);
    }
}
