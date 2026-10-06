using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

// {{cite Q}} as a choice wherever the site writes a reference outside its own {{cite Q}} box: the
// taxobox status_ref on taxon pages and the citations the status update page replaces. The group
// page's species tables are in SpeciesTableTests.
public sealed class CiteQOptionTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task TaxoboxStatusRefUsesCiteQWhenChosenAndTheCiteIucnBoxStays() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}?cite=q");

        Assert.Contains($"| status_ref = <ref name=\"iucn\">{{{{cite Q|{FixtureDb.CassowaryLatestItem}", Html.Textarea(html, "wikitext-speciesbox"));
        var cite = Html.Textarea(html, "wikitext-cite");
        Assert.Contains("{{cite iucn", cite);
        Assert.DoesNotContain("cite Q", cite);
        Assert.Contains("value=\"q\" checked=\"checked\"", html);
    }

    [Fact]
    public void TheChoiceIsCarriedToOtherAssessments() {
        var options = Pages.WikitextOptions.FromQuery(null, null, null, null, null, null) with { Template = Pages.ReferenceTemplate.CiteQ };

        Assert.Equal("?assessment=5&cite=q", options.ToQuery(5, Pages.DefaultRefNames.LatestGlobal));
        Assert.Equal("", Pages.WikitextOptions.Default.ToQuery(null, Pages.DefaultRefNames.LatestGlobal));
    }

    [Fact]
    public async Task TaxoboxStatusRefKeepsCiteIucnByDefault() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}");

        Assert.Contains("| status_ref = <ref name=\"iucn\">{{cite iucn", Html.Textarea(html, "wikitext-speciesbox"));
    }

    [Fact]
    public async Task WithNoWikidataItemTheChoiceSaysStatusRefUsesCiteIucn() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?cite=q");

        Assert.Contains("| status_ref = <ref name=\"iucn\">{{cite iucn", Html.Textarea(html, "wikitext-speciesbox"));
        Assert.Contains("This assessment has no Wikidata item, so status_ref uses {{cite iucn}}.", html);
    }

    private static string Citation(long taxonId, long assessmentId, int year, string name) => new IucnCitationParts {
        TaxonId = taxonId, AssessmentId = assessmentId, Year = year, ScientificName = name,
        Authors = [new CitationAuthor(CitationAuthorKind.Person, "Smith, B.D.", "Smith", "B.D.")],
    }.ToJson();

    private static StatusUpdateResult Update(string text, bool citeQ, string? item) {
        var lookup = new FakeStatusLookup()
            .Taxon(12119, "Lipotes vexillifer", "CR", 2017, 50358152, pe: true, citationJson: Citation(12119, 50358152, 2017, "Lipotes vexillifer"),
                wikidataItem: item, itemProperties: "P31 P953");
        return new StatusUpdater(lookup, new DateOnly(2026, 9, 1),
            options: new StatusUpdateOptions { UpdateCitations = true, CiteQ = citeQ }).Update(text);
    }

    private const string Taxobox = """
        {{Speciesbox
        | status = PE
        | status_system = IUCN3.1
        | status_ref = <ref name="iucn">{{cite iucn |author=Smith, B.D. |year=2008 |title=''Lipotes vexillifer'' |volume=2008 |article-number=e.T12119A3322533}}</ref>
        | taxon = Lipotes vexillifer
        }}
        Text.<ref>{{cite iucn |year=2008 |title=''Lipotes vexillifer'' |volume=2008 |article-number=e.T12119A3322533}}</ref>
        """;

    [Fact]
    public void UpdatePageReplacesOlderCitationsWithCiteQWhenAsked() {
        var text = Update(Taxobox, citeQ: true, item: "Q123").Text;

        Assert.Contains("| status_ref = <ref name=\"iucn\">{{cite Q|Q123}}</ref>", text);
        Assert.Contains("Text.<ref>{{cite Q|Q123}}</ref>", text);
    }

    [Fact]
    public void UpdatePageWritesCiteIucnWhenTheAssessmentHasNoItemOrCiteQIsOff() {
        Assert.Contains("| status_ref = <ref name=\"iucn\">{{cite iucn |last1=Smith |first1=B.D. |year=2017", Update(Taxobox, citeQ: true, item: null).Text);
        Assert.DoesNotContain("cite Q", Update(Taxobox, citeQ: false, item: "Q123").Text);
    }
}
