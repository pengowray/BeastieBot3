using System.Net;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Pages;
using BeastieBot3.Site.Update;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace BeastieBot3.Site.Tests;

// A Wikipedia URL or wikilink in the search box opens the status update page with that page loaded
// from English Wikipedia (a fake here: tests never reach Wikipedia).
public sealed class WikipediaPageInputTests {
    [Theory]
    [InlineData("https://en.wikipedia.org/wiki/List_of_threatened_birds_of_Brazil", "List of threatened birds of Brazil", null)]
    [InlineData("en.wikipedia.org/wiki/List_of_parrots#Cockatoos", "List of parrots", null)]
    [InlineData("https://en.m.wikipedia.org/wiki/List_of_animals_in_the_Gal%C3%A1pagos_Islands", "List of animals in the Galápagos Islands", null)]
    [InlineData("https://en.wikipedia.org/w/index.php?title=List_of_parrots&oldid=1234567", "List of parrots", 1234567L)]
    [InlineData("https://en.wikipedia.org/w/index.php?title=List+of+parrots&action=edit", "List of parrots", null)]
    [InlineData("https://en.wikipedia.org/wiki/C++", "C++", null)]
    [InlineData("[[List of parrots]]", "List of parrots", null)]
    [InlineData("[[List of parrots|parrots]]", "List of parrots", null)]
    [InlineData(" [[:List of parrots#Cockatoos]] ", "List of parrots", null)]
    [InlineData("\"[[List of critically endangered amphibians]]\"", "List of critically endangered amphibians", null)]
    [InlineData("\u201C[[List of parrots]]\u201D", "List of parrots", null)]
    [InlineData("'[[List of parrots]]'", "List of parrots", null)]
    [InlineData("<https://en.wikipedia.org/wiki/List_of_parrots>", "List of parrots", null)]
    [InlineData("\"https://en.wikipedia.org/wiki/List_of_parrots\"", "List of parrots", null)]
    [InlineData("\" <https://en.wikipedia.org/wiki/List_of_parrots> \"", "List of parrots", null)]
    [InlineData("`[[List of parrots]]`", "List of parrots", null)]
    public void ReadsTheTitle(string text, string title, long? revision) {
        var page = WikipediaPageInput.Parse(text)!;
        Assert.Equal(title, page.Title);
        Assert.Equal(revision, page.RevisionId);
        Assert.True(page.English);
    }

    [Theory]
    [InlineData("Momotidae")]
    [InlineData("List of parrots")]
    [InlineData("T22823A14871490")]
    [InlineData("https://www.iucnredlist.org/species/22823/14871490")]
    [InlineData("[[List of parrots]] and more")]
    [InlineData("\"[[List of parrots]]")]
    [InlineData("\"List of parrots\"")]
    [InlineData("https://en.wikipedia.org/")]
    public void PlainTextAndOtherSitesAreNotWikipediaPages(string text) => Assert.Null(WikipediaPageInput.Parse(text));

    [Fact]
    public void OtherWikipediasAreNamed() {
        var page = WikipediaPageInput.Parse("https://de.wikipedia.org/wiki/Liste_der_Papageien")!;
        Assert.False(page.English);
        Assert.Equal("de", page.Language);
    }

    [Fact]
    public void ReadsAnActionApiAnswer() {
        var result = WikipediaPageSource.Read("""
            {"query":{"redirects":[{"from":"Parrot list","to":"List of parrots"}],"pages":[{"pageid":1,"ns":0,"title":"List of parrots",
            "revisions":[{"revid":42,"parentid":41,"timestamp":"2026-10-01T12:30:00Z","slots":{"main":{"contentmodel":"wikitext","content":"* text"}}}]}]}}
            """, maxTextBytes: 100);
        Assert.Equal(new WikipediaPageText("List of parrots", "* text", 42, new DateTimeOffset(2026, 10, 1, 12, 30, 0, TimeSpan.Zero)), result.Page);
    }

    [Fact]
    public async Task LoadsForAllClientsTogetherAreLimited() {
        // A server that answers every request with the same page, counting the requests.
        var handler = new CountingHandler("""{"query":{"pages":[{"title":"A","revisions":[{"revid":1,"slots":{"main":{"content":"x"}}}]}]}}""");
        var source = new WikipediaPageSource(new HttpClient(handler), "test agent", 1000, loadsPerMinute: 2);
        Assert.NotNull((await source.GetAsync("A", null, default)).Page);
        Assert.NotNull((await source.GetAsync("B", null, default)).Page);
        Assert.Equal(WikipediaPageError.Busy, (await source.GetAsync("C", null, default)).Error);
        // A page already held does not count, and is not asked for again.
        Assert.NotNull((await source.GetAsync("A", null, default)).Page);
        Assert.Equal(2, handler.Requests);
    }

    private sealed class CountingHandler(string body) : HttpMessageHandler {
        public int Requests { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
            Requests++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        }
    }

    [Theory]
    [InlineData("""{"query":{"pages":[{"ns":0,"title":"Nope","missing":true}]}}""", WikipediaPageError.NotFound)]
    [InlineData("""{"query":{"badrevids":{"99":{"revid":99,"missing":true}}}}""", WikipediaPageError.NotFound)]
    [InlineData("""{"error":{"code":"maxlag"}}""", WikipediaPageError.Failed)]
    [InlineData("not json", WikipediaPageError.Failed)]
    [InlineData("""{"query":{"pages":[{"title":"Big","revisions":[{"revid":1,"slots":{"main":{"content":"0123456789A"}}}]}]}}""", WikipediaPageError.TooLarge)]
    public void ReadsWhyThereIsNoPage(string body, WikipediaPageError error) =>
        Assert.Equal(error, WikipediaPageSource.Read(body, maxTextBytes: 10).Error);
}

public sealed class FakeWikipediaSiteFactory : SiteFactory {
    public const string Title = "List of bears";
    public const string Text = "* [[Polar bear]] {{IUCN status|EN|22823/13045100|1|year=2008}}\n";

    private sealed class FakeSource : IWikipediaPageSource {
        public Task<WikipediaPageResult> GetAsync(string title, long? revisionId, CancellationToken cancellationToken) =>
            Task.FromResult(title == Title || revisionId == 7
                ? WikipediaPageResult.Of(new WikipediaPageText(Title, Text, revisionId ?? 1234, new DateTimeOffset(2026, 10, 1, 12, 30, 0, TimeSpan.Zero)))
                : WikipediaPageResult.Failure(WikipediaPageError.NotFound));
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder) {
        base.ConfigureWebHost(builder);
        builder.ConfigureTestServices(services => services.AddSingleton<IWikipediaPageSource>(new FakeSource()));
    }
}

public sealed class WikipediaPageLoadTests(FakeWikipediaSiteFactory factory) : IClassFixture<FakeWikipediaSiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Theory]
    [InlineData("https://en.wikipedia.org/wiki/List_of_bears", "/update-statuses?page=List%20of%20bears#result")]
    [InlineData("[[List of bears]]", "/update-statuses?page=List%20of%20bears#result")]
    [InlineData("https://en.wikipedia.org/w/index.php?title=List_of_bears&oldid=7", "/update-statuses?page=List%20of%20bears&oldid=7#result")]
    public async Task SearchingForAWikipediaPageOpensTheUpdatePage(string query, string location) {
        var response = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(query));
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal(location, response.Headers.Location!.OriginalString);
    }

    [Fact]
    public async Task AnotherWikipediaIsNamedOnTheSearchPage() {
        var html = await (await _client.GetAsync("/search?q=" + Uri.EscapeDataString("https://de.wikipedia.org/wiki/Eisb%C3%A4r"))).Content.ReadAsStringAsync();
        Assert.Contains("the link is to de.wikipedia.org", Html.Text(html));
    }

    [Fact]
    public async Task ThePageIsLoadedAndUpdated() {
        var response = await _client.GetAsync("/update-statuses?page=List%20of%20bears");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("Loaded from English Wikipedia: List of bears, revision 1234 of 1 October 2026, 12:30 UTC.", Html.Text(html));
        Assert.Contains("{{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "update-output"));
        Assert.Equal("\n" + FakeWikipediaSiteFactory.Text, Html.Textarea(html, "update-input"));
        Assert.Contains($"name=\"{UpdateModel.PageField}\" value=\"List of bears\"", html);
    }

    [Fact]
    public async Task AMissingPageSaysSo() {
        var response = await _client.GetAsync("/update-statuses?page=No%20such%20page");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Contains("English Wikipedia has no page called \"No such page\".", Html.Text(await response.Content.ReadAsStringAsync()));
    }
}
