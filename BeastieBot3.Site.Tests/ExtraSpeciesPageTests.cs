using System.Net;

namespace BeastieBot3.Site.Tests;

// The pages of species from the Catalogue of Life and Wikidata that are not on the IUCN Red List
// (/col/{id}, /wikidata/{qid}), and finding them in search. The fixture's extra species are in genus
// Ursus: Ursus americanus (CoL COLAM and Q1001), Ursus arctos (CoL COLAR only) and Ursus maritima
// (Q1003 only, likely the polar bear).
public sealed class ExtraSpeciesPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Theory]
    [InlineData("/col/COLAM")]
    [InlineData("/wikidata/Q1001")]
    public async Task ASpeciesInBothSourcesHasAPageAtEitherAddress(string path) {
        var response = await _client.GetAsync(path);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);
        Assert.Contains("<i>Ursus americanus</i>", html);
        Assert.Contains("Pallas, 1780", text);
        Assert.Contains("American black bear", text);
        Assert.Contains("Not on the IUCN Red List. Listed in the Catalogue of Life and on Wikidata.", text);
        Assert.Contains("href=\"/taxa/genus/ursus\"", html);
        Assert.Contains("href=\"https://www.wikidata.org/wiki/Q1001\"", html);
        Assert.Contains("href=\"https://www.catalogueoflife.org/data/taxon/COLAM\"", html);
        Assert.Contains("<meta name=\"robots\" content=\"noindex\">", html);
    }

    [Fact]
    public async Task ASpeciesLikelyTheSameAsAnIucnTaxonLinksIt() {
        var html = await _client.GetStringAsync("/wikidata/Q1003");
        var text = Html.Text(html);
        Assert.Contains("Not on the IUCN Red List. Listed on Wikidata.", text);
        Assert.Contains("Possible duplicates", text);
        Assert.Contains("Likely the same species as", text);
        Assert.Contains($"href=\"/species/{FixtureDb.PolarBear}\"", html);
        Assert.Contains("Same genus, and the epithets differ only in the Latin gender ending.", text);
    }

    [Fact]
    public async Task TheIucnTaxonPageLinksTheSpeciesThatMayBeTheSame() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        Assert.Contains("Possible duplicates in the Catalogue of Life and Wikidata", Html.Text(html));
        Assert.Contains("href=\"/wikidata/Q1003\"", html);
    }

    [Theory]
    [InlineData("/col/NOPE", "No taxon with Catalogue of Life ID NOPE")]
    [InlineData("/wikidata/Q999999", "No taxon with Wikidata item Q999999")]
    [InlineData("/wikidata/frog", "No taxon with Wikidata item frog")]
    public async Task AnUnknownIdIsNotFound(string path, string heading) {
        var response = await _client.GetAsync(path);
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Contains(heading, Html.Text(await response.Content.ReadAsStringAsync()));
    }

    [Fact]
    public async Task TheCatalogueOfLifeIdOfAnIucnTaxonGoesToItsPage() {
        var response = await _client.GetAsync("/col/4QHKG");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task TheWikidataItemOfAnIucnTaxonGoesToSearch() {
        var response = await _client.GetAsync("/wikidata/Q33609");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/search?q=Q33609", response.Headers.Location?.OriginalString);
    }

    [Theory]
    [InlineData("Ursus arctos", "/col/COLAR")]
    [InlineData("Q1001", "/col/COLAM")]
    [InlineData("https://www.wikidata.org/wiki/Q1003", "/wikidata/Q1003")]
    public async Task SearchGoesToTheOnlySpeciesWithTheNameOrItem(string q, string location) {
        var response = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(q));
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal(location, response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task SearchListsTheSpeciesAfterTheIucnTaxa() {
        var html = await _client.GetStringAsync("/search?q=ursus&all=1");
        var text = Html.Text(html);
        var iucn = text.IndexOf("Assessed taxa", StringComparison.Ordinal);
        var extra = text.IndexOf("Species not on the IUCN Red List", StringComparison.Ordinal);
        Assert.True(iucn >= 0 && extra > iucn, text);
        Assert.Contains("From the Catalogue of Life and Wikidata.", text);
        Assert.Contains("href=\"/col/COLAM\"", html);
        Assert.Contains("href=\"/col/COLAR\"", html);
        Assert.Contains("href=\"/wikidata/Q1003\"", html);
        Assert.Contains("CoL and Wikidata", text);
    }

    [Fact]
    public async Task SearchFindsASpeciesByItsEnglishName() {
        var html = await _client.GetStringAsync("/search?q=" + Uri.EscapeDataString("black bear"));
        Assert.Contains("href=\"/col/COLAM\"", html);
        Assert.DoesNotContain("No taxa found", Html.Text(html));
    }
}
