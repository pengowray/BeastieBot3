using System.Net;

namespace BeastieBot3.Site.Tests;

// The pages of higher taxa and the linked classification on taxon pages, over the fixture's
// groups: Animalia > Chordata > Mammalia > Carnivora > Caniformia (Catalogue of Life) > Ursidae >
// Ursus, with the polar bear in Ursus, and two genera named Abronia.
public sealed class GroupPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task TaxonPageLinksItsRanksAndHidesTheCatalogueOfLifeRank() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");

        Assert.Contains("href=\"/taxa/family/ursidae\"", html);
        Assert.Contains("href=\"/taxa/order/carnivora\"", html);
        Assert.Contains("<li class=\"col-group\">", html);
        Assert.Contains("Show 1 rank from the Catalogue of Life", html);
        Assert.Contains("<span class=\"group-common\">bears</span>", html);
    }

    [Fact]
    public async Task GroupPageShowsCountsAndAListInTheListsFormat() {
        var response = await _client.GetAsync("/taxa/family/ursidae");
        var html = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("Family Ursidae", html);
        Assert.Contains("rel=\"canonical\" href=\"http://localhost/taxa/family/ursidae\"", html);
        Assert.Contains("Common names in the Catalogue of Life (unchecked):", html);
        // Mammals default to common name only, with no rank headings below a family.
        Assert.Equal("== Vulnerable ==\n* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "list-wikitext"));
    }

    [Fact]
    public async Task ListOptionsComeFromTheQuery() {
        var html = await _client.GetStringAsync("/taxa/order/carnivora?style=sci&status=0&h=family&tpl=0");

        Assert.Equal("== Family Ursidae ==\nMembers of the [[Bear|Ursidae]] family are called bears.\n* [[Polar bear|''Ursus maritimus'']], Polar bear",
            Html.Textarea(html, "list-wikitext"));
    }

    [Fact]
    public async Task TwoGeneraWithOneNameAreBothListed() {
        var html = await _client.GetStringAsync("/taxa/genus/abronia");

        Assert.Contains("2 genera named Abronia", html);
        Assert.Contains("href=\"/taxa/genus/abronia?kingdom=plantae\"", html);
        Assert.Contains("href=\"/taxa/genus/abronia?kingdom=animalia\"", html);
        var plant = await _client.GetStringAsync("/taxa/genus/abronia?kingdom=plantae");
        Assert.Contains("href=\"/taxa/kingdom/plantae\"", plant);
        Assert.DoesNotContain("href=\"/taxa/kingdom/animalia\"", plant);
    }

    [Fact]
    public async Task UnknownRankAndNameIsNotFound() {
        var response = await _client.GetAsync("/taxa/family/nosuchidae");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Contains("Family nosuchidae not found on this site", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SearchGoesToTheOnlyGroupWithTheName() {
        var response = await _client.GetAsync("/search?q=Ursidae");

        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/taxa/family/ursidae", response.Headers.Location?.OriginalString);
    }
}
