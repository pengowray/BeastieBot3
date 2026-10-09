using System.Net;

namespace BeastieBot3.Site.Tests;

// The tool pages behind the row of tool tabs: /tools, /cite, /update-statuses, /species-list-maker.
public sealed class ToolPagesTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task EveryToolPageHasTheTabsWithItsOwnMarked() {
        foreach (var (url, current) in new[] {
                     ("/tools", (string?)null), ("/cite", "/cite"), ("/update-statuses", "/update-statuses"), ("/species-list-maker", "/species-list-maker"),
                     ($"/species/{FixtureDb.PolarBear}/wikitext", "/cite"), ($"/species/{FixtureDb.PolarBear}/wikidata", "/cite"),
                     ("/taxa/family/ursidae/list", "/species-list-maker"),
                 }) {
            var html = await _client.GetStringAsync(url);
            Assert.Contains("<nav class=\"tool-tabs\"", html);
            if (current is not null) {
                Assert.Contains($"<a href=\"{current}\" aria-current=\"page\">", html);
            }
        }
    }

    [Theory]
    [InlineData("/cite?q=Ursus+maritimus", "/species/22823/wikitext")]
    [InlineData("/cite?q=Ursus+maritimus&for=wikidata", "/species/22823/wikidata")]
    [InlineData("/cite?q=Ursus+maritimus&for=wikipedia&wiki=fr", "/species/22823/wikitext?wiki=fr")]
    public async Task CiteGoesStraightToTheToolPageOfTheOneTaxonFound(string url, string location) {
        var response = await _client.GetAsync(url);
        Assert.Equal(HttpStatusCode.Found, response.StatusCode);
        Assert.Equal(location, response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task CiteWithAnAssessmentIdGoesToThatAssessment() {
        var response = await _client.GetAsync($"/cite?q=A{FixtureDb.PolarBear2008}&for=wikidata");
        Assert.Equal($"/species/{FixtureDb.PolarBear}/wikidata?assessment={FixtureDb.PolarBear2008}", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task CiteListsSeveralTaxaLinkingTheirToolPages() {
        var html = await _client.GetStringAsync("/cite?q=Panthera&for=wikidata");
        Assert.Contains($"href=\"/species/{FixtureDb.Tiger}/wikidata\"", html);
        Assert.Contains("<input type=\"radio\" name=\"for\" value=\"wikidata\" checked=\"checked\" data-hash-choice=\"wikidata\">", html);
    }

    [Fact]
    public async Task ListMakerGoesStraightToTheListOfTheOneGroupFound() {
        var response = await _client.GetAsync("/species-list-maker?q=Ursidae");
        Assert.Equal(HttpStatusCode.Found, response.StatusCode);
        Assert.Equal("/taxa/family/ursidae/list", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task ListMakerOffersTheGroupsOfASpecies() {
        var html = await _client.GetStringAsync("/species-list-maker?q=Ursus+maritimus");
        Assert.Contains("href=\"/taxa/genus/ursus/list\"", html);
        Assert.Contains("href=\"/taxa/family/ursidae/list\"", html);
        Assert.Contains("<a href=\"/update-statuses\">", html);
    }

    [Fact]
    public async Task UpdateStatusesIsTheUpdatePage_WithAnExampleAndItsFormPostingToUpdate() {
        var html = await _client.GetStringAsync("/update-statuses");
        Assert.Contains("<form class=\"update-form\" id=\"update-form\" method=\"post\" action=\"/update#result\"", html);
        Assert.Contains("id=\"update-example-heading\"", html);
        Assert.Contains("{{IUCN status|VU|712/121745669|1|year=2016}}", html);
        Assert.Contains("<link rel=\"canonical\" href=\"http://localhost/update-statuses\">", html);
        // The settings are folded until there is a result.
        Assert.Contains("<details class=\"update-settings\">", html);
    }
}
