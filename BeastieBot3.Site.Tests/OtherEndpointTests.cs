using System.Net;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace BeastieBot3.Site.Tests;

public sealed class OtherEndpointTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task NameWithOneTaxonRedirects() {
        var response = await _client.GetAsync("/name/Ursus_maritimus");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}", response.Headers.Location?.OriginalString);

        var common = await _client.GetAsync("/name/polar%20BEAR");
        Assert.Equal($"/species/{FixtureDb.PolarBear}", common.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task NameWithSeveralTaxaListsThem() {
        var response = await _client.GetAsync("/name/Big_cat");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);
        Assert.Contains("Taxa with the name “Big cat”", text);
        Assert.Contains("2 taxa have “Big cat” as their scientific name, a common name or a synonym. Select one to see its page.", text);
        Assert.Contains($"/species/{FixtureDb.Tiger}\"", html);
        Assert.Contains($"/species/{FixtureDb.Lion}\"", html);
    }

    [Fact]
    public async Task NameWithNoTaxonGoesToSearch() {
        var response = await _client.GetAsync("/name/Nothing_here");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/search?q=Nothing%20here", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task HealthzIsOk() {
        var response = await _client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("ok", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task SuggestReturnsNamesIdsAndCategoryOnly() {
        var response = await _client.GetAsync("/api/suggest?q=Urs");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var items = doc.RootElement.EnumerateArray().ToList();
        var bear = Assert.Single(items);
        Assert.Equal(["taxonId", "name", "commonName", "category"], bear.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal(FixtureDb.PolarBear, bear.GetProperty("taxonId").GetInt64());
        Assert.Equal("Ursus maritimus", bear.GetProperty("name").GetString());
        Assert.Equal("Polar bear", bear.GetProperty("commonName").GetString());
        Assert.Equal("VU", bear.GetProperty("category").GetString());
    }

    [Fact]
    public async Task SuggestIsCappedAtTenAndKeepsPossiblyExtinct() {
        using var many = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=Fillerus"));
        Assert.Equal(10, many.RootElement.GetArrayLength());

        using var baiji = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=Lipotes"));
        Assert.Equal("CR(PE)", baiji.RootElement[0].GetProperty("category").GetString());

        using var none = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=a"));
        Assert.Equal(0, none.RootElement.GetArrayLength());

        using var regional = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=Gobio"));
        Assert.Equal(JsonValueKind.Null, regional.RootElement[0].GetProperty("category").ValueKind);
    }

    [Fact]
    public async Task NoOtherApi() {
        var response = await _client.GetAsync("/api/taxon/22823");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RobotsTxt() {
        var response = await _client.GetAsync("/robots.txt");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("User-agent: *", body);
        Assert.Contains("Allow: /", body);
        Assert.Contains("Disallow: /search", body);
        Assert.Contains("Disallow: /api/", body);
    }

    [Theory]
    [InlineData("/favicon.ico")]
    [InlineData("/favicon-32x32.png")]
    [InlineData("/apple-touch-icon.png")]
    [InlineData("/logo-80.png")]
    [InlineData("/site.webmanifest")]
    public async Task IconsAreServed(string path) {
        var response = await _client.GetAsync(path);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task HeaderHasTheLogo() {
        var html = await _client.GetStringAsync("/about");
        Assert.Contains("<img class=\"site-logo\" src=\"/logo-80.png\" width=\"40\" height=\"40\" alt=\"\">", html);
        Assert.Contains("<link rel=\"apple-touch-icon\" href=\"/apple-touch-icon.png\">", html);
        Assert.Contains("The bat logo is by spiky.fish.", Html.Text(html));
        Assert.Contains("The server keeps a log of each request for 14 days", Html.Text(html));
        Assert.Contains("<a href=\"https://en.wikipedia.org/wiki/User:Pengo\">User:Pengo</a>", html);
    }

    [Fact]
    public async Task AboutPage() {
        var html = await _client.GetStringAsync("/about");
        var text = Html.Text(html);
        Assert.Contains("About Beastie Bot Species Status", text);
        Assert.Contains("Beastie Bot Species Status is an unofficial website for looking up IUCN Red List assessments and citing them on English Wikipedia.", text);
        Assert.Contains("contact User talk:Example on English Wikipedia. Include the address of the taxon page.", text);
        Assert.Contains("its earlier global assessments, and its latest assessment in each region", text);
        Assert.Contains("These pages show the taxon's earlier assessments, with wikitext for each.", text);
        Assert.Contains("Version 2026-1. Assessment details downloaded from the IUCN Red List API between 18 August and 1 September 2026.", text);
        Assert.Contains("COL26.7 XR", text);
        Assert.Contains("1 October 2026", text);
        Assert.Contains("28 July 2026", text);
        Assert.Contains("Cite the IUCN Red List as: IUCN 2026. The IUCN Red List of Threatened Species. Version 2026-1. https://www.iucnredlist.org", text);
        Assert.Contains("<th scope=\"row\"><a href=\"https://doi.org/10.48580/dgykv\">Catalogue of Life</a></th>", html);
    }

    [Fact]
    public async Task AboutPageAttributesEverySource() {
        var html = await _client.GetStringAsync("/about");
        var text = Html.Text(html);
        Assert.Contains("This site reformats and combines the data from these sources.", text);

        // Every licence cell links to its licence.
        Assert.Equal(3, Regex.Matches(html, "<td><a href=\"https://creativecommons.org/licenses/by/4.0/\">CC BY 4.0</a></td>").Count);
        Assert.Contains("<td><a href=\"https://creativecommons.org/licenses/by-sa/4.0/\">CC BY-SA 4.0</a></td>", html);
        Assert.Contains("<td><a href=\"https://creativecommons.org/publicdomain/zero/1.0/\">CC0</a></td>", html);
        Assert.Contains("<td><a href=\"https://www.crossref.org/documentation/retrieve-metadata/\">CC0</a></td>", html);
        Assert.Contains("<td><a href=\"https://www.iucnredlist.org/terms/terms-of-use\">IUCN Red List Terms of Use</a></td>", html);

        // Every source the taxon pages name beside an English common name says it supplies names.
        foreach (var source in new[] { "IUCN Red List", "Wikidata", "English Wikipedia", "Catalogue of Life" }) {
            Assert.Matches($">{Regex.Escape(source)}</a></th>\\s*<td>[^<]*common names", html);
        }
        Assert.Contains("English common names, synonyms, and links to Catalogue of Life pages", text);
        Assert.Contains("English common names (from article titles and taxoboxes) and links to Wikipedia articles", text);
        Assert.Contains("DOIs of the latest global assessments", text);

        // Crossref, the last DOI source, with the date of the newest DOI check.
        Assert.Contains("<th scope=\"row\"><a href=\"https://www.crossref.org/documentation/retrieve-metadata/rest-api/\">Crossref</a></th>", html);
        Assert.Contains("DOIs not found in IUCN's citation text, GBIF or Wikidata, taken from Crossref's list of the DOIs IUCN has registered. "
            + "For recent assessments not in that list, this site checks possible DOIs at doi.org.", text);
        Assert.Contains("DOIs checked on various dates up to 30 September 2026", text);
        Assert.Contains("which includes a DOI for about 12% of latest assessments", text);
        Assert.DoesNotContain("subpopulation assessments have no citation", text);

        // Citations with links to the material.
        Assert.Contains("<th scope=\"row\"><a href=\"https://doi.org/10.15468/0qnb58\">GBIF: IUCN checklist, published by IUCN</a></th>", html);
        Assert.Contains("GBIF: " + FixtureDb.GbifCitation, text);
        Assert.Contains("<a href=\"https://doi.org/10.15468/0qnb58\">https://doi.org/10.15468/0qnb58</a>", html);
        Assert.Contains("Catalogue of Life: " + FixtureDb.ColCitation, text);
        Assert.Contains("<a href=\"https://doi.org/10.48580/dgykv\">https://doi.org/10.48580/dgykv</a>", html);
        Assert.Contains("Department of Climate Change, Energy, the Environment and Water (DCCEEW), Australian Government", text);
        Assert.Contains("<a href=\"https://www.environment.gov.au/cgi-bin/sprat/public/sprat.pl\">", html);
    }

    [Fact]
    public async Task AboutPageWithoutSourceCitationsInTheDatabase() {
        await using var site = new NoCitationsSiteFactory();
        var html = await site.Client().GetStringAsync("/about");
        var text = Html.Text(html);
        Assert.Contains("<a href=\"https://doi.org/10.15468/0qnb58\">GBIF: IUCN checklist, published by IUCN</a>", html);
        Assert.Contains("GBIF: IUCN checklist on GBIF, published by IUCN. https://doi.org/10.15468/0qnb58", text);
        Assert.Contains("<th scope=\"row\"><a href=\"https://www.catalogueoflife.org\">Catalogue of Life</a></th>", html);
        Assert.Contains("Catalogue of Life: Catalogue of Life, release COL26.7 XR. https://www.catalogueoflife.org", text);
        // No DOI check date: the Crossref row's version cell is empty.
        Assert.Matches(">Crossref</a></th>\\s*<td>[^<]*</td>\\s*<td><a [^>]*>CC0</a></td>\\s*<td></td>", html);
        Assert.DoesNotContain("DOIs checked on various dates", text);
    }

    private sealed class NoCitationsSiteFactory : SiteFactory {
        private static readonly Lazy<string> Db = new(() => FixtureDb.Create("no-citations", withSourceCitations: false));
        protected override string DatabasePath => Db.Value;
    }

    [Fact]
    public async Task UnknownPathIs404Page() {
        var response = await _client.GetAsync("/no/such/page");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("Page not found", text);
        Assert.Contains("Check the address, or search for a taxon.", text);
    }

    [Fact]
    public async Task PostIsNotAllowed() {
        var response = await _client.PostAsync("/search?q=ursus", new StringContent("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.Equal("GET, HEAD", string.Join(", ", response.Content.Headers.Allow));
    }

    [Theory]
    [InlineData("/")]
    [InlineData("/species/22823")]
    [InlineData("/no/such/page")]
    [InlineData("/api/suggest?q=urs")]
    [InlineData("/site.css")]
    [InlineData("/update")]
    public async Task SecurityHeaders(string url) {
        var response = await _client.GetAsync(url);
        var headers = response.Headers;
        var csp = string.Join(";", headers.GetValues("Content-Security-Policy"));
        Assert.Contains("default-src 'self'", csp);
        Assert.Contains("script-src 'self'", csp);
        Assert.Contains("frame-ancestors 'none'", csp);
        Assert.DoesNotContain("unsafe-inline", csp);
        Assert.Equal("nosniff", headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("strict-origin-when-cross-origin", headers.GetValues("Referrer-Policy").Single());
        Assert.Contains("camera=()", headers.GetValues("Permissions-Policy").Single());
        Assert.Equal("DENY", headers.GetValues("X-Frame-Options").Single());
        Assert.False(headers.Contains("Set-Cookie"));
    }

    [Fact]
    public async Task SecurityHeadersOnACachedTaxonPage() {
        // The second request is answered by the output cache.
        const string url = "/species/22823?authors=author&q=cache-test";
        var first = await _client.GetAsync(url);
        var second = await _client.GetAsync(url);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Null(first.Headers.Age);
        Assert.NotNull(second.Headers.Age);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.Contains("default-src 'self'", string.Join(";", second.Headers.GetValues("Content-Security-Policy")));
        Assert.Equal("nosniff", second.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.False(second.Headers.Contains("Set-Cookie"));
    }

    [Fact]
    public async Task PagesHaveNoInlineScriptsOrStyles() {
        foreach (var url in new[] { "/", "/species/22823", "/search?q=Fillerus", "/about", "/no/such/page" }) {
            var html = await (await _client.GetAsync(url)).Content.ReadAsStringAsync();
            Assert.DoesNotMatch("<script(?![^>]*\\bsrc=)", html);
            Assert.DoesNotContain(" style=\"", html);
            Assert.DoesNotContain("<style", html);
            Assert.DoesNotMatch(" on[a-z]+=\"", html);
        }
    }

    [Fact]
    public async Task NoResponseContainsAssessmentNarrative() {
        var urls = new List<string> { "/", "/about", "/search?q=Ursus&all=1", "/api/suggest?q=Urs", "/name/Big_cat" };
        foreach (var id in new[] { FixtureDb.PolarBear, FixtureDb.HouseSparrow, FixtureDb.Baiji, FixtureDb.Tiger, FixtureDb.SumatranTiger,
                     FixtureDb.Lion, FixtureDb.WestAfricanLion, FixtureDb.RegionalOnly, FixtureDb.Variety, FixtureDb.Koala,
                     FixtureDb.AmurLeopard, FixtureDb.WoylieOld, FixtureDb.Woylie }) {
            urls.Add($"/species/{id}");
            urls.Add($"/species/{id}?authors=author&access=none");
        }
        urls.Add($"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}");
        foreach (var url in urls) {
            var body = await (await _client.GetAsync(url)).Content.ReadAsStringAsync();
            Assert.DoesNotContain(FixtureDb.NarrativeMarker, body);
        }
    }

    [Fact]
    public async Task SpeciesPagesAreCacheable() {
        var response = await _client.GetAsync($"/species/{FixtureDb.PolarBear}");
        Assert.Contains("max-age=600", response.Headers.CacheControl?.ToString());
        Assert.True(response.Headers.CacheControl?.Public);
    }

    [Fact]
    public async Task StaticFilesAreServedWithVersionedLinks() {
        var html = await _client.GetStringAsync("/");
        Assert.Matches("<link rel=\"stylesheet\" href=\"/site.css\\?v=[^\"]+\">", html);
        Assert.Matches("<script src=\"/site.js\\?v=[^\"]+\" defer></script>", html);
        var js = await _client.GetAsync("/site.js");
        Assert.Equal(HttpStatusCode.OK, js.StatusCode);
    }
}
