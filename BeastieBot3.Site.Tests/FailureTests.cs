using System.Net;

namespace BeastieBot3.Site.Tests;

public sealed class SchemaMismatchTests(SchemaMismatchSiteFactory factory) : IClassFixture<SchemaMismatchSiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task HealthzIs503() {
        var response = await _client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Equal("unavailable", body);
    }

    [Theory]
    [InlineData("/")]
    [InlineData("/species/22823")]
    [InlineData("/search?q=ursus")]
    [InlineData("/api/suggest?q=urs")]
    public async Task PagesAre503(string url) {
        var response = await _client.GetAsync(url);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task UnavailablePageText() {
        var response = await _client.GetAsync("/");
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("Site unavailable", text);
        Assert.Contains("Try again in a few minutes.", text);
        // The version comes from the database, so the footer leaves it out.
        Assert.DoesNotContain("Data from IUCN Red List version", text);
    }
}

public sealed class ServerErrorTests(BrokenSiteFactory factory) : IClassFixture<BrokenSiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task FailingPageShowsTheServerErrorPageWithoutDetails() {
        var response = await _client.GetAsync($"/species/{FixtureDb.PolarBear}");
        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);
        Assert.Contains("Server error", text);
        Assert.Contains("Try again in a few minutes. If the error happens again, report it to User talk:Example on English Wikipedia.", text);
        Assert.DoesNotContain("SqliteException", html);
        Assert.DoesNotContain("no such table", html);
        Assert.DoesNotContain("at BeastieBot3", html);
        Assert.Contains("default-src 'self'", string.Join(";", response.Headers.GetValues("Content-Security-Policy")));
    }

    [Fact]
    public async Task OtherPagesStillWork() {
        var response = await _client.GetAsync("/about");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}

public sealed class RateLimitTests(RateLimitedSiteFactory factory) : IClassFixture<RateLimitedSiteFactory> {
    [Fact]
    public async Task TooManySearchesGetThe429Page() {
        var client = factory.Client();
        for (var i = 0; i < RateLimitedSiteFactory.Limit; i++) {
            var ok = await client.GetAsync("/search?q=zzzz" + i);
            Assert.Equal(HttpStatusCode.OK, ok.StatusCode);
        }
        var limited = await client.GetAsync("/search?q=zzzz-again");
        Assert.Equal(HttpStatusCode.TooManyRequests, limited.StatusCode);
        var text = Html.Text(await limited.Content.ReadAsStringAsync());
        Assert.Contains("Too many requests", text);
        Assert.Contains("Try again in a minute.", text);
        Assert.True(limited.Headers.Contains("Retry-After"));

        // Health checks are never limited.
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/healthz")).StatusCode);
    }
}
