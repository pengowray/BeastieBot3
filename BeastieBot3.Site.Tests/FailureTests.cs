using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;

namespace BeastieBot3.Site.Tests;

public sealed class SchemaMismatchTests(SchemaMismatchSiteFactory factory) : IClassFixture<SchemaMismatchSiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task HealthzIs503() {
        var response = await _client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Equal($"unavailable: database schema version 999, this build of the site needs {BeastieBot3.Shared.SiteData.SiteDbSchema.Version}", body);
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

    [Fact]
    public async Task LoadingPagesFromWikipediaCountsAgainstTheUpdateLimit() {
        var client = factory.Client();
        for (var i = 0; i < RateLimitedSiteFactory.Limit; i++) {
            // No Wikipedia user agent is set in tests, so the page answers at once without asking Wikipedia.
            var answer = await client.GetAsync("/update?page=List%20of%20bears" + i);
            Assert.NotEqual(HttpStatusCode.TooManyRequests, answer.StatusCode);
        }
        Assert.Equal(HttpStatusCode.TooManyRequests, (await client.GetAsync("/update?page=List%20of%20bears")).StatusCode);
        // The form itself, with no page to load, is not an update.
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/update")).StatusCode);
    }
}

public sealed class TaxonPageLimitTests(TaxonPageLimitedSiteFactory factory) : IClassFixture<TaxonPageLimitedSiteFactory> {
    [Fact]
    public async Task TooManyTaxonPagesInAnHourGetThe429PageWithTheWait() {
        var client = factory.Client();
        for (var i = 0; i < 2; i++) {
            Assert.Equal(HttpStatusCode.OK, (await client.GetAsync($"/species/{FixtureDb.PolarBear}")).StatusCode);
        }
        var limited = await client.GetAsync($"/species/{FixtureDb.Tiger}");
        Assert.Equal(HttpStatusCode.TooManyRequests, limited.StatusCode);
        var text = Html.Text(await limited.Content.ReadAsStringAsync());
        Assert.Contains("This address has opened more taxon and group pages than the site allows in an hour or a day.", text);
        Assert.Matches(@"Try again in \d+ minutes\.", text);

        // Other pages count only against the per-minute limit.
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/about")).StatusCode);
    }
}

/// A clock the test moves by hand.
public sealed class ManualTime : TimeProvider {
    private DateTimeOffset _now = new(2026, 10, 3, 9, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan by) => _now += by;
}

/// A site over its own copy of the fixture, which the test replaces and removes, with a manual
/// clock for the 30-second recheck.
public sealed class ReplaceableSiteFactory : SiteFactory {
    public ManualTime Time { get; } = new();
    public string LivePath { get; } = FixtureDb.Create("live");
    protected override string DatabasePath => LivePath;

    protected override void ConfigureWebHost(IWebHostBuilder builder) {
        base.ConfigureWebHost(builder);
        builder.ConfigureTestServices(services => services.AddSingleton<TimeProvider>(Time));
    }
}

public sealed class DatabaseFileChangeTests(ReplaceableSiteFactory factory) : IClassFixture<ReplaceableSiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static readonly TimeSpan AfterRecheck = TimeSpan.FromSeconds(31);

    [Fact]
    public async Task AReplacedFileIsServedAndARemovedOneMakesHealthzFail() {
        var page = $"/species/{FixtureDb.PolarBear}";
        Assert.Contains("Data from IUCN Red List version 2026-1", Html.Text(await _client.GetStringAsync(page)));
        var cached = await _client.GetAsync(page);
        Assert.NotNull(cached.Headers.Age);

        // A new file moved into place: picked up after the recheck, and the cached page is evicted.
        // Its taxon data differs too, which shows that pooled connections to the old file are gone.
        var next = FixtureDb.Create("next", release: "2026-2");
        using (var connection = new SqliteConnection($"Data Source={next};Pooling=False")) {
            connection.Open();
            using var update = connection.CreateCommand();
            update.CommandText = $"UPDATE taxon SET common_name_en = 'Sea bear' WHERE taxon_id = {FixtureDb.PolarBear}";
            update.ExecuteNonQuery();
        }
        File.Move(next, factory.LivePath, overwrite: true);
        factory.Time.Advance(AfterRecheck);
        Assert.Equal("ok", await _client.GetStringAsync("/healthz"));
        var replaced = await _client.GetAsync(page);
        Assert.Null(replaced.Headers.Age);
        var replacedText = Html.Text(await replaced.Content.ReadAsStringAsync());
        Assert.Contains("Data from IUCN Red List version 2026-2", replacedText);
        Assert.Contains("Ursus maritimus Phipps, 1774 Sea bear", replacedText);

        // The file removed: /healthz says so after the recheck, and pages answer 503.
        var aside = factory.LivePath + ".aside";
        File.Move(factory.LivePath, aside);
        Assert.Equal(HttpStatusCode.OK, (await _client.GetAsync("/healthz")).StatusCode);
        factory.Time.Advance(AfterRecheck);
        var health = await _client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, health.StatusCode);
        Assert.Equal("unavailable: database file not found", await health.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await _client.GetAsync(page)).StatusCode);

        // Put back: ready again at the next check.
        File.Move(aside, factory.LivePath);
        factory.Time.Advance(AfterRecheck);
        Assert.Equal("ok", await _client.GetStringAsync("/healthz"));
        Assert.Equal(HttpStatusCode.OK, (await _client.GetAsync(page)).StatusCode);
    }

    [Fact]
    public async Task AFileWithTheWrongSchemaIsNotServed() {
        // Runs on its own copy, so the other test's moves do not matter.
        var path = FixtureDb.Create("wrong-later");
        await using var site = new SingleFileSiteFactory(path);
        var client = site.Client();
        Assert.Equal("ok", await client.GetStringAsync("/healthz"));

        File.Move(FixtureDb.Create("wrong", schemaVersion: "1"), path, overwrite: true);
        site.Time.Advance(AfterRecheck);
        var health = await client.GetAsync("/healthz");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, health.StatusCode);
        Assert.Equal($"unavailable: database schema version 1, this build of the site needs {BeastieBot3.Shared.SiteData.SiteDbSchema.Version}", await health.Content.ReadAsStringAsync());
    }

    private sealed class SingleFileSiteFactory(string path) : SiteFactory {
        public ManualTime Time { get; } = new();
        protected override string DatabasePath => path;

        protected override void ConfigureWebHost(IWebHostBuilder builder) {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(services => services.AddSingleton<TimeProvider>(Time));
        }
    }
}
