using System.Net;
using System.Threading.RateLimiting;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Pages;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;

namespace BeastieBot3.Site.Tests;

public sealed class FtsQueryTests {
    [Theory]
    [InlineData("panthera ti", "\"panthera\" \"ti\"*")]
    [InlineData("  Ursus   maritimus ", "\"Ursus\" \"maritimus\"*")]
    [InlineData("NEAR(ursus, 2)", "\"NEAR\" \"ursus\" \"2\"*")]
    [InlineData("a:b", "\"a\" \"b\"*")]
    [InlineData("\"quoted\"", "\"quoted\"*")]
    [InlineData("Wilson's storm-petrel", "\"Wilson\" \"s\" \"storm\" \"petrel\"*")]
    [InlineData("Ours blé", "\"Ours\" \"blé\"*")]
    [InlineData("va", "\"va\"*")]
    [InlineData("Panthera t", "\"Panthera\" \"t\"*")]
    // A lone 1-character word is never a prefix: name_fts has no 1-character prefix index.
    [InlineData("a", "\"a\"")]
    [InlineData("s.", "\"s\"")]
    [InlineData("á", "\"á\"")]
    [InlineData("x̃", "\"x̃\"")]
    public void Build(string input, string expected) => Assert.Equal(expected, FtsQuery.Build(input));

    [Theory]
    [InlineData("", true)]
    [InlineData("**", true)]
    [InlineData("a", true)]
    [InlineData("a.", true)]
    [InlineData("s-", true)]
    [InlineData("-s", true)]
    [InlineData("á", true)]
    [InlineData("x̃", true)]
    [InlineData("1", true)]
    [InlineData("va", false)]
    [InlineData("18", false)]
    [InlineData("s s", false)]
    [InlineData("Panthera t", false)]
    public void TooShort(string input, bool expected) => Assert.Equal(expected, FtsQuery.IsTooShort(input));

    [Theory]
    [InlineData("")]
    [InlineData("\"")]
    [InlineData("*")]
    [InlineData(":: ^ -")]
    public void NothingToMatch(string input) => Assert.Null(FtsQuery.Build(input));

    [Fact]
    public void TokensAreCapped() =>
        Assert.Equal(FtsQuery.MaxTokens, FtsQuery.Tokens("a b c d e f g h i j k l").Count);
}

public sealed class DisplayTests {
    [Theory]
    [InlineData("NT", "Near Threatened", "cat-nt")]
    [InlineData("nt", "Not Threatened (1994 or earlier categories)", "cat-other")]
    [InlineData("EX", "Extinct", "cat-ex")]
    [InlineData("Ex", "Extinct (1994 or earlier categories)", "cat-other")]
    [InlineData("LR/nt", "Lower Risk/near threatened", "cat-nt")]
    [InlineData("LR/lc", "Lower Risk/least concern", "cat-lc")]
    [InlineData("LR/cd", "Lower Risk/conservation dependent", "cat-nt")]
    [InlineData("K", "Insufficiently Known (1994 or earlier categories)", "cat-other")]
    [InlineData("I", "Indeterminate (1994 or earlier categories)", "cat-other")]
    [InlineData("DD", "Data Deficient", "cat-grey")]
    [InlineData("RE", "Regionally Extinct", "cat-ew")]
    [InlineData("NR", "Not Recognized", "cat-other")]
    [InlineData("ZZ", "ZZ (old IUCN category)", "cat-other")]
    public void CategoryLabels(string code, string label, string css) {
        var display = IucnCategories.Describe(code, false, false);
        Assert.Equal(label, display.Label);
        Assert.Equal(css, display.CssClass);
        Assert.Equal(code, display.BadgeText);
    }

    private static AssessmentRow Row(string category, string? version, bool latest) =>
        new(1, 1, "Global", latest, category, false, false, null, version, 1998, null, null, null);

    [Fact]
    public void CurrentCodesWithNoCriteriaVersion() {
        var nt = IucnCategories.Describe(Row("NT", null, latest: false));
        Assert.Equal("NT", nt.BadgeText);
        Assert.Equal("No name given by IUCN (1994 or earlier categories)", nt.Label);
        Assert.Equal("cat-other", nt.CssClass);
        Assert.Equal("Extinct", IucnCategories.Describe(Row("EX", null, latest: false)).Label);
        Assert.False(IucnCategories.HasStatusTemplateCode(Row("NT", null, latest: false)));
        Assert.False(IucnCategories.HasTaxoboxCode(Row("EX", null, latest: false)));

        // Never a latest assessment, and never an LR code (the code says which version it is).
        Assert.Equal("Near Threatened", IucnCategories.Describe(Row("NT", null, latest: true)).Label);
        Assert.True(IucnCategories.HasStatusTemplateCode(Row("NT", null, latest: true)));
        Assert.True(IucnCategories.HasStatusTemplateCode(Row("LR/lc", null, latest: false)));
        Assert.True(IucnCategories.HasStatusTemplateCode(Row("NT", "3.1", latest: false)));
    }

    [Theory]
    [InlineData("species", "ANIMALIA", "{{Speciesbox}} status parameters", "{{IUCN status}} and {{Speciesbox}} have no code for this category.")]
    [InlineData("species", "PLANTAE", "{{Speciesbox}} status parameters", "{{IUCN status}} and {{Speciesbox}} have no code for this category.")]
    [InlineData("subspecies", "ANIMALIA", "{{Subspeciesbox}} status parameters", "{{IUCN status}} and {{Subspeciesbox}} have no code for this category.")]
    [InlineData("subspecies", "PLANTAE", "{{Infraspeciesbox}} status parameters", "{{IUCN status}} and {{Infraspeciesbox}} have no code for this category.")]
    [InlineData("subspecies", "FUNGI", "{{Infraspeciesbox}} status parameters", "{{IUCN status}} and {{Infraspeciesbox}} have no code for this category.")]
    [InlineData("variety", "PLANTAE", "{{Infraspeciesbox}} status parameters", "{{IUCN status}} and {{Infraspeciesbox}} have no code for this category.")]
    [InlineData("subpopulation", "ANIMALIA", "Taxobox status parameters", "{{IUCN status}} and the taxobox have no code for this category.")]
    public void TaxoboxByKind(string kind, string kingdom, string label, string noCode) {
        var taxobox = TaxoboxTemplate.For(kind, kingdom);
        Assert.Equal(label, taxobox.Label);
        Assert.Equal(noCode, SiteText.NoTemplateCode(taxobox.Noun));
        Assert.Equal($"{label} are given for global assessments only.", SiteText.TaxoboxGlobalOnly(taxobox.Label));
    }

    [Fact]
    public void PossiblyExtinct() {
        Assert.Equal("Critically Endangered (Possibly Extinct)", IucnCategories.Describe("CR", true, false).Label);
        Assert.Equal("CR (PEW)", IucnCategories.Describe("CR", false, true).BadgeText);
        Assert.Equal("Critically Endangered (Possibly Extinct in the Wild)", IucnCategories.Describe("CR", false, true).Label);
    }

    [Fact]
    public void TemplateCodesAreCaseSensitive() {
        Assert.True(IucnCategories.HasStatusTemplateCode("NT"));
        Assert.False(IucnCategories.HasStatusTemplateCode("nt"));
        Assert.False(IucnCategories.HasStatusTemplateCode("V"));
        Assert.True(IucnCategories.HasStatusTemplateCode("LR/cd"));
        Assert.False(IucnCategories.HasTaxoboxCode("RE"));
        Assert.True(IucnCategories.HasStatusTemplateCode("RE"));
    }

    [Fact]
    public void DateRanges() {
        Assert.Equal("between 18 August and 1 September 2026", SiteFormat.DateRange(new DateOnly(2026, 8, 18), new DateOnly(2026, 9, 1)));
        Assert.Equal("between 30 December 2025 and 2 January 2026", SiteFormat.DateRange(new DateOnly(2025, 12, 30), new DateOnly(2026, 1, 2)));
        Assert.Equal("on 18 August 2026", SiteFormat.DateRange(new DateOnly(2026, 8, 18), new DateOnly(2026, 8, 18)));
    }

    [Fact]
    public void Links() {
        Assert.Equal("https://en.wikipedia.org/wiki/Polar_bear", SiteFormat.WikipediaUrl("Polar bear"));
        Assert.Equal("https://en.wikipedia.org/wiki/Wilson's_storm_petrel_(bird)", SiteFormat.WikipediaUrl("Wilson's storm petrel (bird)"));
        Assert.Equal("https://en.wikipedia.org/wiki/B%C3%A9a%3F%23", SiteFormat.WikipediaUrl("Béa?#"));
    }

    [Fact]
    public void Numbers() => Assert.Equal("179,000", SiteFormat.Number(179000));

    [Fact]
    public void ChildrenHeading() {
        Assert.Equal("Subspecies", SiteText.HeadingChildren(true, false, false));
        Assert.Equal("Subspecies and subpopulations", SiteText.HeadingChildren(true, false, true));
        Assert.Equal("Subspecies, varieties and subpopulations", SiteText.HeadingChildren(true, true, true));
        Assert.Equal("Varieties", SiteText.HeadingChildren(false, true, false));
    }

    [Fact]
    public void Plurals() {
        Assert.Equal("1 taxon found", SiteText.SearchCount(1));
        Assert.Equal("1,234 taxa found", SiteText.SearchCount(1234));
        Assert.Equal("1 regional assessment", SiteText.NoGlobalLinkText(1));
    }

    [Fact]
    public void NoEmDashes() {
        foreach (var field in typeof(SiteText).GetFields()) {
            if (field.GetValue(null) is string value) {
                Assert.DoesNotContain("—", value);
            }
        }
    }
}

public sealed class WikitextOptionsTests {
    [Fact]
    public void DefaultsWhenNothingIsGiven() {
        var options = WikitextOptions.FromQuery(null, null, null, null, null, null);
        Assert.Equal(WikitextOptions.Default, options);
        Assert.Equal(string.Empty, options.ToQuery(null, "iucn"));
    }

    [Fact]
    public void UntickedBoxesCountOnlyWhenTheFormWasSent() {
        Assert.False(WikitextOptions.FromQuery(null, null, "1", null, null, null).WrapInRef);
        Assert.True(WikitextOptions.FromQuery(null, null, null, null, null, null).WrapInRef);
    }

    [Fact]
    public void RoundTrip() {
        var options = new WikitextOptions(CiteAuthorStyle.LastFirst, WikitextOptions.AccessNone, false, "my ref", true, "iucn");
        Assert.Equal("?assessment=5&authors=lastfirst&access=none&opts=1&amp=1&refname=my%20ref", options.ToQuery(5, "iucn2008"));
        Assert.Equal(options with { DefaultRefName = "iucn2008" }, WikitextOptions.FromQuery("lastfirst", "none", "1", null, "my ref", "1", "iucn2008"));
    }

    [Fact]
    public void EmptyAndMissingRefNameMeanTheSame() {
        // The output cache keys on query values, and an empty value looks like a missing one.
        Assert.Equal(string.Empty, WikitextOptions.FromQuery(null, null, "1", "1", "", null).RefName);
        Assert.Equal(string.Empty, WikitextOptions.FromQuery(null, null, "1", "1", null, null).RefName);
        Assert.Equal("iucn", WikitextOptions.FromQuery(null, null, null, null, "", null).RefName);
        Assert.Equal("iucn", WikitextOptions.FromQuery(null, null, null, null, null, null).RefName);

        var plainRef = WikitextOptions.Default with { RefName = string.Empty };
        Assert.Equal("?opts=1&ref=1", plainRef.ToQuery(null, "iucn"));
        Assert.Equal(plainRef, WikitextOptions.FromQuery(null, null, "1", "1", null, null));
    }

    [Fact]
    public void TheDefaultRefNameIsThatOfTheAssessmentShown() {
        Assert.Equal("iucn2008", WikitextOptions.FromQuery(null, null, null, null, null, null, "iucn2008").RefName);
        // The form sends the pre-filled default back; it stays the default, not the visitor's choice.
        var submitted = WikitextOptions.FromQuery("lastfirst", null, "1", "1", "iucn", null, "iucn");
        Assert.Null(submitted.CustomRefName);
        Assert.Equal("?assessment=5&authors=lastfirst", submitted.ToQuery(5, "iucn2008"));
    }

    [Fact]
    public void LinksToOtherAssessmentsUseTheirOwnDefault() {
        var onEurope = WikitextOptions.FromQuery(null, null, null, null, null, null, "iucn-europe");
        // To the latest global assessment: no refname, so its default "iucn" applies.
        Assert.Equal(string.Empty, onEurope.ToQuery(null, "iucn"));

        // When the form part is needed for another option, the target's default is written out,
        // because with opts=1 a missing refname means a plain <ref>.
        var withAmp = onEurope with { Amp = true };
        Assert.Equal("?assessment=9&opts=1&ref=1&amp=1&refname=iucn2008", withAmp.ToQuery(9, "iucn2008"));
        Assert.Equal("iucn2008", WikitextOptions.FromQuery(null, null, "1", "1", "iucn2008", "1", "iucn2008").RefName);

        // A name the visitor chose goes along.
        var chosen = onEurope with { RefName = "sparrow" };
        Assert.Equal("?opts=1&ref=1&refname=sparrow", chosen.ToQuery(null, "iucn"));
        // A plain <ref> goes along too.
        var plain = onEurope with { RefName = string.Empty };
        Assert.Equal("?opts=1&ref=1", plain.ToQuery(null, "iucn"));
    }

    private static AssessmentRow Row(long id, string scope, int? year, bool latest = false) =>
        new(id, 1, scope, latest, "LC", false, false, null, "3.1", year, null, null, null);

    [Fact]
    public void DefaultRefNameRules() {
        var latest = Row(10, "Global", 2019, latest: true);
        var replaced = Row(11, "Global", 2019);
        var older = Row(12, "Global", 2008);
        var noYear = Row(13, "Global", null);
        AssessmentRow[] history = [latest, replaced, older, noYear];
        Assert.Equal("iucn", Pages.DefaultRefNames.For(latest, 10, history));
        Assert.Equal("iucn2019-11", Pages.DefaultRefNames.For(replaced, 10, history));
        Assert.Equal("iucn2008", Pages.DefaultRefNames.For(older, 10, history));
        Assert.Equal("iucn-13", Pages.DefaultRefNames.For(noYear, 10, history));
        Assert.Equal("iucn-gulf-of-mexico", Pages.DefaultRefNames.For(Row(20, "Gulf of Mexico", 2015), 10, history));
        Assert.Equal("iucn-s-africa-fw", Pages.DefaultRefNames.For(Row(21, "S. Africa FW", 2015), 10, history));
        Assert.Equal("iucn-global-pan-africa-western-africa", Pages.DefaultRefNames.For(Row(22, "Global, Pan-Africa & Western Africa", 2015), 10, history));
        Assert.Equal("iucn-reunion", Pages.DefaultRefNames.For(Row(23, "Réunion", 2015), 10, history));
        var longName = Pages.DefaultRefNames.For(Row(24, "Northern and Western Africa and the Mediterranean coast", 2015), 10, history);
        Assert.True(longName.Length <= WikitextOptions.MaxRefNameLength);
        Assert.False(longName.EndsWith('-'));
    }

    [Fact]
    public void LongRefNamesAreCut() =>
        Assert.Equal(WikitextOptions.MaxRefNameLength, WikitextOptions.FromQuery(null, null, null, null, new string('x', 200), null).RefName.Length);
}

public sealed class RateLimitKeyTests {
    [Fact]
    public void Ipv6ClientsAreCountedPerSlash64() {
        var a = SiteRateLimits.ClientKey(IPAddress.Parse("2001:db8:1:2:aaaa:bbbb:cccc:dddd"));
        var b = SiteRateLimits.ClientKey(IPAddress.Parse("2001:db8:1:2::1"));
        var c = SiteRateLimits.ClientKey(IPAddress.Parse("2001:db8:1:3::1"));
        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }

    private static HttpContext Request(string path, string ip) {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        context.Connection.RemoteIpAddress = IPAddress.Parse(ip);
        return context;
    }

    [Fact]
    public void SearchesAcrossAllClientsShareOneConcurrencyLimit() {
        var limits = new RateLimitOptions { SearchPerMinute = 1000, SuggestPerMinute = 1000, PagesPerMinute = 1000, ConcurrentSearches = 2, SearchQueueLength = 0 };
        using var limiter = SiteRateLimits.BuildGlobalLimiter(limits);

        using var first = limiter.AttemptAcquire(Request("/search", "203.0.113.1"));
        using var second = limiter.AttemptAcquire(Request("/api/suggest", "203.0.113.2"));
        Assert.True(first.IsAcquired);
        Assert.True(second.IsAcquired);

        // A third search from yet another client waits for a slot; with no queue it is turned away.
        using (var third = limiter.AttemptAcquire(Request("/search", "203.0.113.3"))) {
            Assert.False(third.IsAcquired);
        }
        // Other pages are not affected.
        using (var page = limiter.AttemptAcquire(Request("/species/22823", "203.0.113.3"))) {
            Assert.True(page.IsAcquired);
        }

        first.Dispose();
        using var fourth = limiter.AttemptAcquire(Request("/search", "203.0.113.3"));
        Assert.True(fourth.IsAcquired);
    }

    [Fact]
    public void AClientOverItsLimitIsTurnedAwayBeforeTheSharedLimit() {
        var limits = new RateLimitOptions { SearchPerMinute = 1, ConcurrentSearches = 10, SearchQueueLength = 0 };
        using var limiter = SiteRateLimits.BuildGlobalLimiter(limits);
        using var first = limiter.AttemptAcquire(Request("/search", "203.0.113.9"));
        Assert.True(first.IsAcquired);
        using var second = limiter.AttemptAcquire(Request("/search", "203.0.113.9"));
        Assert.False(second.IsAcquired);
        Assert.True(second.TryGetMetadata(MetadataName.RetryAfter, out _));
    }

    [Fact]
    public void Ipv4ClientsAreCountedOneByOne() {
        Assert.Equal("203.0.113.7", SiteRateLimits.ClientKey(IPAddress.Parse("203.0.113.7")));
        Assert.Equal("203.0.113.7", SiteRateLimits.ClientKey(IPAddress.Parse("::ffff:203.0.113.7")));
        Assert.Equal("unknown", SiteRateLimits.ClientKey(null));
    }
}

public sealed class DatabasePathTests {
    [Fact]
    public void HomeIsExpanded() {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        Assert.Equal(Path.Combine(home, "datasets/beastiebot/site.sqlite"), SiteDatabase.ExpandHome("~/datasets/beastiebot/site.sqlite"));
        Assert.Equal("/srv/site.sqlite", SiteDatabase.ExpandHome("/srv/site.sqlite"));
        Assert.Null(SiteDatabase.ExpandHome("  "));
    }

    [Fact]
    public void SpratReportDate() {
        Assert.Equal("1 October 2026", AboutModel.SpratReportDate("01102026-023504-report.csv"));
        Assert.Equal("report.csv", AboutModel.SpratReportDate("report.csv"));
    }
}

public sealed class SearchCancellationTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    [Fact]
    public void ACancelledSearchThrows() {
        var queries = factory.Services.GetRequiredService<SiteQueries>();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.Throws<OperationCanceledException>(() => queries.Search("Ursus", 10, cancellationToken: cancelled.Token));
    }

    [Fact]
    public void CancellingInterruptsARunningStatement() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        // Counts for ever unless interrupted.
        command.CommandText = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT COUNT(*) FROM c";
        using var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        using (SiteQueries.InterruptOnCancel(connection, source.Token)) {
            var ex = Assert.Throws<SqliteException>(() => command.ExecuteScalar());
            Assert.Equal(SiteQueries.SqliteInterruptCode, ex.SqliteErrorCode);
        }
        // The connection still works afterwards.
        command.CommandText = "SELECT 1";
        Assert.Equal(1L, command.ExecuteScalar());
    }
}

public sealed class SiteUrlsTests {
    private static HttpRequest Request(string scheme, string host) {
        var context = new DefaultHttpContext();
        context.Request.Scheme = scheme;
        context.Request.Host = new HostString(host);
        return context.Request;
    }

    [Theory]
    [InlineData(null, "https", "SPECIES.Example.org", "https://species.example.org/species/1")]
    [InlineData(null, "https", "species.example.org:443", "https://species.example.org/species/1")]
    [InlineData(null, "http", "localhost:5080", "http://localhost:5080/species/1")]
    [InlineData("https://Species.Example.org", "http", "127.0.0.1:5080", "https://species.example.org/species/1")]
    [InlineData("https://species.example.org/", "http", "x", "https://species.example.org/species/1")]
    [InlineData("not a url", "http", "Host.Example", "http://host.example/species/1")]
    [InlineData("ftp://species.example.org", "http", "host.example", "http://host.example/species/1")]
    public void Absolute(string? baseUrl, string scheme, string host, string expected) =>
        Assert.Equal(expected, SiteUrls.Absolute(baseUrl, Request(scheme, host), "/species/1"));
}
