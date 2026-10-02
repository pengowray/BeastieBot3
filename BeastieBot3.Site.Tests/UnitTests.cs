using System.Net;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Pages;
using BeastieBot3.Site.Web;

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
    public void Build(string input, string expected) => Assert.Equal(expected, FtsQuery.Build(input));

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
        Assert.Equal(string.Empty, options.ToQuery(null));
    }

    [Fact]
    public void UntickedBoxesCountOnlyWhenTheFormWasSent() {
        Assert.False(WikitextOptions.FromQuery(null, null, "1", null, null, null).WrapInRef);
        Assert.True(WikitextOptions.FromQuery(null, null, null, null, null, null).WrapInRef);
    }

    [Fact]
    public void RoundTrip() {
        var options = new WikitextOptions(CiteAuthorStyle.LastFirst, WikitextOptions.AccessNone, false, "my ref", true);
        Assert.Equal("?assessment=5&authors=lastfirst&access=none&opts=1&amp=1&refname=my%20ref", options.ToQuery(5));
        Assert.Equal(options, WikitextOptions.FromQuery("lastfirst", "none", "1", null, "my ref", "1"));
    }

    [Fact]
    public void EmptyAndMissingRefNameMeanTheSame() {
        // The output cache keys on query values, and an empty value looks like a missing one.
        Assert.Equal(string.Empty, WikitextOptions.FromQuery(null, null, "1", "1", "", null).RefName);
        Assert.Equal(string.Empty, WikitextOptions.FromQuery(null, null, "1", "1", null, null).RefName);
        Assert.Equal(WikitextOptions.DefaultRefName, WikitextOptions.FromQuery(null, null, null, null, "", null).RefName);
        Assert.Equal(WikitextOptions.DefaultRefName, WikitextOptions.FromQuery(null, null, null, null, null, null).RefName);

        var plainRef = WikitextOptions.Default with { RefName = string.Empty };
        Assert.Equal("?opts=1&ref=1", plainRef.ToQuery(null));
        Assert.Equal(plainRef, WikitextOptions.FromQuery(null, null, "1", "1", null, null));
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
