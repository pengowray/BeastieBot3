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

    [Theory]
    [InlineData(null, "Language not given")]
    [InlineData("", "Language not given")]
    [InlineData("und", "Language not given")]
    [InlineData("UND", "Language not given")]
    [InlineData("mis", "Language not given")]
    [InlineData("zxx", "Language not given")]
    [InlineData("en", "English")]
    [InlineData("eng", "English")]
    [InlineData("fr", "French")]
    [InlineData("phi", "Philippine languages")]
    [InlineData("map", "Austronesian languages")]
    [InlineData("sai", "South American Indian languages")]
    [InlineData("cpf", "French-based creoles")]
    public void LanguageNamesForCodes(string? code, string expected) => Assert.Equal(expected, LanguageNames.Name(code));

    [Fact]
    public void LanguageNamesNeverShowTheInvariantCulture() {
        foreach (var code in new[] { "und", "mul", "qaa", "xx", "zz-zz" }) {
            Assert.DoesNotContain("Invariant", LanguageNames.Name(code));
        }
    }

    [Theory]
    [InlineData("en", true)]
    [InlineData("eng", true)]
    [InlineData("EN-gb", true)]
    [InlineData(" en ", true)]
    [InlineData("fr", false)]
    [InlineData(null, false)]
    public void EnglishCodes(string? code, bool expected) => Assert.Equal(expected, LanguageNames.IsEnglish(code));

    [Theory]
    [InlineData("fr", "fr")]
    [InlineData("pt-BR", "pt-br")]
    [InlineData("map", "map")]
    [InlineData("und", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    [InlineData("x\"y", null)]
    [InlineData("toolongcode", null)]
    public void LangAttributes(string? code, string? expected) => Assert.Equal(expected, LanguageNames.LangAttribute(code));

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
        Assert.Equal("1 region", SiteText.NoGlobalLinkText(1));
        Assert.Equal("2 regions", SiteText.NoGlobalLinkText(2));
        Assert.Equal("Show all languages (1 more)", SiteText.ShowMoreLanguages(1));
        Assert.Equal("Show all languages (15 more)", SiteText.ShowMoreLanguages(15));
        Assert.Equal("Show all synonyms (1 more)", SiteText.ShowMoreSynonyms(1));
        Assert.Equal("Show all synonyms (1,200 more)", SiteText.ShowMoreSynonyms(1200));
    }

    [Theory]
    [InlineData("iucn", "IUCN Red List")]
    [InlineData("col", "Catalogue of Life")]
    [InlineData("wikidata", "Wikidata")]
    [InlineData("wikipedia", "Wikipedia")]
    [InlineData("wikipedia-taxobox", "Wikipedia taxobox")]
    [InlineData("something-new", "something-new")]
    public void NameSourceLabels(string source, string label) => Assert.Equal(label, SiteText.SourceLabel(source));

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
        var options = new WikitextOptions(CiteAuthorStyle.AuthorN, WikitextOptions.AccessNone, false, "my ref", true, "iucn");
        Assert.Equal("?assessment=5&authors=author&access=none&opts=1&amp=1&refname=my%20ref", options.ToQuery(5, "iucn2008"));
        Assert.Equal(options with { DefaultRefName = "iucn2008" }, WikitextOptions.FromQuery("author", "none", "1", null, "my ref", "1", "iucn2008"));

        // Last/first is the default: it is not written, and the form's "lastfirst" still reads as it.
        var lastFirst = options with { AuthorStyle = CiteAuthorStyle.LastFirst };
        Assert.Equal("?assessment=5&access=none&opts=1&amp=1&refname=my%20ref", lastFirst.ToQuery(5, "iucn2008"));
        Assert.Equal(lastFirst with { DefaultRefName = "iucn2008" }, WikitextOptions.FromQuery("lastfirst", "none", "1", null, "my ref", "1", "iucn2008"));
        Assert.Equal(lastFirst with { DefaultRefName = "iucn2008" }, WikitextOptions.FromQuery(null, "none", "1", null, "my ref", "1", "iucn2008"));
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
        var submitted = WikitextOptions.FromQuery("author", null, "1", "1", "iucn", null, "iucn");
        Assert.Null(submitted.CustomRefName);
        Assert.Equal("?assessment=5&authors=author", submitted.ToQuery(5, "iucn2008"));
        var lastFirst = WikitextOptions.FromQuery("lastfirst", null, "1", "1", "iucn", null, "iucn");
        Assert.Null(lastFirst.CustomRefName);
        Assert.Equal("?assessment=5", lastFirst.ToQuery(5, "iucn2008"));
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

    [Fact]
    public void FullGivenNamesIsOffUnlessAsked() {
        Assert.False(WikitextOptions.Default.FullGivenNames);
        Assert.False(WikitextOptions.FromQuery(null, null, "1", "1", "iucn", null).FullGivenNames);
        Assert.False(WikitextOptions.FromQuery(null, null, null, null, null, null, fullNames: "0").FullGivenNames);
        // Off by default, so it needs no opts=1 to turn it on.
        Assert.True(WikitextOptions.FromQuery(null, null, null, null, null, null, fullNames: "1").FullGivenNames);
        Assert.True(WikitextOptions.FromQuery(null, null, "1", "1", "iucn", null, fullNames: "1").FullGivenNames);
    }

    [Fact]
    public void FullGivenNamesRoundTrip() {
        var options = WikitextOptions.Default with { FullGivenNames = true };
        Assert.Equal("?fullnames=1", options.ToQuery(null, "iucn"));
        Assert.Equal("?assessment=5&authors=author&fullnames=1&access=none",
            (options with { AuthorStyle = CiteAuthorStyle.AuthorN, Access = WikitextOptions.AccessNone }).ToQuery(5, "iucn2008"));
        Assert.Equal(options, WikitextOptions.FromQuery(null, null, null, null, null, null, fullNames: "1"));
    }

    [Fact]
    public void CiteIucnOptionsCarryEveryChoice() {
        var today = new DateOnly(2026, 10, 3);
        var downloaded = new DateOnly(2026, 8, 18);
        var options = new WikitextOptions(CiteAuthorStyle.LastFirst, WikitextOptions.AccessDownload, true, "tiger", true, "iucn") {
            FullGivenNames = true,
        };
        var cite = options.ToCiteIucnOptions(today, downloaded);
        Assert.Equal(CiteAuthorStyle.LastFirst, cite.AuthorStyle);
        Assert.Equal(downloaded, cite.AccessDate);
        Assert.True(cite.WrapInRef);
        Assert.Equal("tiger", cite.RefName);
        Assert.True(cite.NameListStyleAmp);
        Assert.True(cite.FullGivenNames);
        Assert.False((options with { FullGivenNames = false }).ToCiteIucnOptions(today, downloaded).FullGivenNames);

        Assert.Equal(today, (options with { Access = WikitextOptions.AccessToday }).ToCiteIucnOptions(today, downloaded).AccessDate);
        Assert.Null((options with { Access = WikitextOptions.AccessNone }).ToCiteIucnOptions(today, downloaded).AccessDate);
        Assert.Null(options.ToCiteIucnOptions(today, null).AccessDate);
    }

    [Fact]
    public void CiteQOptionsUseTheSameAccessDateAndRef() {
        var today = new DateOnly(2026, 10, 3);
        var options = WikitextOptions.Default with { Access = WikitextOptions.AccessToday, RefName = "tiger" };
        Assert.Equal(new CiteQOptions { AccessDate = today, WrapInRef = true, RefName = "tiger" }, options.ToCiteQOptions(today, null));
        Assert.Equal(new CiteQOptions { AccessDate = null, WrapInRef = false, RefName = string.Empty },
            (options with { Access = WikitextOptions.AccessNone, WrapInRef = false, RefName = string.Empty }).ToCiteQOptions(today, today));
    }
}

public sealed class GivenNamesTests {
    private static IucnCitationParts Parts(params CitationAuthor[] authors) => new() {
        TaxonId = 1, AssessmentId = 2, Year = 2020, ScientificName = "Aus bus", Authors = authors,
    };

    private static CitationAuthor Person(string last, string initials, string? given = null) =>
        new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials, given);

    [Fact]
    public void NoOptionWithoutFullGivenNames() {
        Assert.Null(GivenNamesCoverage.Of(Parts(Person("Sayer", "C."), Person("Lajus", "D."))));
        Assert.Null(GivenNamesCoverage.Of(Parts(Person("Sayer", "C.", "  "))));
        Assert.Null(GivenNamesCoverage.Of(Parts()));
    }

    [Fact]
    public void OrganisationsAreNotCounted() {
        var coverage = GivenNamesCoverage.Of(Parts(
            new CitationAuthor(CitationAuthorKind.Organisation, "BirdLife International"),
            Person("Sayer", "C.", "Catherine"),
            Person("Lajus", "D."),
            new CitationAuthor(CitationAuthorKind.Verbatim, "Jon Aars")))!;
        Assert.Equal(1, coverage.WithGivenNames);
        Assert.Equal(3, coverage.People);
        Assert.Equal("Sayer, C.", coverage.Example.Display);
    }

    [Theory]
    [InlineData(1, 1, "IUCN gives this author's full given names: “Catherine” for “Sayer, C.”.")]
    [InlineData(2, 2, "IUCN gives full given names for both authors, such as “Catherine” for “Sayer, C.”.")]
    [InlineData(3, 3, "IUCN gives full given names for all 3 authors, such as “Catherine” for “Sayer, C.”.")]
    [InlineData(1, 2, "IUCN gives full given names for 1 of the 2 authors, such as “Catherine” for “Sayer, C.”. The other author is written as in IUCN's citation.")]
    [InlineData(2, 5, "IUCN gives full given names for 2 of the 5 authors, such as “Catherine” for “Sayer, C.”. The other 3 authors are written as in IUCN's citation.")]
    public void HelpLine(int withNames, int people, string expected) =>
        Assert.Equal(expected, SiteText.FullGivenNamesHelp(withNames, people, "Catherine", "Sayer, C."));
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

    [Theory]
    [InlineData(30, "Try again in a minute.")]
    [InlineData(61, "Try again in 2 minutes.")]
    [InlineData(3000, "Try again in 50 minutes.")]
    [InlineData(50000, "Try again in 14 hours.")]
    public void TheWaitIsSaidInMinutesOrHours(int seconds, string expected) => Assert.Equal(expected, SiteText.TooManyWait(seconds));

    [Fact]
    public void TaxonPagesHaveADailyLimitPerClient() {
        var limits = new RateLimitOptions { PagesPerMinute = 1000, TaxonPagesPerHour = 1000, TaxonPagesPerDay = 2 };
        using var limiter = SiteRateLimits.BuildGlobalLimiter(limits);
        Assert.True(limiter.AttemptAcquire(Request("/species/1", "203.0.113.5")).IsAcquired);
        Assert.True(limiter.AttemptAcquire(Request("/taxa/genus/ursus", "203.0.113.5")).IsAcquired);
        Assert.False(limiter.AttemptAcquire(Request("/name/Ursus", "203.0.113.5")).IsAcquired);
        Assert.True(limiter.AttemptAcquire(Request("/about", "203.0.113.5")).IsAcquired);
        Assert.True(limiter.AttemptAcquire(Request("/species/1", "203.0.113.6")).IsAcquired);
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
    public void Citations() {
        Assert.Equal("https://doi.org/10.15468/0qnb58", AboutModel.DoiUrl(" 10.15468/0qnb58 "));
        Assert.Equal("https://doi.org/10.48580/dgykv", AboutModel.DoiUrl("https://doi.org/10.48580/dgykv"));
        Assert.Equal(
            "A &amp; B. <a href=\"https://www.iucnredlist.org\">https://www.iucnredlist.org</a>. See (<a href=\"https://doi.org/10.1/x\">https://doi.org/10.1/x</a>).",
            SiteHtml.Linkify("A & B. https://www.iucnredlist.org. See (https://doi.org/10.1/x)."));
        Assert.Equal("&lt;script&gt;", SiteHtml.Linkify("<script>"));
        // The DOI link is added only when the citation lacks it.
        Assert.Equal("Cited. <a href=\"https://doi.org/10.1/x\">https://doi.org/10.1/x</a>", AboutModel.CitationHtml("Cited.", "https://doi.org/10.1/x"));
        Assert.Equal("Cited <a href=\"https://doi.org/10.1/x\">https://doi.org/10.1/x</a>", AboutModel.CitationHtml("Cited https://doi.org/10.1/x", "https://doi.org/10.1/x"));
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

public sealed class IdQueryTests {
    [Theory]
    [InlineData("e.T22823A14871490", 22823L, 14871490L, null)]
    [InlineData("T22823A14871490", 22823L, 14871490L, null)]
    [InlineData("t22823a14871490", 22823L, 14871490L, null)]
    [InlineData("e.T22823A14871490.en.", 22823L, 14871490L, null)]
    [InlineData("T22823", 22823L, null, null)]
    [InlineData("A14871490", null, 14871490L, null)]
    [InlineData(" 22823 ", null, null, 22823L)]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", 22823L, 14871490L, null)]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", 22823L, 14871490L, null)]
    [InlineData("doi:10.2305/IUCN.UK.2008.RLTS.T22823A9390963.en", 22823L, 9390963L, null)]
    [InlineData("https://www.iucnredlist.org/species/22823/14871490", 22823L, 14871490L, null)]
    [InlineData("iucnredlist.org/species/22823", 22823L, null, null)]
    public void ReadsTheIds(string text, long? taxonId, long? assessmentId, long? number) =>
        Assert.Equal(new IdQuery(taxonId, assessmentId, number), IdQuery.Parse(text));

    [Theory]
    [InlineData("Q13442814", 13442814L)]
    [InlineData("q33609", 33609L)]
    [InlineData(" Q33609. ", 33609L)]
    [InlineData("https://www.wikidata.org/wiki/Q33609", 33609L)]
    [InlineData("https://m.wikidata.org/wiki/Q33609#P141", 33609L)]
    [InlineData("http://www.wikidata.org/entity/Q33609", 33609L)]
    [InlineData("https://www.wikidata.org/wiki/Special:EntityPage/Q33609", 33609L)]
    public void ReadsAWikidataItem(string text, long item) =>
        Assert.Equal(new IdQuery(null, null, null, item), IdQuery.Parse(text));

    [Theory]
    [InlineData("")]
    [InlineData("Ursus maritimus")]
    [InlineData("T22823 maritimus")]
    [InlineData("0")]
    [InlineData("1234567890123456789012")]
    [InlineData("10.1234/some.other.doi")]
    [InlineData("Q")]
    [InlineData("Q0")]
    [InlineData("Qabc")]
    [InlineData("https://www.wikidata.org/wiki/Property:P141")]
    public void IsNullForOtherText(string text) => Assert.Null(IdQuery.Parse(text));
}

public sealed class ProvisionalNameTests {
    [Theory]
    [InlineData("Notogomphus sp. nov. 'gorilla'", true)]
    [InlineData("Hauffenia sp. nov.", true)]
    [InlineData("Pupisoma sp. nov. 1", true)]
    [InlineData("Ferrissia sp. indet.", false)]
    [InlineData("Ursus maritimus", false)]
    [InlineData("Novaculina novella", false)]
    public void Detects(string name, bool expected) => Assert.Equal(expected, SiteFormat.IsProvisionalName(name));

    [Theory]
    [InlineData("Hauffenia sp. nov.", "sp. nov.", "species")]
    [InlineData("Lepidium sp. nov. subsp. nov.", "subsp. nov.", "subspecies")]
    [InlineData("Acacia aneura var. nov.", "var. nov.", "variety")]
    public void NamesTheRank(string name, string marker, string rank) =>
        Assert.Equal((marker, rank), SiteFormat.ProvisionalMarker(name));
}

// Which group a search goes straight to (SearchModel.GroupToGoTo).
public sealed class SearchGroupRuleTests {
    private static GroupHit Group(string name, string? matched = null) =>
        new(new GroupRow(1, null, 0, "family", name, "iucn", true, "ANIMALIA", null, null, null, null, 1, 1, 1, 0, 0), matched);

    private static SearchHit Taxon(bool exact, bool strong) =>
        new(new TaxonSummary(1, "Epomophorus pusillus", null, "species", "Peters's dwarf epauletted fruit bat", "LC", false, false),
            "Fruit Bat", "common", "en", exact, strong);

    [Fact]
    public void GroupFoundByWikipediaTitleBeatsAWeakTaxonMatch() =>
        Assert.NotNull(SearchModel.GroupToGoTo([Group("Pteropodidae", "Fruit bat")], [Taxon(exact: true, strong: false)]));

    [Fact]
    public void StrongTaxonMatchListsBoth() =>
        Assert.Null(SearchModel.GroupToGoTo([Group("Lipotes", "Baiji")], [Taxon(exact: true, strong: true)]));

    [Fact]
    public void TwoGroupsAreListed() =>
        Assert.Null(SearchModel.GroupToGoTo([Group("Abronia"), Group("Abronia")], []));

    [Fact]
    public void NoGroupGoesNowhere() =>
        Assert.Null(SearchModel.GroupToGoTo([], [Taxon(exact: true, strong: false)]));
}
