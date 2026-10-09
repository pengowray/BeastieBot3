using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

// The IUCN Green Status: its section on the species page, its {{cite iucn}} and the year it cites.
public sealed class GreenStatusTests : IClassFixture<SiteFactory> {
    private readonly HttpClient _client;

    public GreenStatusTests(SiteFactory factory) => _client = factory.CreateClient();

    [Theory]
    [InlineData(GreenStatusYearRule.Assessed, 2023, null, 2024, 2023)]
    [InlineData(GreenStatusYearRule.Published, 2023, null, 2024, 2024)]
    [InlineData(GreenStatusYearRule.Published, 2025, null, 2019, 2025)]
    [InlineData(GreenStatusYearRule.Published, 2024, 2026, 2019, 2026)]
    [InlineData(GreenStatusYearRule.Published, 2024, null, null, 2024)]
    public void Year_follows_the_rule(GreenStatusYearRule rule, int assessed, int? published, int? redList, int expected) =>
        Assert.Equal(expected, GreenStatusYear.For(rule, assessed, published, redList));

    [Theory]
    [InlineData(50, 17, 67, "50% (range 17% to 67%)")]
    [InlineData(22, 22, 22, "22% (range 22% to 22%)")]
    [InlineData(17, -42, 58, "17% (range −42% to 58%)")]
    public void Score_is_written_with_its_range(int best, int min, int max, string expected) =>
        Assert.Equal(expected, SiteText.GreenStatusScore(new GreenStatusMetric("Medium", best, min, max)));

    [Theory]
    [InlineData("Carroll, J.", "Reviewer")]
    [InlineData("Gupta, G. & McGowan, P.", "Reviewers")]
    public void People_label_counts_the_names(string names, string expected) =>
        Assert.Equal(expected, SiteText.GreenStatusPeople("reviewers", names));

    // The form Template talk:Cite IUCN found works: |type=, |url= of the Red List page, no
    // |article-number= or |doi=.
    [Fact]
    public void Citation_links_the_red_list_page() {
        var parts = new IucnCitationParts {
            TaxonId = 12520, AssessmentId = 218695618, Year = 2023, ScientificName = "Lynx pardinus",
            Authors = [new CitationAuthor(CitationAuthorKind.Person, "Salcedo, J.", "Salcedo", "J."),
                new CitationAuthor(CitationAuthorKind.Person, "Breitenmoser, U.", "Breitenmoser", "U.")],
        };
        Assert.Equal("{{cite iucn |author=Salcedo, J. |author2=Breitenmoser, U. |year=2023 |type=Green Status assessment |title=''Lynx pardinus'' "
            + "|volume=2023 |url=https://www.iucnredlist.org/species/12520/218695618 |access-date=8 October 2026}}",
            CiteIucnRenderer.RenderGreenStatus(parts, "https://www.iucnredlist.org/species/12520/218695618",
                new CiteIucnOptions { AccessDate = new DateOnly(2026, 10, 8) }));
    }

    [Fact]
    public async Task Tiger_page_shows_its_green_status() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.Tiger}")).Replace("&#x27;", "'");
        var section = Html.Section(html, "green-status");
        var text = Html.Text(section);

        Assert.Contains("<h2 id=\"green-status-heading\">IUCN Green Status of Species</h2>", section);
        Assert.Contains("Species recovery category Largely Depleted Species Recovery Score 19% (range 12% to 29%) Date assessed 30 June 2021", text);
        Assert.Equal(new[] { "Medium", "17% (range −42% to 58%)" }, Html.TableRows(section)[1][1..]);
        Assert.Contains("Assessors Goodrich, J. & Smith, A. Reviewer Carroll, J. Facilitator Cygan, M.G.W.", text);
        Assert.DoesNotContain("Contributor", text);
        Assert.Contains($"<a href=\"https://www.iucnredlist.org/species/{FixtureDb.Tiger}/{FixtureDb.TigerLatest}\">Green Status on the IUCN Red List website</a>", section);
        // After the regional assessments' place and before the names.
        Assert.True(Html.IndexOf(html, "id=\"green-status\"") < Html.IndexOf(html, "id=\"names-heading\""));
    }

    [Fact]
    public async Task Citation_box_uses_the_year_assessed_unless_asked() {
        var assessed = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikitext");
        var published = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}/wikitext?gsyear=published");

        Assert.Contains("|year=2021 |type=Green Status assessment |title=''Panthera tigris'' |volume=2021 "
            + $"|url=https://www.iucnredlist.org/species/{FixtureDb.Tiger}/{FixtureDb.TigerLatest} |access-date=8 October 2026}}}}</ref>", Html.Textarea(assessed, "wikitext-green"));
        Assert.Contains("<ref name=\"iucn-green\">", Html.Textarea(assessed, "wikitext-green"));
        Assert.Contains("|year= is the year assessed.", Html.Text(assessed));
        // The best guess: no known release, so the later of 2021 and the 2022 Red List assessment.
        Assert.Contains("|year=2022 |type=Green Status assessment", Html.Textarea(published, "wikitext-green"));
        Assert.Contains("name=\"gsyear\" value=\"published\" checked=\"checked\"", published);
    }

    [Fact]
    public async Task Taxon_without_a_green_status_has_no_section_or_option() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.DoesNotContain("green-status", html);
        Assert.DoesNotContain("wikitext-green", html);
        Assert.DoesNotContain("name=\"gsyear\"", html);
    }
}
