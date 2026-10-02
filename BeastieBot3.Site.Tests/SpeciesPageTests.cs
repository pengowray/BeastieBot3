using System.Net;

namespace BeastieBot3.Site.Tests;

public sealed class SpeciesPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private Task<string> Page(string query = "") => _client.GetStringAsync($"/species/{FixtureDb.PolarBear}{query}");

    [Fact]
    public async Task SectionsComeInOrder() {
        var html = await Page();
        string[] markers = [
            "<h1>",
            "<span class=\"sci-name\"><i>Ursus maritimus</i></span>",
            "class=\"taxon-common-name\">Polar bear",
            "class=\"classification\"",
            ">Latest global assessment</h2>",
            ">Wikitext for Wikipedia</h2>",
            ">Assessment history</h2>",
            ">Names</h2>",
            ">Links to other sites</h2>",
            "class=\"data-note\"",
        ];
        var last = -1;
        foreach (var marker in markers) {
            var at = html.IndexOf(marker, last + 1, StringComparison.Ordinal);
            Assert.True(at > last, $"'{marker}' should come after the previous section");
            last = at;
        }
    }

    [Fact]
    public async Task HeaderAndStatusSummary() {
        var html = await Page();
        var text = Html.Text(html);
        Assert.Contains("<span class=\"authority\">Phipps, 1774</span>", html);
        Assert.Contains("Kingdom Animalia", text);
        Assert.Contains("Family Ursidae", text);
        Assert.Contains("<span class=\"rank\">Genus</span> <i>Ursus</i>", html);
        Assert.Contains("<span class=\"badge cat-vu\">VU</span> <span class=\"category-label\">Vulnerable</span>", html);
        Assert.Contains("Criteria A3c", text);
        Assert.Contains("Population trend Unknown", text);
        Assert.Contains("Date assessed 21 March 2015", text);
        Assert.Contains("Year published 2015", text);
        Assert.Contains("Data from Red List version 2026-1", text);
        Assert.Contains("<a href=\"https://www.iucnredlist.org/species/22823/14871490\">Read the full assessment on the IUCN Red List website</a>", html);
        Assert.Contains($"<link rel=\"canonical\" href=\"http://localhost/species/{FixtureDb.PolarBear}\">", html);
        Assert.Contains("<title>Ursus maritimus (Polar bear) | Beastie Bot Species Status</title>", html);
    }

    [Fact]
    public async Task DefaultWikitext() {
        var html = await Page();
        var cite = Html.Textarea(html, "wikitext-cite");
        Assert.Equal(
            "<ref name=\"iucn\">{{cite iucn |author=Wiig, Ø. |author2=Amstrup, S. |author3=Atwood, T. |author4=Laidre, K. " +
            "|author5=Jon Aars |author6=Thiemann, G. |year=2015 |title=''Ursus maritimus'' |volume=2015 " +
            "|article-number=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=18 August 2026}}</ref>",
            cite);
        Assert.Equal("{{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "wikitext-status"));
        var speciesbox = Html.Textarea(html, "wikitext-speciesbox");
        Assert.NotNull(speciesbox);
        Assert.StartsWith("| status = VU\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\">{{cite iucn |author=Wiig, Ø.", speciesbox);

        var text = Html.Text(html);
        Assert.Contains("DOI from GBIF's copy of the IUCN checklist.", text);
        Assert.Contains($"Check these author names, given exactly as IUCN wrote them: {FixtureDb.VerbatimAuthor}", text);
        Assert.Contains("<summary>Citation as given by IUCN</summary>", html);
        Assert.Contains("aria-label=\"Copy {{cite iucn}} wikitext\" hidden>Copy</button>", html);
        Assert.Contains("Date downloaded from IUCN (18 August 2026)", text);
    }

    [Fact]
    public async Task LastFirstAuthorsAndNoAccessDate() {
        var cite = Html.Textarea(await Page("?authors=lastfirst&access=none"), "wikitext-cite")!;
        Assert.Contains("|last1=Wiig |first1=Ø. |last2=Amstrup |first2=S.", cite);
        Assert.Contains("|author5=Jon Aars", cite);
        Assert.DoesNotContain("access-date", cite);
    }

    [Fact]
    public async Task TodayAsAccessDate() {
        var today = DateTime.UtcNow.ToString("d MMMM yyyy", System.Globalization.CultureInfo.InvariantCulture);
        var cite = Html.Textarea(await Page("?access=today"), "wikitext-cite")!;
        Assert.Contains($"|access-date={today}}}", cite);
    }

    [Fact]
    public async Task RefWrappingRefNameAndAmp() {
        var plain = Html.Textarea(await Page("?opts=1"), "wikitext-cite")!;
        Assert.StartsWith("{{cite iucn ", plain);
        Assert.DoesNotContain("name-list-style", plain);

        var html = await Page("?opts=1&ref=1&refname=polar&amp=1");
        var named = Html.Textarea(html, "wikitext-cite")!;
        Assert.StartsWith("<ref name=\"polar\">{{cite iucn ", named);
        Assert.Contains("|name-list-style=amp", named);
        // status_ref is always a ref, with the same name.
        Assert.Contains("| status_ref = <ref name=\"polar\">", Html.Textarea(html, "wikitext-speciesbox"));

        var unnamed = Html.Textarea(await Page("?opts=1&ref=1&refname="), "wikitext-cite")!;
        Assert.StartsWith("<ref>{{cite iucn ", unnamed);

        var quoted = Html.Textarea(await Page("?opts=1&ref=1&refname=" + Uri.EscapeDataString("a\"><script>")), "wikitext-cite")!;
        Assert.StartsWith("<ref name=\"ascript\">", quoted);
    }

    [Fact]
    public async Task OptionsFormKeepsTheChoices() {
        var html = await Page("?authors=lastfirst&access=today&opts=1&amp=1");
        Assert.Contains("value=\"lastfirst\" checked=\"checked\"", html);
        Assert.Contains("value=\"today\" checked=\"checked\"", html);
        Assert.Contains("name=\"amp\" value=\"1\" checked=\"checked\"", html);
        Assert.DoesNotContain("name=\"ref\" value=\"1\" checked", html);
        Assert.Contains("<form class=\"options-form\" method=\"get\" action=\"/species/22823#wikitext\">", html);
        // History links keep the options.
        Assert.Contains($"href=\"/species/22823?assessment={FixtureDb.PolarBear2008}&amp;authors=lastfirst&amp;access=today&amp;opts=1&amp;amp=1#wikitext\"", html);
    }

    [Fact]
    public async Task EarlierAssessmentWikitext() {
        var html = await Page($"?assessment={FixtureDb.PolarBear2008}");
        var text = Html.Text(html);
        Assert.Contains("Wikitext for an earlier assessment: Vulnerable, published 2008.", text);
        Assert.Contains("<a href=\"/species/22823#wikitext\">Show wikitext for the latest assessment</a>", html);
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.Contains("|author=Schliebe, S. |display-authors=etal |year=2008", cite);
        Assert.Contains("|article-number=e.T22823A13045100", cite);
        Assert.Contains("No DOI found in IUCN's citation text, GBIF or Wikidata. {{cite iucn}} works without a DOI.", text);
        Assert.Equal("{{IUCN status|VU|22823/13045100|1|year=2008}}", Html.Textarea(html, "wikitext-status"));
        // The status summary still shows the latest assessment.
        Assert.Contains("Year published 2015", text);
        Assert.Contains("<input type=\"hidden\" name=\"assessment\" value=\"13045100\">", html);
        Assert.Contains("<span class=\"shown-label\">Shown</span>", html);
    }

    [Fact]
    public async Task LowerRiskAssessmentUsesIucn23() {
        var html = await Page($"?assessment={FixtureDb.PolarBear1996}");
        Assert.Contains("Lower Risk/conservation dependent", Html.Text(html));
        Assert.Equal("{{IUCN status|LR/cd|22823/13045101|1|year=1996}}", Html.Textarea(html, "wikitext-status"));
        // No citation, so no Speciesbox box either.
        Assert.Null(Html.Textarea(html, "wikitext-speciesbox"));
    }

    [Fact]
    public async Task OldCategoryCodesKeepTheirCase() {
        var html = await Page($"?assessment={FixtureDb.PolarBear1988Nt}");
        var text = Html.Text(html);
        // "nt" is the pre-1994 Not Threatened, not Near Threatened.
        Assert.Contains("<span class=\"badge cat-other\">nt</span> <span class=\"category-label\">Not Threatened (1994 or earlier categories)</span>", html);
        Assert.DoesNotContain("{{IUCN status|NT", html);
        Assert.Null(Html.Textarea(html, "wikitext-status"));
        Assert.Contains("{{IUCN status}} and {{Speciesbox}} have no code for this category.", text);
        Assert.DoesNotContain("{{IUCN status}} wikitext is available.", text);

        var baiji = await _client.GetStringAsync($"/species/{FixtureDb.Baiji}?assessment={FixtureDb.Baiji1986Ex}");
        Assert.Contains("<span class=\"badge cat-other\">Ex</span> <span class=\"category-label\">Extinct (1994 or earlier categories)</span>", baiji);
        Assert.DoesNotContain("{{IUCN status|EX", baiji);
    }

    [Fact]
    public async Task AssessmentOfAnotherTaxonIsIgnored() {
        var html = await Page($"?assessment={FixtureDb.LionLatest}");
        Assert.Equal("{{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "wikitext-status"));
        Assert.DoesNotContain("15951", Html.Textarea(html, "wikitext-cite"));
    }

    [Fact]
    public async Task HistoryNewestFirstWithLatestLabel() {
        var html = await Page();
        var y2015 = html.IndexOf("<th scope=\"row\">2015 <span class=\"tag\">Latest</span>", StringComparison.Ordinal);
        var y2008 = html.IndexOf("<th scope=\"row\">2008", StringComparison.Ordinal);
        var y1996 = html.IndexOf("<th scope=\"row\">1996", StringComparison.Ordinal);
        var y1988 = html.IndexOf("<th scope=\"row\">1988", StringComparison.Ordinal);
        Assert.True(y2015 > 0 && y2008 > y2015 && y1996 > y2008 && y1988 > y1996);
        Assert.Contains("aria-label=\"Show wikitext for the assessment published in 2008\">Show wikitext</a>", html);
        Assert.Contains("<a href=\"https://www.iucnredlist.org/species/22823/13045100\">IUCN Red List website</a>", html);
    }

    [Fact]
    public async Task NamesAndLinks() {
        var html = await Page();
        var text = Html.Text(html);
        Assert.Contains("English common names", text);
        Assert.Contains("Polar bear IUCN's main English name IUCN Red List, Wikidata", text);
        Assert.Contains("White bear Catalogue of Life", text);
        Assert.Contains("Common names in other languages", text);
        Assert.Contains("French Ours blanc, Ours polaire", text);
        Assert.Contains("Spanish Oso polar", text);
        Assert.Contains("<li><i>Thalarctos maritimus</i></li>", html);
        Assert.Contains("<li><i>Ursus marinus</i> Pallas, 1776</li>", html);
        Assert.Contains("<a href=\"https://en.wikipedia.org/wiki/Polar_bear\">Polar bear</a>", html);
        Assert.Contains("<a href=\"https://www.wikidata.org/wiki/Q33609\">Q33609</a>", html);
        Assert.Contains("<a href=\"https://www.catalogueoflife.org/data/taxon/4QHKG\"><i>Ursus maritimus</i></a>", html);
        Assert.Contains("Data from IUCN Red List version 2026-1, downloaded from the IUCN Red List API between 18 August and 1 September 2026. Other data sources and their licences are listed on the About page.", text);
    }

    [Fact]
    public async Task PossiblyExtinctAndDoiFromWikidata() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Baiji}");
        var text = Html.Text(html);
        Assert.Contains("<span class=\"badge cat-cr\">CR (PE)</span> <span class=\"category-label\">Critically Endangered (Possibly Extinct)</span>", html);
        Assert.Equal("{{IUCN status|CR(PE)|12119/50358152|1|year=2017}}", Html.Textarea(html, "wikitext-status"));
        Assert.StartsWith("| status = PE\n| status_system = IUCN3.1\n", Html.Textarea(html, "wikitext-speciesbox"));
        Assert.Contains("DOI from Wikidata.", text);
        Assert.Contains("Criteria A2cd; C2a(ii); D", text);
    }

    [Fact]
    public async Task OrganisationAuthorAndRegionalAssessment() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}");
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.Contains("{{cite iucn |author=BirdLife International |year=2019", cite);
        var text = Html.Text(html);
        Assert.DoesNotContain("DOI from", text);
        Assert.DoesNotContain("No DOI found", text);
        Assert.Contains("Regional assessments", text);
        Assert.Contains("Europe LC Least Concern", text);
        Assert.Contains($"href=\"/species/{FixtureDb.HouseSparrow}?assessment={FixtureDb.HouseSparrowEurope}#wikitext\"", html);

        var regional = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}?assessment={FixtureDb.HouseSparrowEurope}");
        var regionalText = Html.Text(regional);
        Assert.Contains("Wikitext for the Europe assessment: Least Concern, published 2021.", regionalText);
        Assert.Contains("{{Speciesbox}} status parameters are given for global assessments only.", regionalText);
        Assert.Contains("|title=''Passer domesticus'' (Europe assessment)", Html.Textarea(regional, "wikitext-cite"));
        Assert.Null(Html.Textarea(regional, "wikitext-speciesbox"));
    }

    [Fact]
    public async Task SubspeciesShowsParentAndParentListsChildren() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.SumatranTiger}");
        Assert.Contains($"<p class=\"parent-line\">Subspecies of <a href=\"/species/{FixtureDb.Tiger}\"><i>Panthera tigris</i></a></p>", html);
        Assert.Contains("<i>Panthera tigris</i> ssp. <i>sumatrae</i>", html);

        var tiger = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains("<h2 id=\"children-heading\">Subspecies</h2>", tiger);
        Assert.Contains($"<a href=\"/species/{FixtureDb.SumatranTiger}\">", tiger);

        var lion = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.Contains("<h2 id=\"children-heading\">Subpopulations</h2>", lion);
    }

    [Fact]
    public async Task SubpopulationWithoutCitation() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.WestAfricanLion}");
        var text = Html.Text(html);
        Assert.Contains($"Subpopulation of <a href=\"/species/{FixtureDb.Lion}\"><i>Panthera leo</i></a>", html);
        Assert.Contains("<i>Panthera leo</i> West Africa subpopulation", html);
        Assert.Contains("No citation for this assessment yet: its details have not been downloaded from the IUCN Red List. {{IUCN status}} wikitext is available. To cite the assessment, use its page on the IUCN Red List website.", text);
        Assert.Null(Html.Textarea(html, "wikitext-cite"));
        Assert.Null(Html.Textarea(html, "wikitext-speciesbox"));
        Assert.Equal("{{IUCN status|CR|68933833/68933837|1|year=2015}}", Html.Textarea(html, "wikitext-status"));
        Assert.DoesNotContain("options-form", html);
    }

    [Fact]
    public async Task RegionalOnlyTaxon() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.RegionalOnly}");
        var text = Html.Text(html);
        Assert.Contains("<h2 id=\"status-heading\">IUCN Red List status</h2>", html);
        Assert.Contains("No global assessment. This taxon has <a href=\"#regional\">2 regional assessments</a>.", html);
        // The latest regional assessment is the Mediterranean one (2010).
        Assert.Contains("Region Mediterranean", text);
        Assert.Contains("Wikitext for the Mediterranean assessment: Data Deficient, published 2010.", text);
        Assert.Contains("<section class=\"regional\" id=\"regional\"", html);
        Assert.DoesNotContain("Assessment history", text);
    }

    [Fact]
    public async Task SpratAndEpbc() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Koala}");
        Assert.Contains($"href=\"https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id={FixtureDb.KoalaSprat}\"", html);
        Assert.Contains("Listed as Endangered under Australia's <abbr title=\"Environment Protection and Biodiversity Conservation Act 1999\">EPBC Act</abbr>", html.Replace("&#x27;", "'"));
        Assert.Contains("<abbr title=\"Species Profile and Threats Database, Australian Government\">SPRAT profile</abbr>", html);
    }

    [Fact]
    public async Task UnknownTaxonIs404WithTheTaxonText() {
        var response = await _client.GetAsync("/species/999999999");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("No taxon with IUCN id 999999999", text);
        Assert.Contains("This site uses IUCN Red List version 2026-1. Check the id, or search for the taxon by name.", text);
        Assert.Contains("Search for a taxon", text);
    }

    [Fact]
    public async Task NonNumericTaxonIdIsPageNotFound() {
        var response = await _client.GetAsync("/species/abc");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("Page not found", text);
        Assert.Contains("Check the address, or search for a taxon.", text);
    }
}
