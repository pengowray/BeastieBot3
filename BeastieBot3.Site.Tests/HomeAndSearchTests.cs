using System.Net;
using System.Text.Json;

namespace BeastieBot3.Site.Tests;

public sealed class HomeAndSearchTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task HomeShowsNameSearchExamplesAndDatasetSummary() {
        var response = await _client.GetAsync("/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);

        Assert.Contains("<h1>Beastie Bot Species Status</h1>", html);
        Assert.Contains("Unofficial site for looking up the IUCN Red List category of any species", text);
        Assert.Contains("<form class=\"search-form search-form-main\" action=\"/search\" method=\"get\"", html);
        Assert.Contains("Search for a taxon", text);
        Assert.Contains("href=\"/search?q=Panthera+tigris\"><i>Panthera tigris</i>", html);
        Assert.Contains("On each taxon page:", text);
        Assert.Contains($"IUCN Red List version 2026-1: {FixtureDb.GlobalTaxonCount} taxa with a global assessment, ", text);
        Assert.Contains("assessments in total", text);
        // The home page has its own search box, so the header has none.
        Assert.DoesNotContain("search-form-header", html);
        Assert.Contains("This site is unofficial and is not affiliated with or endorsed by IUCN.", text);
        Assert.Contains("Data from IUCN Red List version 2026-1, used under the IUCN Red List Terms of Use, and from the other sources listed on the About page.", text);
    }

    [Fact]
    public async Task ExactScientificNameRedirectsToTheTaxonPage() {
        var response = await _client.GetAsync("/search?q=ursus+MARITIMUS");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task ExactSynonymRedirectsAndTheTaxonPageSaysWhy() {
        var response = await _client.GetAsync("/search?q=Thalarctos+maritimus");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var location = response.Headers.Location!.OriginalString;
        Assert.Equal($"/species/{FixtureDb.PolarBear}?q=Thalarctos%20maritimus", location);

        var page = await _client.GetStringAsync(location);
        var text = Html.Text(page);
        Assert.Contains("“Thalarctos maritimus” is a synonym of this taxon.", text);
        Assert.Contains("See all search results for “Thalarctos maritimus”", text);
        Assert.Contains("href=\"/search?q=Thalarctos%20maritimus&amp;all=1\"", page);
    }

    [Fact]
    public async Task ArrivalLineIsOnlyShownForANameOfThatTaxon() {
        var page = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?q=Buy+cheap+pills");
        Assert.DoesNotContain("Buy cheap pills", page);
        Assert.DoesNotContain("class=\"arrival\"", page);
    }

    [Fact]
    public async Task ArrivingByTheShownCommonNameKeepsOnlyTheResultsLink() {
        var shown = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?q=polar+BEAR");
        Assert.DoesNotContain("is a common name of this taxon", Html.Text(shown));
        Assert.Contains("href=\"/search?q=polar%20BEAR&amp;all=1\">See all search results for “polar BEAR”</a>", shown);

        // Another common name is not on the page, so the sentence stays.
        var other = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?q=White+bear"));
        Assert.Contains("“White bear” is a common name of this taxon. See all search results for “White bear”", other);
    }

    [Fact]
    public async Task AllResultsLinkDoesNotRedirect() {
        var response = await _client.GetAsync("/search?q=Thalarctos+maritimus&all=1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("Results for “Thalarctos maritimus”", text);
        Assert.Contains("1 taxon found", text);
        Assert.Contains("Matched synonym: Thalarctos maritimus", text);
    }

    [Fact]
    public async Task ExactCommonNameRedirects() {
        var response = await _client.GetAsync("/search?q=koala");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.Koala}?q=koala", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task PrefixMatchListsTheTaxon() {
        var response = await _client.GetAsync("/search?q=Ursus+mar");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains($"<a href=\"/species/{FixtureDb.PolarBear}\"><i>Ursus maritimus</i></a>", html);
        Assert.Contains("Polar bear", html);
        Assert.Contains("<span class=\"badge cat-vu\">VU</span>", html);
    }

    [Fact]
    public async Task SynonymPrefixMatchShowsAMatchNote() {
        var html = await _client.GetStringAsync("/search?q=Thalarc");
        Assert.Contains("Matched synonym: <i>Thalarctos maritimus</i>", html);
    }

    [Fact]
    public async Task CommonNameInAnotherLanguageShowsTheLanguage() {
        var html = await _client.GetStringAsync("/search?q=Ours+bl");
        Assert.Contains("Matched common name: Ours blanc (French)", Html.Text(html));
        Assert.Contains("<span lang=\"fr\">Ours blanc</span> (French)", html);

        var collective = Html.Text(await _client.GetStringAsync("/search?q=Harim"));
        Assert.Contains("Matched common name: Harimau (Austronesian languages)", collective);

        // A name with the code "und" has no language to show.
        var undetermined = await _client.GetStringAsync("/search?q=Nanu");
        Assert.Contains("Matched common name: Nanuq</div>", undetermined);
    }

    [Fact]
    public async Task ResultsShowKindLabelsAndExactMatchesFirst() {
        // "Panthera tigris" names one taxon exactly, so the search goes to its page.
        var redirect = await _client.GetAsync("/search?q=Panthera+tigris");
        Assert.Equal(HttpStatusCode.Redirect, redirect.StatusCode);

        var listed = await _client.GetStringAsync("/search?q=Panthera+tigris&all=1");
        var tiger = Html.IndexOf(listed, $"/species/{FixtureDb.Tiger}\"");
        var sumatran = Html.IndexOf(listed, $"/species/{FixtureDb.SumatranTiger}\"");
        Assert.True(tiger > 0 && sumatran > tiger, "the exact match comes first");
        Assert.Contains("<span class=\"kind\">Subspecies</span>", listed);
        Assert.Contains("<i>Panthera tigris</i> ssp. <i>sumatrae</i>", listed);
    }

    [Fact]
    public async Task NameSharedByTwoTaxaListsBoth() {
        var response = await _client.GetAsync("/search?q=big+cat");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains($"/species/{FixtureDb.Tiger}\"", html);
        Assert.Contains($"/species/{FixtureDb.Lion}\"", html);
        Assert.Contains("2 taxa found", Html.Text(html));
        Assert.Contains("Matched common name: Big cat (English)", Html.Text(html));
    }

    [Fact]
    public async Task VarietyAndNoGlobalLabels() {
        var cupressus = Html.Text(await _client.GetStringAsync("/search?q=Cupressus+ariz"));
        Assert.Contains("Variety", cupressus);

        var gobio = Html.Text(await _client.GetStringAsync("/search?q=Gobio+kov&all=1"));
        Assert.Contains("No global assessment", gobio);
    }

    [Theory]
    [InlineData("a")]
    [InlineData("a.")]
    [InlineData("s-")]
    [InlineData(".s")]
    [InlineData("á")]
    [InlineData("x̃")]
    [InlineData("**")]
    public async Task TooShortQueryRunsNoSearch(string query) {
        var response = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(query));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);
        Assert.Contains("Search term too short. Enter at least 2 letters or digits.", text);
        Assert.DoesNotContain("Results for", text);
        Assert.Contains("<title>Search for a taxon | Beastie Bot Species Status</title>", html);

        using var suggest = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=" + Uri.EscapeDataString(query)));
        Assert.Equal(0, suggest.RootElement.GetArrayLength());
    }

    [Theory]
    [InlineData("/search")]
    [InlineData("/search?q=a")]
    public async Task SearchPageWithoutResultsHasAHeading(string url) {
        var html = await _client.GetStringAsync(url);
        Assert.Contains("<h1>Search for a taxon</h1>", html);
        // The heading says what the label says, so the label is for screen readers only.
        Assert.Contains("<label for=\"q-main\" class=\"visually-hidden\">Search for a taxon</label>", html);
        Assert.True(html.IndexOf("<h1>", StringComparison.Ordinal) < html.IndexOf("<form class=\"search-form search-form-main\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task MultiWordQueryEndingInOneLetterStillSearches() {
        var html = await _client.GetStringAsync("/search?q=Panthera+t&all=1");
        Assert.Contains($"/species/{FixtureDb.Tiger}\"", html);
        Assert.Contains($"/species/{FixtureDb.SumatranTiger}\"", html);
    }

    [Fact]
    public async Task SearchPagesAreCachedByTheQueryAsTyped() {
        var first = await _client.GetAsync("/search?q=Ursus+ma");
        var second = await _client.GetAsync("/search?q=Ursus%20%20ma");
        Assert.Null(first.Headers.Age);
        Assert.NotNull(second.Headers.Age);

        // Another spelling of the same words gets its own entry, so the page shows what this
        // visitor typed.
        var upper = await _client.GetAsync("/search?q=URSUS+MA");
        Assert.Null(upper.Headers.Age);
        Assert.Contains("Results for “URSUS MA”", Html.Text(await upper.Content.ReadAsStringAsync()));

        // A second q parameter is ignored by the page and by the cache key alike.
        var extra = await _client.GetAsync("/search?q=Ursus+ma&q=zzz");
        Assert.NotNull(extra.Headers.Age);
        Assert.Contains("Results for “Ursus ma”", Html.Text(await extra.Content.ReadAsStringAsync()));
    }

    [Fact]
    public async Task SuggestionsAreCachedByTheFoldedQuery() {
        var first = await _client.GetAsync("/api/suggest?q=Lipot");
        var second = await _client.GetAsync("/api/suggest?q=LIPOT");
        Assert.Null(first.Headers.Age);
        Assert.NotNull(second.Headers.Age);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());

        var other = await _client.GetAsync("/api/suggest?q=Lipot&q=Ursus");
        using var doc = JsonDocument.Parse(await other.Content.ReadAsStringAsync());
        Assert.Equal(FixtureDb.Baiji, doc.RootElement[0].GetProperty("taxonId").GetInt64());
    }

    [Fact]
    public async Task NoResultsMessage() {
        var text = Html.Text(await _client.GetStringAsync("/search?q=Zzyzx+qwerty"));
        Assert.Contains("No taxa found for “Zzyzx qwerty”. Check the spelling, or search for the scientific name.", text);
    }

    [Fact]
    public async Task ManyResultsAreCappedAtFifty() {
        var html = await _client.GetStringAsync("/search?q=Fillerus");
        var text = Html.Text(html);
        Assert.Contains($"Showing the first 50 of {FixtureDb.FillerCount} taxa. Type more of the name to narrow the search.", text);
        var rows = System.Text.RegularExpressions.Regex.Matches(html, "<a href=\"/species/9\\d{5}\">").Count;
        Assert.Equal(50, rows);
    }

    [Theory]
    [InlineData("\"")]
    [InlineData("\"\"")]
    [InlineData("*")]
    [InlineData("**")]
    [InlineData("::")]
    [InlineData("a:b")]
    [InlineData("ursus:maritimus")]
    [InlineData("NEAR(")]
    [InlineData("NEAR(ursus maritimus, 2)")]
    [InlineData("ursus AND")]
    [InlineData("OR ursus")]
    [InlineData("NOT")]
    [InlineData("-x")]
    [InlineData("^ursus")]
    [InlineData("ursus\"")]
    [InlineData("(ursus")]
    [InlineData("{ursus}")]
    [InlineData("ursus*")]
    [InlineData("100%")]
    [InlineData("a_b")]
    [InlineData("'; DROP TABLE taxon; --")]
    public async Task FtsSyntaxInTheQueryIsNotAnError(string query) {
        var response = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(query));
        Assert.True(response.StatusCode is HttpStatusCode.OK or HttpStatusCode.Redirect, $"{query}: {response.StatusCode}");
        var suggest = await _client.GetAsync("/api/suggest?q=" + Uri.EscapeDataString(query));
        Assert.Equal(HttpStatusCode.OK, suggest.StatusCode);
    }

    [Fact]
    public async Task QueryIsHtmlEncoded() {
        var html = await _client.GetStringAsync("/search?q=" + Uri.EscapeDataString("<script>alert(1)</script>"));
        Assert.DoesNotContain("<script>alert(1)</script>", html);
        Assert.Contains("&lt;script&gt;", html);
    }

    [Theory]
    [InlineData("T22823")]
    [InlineData("22823")]
    [InlineData("e.T22823A14871490")]
    [InlineData("A14871490")]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    public async Task AnIdOfTheTaxonOrItsLatestAssessmentRedirectsToTheTaxonPage(string q) {
        var response = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(q));
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task AnEarlierAssessmentIdRedirectsWithThatAssessmentShown() {
        var response = await _client.GetAsync($"/search?q=e.T{FixtureDb.PolarBear}A{FixtureDb.PolarBear2008}");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}#wikitext", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task AnAssessmentIdNotOnTheSiteListsTheTaxonWithANote() {
        var response = await _client.GetAsync($"/search?q=T{FixtureDb.PolarBear}A999999999");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("This site has no assessment with IUCN assessment ID 999999999.", text);
        Assert.Contains($"Matched IUCN taxon ID: {FixtureDb.PolarBear}", text);
    }

    [Fact]
    public async Task AllResultsListsAnAssessmentIdMatch() {
        var html = await _client.GetStringAsync($"/search?q=A{FixtureDb.PolarBear2008}&all=1");
        Assert.Contains($"Matched IUCN assessment ID: {FixtureDb.PolarBear2008} (Global, 2008)", Html.Text(html));
        Assert.Contains($"href=\"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}#wikitext\"", html);
    }

    [Fact]
    public async Task SuggestFindsATaxonById() {
        var json = await _client.GetStringAsync($"/api/suggest?q=T{FixtureDb.PolarBear}");
        Assert.Contains($"\"taxonId\":{FixtureDb.PolarBear}", json);
    }

    [Fact]
    public async Task ATaxonIdWithAnotherTaxonsAssessmentIdListsBoth() {
        var response = await _client.GetAsync($"/search?q=T{FixtureDb.Tiger}A{FixtureDb.PolarBear2008}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains($"Matched IUCN taxon ID: {FixtureDb.Tiger}", text);
        Assert.Contains($"Matched IUCN assessment ID: {FixtureDb.PolarBear2008}", text);
    }

    [Fact]
    public async Task ASearchWithAMisspelledWordSuggestsTheCorrectedName() {
        var html = await factory.CreateClient().GetStringAsync("/search?q=Pantera+leoo");
        Assert.Contains(BeastieBot3.Site.Display.SiteText.SimilarNames, html);
        Assert.Contains("<a href=\"/search?q=Panthera%20leo\">Panthera leo</a>", html);
    }

    [Fact]
    public async Task ASearchThatFindsNothingAndIsNotAMisspellingSuggestsNothing() {
        var html = await factory.CreateClient().GetStringAsync("/search?q=qwxzvbnm");
        Assert.DoesNotContain(BeastieBot3.Site.Display.SiteText.SimilarNames, html);
    }

    [Theory]
    [InlineData(FixtureDb.Tiger, "The latest assessment is")]
    [InlineData(FixtureDb.SumatranTiger, "Up to date, with a reference to the latest assessment.")]
    [InlineData(FixtureDb.Lion, "but the reference cites another assessment (ID 99999).")]
    public async Task TheSpeciesPageComparesTheTaxoboxStatusWithTheLatestAssessment(long taxonId, string text) {
        var html = await factory.CreateClient().GetStringAsync($"/species/{taxonId}");
        Assert.Contains("Status in the Wikipedia taxobox", html);
        Assert.Contains(text, html);
        Assert.Contains("From the copy of the article downloaded on 29 November 2025.", html);
        // Under the taxobox status parameters box, not in the links section.
        Assert.True(html.IndexOf("status parameters", StringComparison.Ordinal) < html.IndexOf("Status in the Wikipedia taxobox", StringComparison.Ordinal));
        Assert.DoesNotContain("<dt>Status in the Wikipedia taxobox</dt>", html);
    }

    [Fact]
    public async Task TheFooterGivesTheDateTheDataWasLastUpdated() {
        var html = await factory.CreateClient().GetStringAsync("/about");
        Assert.Contains("<p>Data last updated on 2 October 2026.</p>", html);
    }

    [Fact]
    public async Task TheSpeciesPageLinksOtherDatabasesFromTheIdsOnItsWikidataItem() {
        var html = await factory.CreateClient().GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains("<dt>Other databases</dt>", html);
        Assert.Contains("<a href=\"https://www.gbif.org/species/5219416\">GBIF</a> · ", html);
        Assert.Contains("<a href=\"https://www.inaturalist.org/taxa/41967\">iNaturalist</a>", html);
    }
}
