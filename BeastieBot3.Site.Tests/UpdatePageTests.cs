using System.Net;
using System.Text;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

public sealed class UpdatePageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static MultipartFormDataContent Form(string text) {
        var form = new MultipartFormDataContent();
        form.Add(new StringContent(text, Encoding.UTF8), UpdateModel.TextField);
        return form;
    }

    private async Task<(HttpResponseMessage Response, string Html)> Post(string text) {
        var response = await _client.PostAsync("/update", Form(text));
        return (response, await response.Content.ReadAsStringAsync());
    }

    // The textarea starts with one newline, which a browser drops.
    private static string? Output(string html) => Html.Textarea(html, "update-output") is { } text && text.StartsWith('\n') ? text[1..] : null;

    [Theory]
    [InlineData("https://en.wikipedia.org/wiki/List_of_parrots", "/update-statuses?page=List%20of%20parrots#result")]
    [InlineData("  <https://en.m.wikipedia.org/wiki/List_of_parrots>\n", "/update-statuses?page=List%20of%20parrots#result")]
    [InlineData("https://en.wikipedia.org/w/index.php?title=List_of_parrots&oldid=123", "/update-statuses?page=List%20of%20parrots&oldid=123#result")]
    [InlineData("[[List of parrots]]", "/update-statuses?page=List%20of%20parrots#result")]
    public async Task AWikipediaUrlOnItsOwnLoadsThePage(string text, string location) {
        var (response, _) = await Post(text);
        Assert.Equal(HttpStatusCode.Found, response.StatusCode);
        Assert.Equal(location, response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task AUrlOfAnotherWikipediaSaysSo_AndAUrlInsideWikitextIsWikitext() {
        var (response, html) = await Post("https://de.wikipedia.org/wiki/Papageien");
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("Not English Wikipedia: the link is to de.wikipedia.org.", Html.Text(html));

        var (inText, _) = await Post("* {{IUCN status|VU|22823/1|1}} see https://en.wikipedia.org/wiki/Polar_bear\n");
        Assert.Equal(HttpStatusCode.OK, inText.StatusCode);
    }

    [Fact]
    public async Task GetShowsTheForm() {
        var response = await _client.GetAsync("/update");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("enctype=\"multipart/form-data\"", html);
        Assert.Contains("name=\"text\"", html);
        Assert.Contains("Update IUCN statuses", Html.Text(html));
        Assert.Null(Html.Textarea(html, "update-output"));
    }

    [Fact]
    public async Task TheResultBoxHasAFixedHeightAndTheReportShowsChangedItemsFirst() {
        var input = "* [[Polar bear]] {{IUCN status|EN|22823/13045100|1|year=2008}}\n* {{IUCN status|VU|22823/14871490|1|year=2015}}\n";
        var (_, html) = await Post(input);
        Assert.Contains("class=\"update-output\" readonly", html);
        Assert.Contains("Updated wikitext (read only)", html);
        Assert.Contains("<input type=\"radio\" name=\"show\" id=\"show-changed\" checked=\"checked\"> Changed (1)", html);
        Assert.Contains("id=\"show-left\"> Left as is (0)", html);
        Assert.Contains("id=\"show-all\"> All items (2)", html);
        Assert.Equal("IUCN Red List 2026-1: Ursus maritimus EN\u2192VU (assisted by Species Check)",
            Html.Textarea(html, "update-edit-summary"));
    }

    [Fact]
    public async Task WithARegionChosenStatusesComeFromTheRegionsLatestAssessments() {
        var text = "* ''Passer domesticus'' {{IUCN status|VU|103818789/155522130|1|year=2019}}\n* ''Ursus maritimus'' {{IUCN status|VU}}\n";
        var form = Form(text);
        form.Add(new StringContent("Europe"), UpdateModel.RegionField);
        var html = await (await _client.PostAsync("/update", form)).Content.ReadAsStringAsync();
        Assert.Contains("{{IUCN status|LC|103818789/166245544|1|year=2021}}", Output(html));
        Assert.Contains("No assessment for Europe.", Html.Text(html));
        Assert.Contains("Compared with the latest assessments for Europe.", Html.Text(html));
        Assert.Contains("<option value=\"Europe\" selected=\"selected\">Europe (", html);
        Assert.StartsWith("IUCN Red List 2026-1, Europe assessments: Passer domesticus VU\u2192LC", Html.Textarea(html, "update-edit-summary"));
    }

    [Fact]
    public async Task AnUnknownRegionComparesWithGlobalAssessments() {
        var form = Form("* ''Passer domesticus'' {{IUCN status|VU|103818789/155522130|1|year=2019}}\n");
        form.Add(new StringContent("Atlantis"), UpdateModel.RegionField);
        var html = await (await _client.PostAsync("/update", form)).Content.ReadAsStringAsync();
        Assert.Contains("{{IUCN status|LC|103818789/155522130|1|year=2019}}", Output(html));
        Assert.DoesNotContain("Compared with the latest assessments for", Html.Text(html));
    }

    [Fact]
    public async Task TheEpbcActStatusIsShownWhenAskedFor() {
        var text = "* ''Phascolarctos cinereus'' {{IUCN status|EN}}\n* ''Casuarius casuarius'' {{IUCN status|LC}}\n";
        var form = Form(text);
        form.Add(new StringContent("1"), UpdateModel.EpbcField);
        var html = await (await _client.PostAsync("/update", form)).Content.ReadAsStringAsync();
        Assert.Contains("<th scope=\"col\">EPBC Act</th>", html);
        Assert.Contains("<td>EN (combined populations of Qld, NSW and the ACT)</td>", html);
        Assert.Contains("<td>EN</td>", html);
        var (_, without) = await Post(text);
        Assert.DoesNotContain("EPBC Act</th>", without);
    }

    [Theory]
    [InlineData("List of reptiles of Australia", true)]
    [InlineData("List of Australian birds", true)]
    [InlineData("List of mammals of Australasia", true)]
    [InlineData("List of birds of Austria", false)]
    public void AustralianTitlesShowTheEpbcActStatus(string title, bool expected) => Assert.Equal(expected, UpdateModel.IsAustralianTitle(title));

    [Fact]
    public async Task WithNothingChangedTheReportShowsTheItemsLeftAsIs() {
        var (_, html) = await Post("* {{IUCN status|VU|999999/1|1}}\n");
        Assert.Contains("id=\"show-left\" checked=\"checked\">", html);
    }

    [Fact]
    public async Task PageIsLinkedFromTheNavigationAndHome() {
        var home = await (await _client.GetAsync("/")).Content.ReadAsStringAsync();
        Assert.Contains("<a href=\"/update-statuses\">Update IUCN statuses</a>", home);
        Assert.Contains("Update the IUCN statuses in an article or list", home);
        var about = await (await _client.GetAsync("/about")).Content.ReadAsStringAsync();
        Assert.Contains("status update page", Html.Text(about));
    }

    [Fact]
    public async Task StatusTemplateIsUpdatedAndReported() {
        var input = "Intro.\n* [[Polar bear]] {{IUCN status|EN|22823/13045100|1|year=2008}}\n* {{IUCN status|EN|2785/6143|1|year=2008}} woylie\n";
        var (response, html) = await Post(input);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("no-store", response.Headers.CacheControl?.ToString());
        Assert.Equal(
            "Intro.\n* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}\n* {{IUCN status|CR|2790/2790001|1|year=2015}} woylie\n",
            Output(html));
        var text = Html.Text(html);
        Assert.Contains("2 items found: 2 changed, 0 already up to date, 0 left as is.", text);
        Assert.Contains("Old taxon id 2785 replaced with the current taxon id.", text);
        Assert.Contains("href=\"/species/22823\"", html);
        Assert.Contains("href=\"/species/2790\"", html);
        // The pasted text is shown again in the form.
        Assert.Equal("\n" + input, Html.Textarea(html, "update-input"));
    }

    [Fact]
    public async Task TableCellsAreFoundByName() {
        var input = """
            {| class="wikitable"
            ! Species !! IUCN status
            |-
            | ''[[Ursus maritimus]]'' || EN
            |-
            | ''Ursus imaginarius'' || LC
            |-
            | [[Tiger|''Panthera tigris'']] {{efn|Also ''Felis tigris''.}} || {{IUCN status|EN}}
            |}
            """;
        var (_, html) = await Post(input);
        var output = Output(html)!;
        Assert.Contains("| ''[[Ursus maritimus]]'' || VU\n", output);
        Assert.Contains("| ''Ursus imaginarius'' || LC\n", output);
        Assert.Contains("|| {{IUCN status|EN}}\n", output);
        var text = Html.Text(html);
        Assert.Contains("3 items found: 1 changed, 1 already up to date, 1 left as is.", text);
        Assert.Contains("No taxon in this Red List version has the name Ursus imaginarius.", text);
    }

    [Fact]
    public async Task SpeciesboxGetsTheLatestStatusAndCitation() {
        var input = """
            {{Speciesbox
            | name = Baiji
            | status = CR
            | status_system = IUCN3.1
            | status_ref = <ref name="iucn">{{cite iucn |author=Smith, B.D. |year=2008 |title=''Lipotes vexillifer'' |volume=2008 |article-number=e.T12119A3322533}}</ref>
            | taxon = Lipotes vexillifer
            }}
            """;
        var (_, html) = await Post(input);
        var output = Output(html)!;
        Assert.Contains("| status = PE\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\">{{cite iucn |last1=Smith |first1=B.D. |last2=Wang |first2=D.", output);
        Assert.Contains("|article-number=e.T12119A50358152 |doi=10.2305/IUCN.UK.2017-3.RLTS.T12119A50358152.en |access-date=18 August 2026}}</ref>\n| taxon = Lipotes vexillifer\n}}", output);
        Assert.Contains("Replaced the {{cite iucn}} in status_ref with a citation of the latest assessment.", Html.Text(html));
    }

    [Fact]
    public async Task NothingFound() {
        var (_, html) = await Post("Plain text with no statuses.");
        Assert.Equal("Plain text with no statuses.", Output(html));
        Assert.Contains("No IUCN statuses found.", Html.Text(html));
    }

    [Fact]
    public async Task EmptyTextIsAnError() {
        var (response, html) = await Post("  \n ");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("No text to update.", Html.Text(html));
        Assert.Null(Html.Textarea(html, "update-output"));
    }

    [Fact]
    public async Task TextOver2MbIsRefused() {
        var (response, html) = await Post(new string('x', UpdateModel.MaxTextBytes + 1));
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Contains("Text too large: the limit is 2 MB.", Html.Text(html));
        Assert.Null(Html.Textarea(html, "update-output"));
    }

    [Fact]
    public async Task OversizeBodyIs413() {
        var (response, html) = await Post(new string('x', 3 * 1024 * 1024));
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Contains("Text too large: the limit is 2 MB.", Html.Text(html));
    }

    [Fact]
    public async Task BodyThatIsNotAFormIs400() {
        var response = await _client.PostAsync("/update", new StringContent("x", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("Could not read the form.", Html.Text(await response.Content.ReadAsStringAsync()));
    }

    [Theory]
    [InlineData("/about")]
    [InlineData("/species/22823")]
    [InlineData("/update/x")]
    public async Task PostIsAllowedOnlyOnTheUpdatePage(string path) {
        var response = await _client.PostAsync(path, Form("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
    }

    [Fact]
    public async Task OtherMethodsOnTheUpdatePageAre405() {
        var response = await _client.PutAsync("/update", Form("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.Equal("GET, HEAD, POST", string.Join(", ", response.Content.Headers.Allow));
    }

    [Fact]
    public async Task TrailingSlashAndCaseArePosted() {
        var response = await _client.PostAsync("/Update", Form("{{IUCN status|EN|22823/1|1|year=2008}}"));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}

public sealed class UpdateRateLimitTests(RateLimitedSiteFactory factory) : IClassFixture<RateLimitedSiteFactory> {
    [Fact]
    public async Task TooManyUpdatesGetThe429Page() {
        var client = factory.Client();
        for (var i = 0; i < RateLimitedSiteFactory.Limit; i++) {
            var form = new MultipartFormDataContent { { new StringContent("x" + i), UpdateModel.TextField } };
            Assert.Equal(HttpStatusCode.OK, (await client.PostAsync("/update", form)).StatusCode);
        }
        var again = new MultipartFormDataContent { { new StringContent("x"), UpdateModel.TextField } };
        var limited = await client.PostAsync("/update", again);
        Assert.Equal(HttpStatusCode.TooManyRequests, limited.StatusCode);
        Assert.Contains("Too many requests", Html.Text(await limited.Content.ReadAsStringAsync()));
        // The form itself is a page and is counted with the pages.
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/update")).StatusCode);
    }

}
