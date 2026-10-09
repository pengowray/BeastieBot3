using System.Net;

namespace BeastieBot3.Site.Tests;

public sealed class SpeciesPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private Task<string> Page(string query = "") => _client.GetStringAsync($"/species/{FixtureDb.PolarBear}{query}");

    // The polar bear's wikitext page.
    private Task<string> Tool(string query = "") => _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext{query}");

    [Fact]
    public async Task SectionsComeInOrder() {
        var html = await Page();
        string[] markers = [
            "<h1>",
            "<span class=\"sci-name\"><i>Ursus maritimus</i></span>",
            "class=\"taxon-common-name\">Polar bear",
            "class=\"classification",
            ">Latest global assessment</h2>",
            ">Assessment history</h2>",
            ">Subspecies and subpopulations (IUCN)</h2>",
            ">Names</h2>",
            ">Links to other sites</h2>",
            ">Tools</h2>",
            "class=\"site-footer\"",
        ];
        var last = -1;
        foreach (var marker in markers) {
            var at = html.IndexOf(marker, last + 1, StringComparison.Ordinal);
            Assert.True(at > last, $"'{marker}' should come after the previous section");
            last = at;
        }
    }

    [Fact]
    public async Task TheTaxonPageHasNoWikitextAndLinksItsWikitextPage() {
        var html = await Page();
        Assert.DoesNotContain("id=\"wikitext\"", html);
        Assert.DoesNotContain(">Show wikitext</a>", html);
        Assert.DoesNotContain(">Wikitext</th>", html);
        Assert.Contains($"<a href=\"/species/{FixtureDb.PolarBear}/wikitext\">Wikitext and citations</a>", Section(html, "tools"));
        Assert.Contains("<a href=\"/taxa/genus/ursus/list\">", Section(html, "tools"));
        // The Tools menu in the header has the page's own tool first.
        var menu = html[html.IndexOf("<details class=\"nav-menu\">", StringComparison.Ordinal)..];
        Assert.True(menu.IndexOf($"/species/{FixtureDb.PolarBear}/wikitext", StringComparison.Ordinal) < menu.IndexOf("/update", StringComparison.Ordinal));

        var tool = await Tool();
        Assert.Contains($"<a href=\"/species/{FixtureDb.PolarBear}\">", tool);
        Assert.Contains($"<link rel=\"canonical\" href=\"http://localhost/species/{FixtureDb.PolarBear}/wikitext\">", tool);
        Assert.Contains(">Show wikitext</a>", tool);
    }

    [Fact]
    public async Task ATaxonPageAddressWithWikitextOptionsGoesToTheWikitextPage() {
        var response = await _client.GetAsync($"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}&authors=author&q=bear");
        Assert.Equal(HttpStatusCode.Found, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}&authors=author", response.Headers.Location?.OriginalString);
        // A search for a common name keeps the taxon page.
        Assert.Equal(HttpStatusCode.OK, (await _client.GetAsync($"/species/{FixtureDb.PolarBear}?q=polar+bear")).StatusCode);
    }

    [Fact]
    public async Task TheTaxonPageShowsWhoAssessedTheLatestAssessmentAndIucnsCitation() {
        static string Status(string html) {
            var status = html[html.IndexOf("<section class=\"status\"", StringComparison.Ordinal)..];
            return status[..status.IndexOf("</section>", StringComparison.Ordinal)];
        }
        Assert.Contains("id=\"iucn-citation\"", Status(await Page()));
        Assert.Contains("id=\"iucn-credits\"", Status(await _client.GetStringAsync($"/species/{FixtureDb.Tiger}")));
    }

    [Fact]
    public async Task HeaderAndStatusSummary() {
        var html = await Page();
        var text = Html.Text(html);
        Assert.Contains("<span class=\"authority\">Phipps, 1774</span>", html);
        Assert.Contains("Kingdom Animalia", text);
        Assert.Contains("Family Ursidae", text);
        Assert.Contains("<a href=\"/taxa/genus/ursus\"><i>Ursus</i></a>", html);
        Assert.Contains("<span class=\"badge cat-vu\">VU</span> <span class=\"category-label\">Vulnerable</span>", html);
        Assert.Contains("Criteria A3c", text);
        Assert.Contains("Population trend Unknown", text);
        Assert.Contains("Date assessed 21 March 2015", text);
        Assert.Contains("Year published 2015", Html.Text(await Page()));
        Assert.Contains("Data from Red List version 2026-1", text);
        Assert.Contains("<a href=\"https://www.iucnredlist.org/species/22823/14871490\">Read the full assessment on the IUCN Red List website</a>", html);
        Assert.Contains($"<link rel=\"canonical\" href=\"http://localhost/species/{FixtureDb.PolarBear}\">", html);
        Assert.Contains("<title>Ursus maritimus (Polar bear) | Beastie Bot Species Status</title>", html);
    }

    [Fact]
    public async Task CanonicalHostIsLowerCaseWhateverTheFirstVisitorSent() {
        async Task<string> Get(string host) {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/species/{FixtureDb.Koala}");
            request.Headers.Host = host;
            var response = await _client.SendAsync(request);
            return await response.Content.ReadAsStringAsync();
        }
        var expected = $"<link rel=\"canonical\" href=\"http://species.example.org/species/{FixtureDb.Koala}\">";
        Assert.Contains(expected, await Get("SpEcIeS.Example.ORG"));
        // The second request is answered from the output cache, which ignores the host's case.
        Assert.Contains(expected, await Get("species.example.org"));
        Assert.Contains($"<link rel=\"canonical\" href=\"http://species.example.org:8080/species/{FixtureDb.Koala}\">", await Get("Species.Example.org:8080"));
    }

    [Fact]
    public async Task DefaultWikitext() {
        var html = await Tool();
        var cite = Html.Textarea(html, "wikitext-cite");
        Assert.Equal(
            "<ref name=\"iucn\">{{cite iucn |last1=Wiig |first1=Ø. |last2=Amstrup |first2=S. |last3=Atwood |first3=T. |last4=Laidre |first4=K. " +
            "|author5=Jon Aars |last6=Thiemann |first6=G. |year=2015 |title=''Ursus maritimus'' |volume=2015 " +
            "|article-number=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=18 August 2026}}</ref>",
            cite);
        Assert.Equal("{{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "wikitext-status"));
        var speciesbox = Html.Textarea(html, "wikitext-speciesbox");
        Assert.NotNull(speciesbox);
        Assert.StartsWith("| status = VU\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\">{{cite iucn |last1=Wiig |first1=Ø.", speciesbox);

        var text = Html.Text(html);
        Assert.Contains("DOI from GBIF's copy of the IUCN checklist.", text);
        Assert.Contains($"Check these author names, given exactly as IUCN wrote them: {FixtureDb.VerbatimAuthor}", text);
        Assert.Contains("<summary>Citation as given by IUCN</summary>", html);
        Assert.Contains("aria-label=\"Copy {{cite iucn}} wikitext\" hidden>Copy</button>", html);
        Assert.Contains("Date downloaded from IUCN (18 August 2026)", text);
    }

    [Fact]
    public async Task LastFirstAuthorsAndNoAccessDate() {
        var cite = Html.Textarea(await Tool("?access=none"), "wikitext-cite")!;
        Assert.Contains("|last1=Wiig |first1=Ø. |last2=Amstrup |first2=S.", cite);
        Assert.Contains("|author5=Jon Aars", cite);
        Assert.DoesNotContain("access-date", cite);
    }

    [Fact]
    public async Task AuthorNAuthorsAndNoAccessDate() {
        var cite = Html.Textarea(await Tool("?authors=author&access=none"), "wikitext-cite")!;
        Assert.Contains("|author=Wiig, Ø. |author2=Amstrup, S.", cite);
        Assert.Contains("|author5=Jon Aars", cite);
        Assert.DoesNotContain("access-date", cite);
    }

    [Fact]
    public async Task TodayAsAccessDate() {
        var today = DateTime.UtcNow.ToString("d MMMM yyyy", System.Globalization.CultureInfo.InvariantCulture);
        var html = await Tool("?access=today");
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.Contains($"|access-date={today}}}", cite);
        // The date is the server's UTC date, and the label says so.
        Assert.Contains($"Today ({today}, UTC)", Html.Text(html));
    }

    [Fact]
    public async Task EachWikitextBoxHasItsOwnCopyStatus() {
        var html = await Tool();
        // The three boxes before the citation options; the {{cite Q}} part after them may add more.
        var start = Html.IndexOf(html, "id=\"wikitext-output\"");
        var main = html[start..Html.IndexOf(html, "<form class=\"options-form\"")];
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(main, "<div class=\"wikitext-box-head\">").Count);
        var boxes = System.Text.RegularExpressions.Regex.Matches(html, "<div class=\"wikitext-box-head\">").Count;
        Assert.Equal(boxes, System.Text.RegularExpressions.Regex.Matches(html,
            "hidden>Copy</button>\\s*<p class=\"copy-status\" role=\"status\" aria-live=\"polite\"></p>\\s*</div>").Count);
        var failed = System.Text.RegularExpressions.Regex.Match(html, "data-copy-failed=\"([^\"]*)\"").Groups[1].Value;
        Assert.Equal("Copy failed. The wikitext is selected: copy it with Ctrl+C (⌘C on a Mac) or your browser's Copy command.", WebUtility.HtmlDecode(failed));
        Assert.Contains("data-copy-failed-button=\"Copy failed\"", html);
    }

    [Fact]
    public async Task ClassificationIsNotANavigationLandmark() {
        var html = await Page();
        Assert.DoesNotContain("<nav class=\"classification\"", html);
        Assert.Contains("<div class=\"classification has-col-toggle\">", html);
        Assert.Contains("<h2 id=\"short-classification-heading\" class=\"visually-hidden\">Short classification</h2>", html);
    }

    [Fact]
    public async Task RefWrappingRefNameAndAmp() {
        var plain = Html.Textarea(await Tool("?opts=1"), "wikitext-cite")!;
        Assert.StartsWith("{{cite iucn ", plain);
        Assert.DoesNotContain("name-list-style", plain);

        var html = await Tool("?opts=1&ref=1&refname=polar&amp=1");
        var named = Html.Textarea(html, "wikitext-cite")!;
        Assert.StartsWith("<ref name=\"polar\">{{cite iucn ", named);
        Assert.Contains("|name-list-style=amp", named);
        // status_ref is always a ref, with the same name.
        Assert.Contains("| status_ref = <ref name=\"polar\">", Html.Textarea(html, "wikitext-speciesbox"));

        var unnamed = Html.Textarea(await Tool("?opts=1&ref=1&refname="), "wikitext-cite")!;
        Assert.StartsWith("<ref>{{cite iucn ", unnamed);

        var quoted = Html.Textarea(await Tool("?opts=1&ref=1&refname=" + Uri.EscapeDataString("a\"><script>")), "wikitext-cite")!;
        Assert.StartsWith("<ref name=\"ascript\">", quoted);
    }

    [Fact]
    public async Task DefaultRefNameDependsOnTheAssessment() {
        var latest = await Tool();
        Assert.Contains("name=\"refname\" value=\"iucn\"", latest);
        Assert.Contains("Use a ref name that no other citation in the article uses, unless this citation replaces the citation with that name.", Html.Text(latest));
        // The link to an earlier assessment does not carry "iucn" to it.
        Assert.Contains($"href=\"/species/22823/wikitext?assessment={FixtureDb.PolarBear2008}#wikitext\"", latest);

        var earlier = await Tool($"?assessment={FixtureDb.PolarBear2008}");
        Assert.StartsWith("<ref name=\"iucn2008\">{{cite iucn ", Html.Textarea(earlier, "wikitext-cite"));
        Assert.Contains("| status_ref = <ref name=\"iucn2008\">", Html.Textarea(earlier, "wikitext-speciesbox"));
        Assert.Contains("name=\"refname\" value=\"iucn2008\"", earlier);
        Assert.Contains("<a href=\"/species/22823/wikitext#wikitext\">Show wikitext for the latest assessment</a>", earlier);

        var regional = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}/wikitext?assessment={FixtureDb.HouseSparrowEurope}");
        Assert.StartsWith("<ref name=\"iucn-europe\">{{cite iucn ", Html.Textarea(regional, "wikitext-cite"));
        Assert.Contains($"<a href=\"/species/{FixtureDb.HouseSparrow}/wikitext#wikitext\">Show wikitext for the latest assessment</a>", regional);

        // Two global assessments published in 2010: the replaced one's name has its id.
        var replaced = await _client.GetStringAsync($"/species/{FixtureDb.Micropyropsis}/wikitext?assessment={FixtureDb.MicropyropsisReplaced}");
        Assert.StartsWith($"<ref name=\"iucn2010-{FixtureDb.MicropyropsisReplaced}\">", Html.Textarea(replaced, "wikitext-cite"));
        var errata = await _client.GetStringAsync($"/species/{FixtureDb.Micropyropsis}/wikitext");
        Assert.StartsWith("<ref name=\"iucn\">", Html.Textarea(errata, "wikitext-cite"));
    }

    [Fact]
    public async Task ARefNameTheVisitorChoseGoesToOtherAssessments() {
        var chosen = await Tool("?opts=1&ref=1&refname=polar");
        Assert.Contains($"href=\"/species/22823/wikitext?assessment={FixtureDb.PolarBear2008}&amp;opts=1&amp;ref=1&amp;refname=polar#wikitext\"", chosen);

        // The form sends the pre-filled default back with another option: the link to the 2008
        // assessment names that assessment's default.
        var submitted = await Tool("?opts=1&ref=1&refname=iucn&amp=1");
        Assert.Contains($"href=\"/species/22823/wikitext?assessment={FixtureDb.PolarBear2008}&amp;opts=1&amp;ref=1&amp;amp=1&amp;refname=iucn2008#wikitext\"", submitted);
    }

    [Fact]
    public async Task OptionsFormKeepsTheChoices() {
        var html = await Tool("?authors=author&access=today&opts=1&amp=1");
        Assert.Contains("value=\"author\" checked=\"checked\"", html);
        // Last/first is the default: its radio button is checked without an authors parameter, and an
        // explicit authors=lastfirst (sent by the form's radio button) means the same.
        Assert.Contains("value=\"lastfirst\" checked=\"checked\"", await Tool("?opts=1"));
        Assert.Contains("value=\"lastfirst\" checked=\"checked\"", await Tool("?authors=lastfirst&opts=1"));
        Assert.Contains("value=\"today\" checked=\"checked\"", html);
        Assert.Contains("name=\"amp\" value=\"1\" checked=\"checked\"", html);
        Assert.DoesNotContain("name=\"ref\" value=\"1\" checked", html);
        Assert.Contains("<form class=\"options-form\" method=\"get\" action=\"/species/22823/wikitext#wikitext\">", html);
        // History links keep the options.
        Assert.Contains($"href=\"/species/22823/wikitext?assessment={FixtureDb.PolarBear2008}&amp;authors=author&amp;access=today&amp;opts=1&amp;amp=1#wikitext\"", html);
    }

    [Fact]
    public async Task EarlierAssessmentWikitext() {
        var html = await Tool($"?assessment={FixtureDb.PolarBear2008}");
        var text = Html.Text(html);
        Assert.Contains("Wikitext for an earlier assessment: Vulnerable, published 2008.", text);
        Assert.Contains("<a href=\"/species/22823/wikitext#wikitext\">Show wikitext for the latest assessment</a>", html);
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.Contains("|last1=Schliebe |first1=S. |display-authors=etal |year=2008", cite);
        Assert.Contains("|article-number=e.T22823A13045100", cite);
        Assert.Contains("No DOI found in IUCN's citation text, GBIF or Wikidata. {{cite iucn}} works without a DOI.", text);
        Assert.Equal("{{IUCN status|VU|22823/13045100|1|year=2008}}", Html.Textarea(html, "wikitext-status"));
        // The status summary still shows the latest assessment.
        Assert.Contains("Year published 2015", Html.Text(await Page()));
        Assert.Contains("<input type=\"hidden\" name=\"assessment\" value=\"13045100\">", html);
        Assert.Contains("<span class=\"shown-label\">Shown</span>", html);
    }

    [Fact]
    public async Task LowerRiskAssessmentUsesIucn23() {
        var html = await Tool($"?assessment={FixtureDb.PolarBear1996}");
        Assert.Contains("Lower Risk/conservation dependent", Html.Text(html));
        Assert.Equal("{{IUCN status|LR/cd|22823/13045101|1|year=1996}}", Html.Textarea(html, "wikitext-status"));
        // No citation, so no Speciesbox box either.
        Assert.Null(Html.Textarea(html, "wikitext-speciesbox"));
    }

    [Fact]
    public async Task OldCategoryCodesKeepTheirCase() {
        var html = await Tool($"?assessment={FixtureDb.PolarBear1988Nt}");
        var text = Html.Text(html);
        // "nt" is the pre-1994 Not Threatened, not Near Threatened.
        Assert.Contains("<span class=\"badge cat-other\">nt</span> <span class=\"category-label\">Not Threatened (1994 or earlier categories)</span>", html);
        Assert.DoesNotContain("{{IUCN status|NT", html);
        Assert.Null(Html.Textarea(html, "wikitext-status"));
        Assert.Contains("{{IUCN status}} and {{Speciesbox}} have no code for this category.", text);
        Assert.DoesNotContain("{{IUCN status}} wikitext is available.", text);

        var baiji = await _client.GetStringAsync($"/species/{FixtureDb.Baiji}/wikitext?assessment={FixtureDb.Baiji1986Ex}");
        Assert.Contains("<span class=\"badge cat-other\">Ex</span> <span class=\"category-label\">Extinct (1994 or earlier categories)</span>", baiji);
        Assert.DoesNotContain("{{IUCN status|EX", baiji);
    }

    [Theory]
    [InlineData(FixtureDb.PolarBear, "{{Speciesbox}}")]
    [InlineData(FixtureDb.SumatranTiger, "{{Subspeciesbox}}")]
    [InlineData(FixtureDb.PlantSubspecies, "{{Infraspeciesbox}}")]
    public async Task TaxoboxLabelFollowsTheKindOfTaxon(long taxonId, string taxobox) {
        var html = await _client.GetStringAsync($"/species/{taxonId}/wikitext");
        Assert.Contains($"<label for=\"wikitext-speciesbox\">{taxobox} status parameters</label>", html);
        Assert.Contains($"aria-label=\"Copy {taxobox} wikitext\"", html);
    }

    [Fact]
    public async Task CurrentCodesOnEarlierVersionRowsGetNoTemplates() {
        var subspecies = await _client.GetStringAsync($"/species/{FixtureDb.PlantSubspecies}/wikitext");
        // The latest NT is Near Threatened; the 1998 NT with no criteria version is not named.
        Assert.Contains("<span class=\"badge cat-nt\">NT</span> <span class=\"category-label\">Near Threatened</span>", subspecies);
        Assert.Contains("<span class=\"badge cat-other\">NT</span> <span class=\"category-label\">No name given by IUCN (1994 or earlier categories)</span>", subspecies);

        var nt = await _client.GetStringAsync($"/species/{FixtureDb.PlantSubspecies}/wikitext?assessment={FixtureDb.PlantSubspecies1998Nt}");
        var ntText = Html.Text(nt);
        Assert.Contains("Wikitext for an earlier assessment: No name given by IUCN (1994 or earlier categories), published 1998.", ntText);
        Assert.Contains("This assessment uses an earlier version of the IUCN categories, so no {{IUCN status}} or {{Infraspeciesbox}} wikitext is given for it.", ntText);
        Assert.DoesNotContain("have no code for this category", ntText);
        Assert.NotNull(Html.Textarea(nt, "wikitext-cite"));
        Assert.Null(Html.Textarea(nt, "wikitext-status"));
        Assert.Null(Html.Textarea(nt, "wikitext-speciesbox"));

        var ex = await _client.GetStringAsync($"/species/{FixtureDb.Bromus}/wikitext?assessment={FixtureDb.Bromus1998Ex}");
        Assert.Contains("<span class=\"badge cat-ex\">EX</span> <span class=\"category-label\">Extinct</span>", ex);
        Assert.Contains("This assessment uses an earlier version of the IUCN categories", Html.Text(ex));
        Assert.Null(Html.Textarea(ex, "wikitext-status"));
        Assert.Null(Html.Textarea(ex, "wikitext-speciesbox"));
    }

    [Fact]
    public async Task AssessmentOfAnotherTaxonIsIgnored() {
        var html = await Tool($"?assessment={FixtureDb.LionLatest}");
        Assert.Equal("{{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "wikitext-status"));
        Assert.DoesNotContain("15951", Html.Textarea(html, "wikitext-cite"));
    }

    [Fact]
    public async Task HistoryNewestFirstWithLatestLabel() {
        var html = await Tool();
        var y2015 = html.IndexOf("<th scope=\"row\">2015 <span class=\"tag\">Latest</span>", StringComparison.Ordinal);
        var y2008 = html.IndexOf("<th scope=\"row\">2008", StringComparison.Ordinal);
        var y1996 = html.IndexOf("<th scope=\"row\">1996", StringComparison.Ordinal);
        var y1988 = html.IndexOf("<th scope=\"row\">1988", StringComparison.Ordinal);
        Assert.True(y2015 > 0 && y2008 > y2015 && y1996 > y2008 && y1988 > y1996);
        Assert.Contains("aria-label=\"Show wikitext for the assessment published in 2008\">Show wikitext</a>", html);
        Assert.Contains("<a href=\"https://www.iucnredlist.org/species/22823/13045100\">IUCN Red List website</a>", html);
    }

    [Fact]
    public async Task ErrataVersionAndTheAssessmentItReplacedAreTold() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Micropyropsis}/wikitext");
        var text = Html.Text(html);
        Assert.Contains("2010 Latest Errata version, published 2016", text);
        Assert.Contains("2010 Replaced by the errata version", text);
        Assert.Contains($"href=\"/species/{FixtureDb.Micropyropsis}/wikitext?assessment={FixtureDb.MicropyropsisReplaced}#wikitext\" aria-label=\"Show wikitext for the assessment published in 2010 (replaced by the errata version)\"", html);
        // Regional rows carry the region and the note in the link's name.
        Assert.Contains("Europe EN Endangered B1ab(iii)+2ab(iii) 2011 Errata version, published 2016", text);
        Assert.Contains("aria-label=\"Show wikitext for the Europe assessment published in 2011 (errata version, published 2016)\"", html);

        var replaced = await _client.GetStringAsync($"/species/{FixtureDb.Micropyropsis}/wikitext?assessment={FixtureDb.MicropyropsisReplaced}");
        Assert.Contains("Wikitext for an earlier assessment: Endangered, published 2010. Replaced by the errata version.", Html.Text(replaced));
        Assert.Contains("aria-label=\"Show wikitext for the assessment published in 2010 (errata version, published 2016)\"", replaced);
    }

    [Fact]
    public async Task AmendedVersionAndTheAssessmentItReplacedAreTold() {
        var text = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}"));
        Assert.Contains("2019 Latest Amended version of the 2018 assessment", text);
        Assert.Contains("2018 Replaced by the amended version", text);
    }

    [Fact]
    public async Task OtherLanguagesListEachNameOnceWithAllItsSources() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var text = Html.Text(html);
        // Spellings that differ only in case are one name, shown as IUCN spells it when the
        // spellings have as many sources each; sources in a fixed order.
        Assert.Contains("French Tigre IUCN Red List, Wikidata, Catalogue of Life, Wikipedia", text);
        // The name with most sources first.
        Assert.Contains("German Tiger Wikidata, Catalogue of Life, Wikipedia Königstiger Wikidata", text);
        Assert.Matches("<th scope=\"rowgroup\" rowspan=\"2\">German</th>\\s*<td lang=\"de\">Tiger</td>", html);
        Assert.Contains("Japanese トラ tora Wikidata, Catalogue of Life, Wikipedia", text);
        Assert.Contains("Chinese 老虎 lǎo hǔ Wikidata, Catalogue of Life 虎 hǔ Wikipedia", text);
        // Languages by name; the 11th and later are hidden until the box is ticked.
        var languages = new[] {
            "Austronesian languages", "Chinese", "Dutch", "French", "German", "Italian", "Japanese", "Korean", "Polish", "Portuguese",
            "Russian", "Spanish", "Swedish",
        };
        var at = languages.Select(l => Html.IndexOf(html, $">{l}</th>")).ToList();
        Assert.All(at, i => Assert.True(i > 0));
        Assert.Equal(at.OrderBy(i => i), at);
        var hidden = Html.Between(html, $">{languages[9]}</th>", "</table>");
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(hidden, "<tbody class=\"more-item\">").Count);
        Assert.Contains("Show all languages (3 more)", text);
    }

    [Fact]
    public async Task NamesAndLinks() {
        var html = await Page();
        var text = Html.Text(html);
        Assert.Contains("English common names", text);
        Assert.Contains("Polar bear IUCN's main English name IUCN Red List, Wikidata", text);
        Assert.Contains("White bear Catalogue of Life", text);
        Assert.Contains("Thalassic bear Wikipedia taxobox", text);
        Assert.Contains("Common names in other languages", text);
        Assert.Contains("Language Name Source French Ours blanc Wikidata Ours polaire IUCN Red List Spanish Oso polar IUCN Red List", text);
        Assert.Matches("<th scope=\"rowgroup\" rowspan=\"2\">French</th>\\s*<td lang=\"fr\">Ours blanc</td>", html);
        // "eng" is English; "und" is listed last, as no language, with no lang attribute.
        Assert.Contains("Ice bear IUCN Red List", text);
        Assert.DoesNotContain("Invariant", text);
        Assert.Matches("<th scope=\"rowgroup\" rowspan=\"1\">Language not given</th>\\s*<td>Nanuq</td>\\s*<td>IUCN Red List</td>\\s*</tr>\\s*</tbody>\\s*</table>", html);
        Assert.Contains("<th scope=\"row\"><i>Thalarctos maritimus</i> <span class=\"authority\">(Phipps, 1774)</span></th>", html);
        Assert.Contains("<td>IUCN Red List, Catalogue of Life (authority: <span class=\"authority\">Phipps, 1774</span>)</td>", html);
        Assert.Contains("<th scope=\"row\"><i>Ursus marinus</i> <span class=\"authority\">Pallas, 1776</span></th>", html);
        Assert.Contains("<th scope=\"row\"><i>Ursus polaris</i></th>", html);
        Assert.Contains("<td>Wikidata</td>", html);
        Assert.Contains("<a href=\"https://en.wikipedia.org/wiki/Polar_bear\">Polar bear</a>", html);
        Assert.Contains("<a href=\"https://www.wikidata.org/wiki/Q33609\">Q33609</a>", html);
        Assert.Contains("<a href=\"https://www.catalogueoflife.org/data/taxon/4QHKG\"><i>Ursus maritimus</i></a>", html);
        // One footer: the dates the assessments were downloaded are in it, not in a note of the page's own.
        Assert.Contains("Assessments downloaded from the IUCN Red List API between 18 August and 1 September 2026.", text);
        Assert.DoesNotContain("data-note", html);
    }

    [Fact]
    public async Task PossiblyExtinctAndDoiFromWikidata() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Baiji}/wikitext");
        var text = Html.Text(html);
        Assert.Contains("<span class=\"badge cat-cr\">CR (PE)</span> <span class=\"category-label\">Critically Endangered (Possibly Extinct)</span>", html);
        Assert.Equal("{{IUCN status|CR(PE)|12119/50358152|1|year=2017}}", Html.Textarea(html, "wikitext-status"));
        Assert.StartsWith("| status = PE\n| status_system = IUCN3.1\n", Html.Textarea(html, "wikitext-speciesbox"));
        Assert.Contains("DOI from Wikidata.", text);
        Assert.Contains("Criteria A2cd; C2a(ii); D", Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.Baiji}")));
    }

    [Fact]
    public async Task OrganisationAuthorAndRegionalAssessment() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}/wikitext");
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.Contains("{{cite iucn |author=BirdLife International |year=2019", cite);
        var text = Html.Text(html);
        Assert.DoesNotContain("DOI from", text);
        Assert.DoesNotContain("No DOI found", text);
        Assert.Contains("Regional assessments", text);
        Assert.Contains("Europe LC Least Concern", text);
        Assert.Contains($"href=\"/species/{FixtureDb.HouseSparrow}/wikitext?assessment={FixtureDb.HouseSparrowEurope}#wikitext\"", html);

        var regional = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}/wikitext?assessment={FixtureDb.HouseSparrowEurope}");
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

        Assert.Contains("<h2 id=\"related-heading\">Species</h2>", html);
        Assert.Contains($"<a href=\"/species/{FixtureDb.Tiger}\"><i>Panthera tigris</i></a>", Section(html, "related"));

        var tiger = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains("<h2 id=\"related-heading\">Subspecies (IUCN)</h2>", tiger);
        Assert.Contains($"<a href=\"/species/{FixtureDb.SumatranTiger}\">", Section(tiger, "related"));

        var lion = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.Contains("<h2 id=\"related-heading\">Subpopulations (IUCN)</h2>", lion);
    }

    [Fact]
    public async Task SpeciesWithNoSubspeciesSaysSo() {
        var bear = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}"));
        Assert.Contains("Subspecies and subpopulations (IUCN) IUCN has not assessed any subspecies or subpopulations of this species.", bear);

        var brome = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.Bromus}"));
        Assert.Contains("Subspecies, varieties and subpopulations (IUCN) IUCN has not assessed any subspecies, varieties or subpopulations of this species.", brome);
    }

    [Fact]
    public async Task SubspeciesOfAnUnassessedSpeciesListsTheOtherSubspecies() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PlantSubspecies}");
        var related = Section(html, "related");
        Assert.Contains("IUCN has not assessed the species <span class=\"sci-name\"><i>Hirtella zanzibarica</i></span> as a whole.", related);
        Assert.Contains("<h3>Other subspecies of this species</h3>", related);
        Assert.Contains($"<a href=\"/species/{FixtureDb.PlantSubspeciesSibling}\">", related);
        Assert.DoesNotContain($"<a href=\"/species/{FixtureDb.PlantSubspecies}\">", related);
    }

    [Fact]
    public async Task AnAssessmentWithNoScopeIsListedAndNeverCountedAsGlobal() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.NoScopeOnly}");
        var text = Html.Text(html);
        Assert.Contains("No global assessment. IUCN published the current assessment of this taxon with no geographic scope.", text);
        Assert.DoesNotContain("This taxon has been assessed in", text);
        Assert.Contains("<h2 id=\"regional-heading\">Assessments with no geographic scope</h2>", html);
        Assert.Contains("<th scope=\"row\">No scope given</th>", html);
        Assert.Contains("Region No scope given", text);
        Assert.Contains("Wikitext for the assessment with no geographic scope: Data Deficient, published 2011.",
            Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.NoScopeOnly}/wikitext")));
    }

    [Fact]
    public async Task AProvisionalNameSaysSo() {
        var text = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.NoScopeOnly}"));
        Assert.Contains("Provisional name: \"sp. nov.\" means new species. The species had not been formally described when IUCN assessed it.", text);
        Assert.DoesNotContain("Synonyms table", text);
        Assert.DoesNotContain("Provisional name", Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}")));
    }

    [Fact]
    public async Task AnAssessmentTheApiDoesNotFindIsMarked() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        Assert.Contains($"{FixtureDb.PolarBear1988Nt}\">IUCN Red List website</a> <span class=\"version-note\">Not found in the IUCN API</span>", html);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(html, "Not found in the IUCN API"));
    }

    private static string Section(string html, string id) {
        var start = html.IndexOf($"<section class=\"{id}\"", StringComparison.Ordinal);
        Assert.True(start >= 0, $"no section {id}");
        return html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
    }

    [Fact]
    public async Task SubpopulationWithoutCitation() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.WestAfricanLion}/wikitext");
        var text = Html.Text(html);
        Assert.Contains($"Subpopulation of <a href=\"/species/{FixtureDb.Lion}\"><i>Panthera leo</i></a>", await _client.GetStringAsync($"/species/{FixtureDb.WestAfricanLion}"));
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
        Assert.Contains("No global assessment. This taxon has been assessed in <a href=\"#regional\">2 regions</a>.", html);
        // The latest regional assessment is the Mediterranean one (2010).
        Assert.Contains("Region Mediterranean", text);
        var tool = await _client.GetStringAsync($"/species/{FixtureDb.RegionalOnly}/wikitext");
        Assert.Contains("Wikitext for the Mediterranean assessment: Data Deficient, published 2010.", Html.Text(tool));
        Assert.Contains("<section class=\"regional\" id=\"regional\"", html);
        Assert.DoesNotContain("Assessment history", text);
        // The table lists the latest assessment in each region; the 2006 Europe one is left out.
        Assert.Contains($"href=\"/species/{FixtureDb.RegionalOnly}/wikitext?assessment={FixtureDb.RegionalOnlyEurope}", tool);
        Assert.DoesNotContain(FixtureDb.RegionalOnlyEurope2006.ToString(), html);
    }


    [Fact]
    public async Task UnknownTaxonIs404WithTheTaxonText() {
        var response = await _client.GetAsync("/species/999999999");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var text = Html.Text(await response.Content.ReadAsStringAsync());
        Assert.Contains("No taxon with IUCN id 999999999", text);
        Assert.Contains("This id is not in IUCN Red List version 2026-1, and this site has no earlier assessments with this id. Check the id, or search for the taxon by name.", text);
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

public sealed class BaseUrlTests(BaseUrlSiteFactory factory) : IClassFixture<BaseUrlSiteFactory> {
    [Fact]
    public async Task CanonicalUsesTheConfiguredAddress() {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/species/{FixtureDb.PolarBear}");
        request.Headers.Host = "other.example:8080";
        var html = await (await factory.Client().SendAsync(request)).Content.ReadAsStringAsync();
        Assert.Contains($"<link rel=\"canonical\" href=\"https://species.example.org/species/{FixtureDb.PolarBear}\">", html);
    }
}
