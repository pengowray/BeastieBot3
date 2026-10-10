namespace BeastieBot3.Site.Tests;

// The pages of taxa that IUCN has under two names: a "_new" record made for a national assessment
// (Balaenoptera edeni_new beside Balaenoptera edeni) and a provisional name beside the taxon named
// with its quoted epithet (Notogomphus sp. nov. 'gorilla' and Notogomphus gorilla); and IUCN's
// species codes, which search finds and the names section leaves out.
public sealed class WorkingAndProvisionalNameTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private Task<string> Page(long taxonId, string query = "") => _client.GetStringAsync($"/species/{taxonId}{query}");

    private static string Section(string html, string cssClass) {
        var start = html.IndexOf($"<section class=\"{cssClass}\"", StringComparison.Ordinal);
        Assert.True(start >= 0, $"no section {cssClass}");
        return html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
    }

    [Fact]
    public async Task TheTaxonListsTheAssessmentOfItsNewRecordInTheRegionalTable() {
        var html = await Page(FixtureDb.Brydes);
        var regional = Section(html, "regional");

        Assert.Contains("<h2 id=\"regional-heading\">Assessments with no geographic scope</h2>", regional);
        Assert.Contains("No scope given", regional);
        Assert.Contains($"<a href=\"/species/{FixtureDb.BrydesNew}\">IUCN id {FixtureDb.BrydesNew}</a>", regional);
        Assert.Contains("<i>Balaenoptera edeni_new</i>", regional);
        Assert.Contains($"/species/{FixtureDb.BrydesNew}/{FixtureDb.BrydesNewUae}", regional);
        // The taxon's own global assessment stays the status, and the record is not an earlier id.
        Assert.Contains(">Latest global assessment</h2>", html);
        Assert.DoesNotContain("earlier-id", html);
        Assert.DoesNotContain("Combined assessment history", html);
    }

    [Fact]
    public async Task OnTheCitationsPageTheNewRecordsRowLinksItsOwnCitationsPage() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Brydes}/wikitext");
        var regional = Section(html, "regional");
        Assert.Contains($"href=\"/species/{FixtureDb.BrydesNew}/wikitext?assessment={FixtureDb.BrydesNewUae}", regional);
    }

    // The guess from rules/iucn-probable-scopes.yml is under "No scope given", with its evidence in
    // the help, in the status summary and in the table; an assessment the file does not cover has none.
    [Fact]
    public async Task AnAssessmentWithNoScopeShowsItsProbableScope() {
        var html = await Page(FixtureDb.BrydesNew);
        Assert.Equal(2, CountOf(html, "<div class=\"probable-scope\">Probable scope: United Arab Emirates (national assessment)</div>"));
        Assert.Contains("The probable scope is this site's guess, checked by hand. Evidence: The citation credits the UAE National Red List Workshop.", Html.Text(html));
        // On the taxon's page too, in the row of the "_new" record.
        Assert.Contains("Probable scope: United Arab Emirates", Section(await Page(FixtureDb.Brydes), "regional"));
        Assert.DoesNotContain("probable-scope", await Page(FixtureDb.NoScopeOnly));
    }

    private static int CountOf(string text, string part) {
        var count = 0;
        for (var at = text.IndexOf(part, StringComparison.Ordinal); at >= 0; at = text.IndexOf(part, at + part.Length, StringComparison.Ordinal)) {
            count++;
        }
        return count;
    }

    [Fact]
    public async Task TheNewRecordSaysWhichTaxonItIsARecordOf() {
        var html = await Page(FixtureDb.BrydesNew);
        var note = html[html.IndexOf("working-name-note", StringComparison.Ordinal)..];
        note = note[..note.IndexOf("</p>", StringComparison.Ordinal)];

        Assert.Contains($"<a href=\"/species/{FixtureDb.Brydes}\">", note);
        Assert.Contains($"(IUCN id {FixtureDb.Brydes})", note);
        Assert.Contains("IUCN made this second record for an assessment published with no geographic scope.", Html.Text(note));
        // No regional table row of its own record under "Published as".
        Assert.DoesNotContain("published-under", html);
    }

    [Fact]
    public async Task ANewRecordWithNoIucnTaxonOfItsNameLinksTheSpeciesFromTheCatalogueOfLife() {
        var html = await Page(FixtureDb.AquilegiaNew);
        var note = html[html.IndexOf("working-name-note", StringComparison.Ordinal)..];
        note = note[..note.IndexOf("</p>", StringComparison.Ordinal)];
        Assert.Contains("<a href=\"/col/FZTZ\" class=\"sci-name\"><i>Aquilegia ottonis</i></a> (Catalogue of Life, Wikidata).", note);
        Assert.Contains("IUCN has no record of Aquilegia ottonis other than this one.", Html.Text(note));
        // The pair is not listed again with the possible duplicates.
        Assert.DoesNotContain("id=\"duplicates\"", html);
    }

    [Fact]
    public async Task AProvisionalNameListsTheIucnTaxonNamedWithItsEpithetAsAPossibleSynonym() {
        var names = Section(await Page(FixtureDb.NotogomphusProvisional), "names");
        Assert.Contains(">Possible synonyms</h3>", names);
        Assert.Contains($"<a href=\"/species/{FixtureDb.NotogomphusGorilla}\" class=\"sci-name\"><i>Notogomphus gorilla</i></a>", names);
        Assert.Contains($"IUCN Red List (IUCN id {FixtureDb.NotogomphusGorilla})", names);

        // And the other way.
        var described = Section(await Page(FixtureDb.NotogomphusGorilla), "names");
        Assert.Contains($"<a href=\"/species/{FixtureDb.NotogomphusProvisional}\" class=\"sci-name\">", described);
        Assert.Contains($"IUCN Red List (IUCN id {FixtureDb.NotogomphusProvisional})", described);
    }

    [Fact]
    public async Task AProvisionalNameListsTheWikidataSpeciesNamedWithItsEpithet() {
        var html = await Page(FixtureDb.NotogomphusLateralisProvisional);
        var names = Section(html, "names");
        Assert.Contains("<a href=\"/wikidata/Q10337705\" class=\"sci-name\"><i>Notogomphus lateralis</i></a>", names);
        Assert.Contains("<td>Wikidata</td>", names);
        Assert.DoesNotContain("id=\"duplicates\"", html);
        // The provisional note points to them.
        var header = html[html.IndexOf("class=\"taxon-header\"", StringComparison.Ordinal)..];
        Assert.Contains("Possible synonyms", Html.Text(header[..header.IndexOf("</header>", StringComparison.Ordinal)]));
    }

    // A code is found after every name and never opens a taxon by itself: "crow" lists the eagle
    // whose English name starts with it before the owl whose bird code it is. "bird code: CROW"
    // (and the like) searches codes only and goes straight to the taxon.
    [Fact]
    public async Task ACodeIsFoundAfterEveryNameUnlessTheSearchSaysItIsACode() {
        var crow = await _client.GetAsync("/search?q=crow");
        Assert.Equal(System.Net.HttpStatusCode.OK, crow.StatusCode);
        var list = await crow.Content.ReadAsStringAsync();
        var eagle = list.IndexOf($"/species/{FixtureDb.CrownedEagle}\"", StringComparison.Ordinal);
        var owl = list.IndexOf($"/species/{FixtureDb.CrestedOwl}\"", StringComparison.Ordinal);
        Assert.True(eagle >= 0 && owl > eagle, $"eagle {eagle}, owl {owl}");
        Assert.Contains(BeastieBot3.Site.Display.SiteText.MatchCodeLabel + " CROW", Html.Text(list));

        foreach (var query in new[] { "bird code: CROW", "bird code crow", "code: CROW", "Code CROW" }) {
            var found = await _client.GetAsync("/search?q=" + Uri.EscapeDataString(query));
            Assert.Equal(System.Net.HttpStatusCode.Redirect, found.StatusCode);
            Assert.StartsWith($"/species/{FixtureDb.CrestedOwl}?q=", found.Headers.Location?.OriginalString);
        }
        var page = await Page(FixtureDb.CrestedOwl, "?q=" + Uri.EscapeDataString("bird code: crow"));
        Assert.Contains("“CROW” is a code that the Catalogue of Life lists among this taxon's English names.", Html.Text(page));
        Assert.DoesNotContain(">CROW<", Section(page, "names"));
    }

    [Fact]
    public async Task AnIucnSpeciesCodeIsFoundTheSameWay() {
        var search = await _client.GetAsync("/search?q=" + Uri.EscapeDataString("Species code: Po"));
        Assert.Equal(System.Net.HttpStatusCode.Redirect, search.StatusCode);
        Assert.StartsWith($"/species/{FixtureDb.Posidonia}?q=", search.Headers.Location?.OriginalString);
        var html = await Page(FixtureDb.Posidonia, "?q=" + Uri.EscapeDataString("Species code: Po"));
        Assert.Contains("“Po” is a species code that IUCN lists for this taxon.", Html.Text(html));
        Assert.DoesNotContain(">Po<", Section(html, "names"));
    }

    // A name that a source lists as a synonym is not repeated as a possible synonym, and the pair
    // is not listed with the possible duplicates either.
    [Fact]
    public async Task AFormalNameThatIsListedAsASynonymIsNotRepeated() {
        var html = await Page(FixtureDb.HeptapleurumProvisional);
        var names = Section(html, "names");
        Assert.Contains("Heptapleurum nanocephalum", names);
        Assert.DoesNotContain("Possible synonyms", names);
        Assert.DoesNotContain("id=\"duplicates\"", html);
        Assert.Contains("its published name may be in the Synonyms table.", Html.Text(html));
    }
}
