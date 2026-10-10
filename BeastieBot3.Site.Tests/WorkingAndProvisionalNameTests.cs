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

    [Fact]
    public async Task ASpeciesCodeIsFoundBySearchAndLeftOutOfTheNames() {
        // The only exact match: the search goes to the taxon page, which says how the text names it.
        var search = await _client.GetAsync("/search?q=Po");
        Assert.Equal(System.Net.HttpStatusCode.Redirect, search.StatusCode);
        Assert.Equal($"/species/{FixtureDb.Posidonia}?q=Po", search.Headers.Location?.OriginalString);
        var html = await Page(FixtureDb.Posidonia, "?q=Po");
        Assert.Contains(BeastieBot3.Site.Display.SiteText.ArrivedCode("Po"), Html.Text(html));
        Assert.Contains("<i>Posidonia oceanica</i>", html);
        Assert.DoesNotContain(">Po<", Section(html, "names"));

        // Listed with all results, with the match note.
        var all = await _client.GetStringAsync("/search?q=Po&all=1");
        Assert.Contains(BeastieBot3.Site.Display.SiteText.MatchCodeLabel, Html.Text(all));
    }
}
