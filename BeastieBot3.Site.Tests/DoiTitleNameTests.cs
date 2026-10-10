namespace BeastieBot3.Site.Tests;

/// The name in the title registered for an assessment's DOI (IucnCitationParts.RegisteredName) when
/// it differs from the taxon's current name: under the row in the assessment tables, and in the
/// citation title on the citations page, with the current name as an option. In the fixture, the
/// polar bear's 2008 assessment has the DOI title name "Thalarctos maritimus".
public sealed class DoiTitleNameTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private const string Legend = "<legend>Name in the citation title</legend>";
    private const string Note = "Name in DOI title: the scientific name in the title registered with Crossref for the assessment's DOI.";

    [Fact]
    public async Task HistoryTable_ShowsTheNameUnderTheRowWhoseDoiTitleDiffers() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        var rows = Html.TableRows(Html.Between(html, "id=\"history-heading\"", "</table>"));
        Assert.Contains(rows, r => r[0] == "2008 Name in DOI title: Thalarctos maritimus");
        Assert.Contains(rows, r => r[0] == "2015 Latest");
        Assert.Contains("<span class=\"doi-title-name\">Name in DOI title: <span class=\"sci-name\"><i>Thalarctos maritimus</i></span></span>", html);
        Assert.Contains(Note, Html.Text(html));
        // One note under the table.
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(html, "doi-title-name-note"));
    }

    [Fact]
    public async Task HistoryTable_NoNoteWhenNoDoiTitleDiffers() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.DoesNotContain("doi-title-name", html);
        Assert.DoesNotContain("Name in DOI title", html);
    }

    [Fact]
    public async Task Citation_UsesTheDoiTitleNameByDefault_AndOffersTheCurrentName() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}");
        Assert.Contains("|title=''Thalarctos maritimus'' |volume=2008", Html.Textarea(html, "wikitext-cite"));
        Assert.Contains(Legend, html);
        Assert.Contains("<input type=\"radio\" name=\"titlename\" value=\"doi\" checked=\"checked\" aria-describedby=\"titlename-help\"> Name in DOI title (<span class=\"sci-name\"><i>Thalarctos maritimus</i></span>)</label>", html);
        Assert.Contains("<input type=\"radio\" name=\"titlename\" value=\"current\" aria-describedby=\"titlename-help\"> Current name (<span class=\"sci-name\"><i>Ursus maritimus</i></span>)</label>", html);
        Assert.Contains("Current name: the name IUCN's own citations now use for every assessment, including older assessments.", Html.Text(html));

        var current = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}&titlename=current");
        Assert.Contains("|title=''Ursus maritimus'' |volume=2008", Html.Textarea(current, "wikitext-cite"));
        Assert.Contains("<input type=\"radio\" name=\"titlename\" value=\"current\" checked=\"checked\"", current);
    }

    [Fact]
    public async Task Citation_NoOptionWhenTheNamesAgree_ButTheChoiceIsKept() {
        var latest = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext");
        Assert.DoesNotContain(Legend, latest);
        Assert.DoesNotContain("name=\"titlename\"", latest);
        Assert.Contains("|title=''Ursus maritimus''", Html.Textarea(latest, "wikitext-cite"));

        var kept = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?titlename=current");
        Assert.DoesNotContain(Legend, kept);
        Assert.Contains("<input type=\"hidden\" name=\"titlename\" value=\"current\">", kept);
        Assert.Contains($"href=\"/species/22823/wikitext?assessment={FixtureDb.PolarBear2008}&amp;titlename=current#wikitext\"", kept);
    }

    [Fact]
    public async Task ReferencePageAddressWithTheOption_GoesToTheCitationsPage() {
        var response = await _client.GetAsync($"/species/{FixtureDb.PolarBear}?titlename=current");
        Assert.Equal(System.Net.HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal($"/species/{FixtureDb.PolarBear}/wikitext?titlename=current", response.Headers.Location?.OriginalString);
    }

    [Fact]
    public async Task OtherWikipedias_UseTheDoiTitleNameToo() {
        var zh = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}&wiki=zh");
        Assert.Contains("|title=''Thalarctos maritimus''", Html.Textarea(zh, "wikitext-cite"));
        Assert.Contains(Legend, zh);
        var zhCurrent = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}&wiki=zh&titlename=current");
        Assert.Contains("|title=''Ursus maritimus''", Html.Textarea(zhCurrent, "wikitext-cite"));
    }
}

/// IUCN synonyms that are the scientific name of another taxon in the release, in the Names section.
/// In the fixture, Platanista minor lists Platanista gangetica as a synonym, and Platanista gangetica
/// is a taxon in the release.
public sealed class SynonymTaxaTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task SynonymThatIsAnotherTaxonsName_LinksToIt() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Minor}");
        var table = Html.Between(html, "<table class=\"names-table synonyms-table\">", "</table>");
        Assert.Contains($"<div class=\"match-note\">Also the scientific name of <a href=\"/species/{FixtureDb.Gangetica}\">IUCN id {FixtureDb.Gangetica}</a></div>", table);
        Assert.DoesNotContain("listed-as-synonym", html);
    }

    [Fact]
    public async Task TaxonWhoseNameIsAnotherTaxonsSynonym_SaysSo() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Gangetica}");
        var names = Html.Between(html, "<h2 id=\"names-heading\">", "</section>");
        Assert.Contains("<h3>Synonyms</h3>", names);
        Assert.Contains($"<p class=\"names-intro listed-as-synonym\">IUCN lists <span class=\"sci-name\"><i>Platanista gangetica</i></span> as a synonym of <a href=\"/species/{FixtureDb.Minor}\" class=\"sci-name\"><i>Platanista minor</i></a>.</p>", names);
    }

    [Fact]
    public async Task OldIdsPage_HasNoSynonymLinks() {
        // An old id's links are in its combined history.
        var html = await _client.GetStringAsync($"/species/{FixtureDb.GangeticaOld}");
        Assert.DoesNotContain("listed-as-synonym", html);
    }
}

public sealed class RankMarkerKeysTests {
    [Fact]
    public void Trinomial_GetsTheMarkedForms() {
        Assert.Equal(["sarotherodon tournieri liberiensis", "sarotherodon tournieri ssp. liberiensis",
            "sarotherodon tournieri subsp. liberiensis", "sarotherodon tournieri var. liberiensis"],
            Data.RankMarkerKeys.For("Sarotherodon tournieri liberiensis"));
    }

    [Fact]
    public void MarkedName_GetsTheUnmarkedAndOtherMarkedForms() {
        Assert.Equal(["cebuella pygmaea ssp. pygmaea", "cebuella pygmaea pygmaea", "cebuella pygmaea subsp. pygmaea", "cebuella pygmaea var. pygmaea"],
            Data.RankMarkerKeys.For("Cebuella pygmaea ssp. pygmaea"));
    }

    [Theory]
    [InlineData("Ursus maritimus")]
    [InlineData("Acipenser nudiventris Caspian Sea subpopulation")]
    [InlineData("Notogomphus sp. nov. 'gorilla'")]
    public void OtherNames_AreTheirOwnKey(string name) {
        Assert.Equal([BeastieBot3.Shared.SiteData.SiteNameKey.Fold(name)], Data.RankMarkerKeys.For(name));
    }
}

/// "Named in IUCN's taxonomic notes" (notes_taxon). In the fixture, Platanista gangetica's notes name
/// Platanista minor as "Platanista gangetica minor", and Platanista minor's name Platanista gangetica.
public sealed class NotesTaxaPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task ListsTheNamedTaxa_WithTheNameTheNotesUse() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Gangetica}");
        var section = Html.Between(html, "<h3 id=\"notes-taxa\">", "</section>");
        Assert.Contains("Named in IUCN's taxonomic notes", Html.Text(html));
        Assert.Contains($"Found by searching the taxonomic notes of <a href=\"https://www.iucnredlist.org/species/{FixtureDb.Gangetica}/{FixtureDb.GangeticaLatest}\">the 2022 global assessment</a>"
            + " for the scientific names and IUCN synonyms of other taxa in Red List version 2026-1.", section);
        Assert.Contains($"<a href=\"/species/{FixtureDb.Minor}\"><i>Platanista minor</i></a>", section);
        Assert.Contains("<div class=\"match-note\">Name in the notes: <i>Platanista gangetica minor</i></div>", section);
    }

    [Fact]
    public async Task NoNameNote_WhenTheNotesUseTheTaxonsOwnName() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Minor}");
        var section = Html.Between(html, "<h3 id=\"notes-taxa\">", "</section>");
        Assert.Contains($"<a href=\"/species/{FixtureDb.Gangetica}\"><i>Platanista gangetica</i></a>", section);
        Assert.DoesNotContain("Name in the notes", section);
    }

    [Fact]
    public async Task NoSection_WhenTheNotesNameNoOtherTaxon() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        Assert.DoesNotContain("notes-taxa", html);
        Assert.DoesNotContain("Named in IUCN's taxonomic notes", html);
    }
}
