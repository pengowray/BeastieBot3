using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Tests;

// The citations and taxobox lines for the Wikipedias other than English (OtherWikipedias). Each
// expected value follows the wiki's template source and a live article (see the evidence comment at
// the top of OtherWikipedias.cs).
public sealed class OtherWikipediasTests {
    private static readonly DateOnly Accessed = new(2026, 10, 9);

    private static readonly IucnCitationParts PolarBear = new() {
        TaxonId = 22823, AssessmentId = 14871490, Year = 2015, ScientificName = "Ursus maritimus",
        Authors = [
            new CitationAuthor(CitationAuthorKind.Person, "Wiig, Ø.", "Wiig", "Ø."),
            new CitationAuthor(CitationAuthorKind.Person, "Amstrup, S.", "Amstrup", "S."),
            new CitationAuthor(CitationAuthorKind.Person, "Thiemann, G.", "Thiemann", "G."),
        ],
        Doi = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en",
    };

    private static readonly AssessmentFacts PolarBearFacts = new("VU", false, false, "3.1", "A3c", 2015, "Phipps, 1774", "species");

    private static readonly CiteIucnOptions Options = new() { AccessDate = Accessed, WrapInRef = true, RefName = "iucn" };

    [Fact]
    public void German() => Assert.Equal(
        "<ref name=\"iucn\">{{IUCN |Year=2015 |ID=22823 |ScientificName=Ursus maritimus |AssessmentID=14871490 |YearAssessed=2015 "
        + "|Assessor=Ø. Wiig, S. Amstrup, G. Thiemann |Abruf=2026-10-09}}</ref>",
        OtherWikipedias.Citation("de", PolarBear, PolarBearFacts, Options));

    [Fact]
    public void French() {
        Assert.Equal("<ref name=\"iucn\">{{UICN|22823|''Ursus maritimus'' Phipps, 1774|consulté le=9 octobre 2026}}</ref>",
            OtherWikipedias.Citation("fr", PolarBear, PolarBearFacts, Options));
        Assert.Equal("{{Taxobox UICN | VU | A3c }}", OtherWikipedias.TaxoboxLines("fr", PolarBearFacts, 22823, null));
        // Possibly extinct is PE; LR/cd is CD; no criteria, no second parameter.
        Assert.Equal("{{Taxobox UICN | PE | A2abcd }}", OtherWikipedias.TaxoboxLines("fr", PolarBearFacts with { Category = "CR", PossiblyExtinct = true, Criteria = "A2abcd" }, 1, null));
        Assert.Equal("{{Taxobox UICN | CD }}", OtherWikipedias.TaxoboxLines("fr", PolarBearFacts with { Category = "LR/cd", CriteriaVersion = "2.3", Criteria = null }, 1, null));
    }

    [Fact]
    public void FrenchSubspeciesHasItsRank() =>
        Assert.Contains("|rang=sous-espèce|", OtherWikipedias.Citation("fr", PolarBear with { ScientificName = "Panthera leo ssp. persica" },
            PolarBearFacts with { Kind = "subspecies", Authority = null }, Options));

    [Fact]
    public void Spanish() {
        Assert.Equal("<ref name=\"iucn\">{{IUCN |título=Ursus maritimus |asesores=Wiig, Ø., Amstrup, S. & Thiemann, G. |año=2015 |edición=2015-4 "
            + "|consultado=9 de octubre de 2026}}</ref>",
            OtherWikipedias.Citation("es", PolarBear, PolarBearFacts, Options));
        Assert.Equal("| status = VU\n| status_system = IUCN3.1\n| status_ref = <ref/>", OtherWikipedias.TaxoboxLines("es", PolarBearFacts, 22823, "<ref/>"));
    }

    [Fact]
    public void Polish() {
        Assert.Equal("<ref name=\"iucn\">{{IUCN |id=22823 |nazwa=Ursus maritimus |autor=Ø. Wiig, S. Amstrup, G. Thiemann |iucn rok=2015 |wersja=2015-4 "
            + "|doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |data dostępu=2026-10-09}}</ref>",
            OtherWikipedias.Citation("pl", PolarBear, PolarBearFacts, Options));
        Assert.Equal("| status IUCN = VU\n| IUCN id = 22823", OtherWikipedias.TaxoboxLines("pl", PolarBearFacts, 22823, "<ref/>"));
        // The box has no possibly extinct or Lower Risk codes, and no NE.
        Assert.Equal("| status IUCN = CR\n| IUCN id = 1", OtherWikipedias.TaxoboxLines("pl", PolarBearFacts with { Category = "CR", PossiblyExtinct = true }, 1, null));
        Assert.Equal("| status IUCN = NT\n| IUCN id = 1", OtherWikipedias.TaxoboxLines("pl", PolarBearFacts with { Category = "LR/cd", CriteriaVersion = "2.3" }, 1, null));
        Assert.Equal("| IUCN id = 1", OtherWikipedias.TaxoboxLines("pl", PolarBearFacts with { Category = "NE" }, 1, null));
    }

    [Fact]
    public void Portuguese() {
        Assert.Equal("<ref name=\"iucn\">{{citar iucn |author=Wiig, Ø. |author2=Amstrup, S. |author3=Thiemann, G. |year=2015 |title=''Ursus maritimus'' "
            + "|volume=2015 |page=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=2026-10-09}}</ref>",
            OtherWikipedias.Citation("pt", PolarBear, PolarBearFacts, Options));
        Assert.Equal("| estado = VU\n| sistema_estado = iucn3.1\n| estado_ref = <ref/>", OtherWikipedias.TaxoboxLines("pt", PolarBearFacts, 22823, "<ref/>"));
        // Módulo:Iucn accepts only DOIs ending .en.
        Assert.DoesNotContain("doi=", OtherWikipedias.Citation("pt", PolarBear with { Doi = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.es" }, PolarBearFacts, Options));
    }

    [Fact]
    public void Ukrainian() {
        Assert.Equal("<ref name=\"iucn\">{{Cite IUCN |author=Wiig, Ø. |author2=Amstrup, S. |author3=Thiemann, G. |year=2015 |title=''Ursus maritimus'' "
            + "|volume=2015 |page=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=2026-10-09}}</ref>",
            OtherWikipedias.Citation("uk", PolarBear, PolarBearFacts, Options));
        Assert.Equal("| status = VU\n| status_system = IUCN3.1", OtherWikipedias.TaxoboxLines("uk", PolarBearFacts, 22823, null));
    }

    [Fact]
    public void Japanese() {
        Assert.Equal("<ref name=\"iucn\">{{cite iucn |author=Wiig, Ø. |author2=Amstrup, S. |author3=Thiemann, G. |year=2015 |title=''Ursus maritimus'' "
            + "|volume=2015 |article-number=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=9 October 2026}}</ref>",
            OtherWikipedias.Citation("ja", PolarBear, PolarBearFacts, Options));
        Assert.Equal("| status = VU\n| status_ref = <ref/>", OtherWikipedias.TaxoboxLines("ja", PolarBearFacts, 22823, "<ref/>"));
        // The 1994 categories carry the version in the code; PE has none.
        Assert.Equal("| status = EN2.3", OtherWikipedias.TaxoboxLines("ja", PolarBearFacts with { Category = "EN", CriteriaVersion = "2.3" }, 1, null));
        Assert.Equal("| status = LR/nt", OtherWikipedias.TaxoboxLines("ja", PolarBearFacts with { Category = "LR/nt", CriteriaVersion = "2.3" }, 1, null));
    }

    [Fact]
    public void Chinese() {
        var errata = PolarBear with { TaxonId = 15951, AssessmentId = 115130419, Year = 2016, ErrataYear = 2017, ScientificName = "Panthera leo",
            Doi = "10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en" };
        Assert.Equal("<ref name=\"iucn\">{{IUCN |author1=Wiig, Ø. |author2=Amstrup, S. |author3=Thiemann, G. |year=2016 |id=15951 |title=''Panthera leo'' "
            + "|errata=errata version published in 2017 |page=e.T15951A115130419 |doi=10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en "
            + "|access-date=2026-10-09}}</ref>",
            OtherWikipedias.Citation("zh", errata, PolarBearFacts, Options));
        Assert.Equal("| status = VU\n| status_system = IUCN3.1", OtherWikipedias.TaxoboxLines("zh", PolarBearFacts, 22823, null));
    }

    [Fact]
    public void GermanTaxoboxesHaveNoStatus() => Assert.Null(OtherWikipedias.TaxoboxLines("de", PolarBearFacts, 22823, null));

    [Theory]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", "2015-4")]
    [InlineData("10.2305/iucn.uk.2011-2.rlts.t34926a9898196.en", "2011-2")]
    [InlineData(null, null)]
    public void RedListVersionComesFromTheDoi(string? doi, string? version) => Assert.Equal(version, OtherWikipedias.RedListVersion(doi));
}

public sealed class OtherWikipediasPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task TheWikitextPageWritesForTheChosenWikipedia() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?wiki=fr");
        Assert.Contains("<h2 id=\"wikitext-heading\">Wikitext for French Wikipedia</h2>", html);
        Assert.StartsWith("<ref name=\"iucn\">{{UICN|22823|''Ursus maritimus'' Phipps, 1774|consulté le=", Html.Textarea(html, "wikitext-cite"));
        Assert.Equal("{{Taxobox UICN | VU | A3c }}", Html.Textarea(html, "wikitext-speciesbox"));
        // English-only parts and options are left out.
        Assert.Null(Html.Textarea(html, "wikitext-status"));
        Assert.DoesNotContain("name=\"authors\" value=\"lastfirst\" checked", html);
        Assert.Contains("<input type=\"hidden\" name=\"wiki\" value=\"fr\">", html);
        Assert.Contains("{{UICN}} always links to the taxon&#x27;s current assessment", html);
        // English Wikipedia's taxobox check and the {{cite iucn}} DOI note belong to English only.
        Assert.DoesNotContain("Status in the Wikipedia taxobox", html);
        Assert.DoesNotContain("works without a DOI", html);
        // The row of Wikipedias: French is the current one; English links back with the options.
        Assert.Contains("<span aria-current=\"page\" lang=\"fr\">Français</span>", html);
        Assert.Contains($"href=\"/species/{FixtureDb.PolarBear}/wikitext#wikitext\" data-options-link=\"wiki-en\"", html);
    }

    [Fact]
    public async Task GermanHasNoTaxoboxLines() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?wiki=de");
        Assert.StartsWith("<ref name=\"iucn\">{{IUCN |Year=2015 |ID=22823 |ScientificName=Ursus maritimus |AssessmentID=14871490", Html.Textarea(html, "wikitext-cite"));
        Assert.Null(Html.Textarea(html, "wikitext-speciesbox"));
        Assert.Contains("German Wikipedia&#x27;s taxoboxes have no conservation status", html);
    }

    [Fact]
    public async Task TheWikiChoiceStaysWithTheOtherOptions() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?wiki=pl&access=none");
        Assert.Contains($"href=\"/species/{FixtureDb.PolarBear}/wikitext?assessment={FixtureDb.PolarBear2008}&amp;access=none&amp;wiki=pl#wikitext\"", html);
        Assert.Contains("| status IUCN = VU\n| IUCN id = 22823", Html.Textarea(html, "wikitext-speciesbox"));
        // An unknown wiki is English.
        Assert.Contains("Wikitext for English Wikipedia", await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}/wikitext?wiki=xx"));
    }
}
