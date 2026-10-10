using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests.SharedWikitext;

// The name in the title registered with Crossref for an assessment's DOI (RegisteredName): when it
// counts as naming the taxon differently, and the citation option that writes it in |title=. The
// 2008 assessment of Cebuella pygmaea (IUCN id 136926) has the DOI title "Cebuella pygmaea ssp.
// pygmaea: Rylands, A.B. & de la Torre, S."; IUCN's citation now gives "Cebuella pygmaea".
public class RegisteredNameTests {
    private static readonly IucnCitationParts Cebuella2008 = new() {
        TaxonId = 136926,
        AssessmentId = 4350391,
        Year = 2008,
        ScientificName = "Cebuella pygmaea",
        Authors = [new(CitationAuthorKind.Person, "Rylands, A.B.", "Rylands", "A.B.")],
        Doi = "10.2305/IUCN.UK.2008.RLTS.T136926A4350391.en",
        DoiSource = DoiSource.Resolved,
        RegisteredName = "Cebuella pygmaea ssp. pygmaea",
    };

    [Theory]
    [InlineData("Cebuella pygmaea ssp. pygmaea", "Cebuella pygmaea", "Cebuella pygmaea ssp. pygmaea")]
    [InlineData("Barbus kissiensis", "Enteromius kissiensis", "Barbus kissiensis")]
    // Only the rank marker, brackets or spacing differ: the same name.
    [InlineData("Apollonias barbujana subsp. ceballosi", "Apollonias barbujana ssp. ceballosi", null)]
    [InlineData("Megaptera novaeangliae (Oceania subpopulation)", "Megaptera novaeangliae Oceania subpopulation", null)]
    [InlineData("Ursus  maritimus", "Ursus maritimus", null)]
    // IUCN's internal names are never shown.
    [InlineData("Physella acuta_new", "Physella acuta", null)]
    [InlineData(null, "Ursus maritimus", null)]
    [InlineData("", "Ursus maritimus", null)]
    public void RegisteredNameDifferentFrom(string? registered, string current, string? expected) {
        var parts = Cebuella2008 with { RegisteredName = registered };
        Assert.Equal(expected, parts.RegisteredNameDifferentFrom(current));
    }

    [Fact]
    public void RegisteredNameDifferentFrom_DecodesEntitiesAndTags() {
        var parts = Cebuella2008 with { RegisteredName = "<i>Thalarctos maritimus</i>" };
        Assert.Equal("Thalarctos maritimus", parts.RegisteredNameDifferentFrom("Ursus maritimus"));
    }

    [Fact]
    public void Render_CurrentNameByDefault() {
        Assert.Contains("|title=''Cebuella pygmaea'' |volume=2008", CiteIucnRenderer.Render(Cebuella2008));
    }

    [Fact]
    public void Render_RegisteredNameInTitle() {
        var cite = CiteIucnRenderer.Render(Cebuella2008, new CiteIucnOptions { RegisteredNameInTitle = true });
        Assert.Contains("|title=''Cebuella pygmaea'' ssp. ''pygmaea'' |volume=2008", cite);
        // Everything else is as before.
        Assert.Contains("|article-number=e.T136926A4350391 |doi=10.2305/IUCN.UK.2008.RLTS.T136926A4350391.en", cite);
    }

    [Fact]
    public void Render_RegisteredNameInTitle_SameNameChangesNothing() {
        var same = Cebuella2008 with { RegisteredName = "Cebuella  pygmaea" };
        Assert.Equal(CiteIucnRenderer.Render(same), CiteIucnRenderer.Render(same, new CiteIucnOptions { RegisteredNameInTitle = true }));
    }

    [Fact]
    public void WithRegisteredNameInTitle_ClearsTheSubpopulationName() {
        var sousa = Cebuella2008 with {
            ScientificName = "Sousa chinensis Eastern Taiwan Strait subpopulation",
            SubpopulationName = "Eastern Taiwan Strait subpopulation",
            RegisteredName = "Sousa chinensis ssp. taiwanensis",
        };
        var swapped = sousa.WithRegisteredNameInTitle();
        Assert.Equal("Sousa chinensis ssp. taiwanensis", swapped.ScientificName);
        Assert.Null(swapped.SubpopulationName);
        var none = Cebuella2008 with { RegisteredName = null };
        Assert.Same(none, none.WithRegisteredNameInTitle());
    }

    [Fact]
    public void OtherWikipedias_French_LeavesOutTheAuthorityWithTheRegisteredName() {
        var facts = new AssessmentFacts("LC", false, false, "3.1", null, 2008, "(Spix, 1823)", "species");
        var french = OtherWikipedias.Find("fr")!;
        Assert.Contains("|''Cebuella pygmaea'' (Spix, 1823)", OtherWikipedias.Citation(french, Cebuella2008, facts, new CiteIucnOptions()));
        var registered = OtherWikipedias.Citation(french, Cebuella2008, facts, new CiteIucnOptions { RegisteredNameInTitle = true });
        Assert.Contains("|''Cebuella pygmaea'' ssp. ''pygmaea''", registered);
        Assert.DoesNotContain("Spix", registered);
    }
}
