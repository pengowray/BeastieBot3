using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins {{cite iucn}} with CiteIucnOptions.FullGivenNames: a person whose given names value[] gave is
// named by them, in both author styles; everyone else keeps IUCN's form. The salmon and Lowry
// outputs were checked against en.wikipedia.org's parser (no CS1 error or maintenance category).
public class CiteIucnFullGivenNamesTests {
    private static CitationAuthor Person(string last, string initials, string? given) =>
        new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials, given);

    // Sayer, C. & Lajus, D. 2023. Salmo salar Kola subpopulation.
    private static readonly IucnCitationParts Salmon = new() {
        TaxonId = 196579964,
        AssessmentId = 196580951,
        Year = 2023,
        ScientificName = "Salmo salar Kola subpopulation",
        SubpopulationName = "Kola",
        Authors = [Person("Sayer", "C.", "Catherine"), Person("Lajus", "D.", "Dmitry")],
    };

    private static readonly CiteIucnOptions Full = new() { FullGivenNames = true };
    private static readonly CiteIucnOptions FullLastFirst = new() { FullGivenNames = true, AuthorStyle = CiteAuthorStyle.LastFirst };

    [Fact]
    public void AuthorN() {
        Assert.Equal(
            "{{cite iucn |author=Sayer, Catherine |author2=Lajus, Dmitry |year=2023 |title=''Salmo salar'' Kola subpopulation " +
            "|volume=2023 |article-number=e.T196579964A196580951}}",
            CiteIucnRenderer.Render(Salmon, Full));
    }

    [Fact]
    public void LastFirst() {
        Assert.Equal(
            "{{cite iucn |last1=Sayer |first1=Catherine |last2=Lajus |first2=Dmitry |year=2023 |title=''Salmo salar'' Kola subpopulation " +
            "|volume=2023 |article-number=e.T196579964A196580951}}",
            CiteIucnRenderer.Render(Salmon, FullLastFirst));
    }

    [Fact]
    public void WithoutTheOption_InitialsAsIucnPrintsThem() {
        Assert.StartsWith("{{cite iucn |author=Sayer, C. |author2=Lajus, D. |year=2023", CiteIucnRenderer.Render(Salmon));
        Assert.StartsWith("{{cite iucn |last1=Sayer |first1=C. |last2=Lajus |first2=D. |year=2023",
            CiteIucnRenderer.Render(Salmon, new CiteIucnOptions { AuthorStyle = CiteAuthorStyle.LastFirst }));
    }

    [Fact]
    public void AuthorsWithoutGivenNames_KeepTheirInitials_OrganisationsStayWhole() {
        var parts = Salmon with {
            Authors = [
                Person("Sayer", "C.", "Catherine"),
                Person("Liddle", "T.A.", null),
                new CitationAuthor(CitationAuthorKind.Organisation, "BirdLife International"),
            ],
        };
        Assert.StartsWith("{{cite iucn |author=Sayer, Catherine |author2=Liddle, T.A. |author3=BirdLife International |year=",
            CiteIucnRenderer.Render(parts, Full));
        Assert.StartsWith("{{cite iucn |last1=Sayer |first1=Catherine |last2=Liddle |first2=T.A. |author3=BirdLife International |year=",
            CiteIucnRenderer.Render(parts, FullLastFirst));
    }

    [Fact]
    public void GenerationalSuffix_FollowsTheGivenNames() {
        // IUCN prints "Lowry II, P.P."; the parser stores Initials "P.P., II".
        var parts = Salmon with { Authors = [new CitationAuthor(CitationAuthorKind.Person, "Lowry II, P.P.", "Lowry", "P.P., II", "Porter P.")] };
        Assert.StartsWith("{{cite iucn |author=Lowry, Porter P. II |year=", CiteIucnRenderer.Render(parts, Full));
        Assert.StartsWith("{{cite iucn |last1=Lowry |first1=Porter P. II |year=", CiteIucnRenderer.Render(parts, FullLastFirst));
    }

    [Fact]
    public void InitialsFirstName_BecomesSurnameFirst() {
        var parts = Salmon with {
            Authors = [new CitationAuthor(CitationAuthorKind.Person, "N.H. Rakotoarivelo", "Rakotoarivelo", "N.H.", "Nirina Hasina")],
        };
        Assert.StartsWith("{{cite iucn |author=Rakotoarivelo, Nirina Hasina |year=", CiteIucnRenderer.Render(parts, Full));
    }

    [Fact]
    public void HyphenatedGivenNamesAndParticleSurnames() {
        var parts = Salmon with {
            Authors = [Person("Veillon", "J.-M.", "Jean-Marie"), Person("van Swaay", "C.", "Chris"), Person("Paulson", "D.R.", "Dennis R.")],
        };
        Assert.StartsWith("{{cite iucn |author=Veillon, Jean-Marie |author2=van Swaay, Chris |author3=Paulson, Dennis R. |year=",
            CiteIucnRenderer.Render(parts, Full));
    }

    [Fact]
    public void GivenNamesOnAnOrganisationOrVerbatimName_AreIgnored() {
        var parts = Salmon with {
            Authors = [
                new CitationAuthor(CitationAuthorKind.Organisation, "Royal Botanic Gardens, Kew", GivenNames: "Kew"),
                new CitationAuthor(CitationAuthorKind.Verbatim, "Neil Cox", GivenNames: "Neil"),
            ],
        };
        Assert.Equal(CiteIucnRenderer.Render(parts), CiteIucnRenderer.Render(parts, Full));
    }

    [Fact]
    public void GivenNamesCannotBreakTheTemplate() {
        var parts = Salmon with { Authors = [Person("Sayer", "C.", "Cath|erine}} {{x")] };
        // The entity's semicolon makes CS1 read several names, so the name is taken as written.
        Assert.StartsWith("{{cite iucn |author=((Sayer, Cath&#124;erine x)) |year=", CiteIucnRenderer.Render(parts, Full));
    }

    [Fact]
    public void GivenNames_SurviveTheJsonRoundTrip() {
        var back = IucnCitationParts.FromJson(Salmon.ToJson())!;
        Assert.Equal(Salmon.Authors, back.Authors);
        // No GivenNames property is written for an author without one.
        Assert.DoesNotContain("GivenNames", (Salmon with { Authors = [Person("Liddle", "T.A.", null)] }).ToJson());
    }
}
