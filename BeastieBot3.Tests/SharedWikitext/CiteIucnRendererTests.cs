using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins the {{cite iucn}} output: parameter order, author styles, the errata/amends years, the DOI
// rules Module:Cite IUCN enforces, and that no value can break the template. The fixtures are real
// assessments (polar bear, giant panda, house sparrow, European rabbit); their rendered output was
// checked against en.wikipedia.org's parser when this was written.
public class CiteIucnRendererTests {
    private static CitationAuthor Person(string last, string initials) => new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials);
    private static CitationAuthor Org(string name) => new(CitationAuthorKind.Organisation, name);
    private static CitationAuthor Verbatim(string name) => new(CitationAuthorKind.Verbatim, name);

    private static readonly IucnCitationParts PolarBear = new() {
        TaxonId = 22823,
        AssessmentId = 14871490,
        Year = 2015,
        ScientificName = "Ursus maritimus",
        Authors = [
            Person("Wiig", "Ø."), Person("Amstrup", "S."), Person("Atwood", "T."), Person("Laidre", "K."),
            Person("Lunn", "N."), Person("Obbard", "M."), Person("Regehr", "E."), Person("Thiemann", "G."),
        ],
        Doi = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en",
        DoiSource = DoiSource.Gbif,
    };

    private static readonly IucnCitationParts GiantPanda = new() {
        TaxonId = 712,
        AssessmentId = 121745669,
        Year = 2016,
        ErrataYear = 2017,
        ScientificName = "Ailuropoda melanoleuca",
        Authors = [Person("Swaisgood", "R."), Person("Wang", "D."), Person("Wei", "F.")],
        Doi = "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en",
        DoiSource = DoiSource.Citation,
    };

    private static readonly IucnCitationParts HouseSparrow = new() {
        TaxonId = 103818789,
        AssessmentId = 155522130,
        Year = 2019,
        AmendsYear = 2018,
        ScientificName = "Passer domesticus",
        Authors = [Org("BirdLife International")],
        Doi = "10.2305/IUCN.UK.2018-2.RLTS.T103818789A155522130.en",
        DoiSource = DoiSource.Citation,
    };

    private static readonly IucnCitationParts EuropeanRabbit = new() {
        TaxonId = 41291,
        AssessmentId = 217911810,
        Year = 2025,
        ScientificName = "Oryctolagus cuniculus",
        RegionalScope = "Europe",
        Authors = [Person("Hackländer", "K.")],
        Doi = "10.2305/IUCN.UK.2025-1.RLTS.T41291A217911810.en",
    };

    [Fact]
    public void PersonList_AuthorN() {
        Assert.Equal(
            "{{cite iucn |author=Wiig, Ø. |author2=Amstrup, S. |author3=Atwood, T. |author4=Laidre, K. |author5=Lunn, N. " +
            "|author6=Obbard, M. |author7=Regehr, E. |author8=Thiemann, G. |year=2015 |title=''Ursus maritimus'' |volume=2015 " +
            "|article-number=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en}}",
            CiteIucnRenderer.Render(PolarBear));
    }

    [Fact]
    public void PersonList_LastFirst() {
        var parts = PolarBear with { Authors = [Person("Wiig", "Ø."), Org("Polar Bear Specialist Group"), Person("Atwood", "T.")] };
        Assert.Equal(
            "{{cite iucn |last1=Wiig |first1=Ø. |author2=Polar Bear Specialist Group |last3=Atwood |first3=T. |year=2015 " +
            "|title=''Ursus maritimus'' |volume=2015 |article-number=e.T22823A14871490 " +
            "|doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en}}",
            CiteIucnRenderer.Render(parts, new CiteIucnOptions { AuthorStyle = CiteAuthorStyle.LastFirst }));
    }

    // CS1 allows no comma in |firstN=: "P.P., II" adds "CS1 maint: multiple names". Lowry II (aid
    // 2006234) and Golamco Jr. (aid 11046733) parse clean on en.wikipedia with the comma left out.
    [Theory]
    [InlineData("P.P., II", "P.P. II")]
    [InlineData("A., Jr.", "A. Jr.")]
    [InlineData("W.B., III", "W.B. III")]
    [InlineData("R.L. , Sr", "R.L. Sr")]
    [InlineData("P.P.", "P.P.")]
    [InlineData("Nur Adillah", "Nur Adillah")]
    public void LastFirst_WritesAGenerationalSuffixWithoutTheComma(string initials, string first) {
        var parts = PolarBear with { Authors = [Person("Lowry", initials)] };
        Assert.StartsWith($"{{{{cite iucn |last1=Lowry |first1={first} |year=2015",
            CiteIucnRenderer.Render(parts, new CiteIucnOptions { AuthorStyle = CiteAuthorStyle.LastFirst }));
    }

    [Fact]
    public void AuthorN_KeepsTheSuffixAsIucnWritesIt() {
        var parts = PolarBear with { Authors = [new CitationAuthor(CitationAuthorKind.Person, "Lowry II, P.P.", "Lowry", "P.P., II")] };
        Assert.StartsWith("{{cite iucn |author=Lowry II, P.P. |year=2015", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void LastFirst_LeadingOrganisationIsAuthor1() {
        var output = CiteIucnRenderer.Render(HouseSparrow, new CiteIucnOptions { AuthorStyle = CiteAuthorStyle.LastFirst });
        Assert.StartsWith("{{cite iucn |author1=BirdLife International |year=2019", output);
    }

    [Fact]
    public void LastFirst_PersonWithoutLastFallsBackToDisplay() {
        var parts = PolarBear with { Authors = [new CitationAuthor(CitationAuthorKind.Person, "Kadarusman")] };
        Assert.StartsWith("{{cite iucn |author1=Kadarusman |year=",
            CiteIucnRenderer.Render(parts, new CiteIucnOptions { AuthorStyle = CiteAuthorStyle.LastFirst }));
    }

    [Fact]
    public void Errata_KeepsThePredecessorDoi() {
        Assert.Equal(
            "{{cite iucn |author=Swaisgood, R. |author2=Wang, D. |author3=Wei, F. |year=2016 |errata=2017 " +
            "|title=''Ailuropoda melanoleuca'' |volume=2016 |article-number=e.T712A121745669 " +
            "|doi=10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en}}",
            CiteIucnRenderer.Render(GiantPanda));
    }

    [Fact]
    public void Amended_WritesAmends() {
        Assert.Equal(
            "{{cite iucn |author=BirdLife International |year=2019 |amends=2018 |title=''Passer domesticus'' |volume=2019 " +
            "|article-number=e.T103818789A155522130 |doi=10.2305/IUCN.UK.2018-2.RLTS.T103818789A155522130.en}}",
            CiteIucnRenderer.Render(HouseSparrow));
    }

    [Fact]
    public void Regional_AddsTheScopeToTheTitle() {
        Assert.Equal(
            "{{cite iucn |author=Hackländer, K. |year=2025 |title=''Oryctolagus cuniculus'' (Europe assessment) |volume=2025 " +
            "|article-number=e.T41291A217911810 |doi=10.2305/IUCN.UK.2025-1.RLTS.T41291A217911810.en}}",
            CiteIucnRenderer.Render(EuropeanRabbit));
    }

    [Theory]
    [InlineData("Panthera pardus ssp. orientalis", null, "''Panthera pardus'' ssp. ''orientalis''")]
    [InlineData("Cupressus arizonica var. glabra", null, "''Cupressus arizonica'' var. ''glabra''")]
    [InlineData("Panthera leo West Africa subpopulation", "West Africa", "''Panthera leo'' West Africa subpopulation")]
    public void Title_UsesScientificNameMarkup(string name, string? subpopulation, string title) {
        var parts = PolarBear with { ScientificName = name, SubpopulationName = subpopulation, Doi = null };
        Assert.Contains($" |title={title} |volume=", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void Title_AnnotationsLeftInTheNameBecomeParameters() {
        var parts = GiantPanda with { ScientificName = "Ailuropoda melanoleuca (errata version published in 2017)", ErrataYear = null };
        Assert.Equal(CiteIucnRenderer.Render(GiantPanda), CiteIucnRenderer.Render(parts));

        var amended = HouseSparrow with { ScientificName = "Passer domesticus (amended version of 2018 assessment)", AmendsYear = null };
        Assert.Equal(CiteIucnRenderer.Render(HouseSparrow), CiteIucnRenderer.Render(amended));

        var regional = EuropeanRabbit with { ScientificName = "Oryctolagus cuniculus (Europe assessment)", RegionalScope = null };
        Assert.Equal(CiteIucnRenderer.Render(EuropeanRabbit), CiteIucnRenderer.Render(regional));

        var both = EuropeanRabbit with { ScientificName = "Oryctolagus cuniculus (Europe assessment)" };
        Assert.Contains("|title=''Oryctolagus cuniculus'' (Europe assessment) |volume=", CiteIucnRenderer.Render(both));
    }

    [Fact]
    public void ErrataAndAmends_OnlyErrataIsWritten() {
        // Module:Cite IUCN handles |amends= only when |errata= is absent; with both, CS1 would report
        // |amends= as an unknown parameter.
        var output = CiteIucnRenderer.Render(GiantPanda with { AmendsYear = 2015 });
        Assert.Contains("|errata=2017", output);
        Assert.DoesNotContain("amends", output);
    }

    [Fact]
    public void Doi_PredecessorIdWithoutErrataIsLeftOut() {
        var output = CiteIucnRenderer.Render(GiantPanda with { ErrataYear = null });
        Assert.DoesNotContain("doi", output);
        Assert.EndsWith("|article-number=e.T712A121745669}}", output);
    }

    [Fact]
    public void Doi_OtherTaxonIsLeftOutEvenForErrata() {
        var output = CiteIucnRenderer.Render(GiantPanda with { Doi = "10.2305/IUCN.UK.2016-2.RLTS.T713A45033386.en" });
        Assert.DoesNotContain("doi", output);
    }

    [Theory]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.de")]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.EN")]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490")]
    [InlineData("IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en junk")]
    [InlineData("")]
    public void Doi_MalformedIsLeftOut(string doi) {
        Assert.DoesNotContain("doi", CiteIucnRenderer.Render(PolarBear with { Doi = doi }));
    }

    [Theory]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("doi:10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData(" 10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en ")]
    public void Doi_ResolverPrefixIsRemoved(string doi) {
        Assert.Contains(" |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en}}", CiteIucnRenderer.Render(PolarBear with { Doi = doi }));
    }

    [Fact]
    public void Doi_SpanishDoiIsKept() {
        var parts = PolarBear with { Doi = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.es" };
        Assert.Contains("|doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.es", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void AccessDate_DayWithoutLeadingZero() {
        var output = CiteIucnRenderer.Render(PolarBear with { Doi = null }, new CiteIucnOptions { AccessDate = new DateOnly(2026, 9, 3) });
        Assert.EndsWith("|article-number=e.T22823A14871490 |access-date=3 September 2026}}", output);
    }

    [Fact]
    public void EtAl_AddsDisplayAuthorsAfterTheAuthors() {
        var parts = PolarBear with { Authors = [Person("Hilton-Taylor", "C.")], AuthorsEtAl = true, Doi = null };
        Assert.StartsWith("{{cite iucn |author=Hilton-Taylor, C. |display-authors=etal |year=2015 |", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void EtAl_InsideANameIsMovedToDisplayAuthors() {
        var parts = PolarBear with {
            Authors = [new CitationAuthor(CitationAuthorKind.Verbatim, "Jaffré, T. <i>et al.</i>")],
            Doi = null,
        };
        Assert.StartsWith("{{cite iucn |author=Jaffré, T. |display-authors=etal |year=2015 |", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void NoAuthors_NoAuthorParametersAndNoEtAl() {
        var parts = PolarBear with { Authors = [], AuthorsEtAl = true };
        var output = CiteIucnRenderer.Render(parts, new CiteIucnOptions { NameListStyleAmp = true });
        Assert.StartsWith("{{cite iucn |year=2015 |title=", output);
        Assert.DoesNotContain("author", output);
        Assert.DoesNotContain("name-list-style", output);
    }

    [Fact]
    public void NameListStyleAmp_FollowsTheAuthorsAndEtAl() {
        var parts = GiantPanda with { AuthorsEtAl = true };
        Assert.StartsWith(
            "{{cite iucn |author=Swaisgood, R. |author2=Wang, D. |author3=Wei, F. |display-authors=etal |name-list-style=amp |year=2016 |",
            CiteIucnRenderer.Render(parts, new CiteIucnOptions { NameListStyleAmp = true }));
    }

    [Fact]
    public void NameListStyleAmp_NotWrittenForOneAuthor() {
        Assert.DoesNotContain("name-list-style", CiteIucnRenderer.Render(HouseSparrow, new CiteIucnOptions { NameListStyleAmp = true }));
    }

    [Fact]
    public void ManyAuthors_AreAllWritten() {
        var authors = Enumerable.Range(1, 14).Select(i => Person($"Surname{(char)('a' + i)}", "A.")).ToList();
        var output = CiteIucnRenderer.Render(PolarBear with { Authors = authors });
        Assert.Contains(" |author=Surnameb, A. |author2=Surnamec, A. ", output);
        Assert.Contains(" |author14=Surnameo, A. |year=2015 ", output);
    }

    [Fact]
    public void OrganisationWithSeveralCommasIsTakenAsWritten() {
        var parts = HouseSparrow with {
            Authors = [Org("Participants of the FFI/IUCN SSC Central Asian regional tree Red Listing workshop, Bishkek, Kyrgyzstan (11-13 July 2006)")],
        };
        Assert.StartsWith(
            "{{cite iucn |author=((Participants of the FFI/IUCN SSC Central Asian regional tree Red Listing workshop, Bishkek, Kyrgyzstan (11-13 July 2006))) |year=",
            CiteIucnRenderer.Render(parts));
    }

    [Theory]
    [InlineData(CitationAuthorKind.Organisation, "Royal Botanic Gardens, Kew", "Royal Botanic Gardens, Kew")]
    [InlineData(CitationAuthorKind.Organisation, "Mollusc Specialist Group 1996", "((Mollusc Specialist Group 1996))")]
    [InlineData(CitationAuthorKind.Person, "Golamco, A., Jr.", "((Golamco, A., Jr.))")]
    [InlineData(CitationAuthorKind.Verbatim, "Luis Canseco, Antonio Muñoz, Paulino Ponce", "Luis Canseco, Antonio Muñoz, Paulino Ponce")]
    public void AcceptAsWritten_OnlyWhereCs1WouldMisreadTheName(CitationAuthorKind kind, string display, string expected) {
        var parts = HouseSparrow with { Authors = [new CitationAuthor(kind, display)] };
        Assert.StartsWith($"{{{{cite iucn |author={expected} |year=", CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void Values_CannotBreakTheTemplate() {
        var parts = HouseSparrow with {
            Authors = [Verbatim("Smith,\n  J. | Jones}"), Verbatim("{{subst:foo}} Brown"), Verbatim("Grey , A.{"), Verbatim("{{{{x}}}}")],
            RegionalScope = "Gulf {{of}} Mexico}",
            Doi = null,
        };
        var output = CiteIucnRenderer.Render(parts);
        Assert.Equal(
            "{{cite iucn |author=Smith, J. &#124; Jones |author2=subst:foo Brown |author3=Grey , A. |author4=x |year=2019 |amends=2018 " +
            "|title=''Passer domesticus'' (Gulf of Mexico assessment) |volume=2019 |article-number=e.T103818789A155522130}}",
            output);
        Assert.DoesNotContain("\n", output);
        // The only braces left are the template's own.
        Assert.Equal(2, output.Count(c => c == '{'));
        Assert.Equal(2, output.Count(c => c == '}'));
    }

    // CS1 shows "invisible character" errors for these. Whitespace ones become a space; the rest are
    // dropped. U+FFFD stays: it marks a lost letter, which a silent deletion would hide.
    [Fact]
    public void Values_LoseTheInvisibleCharactersCs1Reports() {
        var parts = HouseSparrow with {
            Authors = [
                Verbatim("Sm\u200Bith, J."), Verbatim("Jo\u00ADnes\u200D, A."), Verbatim("Br\u0007own\u007F\u0085, B."),
                Verbatim("Gr\u0090ey\u2060\uFEFF, C."), Verbatim("Kry\uFFFDtufek, B."), Verbatim("Tab\tand\u00A0space"),
            ],
            Doi = null,
        };
        Assert.StartsWith(
            "{{cite iucn |author=Smith, J. |author2=Jones, A. |author3=Brown , B. |author4=Grey, C. |author5=Kry\uFFFDtufek, B. " +
            "|author6=Tab and space |year=2019",
            CiteIucnRenderer.Render(parts));
    }

    [Fact]
    public void NeverWritesPageUrlOrLanguage() {
        foreach (var parts in new[] { PolarBear, GiantPanda, HouseSparrow, EuropeanRabbit }) {
            var output = CiteIucnRenderer.Render(parts, new CiteIucnOptions { AccessDate = new DateOnly(2026, 10, 3) });
            Assert.DoesNotContain("|page=", output);
            Assert.DoesNotContain("|url=", output);
            Assert.DoesNotContain("|language=", output);
            Assert.DoesNotContain("|date=", output);
        }
    }

    [Theory]
    [InlineData("iucn", "<ref name=\"iucn\">")]
    [InlineData("iucn status 3 October 2026", "<ref name=\"iucn status 3 October 2026\">")]
    [InlineData(" \"iu'cn/<x>\" ", "<ref name=\"iucnx\">")]
    [InlineData(null, "<ref>")]
    [InlineData("  ", "<ref>")]
    [InlineData("\"/\"", "<ref>")]
    // Cite rejects an all-digit name; other names with digits are fine.
    [InlineData("1", "<ref name=\"iucn-1\">")]
    [InlineData(" 2026 ", "<ref name=\"iucn-2026\">")]
    [InlineData("\"01\"", "<ref name=\"iucn-01\">")]
    [InlineData("iucn 2026", "<ref name=\"iucn 2026\">")]
    [InlineData("20 26", "<ref name=\"20 26\">")]
    [InlineData("٢٠٢٦", "<ref name=\"٢٠٢٦\">")]
    public void WrapInRef_SanitizesTheName(string? refName, string opening) {
        var output = CiteIucnRenderer.Render(HouseSparrow, new CiteIucnOptions { WrapInRef = true, RefName = refName });
        Assert.Equal(opening + CiteIucnRenderer.Render(HouseSparrow) + "</ref>", output);
    }
}
