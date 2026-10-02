using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// Pins the {{cite iucn}} scanner and the comparison `site check-citations` reports. Templates are
// real ones from en-wiki articles.
public class WikiCitationComparerTests {
    // ------------------------------------------------------------ scanner

    [Fact]
    public void Find_ReadsEveryCiteIucnWithNestedLinksAndTemplates() {
        const string wikitext = """
            Text.<ref name="iucn">{{Cite iucn|title= ''Passer domesticus'' |amends=2018 |author= [[BirdLife International]] |year= 2019 |article-number= e.T103818789A155522130 |doi=10.2305/IUCN.UK.2018-2.RLTS.T103818789A155522130.en |access-date=16 March 2022}}</ref>
            <!-- {{cite iucn |author=Commented, A. |article-number=e.T1A2}} -->
            {{cite_IUCN
             | last1 = Wiig | first1 = Ø. | last2 = Amstrup | first2 = S.
             | title = ''{{lang|la|Ursus maritimus}}'' | page = e.T22823A14871490 }}
            {{cite web |title=Not this one}}
            """;

        var cites = CiteIucnTemplateScanner.Find(wikitext).ToList();

        Assert.Equal(2, cites.Count);
        Assert.Equal("[[BirdLife International]]", cites[0].Get("author"));
        Assert.Equal("10.2305/IUCN.UK.2018-2.RLTS.T103818789A155522130.en", cites[0].Get("doi"));
        Assert.Equal((103818789L, 155522130L), CiteIucnTemplateScanner.ArticleIds(cites[0]));
        Assert.Equal("''{{lang|la|Ursus maritimus}}''", cites[1].Get("title"));
        Assert.Equal((22823L, 14871490L), CiteIucnTemplateScanner.ArticleIds(cites[1]));
        Assert.Equal(new[] { "Wiig, Ø.", "Amstrup, S." }, WikiCitationComparer.WikiAuthors(cites[1]));
    }

    [Fact]
    public void Find_UnclosedTemplate_StopsQuietly() {
        Assert.Empty(CiteIucnTemplateScanner.Find("{{cite iucn |author=Open, A. |title=x"));
        Assert.Empty(CiteIucnTemplateScanner.Find(null));
    }

    [Theory]
    [InlineData("[[BirdLife International]]", "BirdLife International")]
    [InlineData("[[Neil Cox|Cox, N.]]", "Cox, N.")]
    [InlineData("''Ursus''&nbsp;maritimus", "Ursus maritimus")]
    [InlineData("Smith<br />, A.", "Smith, A.")]
    public void PlainText_ReducesMarkup(string value, string expected) {
        Assert.Equal(expected, CiteIucnTemplateScanner.PlainText(value));
    }

    // ------------------------------------------------------------ authors

    [Theory]
    [InlineData("Marsh, H.|Sobtzick, S.", "Marsh, H.|Sobtzick, S.", null, "Same")]
    [InlineData("Reis, R|Lima, F.", "Reis, R.|Lima, F.", null, "SameIgnoringPunctuation")]
    [InlineData("Walker, A.|Medeiros, M.J.", "Walker, A. & Medeiros, M.J.", null, "WikiSeveralInOneParameter")]
    [InlineData("Energy Development Corporation (EDC)", "Energy Development Corporation", "EDC", "WikiCollaborationParameter")]
    [InlineData("Pitman, R.L.|Brownell Jr., R.L.", "Pitman, R.L.|Brownell Jr.|R.L.", null, "WikiSplitsAName")]
    [InlineData("IUCN SSC Amphibian Specialist Group|Instituto Boitatá de Etnobiologia e Conservação da Fauna", "IUCN SSC Amphibian Specialist Group", null, "WikiFewer")]
    [InlineData("Bogan, A.E.", "Bogan, A.E.|Mollusc Specialist Group", null, "WikiMore")]
    [InlineData("McCosker, J.|Smith, D.G.|Tighe, K.", "McCosker, J.|Tighe, K.|Smith, D.G.", null, "OtherOrder")]
    [InlineData("Harold, A.|Milligan, R.", "A. Harold|R. Milligan", null, "WikiGivenNameFirst")]
    [InlineData("Reeves, R.|Pitman, R.L.|Ford, J.K.B.", "Reeves|Pitman|Ford", null, "WikiSurnamesOnly")]
    [InlineData("Woinarski, J.C.Z.|Burbidge, A.A.", "Woinarski, J.|Burbidge, A.A.", null, "OtherInitials")]
    [InlineData("Shepherd, C.R.|Burbidge, A.A.", "Shepard, C.|Burbidge, A.A.", null, "SomeNamesDiffer")]
    [InlineData("Wallace, B.P.|Broderick, A.C.", "Seminoff, J.A.", null, "NoNameInCommon")]
    public void CompareAuthors_ClassesTheDifference(string ours, string wiki, string? collaboration, string expected) {
        Assert.Equal(Enum.Parse<AuthorAgreement>(expected),
            WikiCitationComparer.CompareAuthors(ours.Split('|'), wiki.Split('|'), collaboration));
    }

    [Fact]
    public void CompareAuthors_ListKeptWhole_OrNoWikiAuthors() {
        var whole = new[] { "Chobanov, D.P., Hochkirch, A., Skejo, J. Skejo & Willemse, L.P.M." };
        var wiki = new[] { "Chobanov, D.P.", "Hochkirch, A.", "Skejo, J. Skejo", "Willemse, L.P.M." };

        Assert.Equal(AuthorAgreement.OursKeptWhole, WikiCitationComparer.CompareAuthors(whole, wiki, null, oursHasUnsplitList: true));
        Assert.Equal(AuthorAgreement.WikiNoAuthors, WikiCitationComparer.CompareAuthors(wiki, Array.Empty<string>(), null));
    }

    // ------------------------------------------------------------ the whole comparison

    private static WikiCiteIucn Cite(string template) => CiteIucnTemplateScanner.Find(template).Single();

    [Fact]
    public void Compare_GiantPanda_WikiPredecessorDoiAccepted() {
        var parts = new IucnCitationParts {
            TaxonId = 712, AssessmentId = 121745669, Year = 2016, ScientificName = "Ailuropoda melanoleuca", ErrataYear = 2017,
            Authors = new[] {
                new CitationAuthor(CitationAuthorKind.Person, "Swaisgood, R.", "Swaisgood", "R."),
                new CitationAuthor(CitationAuthorKind.Person, "Wang, D.", "Wang", "D."),
                new CitationAuthor(CitationAuthorKind.Person, "Wei, F.", "Wei", "F."),
            },
        };
        var cite = Cite("{{cite iucn |author=Swaisgood, R. |author2=Wang, D. |author3=Wei, F. |year=2016 |errata=2017 |title=''Ailuropoda melanoleuca'' |article-number=e.T712A121745669 |doi=10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en}}");

        var comparison = WikiCitationComparer.Compare(parts, cite, new long[] { 45033386, 102080907 });

        Assert.Equal(AuthorAgreement.Same, comparison.Authors);
        Assert.Equal(DoiAgreement.WikiOnly, comparison.Doi);
        Assert.Equal(DoiVerdict.AcceptedPredecessor, comparison.WikiDoiVerdict);
        Assert.Equal(YearAgreement.Same, comparison.Year);
        Assert.Equal(YearAgreement.Same, comparison.Errata);
        Assert.Equal(YearAgreement.BothMissing, comparison.Amends);
    }

    [Fact]
    public void Compare_DoiDifferingOnlyInRelease_AndAMissingAmends() {
        var parts = new IucnCitationParts {
            TaxonId = 22680153, AssessmentId = 228594770, Year = 2024, ScientificName = "Mareca falcata", AmendsYear = 2023,
            Authors = new[] { new CitationAuthor(CitationAuthorKind.Organisation, "BirdLife International") },
            Doi = "10.2305/IUCN.UK.2024-2.RLTS.T22680153A228594770.en", DoiSource = DoiSource.Citation,
        };
        var cite = Cite("{{cite iucn |author=[[BirdLife International]] |date=2024 |title=''Mareca falcata'' |article-number=e.T22680153A228594770 |doi=10.2305/IUCN.UK.2024-1.RLTS.T22680153A228594770.en}}");

        var comparison = WikiCitationComparer.Compare(parts, cite, null);

        Assert.Equal(AuthorAgreement.Same, comparison.Authors);
        Assert.Equal(DoiAgreement.Different, comparison.Doi);
        Assert.True(comparison.DoiDiffersOnlyInRelease);
        Assert.Equal(YearAgreement.WikiMissing, comparison.Amends);
        Assert.Equal(2024, comparison.WikiYear);
    }

    [Fact]
    public void Compare_MalformedWikiDoi() {
        var parts = new IucnCitationParts { TaxonId = 90985958, AssessmentId = 127719284, Year = 2020, ScientificName = "Dermogenys bruneiensis" };
        var cite = Cite("{{cite iucn |article-number=e.T90985958A127719284 |doi=10.2305/UCN.UK.2020-3.RLTS.T90985958A127719284.en}}");

        var comparison = WikiCitationComparer.Compare(parts, cite, null);

        Assert.Equal(DoiAgreement.WikiMalformed, comparison.Doi);
        Assert.Equal(DoiVerdict.Malformed, comparison.WikiDoiVerdict);
        Assert.Equal(YearAgreement.WikiMissing, comparison.Year);
    }
}
