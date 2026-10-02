using System.Text.Json;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// Pins which DOIs the site may print for an assessment: only one whose taxon id matches and whose
// assessment id is the assessment's own, or, for an errata version, one it replaced. Ids and DOIs
// are real (2026-1 API cache, Wikipedia and GBIF).
public class IucnDoiSelectorTests {
    private static IucnCitationParts Parts(long taxonId, long assessmentId, int year, int? errataYear = null, int? amendsYear = null) => new() {
        TaxonId = taxonId,
        AssessmentId = assessmentId,
        Year = year,
        ScientificName = "x",
        ErrataYear = errataYear,
        AmendsYear = amendsYear,
    };

    // Giant panda: the latest assessment is the 2017 errata version of a 2016 assessment.
    private static readonly IucnCitationParts GiantPanda = Parts(712, 121745669, 2016, errataYear: 2017);
    private static readonly long[] GiantPandaPredecessors = { 45033386, 102080907 };
    private const string GiantPandaDoi = "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en";

    // ------------------------------------------------------------ Normalise

    [Theory]
    [InlineData("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("doi:10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData(" 10.2305/iucn.uk.2016-3.rlts.t712a45033386.EN. ", "10.2305/IUCN.UK.2016-3.RLTS.T712A45033386.en")]
    // A bare-year release, and other languages.
    [InlineData("10.2305/IUCN.UK.2012.RLTS.T190805A1960236.en", "10.2305/IUCN.UK.2012.RLTS.T190805A1960236.en")]
    [InlineData("10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es", "10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es")]
    public void Normalise_GivesTheCanonicalSpelling(string doi, string expected) {
        Assert.Equal(expected, IucnDoiSelector.Normalise(doi));
    }

    [Theory]
    // Malformed DOIs found in en-wiki articles.
    [InlineData("10.2305/10.2305/IUCN.UK.2021-3.RLTS.T22716673A192310098.en")]
    [InlineData("10.2305/UCN.UK.2020-3.RLTS.T90985958A127719284.en")]
    [InlineData("10.2305/IUCN.UK.2019-1.RLTST18488986A18488995.pt")]
    [InlineData("10.2305/IUCN.UK.2011-1.RL.T172352A6874468.en")]
    [InlineData("10.2305/IUCN.UK.2008.RLTS.e.T15957A5333757.en")]
    [InlineData("10.1111/j.1365-2699.2008.01234.x")]
    [InlineData("")]
    [InlineData(null)]
    public void Normalise_RejectsAnythingElse(string? doi) {
        Assert.Null(IucnDoiSelector.Normalise(doi));
    }

    // ------------------------------------------------------------ Check

    [Fact]
    public void Check_OwnIds_Accepted() {
        var dugong = Parts(6909, 160756767, 2019, amendsYear: 2015);
        // Dugong 2019, "amended version of 2015 assessment": its own id with the 2015-4 release.
        Assert.Equal(DoiVerdict.Accepted, IucnDoiSelector.Check(dugong, "10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en", null));
    }

    [Fact]
    public void Check_ErrataVersion_AcceptsTheAssessmentItReplaced() {
        Assert.Equal(DoiVerdict.AcceptedPredecessor, IucnDoiSelector.Check(GiantPanda, GiantPandaDoi, GiantPandaPredecessors));
    }

    [Fact]
    public void Check_EarlierAssessment_RejectedWhenNotAnErrataVersion() {
        // Dugong's 2019 amended version and its 2015 assessment 43792211: amended versions carry their own id.
        var dugong = Parts(6909, 160756767, 2019, amendsYear: 2015);
        Assert.Equal(DoiVerdict.PredecessorWithoutErrata,
            IucnDoiSelector.Check(dugong, "10.2305/IUCN.UK.2015-4.RLTS.T6909A43792211.en", new long[] { 43792211 }));
    }

    [Fact]
    public void Check_ErrataVersion_RejectsAnAssessmentItDidNotReplace() {
        // Gryllotalpa major: the 1996 assessment's 2015 errata version replaced 12998880 (1996);
        // IUCN's own citation gives the DOI of 12998967, published in 2000.
        var gryllotalpa = Parts(9530, 85829841, 1996, errataYear: 2015);
        Assert.Equal(DoiVerdict.AssessmentMismatch,
            IucnDoiSelector.Check(gryllotalpa, "10.2305/IUCN.UK.2000.RLTS.T9530A12998967.en", new long[] { 12998880 }));
    }

    [Fact]
    public void Check_OtherTaxonOrNoDoi() {
        Assert.Equal(DoiVerdict.TaxonMismatch, IucnDoiSelector.Check(GiantPanda, "10.2305/IUCN.UK.2016-2.RLTS.T713A121745669.en", GiantPandaPredecessors));
        Assert.Equal(DoiVerdict.Malformed, IucnDoiSelector.Check(GiantPanda, "10.2305/UCN.UK.2016-2.RLTS.T712A45033386.en", GiantPandaPredecessors));
        Assert.Equal(DoiVerdict.Missing, IucnDoiSelector.Check(GiantPanda, " ", GiantPandaPredecessors));
    }

    // ------------------------------------------------------------ Select

    [Fact]
    public void Select_GiantPanda_TakesThePredecessorDoiFromGbif() {
        // The API citation of aid 121745669 has no DOI. GBIF and en-wiki give the 2016-2 DOI of
        // 45033386, which IUCN redirects to the errata page.
        var choice = IucnDoiSelector.Select(GiantPanda, citationDoi: null, gbifDoi: GiantPandaDoi,
            wikidataDois: null, GiantPandaPredecessors);

        Assert.Equal(new DoiChoice(GiantPandaDoi, DoiSource.Gbif), choice);
    }

    [Fact]
    public void Select_PrefersTheCitation_ThenGbif_ThenWikidata() {
        var dugong = Parts(6909, 160756767, 2019, amendsYear: 2015);
        const string own = "10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en";
        const string otherRelease = "10.2305/IUCN.UK.2019-3.RLTS.T6909A160756767.en";

        Assert.Equal(new DoiChoice(own, DoiSource.Citation),
            IucnDoiSelector.Select(dugong, "https://dx.doi.org/" + own, otherRelease, new[] { otherRelease }, null));
        Assert.Equal(new DoiChoice(otherRelease, DoiSource.Gbif),
            IucnDoiSelector.Select(dugong, null, otherRelease, new[] { own }, null));
        Assert.Equal(new DoiChoice(own, DoiSource.Wikidata),
            IucnDoiSelector.Select(dugong, null, null, new[] { "10.2305/IUCN.UK.2015-4.RLTS.T6909A43792211.en", own }, null));
    }

    [Fact]
    public void Select_SkipsARejectedCitationDoi() {
        var gryllotalpa = Parts(9530, 85829841, 1996, errataYear: 2015);
        const string gbif = "10.2305/IUCN.UK.1996.RLTS.T9530A12998880.en";

        var choice = IucnDoiSelector.Select(gryllotalpa, "10.2305/IUCN.UK.2000.RLTS.T9530A12998967.en", gbif, null, new long[] { 12998880 });

        Assert.Equal(new DoiChoice(gbif, DoiSource.Gbif), choice);
    }

    [Fact]
    public void Select_NeverBuildsADoi() {
        // The polar bear's DOI (2015-4) exists but no source here states it: no DOI, even with the year known.
        var polarBear = Parts(22823, 14871490, 2015);

        Assert.Equal(new DoiChoice(null, DoiSource.None), IucnDoiSelector.Select(polarBear, null, null, Array.Empty<string>(), null));
        Assert.Equal(new DoiChoice(null, DoiSource.None), IucnDoiSelector.Select(polarBear, "10.2305/UCN.UK.2015-4.RLTS.T22823A14871490.en", null, null, null));
    }

    // ------------------------------------------------------------ predecessors from the taxa headers

    private static IReadOnlyList<IucnAssessmentHeader> Headers(string assessmentsJson) {
        using var document = JsonDocument.Parse($$"""{"sis_id":1,"assessments":{{assessmentsJson}}}""");
        return IucnTaxaHeaders.Read(document.RootElement);
    }

    private static string Header(long id, long taxonId, string year, bool latest, string scopes = """[{"code":"1"}]""") =>
        $$"""{"assessment_id":{{id}},"sis_taxon_id":{{taxonId}},"year_published":"{{year}}","latest":{{(latest ? "true" : "false")}},"scopes":{{scopes}}}""";

    [Fact]
    public void PredecessorIds_AreEarlierAssessmentsFromTheSameYear() {
        // Giant panda's headers in the 2026-1 taxa record (category history trimmed).
        var headers = Headers($"[{Header(121745669, 712, "2016", true)},{Header(45033386, 712, "2016", false)},"
            + $"{Header(102080907, 712, "2016", false)},{Header(13069561, 712, "2008", false)},{Header(13069729, 712, "1996", false)}]");

        Assert.Equal(GiantPandaPredecessors, IucnTaxaHeaders.PredecessorIds(headers, 121745669));
        Assert.True(headers[0].Latest);
        Assert.True(IucnTaxaHeaders.IsGlobal(headers[0]));
    }

    [Fact]
    public void PredecessorIds_NeedASharedScope() {
        var headers = Headers($"""[{Header(10, 5, "2025", true, """[{"code":"2"}]""")},{Header(11, 5, "2025", false, """[{"code":"1"}]""")},{Header(12, 5, "2025", false, """[{"code":"2"},{"code":"1"}]""")}]""");

        Assert.Equal(new long[] { 12 }, IucnTaxaHeaders.PredecessorIds(headers, 10));
        Assert.Empty(IucnTaxaHeaders.PredecessorIds(headers, 99));
    }
}
