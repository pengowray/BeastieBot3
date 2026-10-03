using BeastieBot3.Iucn.Doi;

namespace BeastieBot3.Tests.Doi;

// Pins which DOIs `iucn resolve-dois` asks doi.org about, and in what order: the releases of the
// year published by how common they are, a release the CSV exports pin first, an amended version's
// amended year after its own year, and an errata version's predecessors first only for errata
// versions published 2015 to 2018.
public class IucnDoiCandidatesTests {
    private static DoiCandidateRequest Request(int? year, long taxon = 22823, long assessment = 14871490) => new() {
        TaxonId = taxon,
        AssessmentId = assessment,
        YearPublished = year,
    };

    private static List<string> Dois(DoiCandidateRequest request) => IucnDoiCandidates.For(request).Select(c => c.Doi).ToList();

    [Theory]
    [InlineData(1996, "1996")]
    [InlineData(2004, "2004")]
    [InlineData(2008, "2008")]
    [InlineData(2009, "2009-2,2009,2009-1")]
    [InlineData(2012, "2012-1,2012,2012-2")]
    [InlineData(2015, "2015-4,2015-2,2015-1,2015,2015-3")]
    [InlineData(2016, "2016-3,2016-1,2016-2")]
    [InlineData(2018, "2018-2,2018-1,2018")]
    [InlineData(2023, "2023-1")]
    [InlineData(2025, "2025-2,2025-1")]
    [InlineData(2026, "2026-1,2026-2,2026-3")]
    [InlineData(2027, "2027-1,2027-2,2027-3")]
    public void ReleasesFor_ListsTheYearsReleasesMostCommonFirst(int year, string expected) {
        Assert.Equal(expected.Split(','), IucnDoiCandidates.ReleasesFor(year));
    }

    [Theory]
    [InlineData("English", "en")]
    [InlineData("Spanish; Castilian", "es")]
    [InlineData("Portuguese", "pt")]
    [InlineData("French", "fr")]
    [InlineData(" english ", "en")]
    [InlineData("", null)]
    [InlineData(null, null)]
    [InlineData("German", null)]
    public void LanguageCode_MapsTheCsvLanguageColumn(string? csv, string? expected) {
        Assert.Equal(expected, IucnDoiCandidates.LanguageCode(csv));
    }

    [Theory]
    [InlineData("2016-3", 2016)]
    [InlineData("2012", 2012)]
    [InlineData("2012x", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void YearOf_ReadsTheYearOfARelease(string? release, int? year) {
        Assert.Equal(year, IucnDoiCandidates.YearOf(release));
    }

    [Fact]
    public void For_PolarBear2015_TriesEveryReleaseOf2015_With2015_4First() {
        Assert.Equal(new[] {
            "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en",
            "10.2305/IUCN.UK.2015-2.RLTS.T22823A14871490.en",
            "10.2305/IUCN.UK.2015-1.RLTS.T22823A14871490.en",
            "10.2305/IUCN.UK.2015.RLTS.T22823A14871490.en",
            "10.2305/IUCN.UK.2015-3.RLTS.T22823A14871490.en",
        }, Dois(Request(2015)));
    }

    [Fact]
    public void For_UsesTheLanguageSuffix() {
        var dois = Dois(Request(2023) with { Language = "es" });
        Assert.Equal(new[] { "10.2305/IUCN.UK.2023-1.RLTS.T22823A14871490.es" }, dois);
    }

    [Fact]
    public void For_NoYearPublished_GivesNoCandidates() {
        Assert.Empty(IucnDoiCandidates.For(Request(null)));
    }

    [Theory]
    [InlineData(1965)]
    [InlineData(1994)]
    public void For_PublishedBefore1996_GivesNoCandidates(int year) {
        Assert.Empty(IucnDoiCandidates.ReleasesFor(year));
        Assert.Empty(IucnDoiCandidates.For(Request(year)));
    }

    [Fact]
    public void For_NewInThisRelease_PutsThatReleaseFirst() {
        var dois = Dois(Request(2026) with { NewInRelease = "2026-1" });
        Assert.Equal("10.2305/IUCN.UK.2026-1.RLTS.T22823A14871490.en", dois[0]);
        Assert.Equal(3, dois.Count);
    }

    [Fact]
    public void For_NewInRelease_FromAnotherYear_IsNotTried() {
        // A re-added 2023 assessment that is new in the 2026-1 export keeps its 2023 release.
        var dois = Dois(Request(2023) with { NewInRelease = "2026-1" });
        Assert.Equal(new[] { "10.2305/IUCN.UK.2023-1.RLTS.T22823A14871490.en" }, dois);
    }

    [Fact]
    public void For_AmendedVersion_TriesItsOwnYearThenTheAmendedYear() {
        // Dugong 2019, "amended version of 2015 assessment": the DOI is 2015-4 with its own id.
        var dois = Dois(Request(2019, 6909, 160756767) with { AmendsYear = 2015 });
        Assert.Equal(new[] {
            "10.2305/IUCN.UK.2019-3.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2019-2.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2019-1.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2015-2.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2015-1.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2015.RLTS.T6909A160756767.en",
            "10.2305/IUCN.UK.2015-3.RLTS.T6909A160756767.en",
        }, dois);
        Assert.All(IucnDoiCandidates.For(Request(2019, 6909, 160756767) with { AmendsYear = 2015, PredecessorIds = [1] }),
            c => Assert.Equal(DoiCandidateKind.Own, c.Kind));
    }

    [Fact]
    public void For_ErrataPublishedIn2017_TriesThePredecessorFirst() {
        // Giant panda: errata version 121745669 (published 2017) of the 2016 assessment 45033386 has
        // the DOI ...2016-2.RLTS.T712A45033386.en.
        var candidates = IucnDoiCandidates.For(Request(2016, 712, 121745669) with { ErrataYear = 2017, PredecessorIds = [45033386] });
        Assert.Equal(new[] {
            "10.2305/IUCN.UK.2016-3.RLTS.T712A45033386.en",
            "10.2305/IUCN.UK.2016-1.RLTS.T712A45033386.en",
            "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en",
            "10.2305/IUCN.UK.2016-3.RLTS.T712A121745669.en",
            "10.2305/IUCN.UK.2016-1.RLTS.T712A121745669.en",
            "10.2305/IUCN.UK.2016-2.RLTS.T712A121745669.en",
            "10.2305/IUCN.UK.2017-3.RLTS.T712A121745669.en",
            "10.2305/IUCN.UK.2017-1.RLTS.T712A121745669.en",
            "10.2305/IUCN.UK.2017-2.RLTS.T712A121745669.en",
        }, candidates.Select(c => c.Doi));
        Assert.Equal(DoiCandidateKind.Predecessor, candidates[0].Kind);
        Assert.Equal(45033386, candidates[0].AssessmentId);
        Assert.Equal(DoiCandidateKind.Own, candidates[3].Kind);
    }

    [Fact]
    public void For_ErrataPublishedIn2025_TriesItsOwnIdFirst_ThenThePredecessor() {
        var candidates = IucnDoiCandidates.For(Request(2024, 173305, 273888595) with { ErrataYear = 2025, PredecessorIds = [100] });
        Assert.Equal(new[] {
            "10.2305/IUCN.UK.2024-2.RLTS.T173305A273888595.en",
            "10.2305/IUCN.UK.2024-1.RLTS.T173305A273888595.en",
            "10.2305/IUCN.UK.2025-2.RLTS.T173305A273888595.en",
            "10.2305/IUCN.UK.2025-1.RLTS.T173305A273888595.en",
            "10.2305/IUCN.UK.2024-2.RLTS.T173305A100.en",
            "10.2305/IUCN.UK.2024-1.RLTS.T173305A100.en",
        }, candidates.Select(c => c.Doi));
    }

    [Fact]
    public void For_PredecessorsWithoutAnErrataYear_AreNotTried() {
        // IucnDoiSelector accepts a predecessor's DOI only for an errata version.
        var candidates = IucnDoiCandidates.For(Request(2016) with { PredecessorIds = [5, 6] });
        Assert.All(candidates, c => Assert.Equal(DoiCandidateKind.Own, c.Kind));
    }

    [Fact]
    public void For_RepeatsNoCandidate() {
        var candidates = IucnDoiCandidates.For(Request(2016) with { AmendsYear = 2016, ErrataYear = 2016, NewInRelease = "2016-1" });
        Assert.Equal(candidates.Select(c => c.Doi).Distinct().Count(), candidates.Count);
        Assert.Equal("10.2305/IUCN.UK.2016-1.RLTS.T22823A14871490.en", candidates[0].Doi);
    }
}
