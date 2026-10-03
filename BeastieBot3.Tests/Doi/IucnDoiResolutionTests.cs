using BeastieBot3.Iucn.Doi;

namespace BeastieBot3.Tests.Doi;

// Pins how a DOI is chosen from Crossref's list and from doi.org answers: own ids first, a
// predecessor's DOI only for an errata version and only when it points to this assessment's page,
// and an unexpected doi.org answer never saved as "no DOI".
public class IucnDoiResolutionTests {
    private static readonly DateTime Now = new(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc);

    private static DoiTarget Target(long taxon, long assessment, int? year, int? errataYear = null, IReadOnlyList<long>? predecessors = null,
        string language = "en") => new() {
        TaxonId = taxon,
        AssessmentId = assessment,
        YearPublished = year,
        ErrataYear = errataYear,
        PredecessorIds = predecessors ?? Array.Empty<long>(),
        Language = language,
        Scope = "global",
    };

    private static CrossrefIucnWork Work(string release, long taxon, long assessment, long pageAssessment, string language = "en") =>
        new($"10.2305/IUCN.UK.{release}.RLTS.T{taxon}A{assessment}.{language}", taxon, assessment, release, language,
            $"https://www.iucnredlist.org/species/{taxon}/{pageAssessment}", taxon, pageAssessment);

    // ------------------------------------------------------------ Crossref

    [Fact]
    public void Crossref_OwnIds_Chosen() {
        var choice = IucnDoiResolution.ChooseFromCrossref(Target(22823, 14871490, 2015), [Work("2015-4", 22823, 14871490, 14871490)]);
        Assert.Equal("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", choice.Doi);
        Assert.Null(choice.Note);
    }

    [Fact]
    public void Crossref_SeveralOwnDois_PrefersTheOnePointingToThePage_ThenTheLanguage() {
        var target = Target(1, 10, 2020, language: "es");
        var choice = IucnDoiResolution.ChooseFromCrossref(target, [
            Work("2020-3", 1, 10, 99),
            Work("2020-2", 1, 10, 10, "en"),
            Work("2020-1", 1, 10, 10, "es"),
        ]);
        Assert.Equal("10.2305/IUCN.UK.2020-1.RLTS.T1A10.es", choice.Doi);
        Assert.NotNull(choice.Note);
    }

    [Fact]
    public void Crossref_SameLanguageAndPage_PrefersTheMoreCommonRelease() {
        var choice = IucnDoiResolution.ChooseFromCrossref(Target(1, 10, 2016), [Work("2016-1", 1, 10, 10), Work("2016-3", 1, 10, 10)]);
        Assert.Equal("10.2305/IUCN.UK.2016-3.RLTS.T1A10.en", choice.Doi);
    }

    [Fact]
    public void Crossref_ErrataVersion_TakesThePredecessorDoiThatPointsToItsPage() {
        var target = Target(712, 121745669, 2016, errataYear: 2017, predecessors: [45033386]);
        var choice = IucnDoiResolution.ChooseFromCrossref(target, [Work("2016-2", 712, 45033386, 121745669)]);
        Assert.Equal("10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", choice.Doi);
    }

    [Fact]
    public void Crossref_PredecessorDoiForANonErrataVersion_IsNotUsed_AndNoted() {
        var target = Target(712, 121745669, 2016, errataYear: null, predecessors: [45033386]);
        var choice = IucnDoiResolution.ChooseFromCrossref(target, [Work("2016-2", 712, 45033386, 121745669)]);
        Assert.Null(choice.Doi);
        Assert.Contains("10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", choice.Note);
    }

    [Fact]
    public void Crossref_ErrataVersion_TakesALinkedDoiNamingAnAssessmentFromAnotherYear() {
        // Pinus pinea: errata version 129160976 (published 2018) of the 2013 assessment 2977175, which
        // the same-year predecessor rule does not find.
        var target = Target(42391, 129160976, 2013, errataYear: 2018, predecessors: []);
        var choice = IucnDoiResolution.ChooseFromCrossref(target, [Work("2013-1", 42391, 2977175, 129160976)]);
        Assert.Equal("10.2305/IUCN.UK.2013-1.RLTS.T42391A2977175.en", choice.Doi);
        Assert.Contains("2977175", choice.Note);
    }

    [Fact]
    public void Crossref_OtherTaxon_IsNotUsed() {
        var choice = IucnDoiResolution.ChooseFromCrossref(Target(5, 10, 2020), [Work("2020-1", 6, 10, 10)]);
        Assert.Null(choice.Doi);
    }

    [Fact]
    public void Crossref_NothingListed() {
        Assert.Equal(new CrossrefChoice(null, null), IucnDoiResolution.ChooseFromCrossref(Target(5, 10, 2020), []));
    }

    // ------------------------------------------------------------ which assessments go to doi.org

    [Theory]
    [InlineData(null, "Recent")]
    [InlineData("all", "All")]
    [InlineData(" NEVER ", "Never")]
    [InlineData("sometimes", null)]
    public void ParseDoiOrgMode(string? text, string? expected) {
        Assert.Equal(expected, IucnDoiResolution.ParseDoiOrgMode(text)?.ToString());
    }

    [Theory]
    [InlineData(2026, null, true)]
    [InlineData(2025, null, true)]
    [InlineData(2024, null, false)]
    [InlineData(2010, null, false)]
    [InlineData(2010, "2026-1", true)]
    [InlineData(null, null, false)]
    public void Recent_ChecksThisYearAndLastYear_AndAssessmentsNewInThisRelease(int? year, string? newIn, bool expected) {
        var listing = new DateTime(2026, 10, 3, 0, 39, 0, DateTimeKind.Utc);
        var target = Target(1, 2, year) with { NewInRelease = newIn };
        Assert.Equal(expected, IucnDoiResolution.ShouldCheckAtDoiOrg(target, DoiOrgMode.Recent, listing));
        Assert.True(IucnDoiResolution.ShouldCheckAtDoiOrg(target, DoiOrgMode.All, listing));
        Assert.False(IucnDoiResolution.ShouldCheckAtDoiOrg(target, DoiOrgMode.Never, listing));
    }

    // ------------------------------------------------------------ doi.org

    [Fact]
    public async Task Probe_StopsAtTheFirstDoiThatExists() {
        var target = Target(22823, 14871490, 2015);
        var lookup = new FakeLookup(new() { ["10.2305/IUCN.UK.2015-2.RLTS.T22823A14871490.en"] = "https://www.iucnredlist.org/species/22823/14871490" });
        var result = await IucnDoiResolution.ProbeAsync(target, IucnDoiCandidates.For(target.CandidateRequest()), lookup, () => Now, CancellationToken.None);

        Assert.True(result.Complete);
        Assert.Equal("10.2305/IUCN.UK.2015-2.RLTS.T22823A14871490.en", result.Doi);
        Assert.Equal(2, result.Tried);
        Assert.Equal(new[] { DoiLookupVerdicts.NotFound, DoiLookupVerdicts.Accepted }, result.Lookups.Select(l => l.Verdict));
        Assert.Equal(2, lookup.Asked.Count);
    }

    [Fact]
    public async Task Probe_NothingExists_IsCompleteWithNoDoi() {
        var target = Target(1, 2, 2019);
        var result = await IucnDoiResolution.ProbeAsync(target, IucnDoiCandidates.For(target.CandidateRequest()), new FakeLookup(new()), () => Now, CancellationToken.None);
        Assert.True(result.Complete);
        Assert.Null(result.Doi);
        Assert.Equal(3, result.Tried);
        Assert.All(result.Lookups, l => Assert.Equal(Now, l.CheckedAtUtc));
    }

    [Fact]
    public async Task Probe_PredecessorDoiPointingToItsOwnPage_IsSkipped_AndTheOwnDoiFound() {
        // An errata version of 2017: the predecessor's DOI exists but still points to the predecessor.
        var target = Target(712, 121745669, 2016, errataYear: 2017, predecessors: [45033386]);
        var lookup = new FakeLookup(new() {
            ["10.2305/IUCN.UK.2016-3.RLTS.T712A45033386.en"] = "https://www.iucnredlist.org/species/712/45033386",
            ["10.2305/IUCN.UK.2017-1.RLTS.T712A121745669.en"] = "https://www.iucnredlist.org/species/712/121745669",
        });
        var result = await IucnDoiResolution.ProbeAsync(target, IucnDoiCandidates.For(target.CandidateRequest()), lookup, () => Now, CancellationToken.None);

        Assert.Equal("10.2305/IUCN.UK.2017-1.RLTS.T712A121745669.en", result.Doi);
        Assert.Equal(DoiLookupVerdicts.PointsElsewhere, result.Lookups[0].Verdict);
        Assert.NotNull(result.Note);
    }

    [Fact]
    public async Task Probe_PredecessorDoiPointingToThisPage_IsAccepted() {
        var target = Target(712, 121745669, 2016, errataYear: 2017, predecessors: [45033386]);
        var lookup = new FakeLookup(new() {
            ["10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en"] = "https://www.iucnredlist.org/species/712/121745669",
        });
        var result = await IucnDoiResolution.ProbeAsync(target, IucnDoiCandidates.For(target.CandidateRequest()), lookup, () => Now, CancellationToken.None);
        Assert.Equal("10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", result.Doi);
        Assert.Equal(3, result.Tried);
    }

    [Fact]
    public async Task Probe_UnexpectedAnswer_StopsWithoutAResult() {
        var target = Target(1, 2, 2019);
        var lookup = new FakeLookup(new() { ["10.2305/IUCN.UK.2019-1.RLTS.T1A2.en"] = "https://www.iucnredlist.org/species/1/2" },
            unexpected: ["10.2305/IUCN.UK.2019-2.RLTS.T1A2.en"]);
        var result = await IucnDoiResolution.ProbeAsync(target, IucnDoiCandidates.For(target.CandidateRequest()), lookup, () => Now, CancellationToken.None);

        Assert.False(result.Complete);
        Assert.Null(result.Doi);
        Assert.Equal(2, result.Tried);
        Assert.Equal(DoiLookupVerdicts.Unexpected, result.Lookups[^1].Verdict);
    }

    [Fact]
    public void Judge_DoiForAnotherTaxon_FailsTheIdCheck() {
        var target = Target(1, 2, 2019);
        var candidate = new DoiCandidate("10.2305/IUCN.UK.2019-1.RLTS.T9A2.en", "2019-1", 2, DoiCandidateKind.Own);
        var answer = new DoiHandleResult(candidate.Doi, DoiHandleStatus.Exists, 200, 1, null);
        Assert.Equal(DoiLookupVerdicts.FailsIdCheck, IucnDoiResolution.Judge(target, candidate, answer));
    }

    [Fact]
    public void Judge_OwnDoiWithNoUrl_IsAccepted() {
        var target = Target(1, 2, 2019);
        var candidate = new DoiCandidate("10.2305/IUCN.UK.2019-1.RLTS.T1A2.en", "2019-1", 2, DoiCandidateKind.Own);
        var answer = new DoiHandleResult(candidate.Doi, DoiHandleStatus.Exists, 200, 200, null);
        Assert.Equal(DoiLookupVerdicts.Accepted, IucnDoiResolution.Judge(target, candidate, answer));
    }
}
