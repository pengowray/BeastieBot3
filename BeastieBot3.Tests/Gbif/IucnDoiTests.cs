using BeastieBot3.Iucn.Gbif;

namespace BeastieBot3.Tests.Gbif;

// Pins DOI extraction from IUCN citation text and the ids an IUCN DOI or assessment URL names.
public class IucnDoiTests {
    [Theory]
    // GBIF's form: resolver URL at the end of the citation, no full stop after it.
    [InlineData("The IUCN Red List of Threatened Species 2013: https://doi.org/10.2305/IUCN.UK.2013-1.RLTS.T133722A512509.en",
        "10.2305/IUCN.UK.2013-1.RLTS.T133722A512509.en")]
    // The API's form: dx.doi.org, then a full stop and "Accessed on".
    [InlineData("The IUCN Red List of Threatened Species 2015: e.T22823A14871490. https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en. Accessed on 18 August 2026.",
        "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en")]
    [InlineData("See doi:10.2305/IUCN.UK.2012.RLTS.T12345A6789.en)", "10.2305/IUCN.UK.2012.RLTS.T12345A6789.en")]
    [InlineData("10.2305/IUCN.UK.2025-1.RLTS.T201631A2709621.es", "10.2305/IUCN.UK.2025-1.RLTS.T201631A2709621.es")]
    [InlineData("http://doi.org/10.15468/0qnb58", "10.15468/0qnb58")]
    public void Extract_ReturnsTheDoiWithoutItsResolver(string text, string expected) =>
        Assert.Equal(expected, IucnDoi.Extract(text));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Svahnström, V. 2025. Calyptrogyne occidentalis (Sw.) M.Gómez. The IUCN Red List of Threatened Species 2025:")]
    [InlineData("https://www.iucnredlist.org/species/201631/2709621")]
    public void Extract_TextWithoutDoi_GivesNull(string? text) =>
        Assert.Null(IucnDoi.Extract(text));

    [Fact]
    public void Parse_UsualForm_GivesIdsReleaseAndLanguage() =>
        Assert.Equal(
            new IucnDoiParts(15955, 214862019, "2022-1", "en"),
            IucnDoi.Parse("10.2305/IUCN.UK.2022-1.RLTS.T15955A214862019.en"));

    [Fact]
    public void Parse_BareYearRelease() =>
        Assert.Equal(
            new IucnDoiParts(12345, 6789, "2012", "pt"),
            IucnDoi.Parse("10.2305/IUCN.UK.2012.RLTS.T12345A6789.pt"));

    [Fact]
    public void Parse_UnusualForm_StillGivesTheIds() {
        var parts = IucnDoi.Parse("10.2305/UCN.UK.2016-3.RLTS.T21810A22215920");
        Assert.Equal(new IucnDoiParts(21810, 22215920, null, null), parts);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("10.15468/0qnb58")]
    public void Parse_DoiWithoutIucnIds_GivesNull(string? doi) =>
        Assert.Null(IucnDoi.Parse(doi));

    [Fact]
    public void ParseAssessmentUrl_GivesTaxonAndAssessmentIds() =>
        Assert.Equal((133722L, 512509L), IucnDoi.ParseAssessmentUrl("https://www.iucnredlist.org/species/133722/512509"));

    [Theory]
    [InlineData(null)]
    [InlineData("https://www.iucnredlist.org/")]
    [InlineData("https://example.org/species/1/2")]
    public void ParseAssessmentUrl_OtherText_GivesNull(string? url) =>
        Assert.Null(IucnDoi.ParseAssessmentUrl(url));
}
