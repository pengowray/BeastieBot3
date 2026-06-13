using BeastieBot3.Iucn;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins the pure scope/rank predicates extracted from WikipediaListGenerator (R2). These decide
// what counts as a subspecies / variety / regional assessment — i.e. what gets excluded from the
// global-species counts.
public class RecordClassificationTests {
    private static IucnSpeciesRecord Rec(
        string? infraType = null, string? infraName = null, string? subpopulation = null,
        string? scopes = null, string genus = "Panthera", string species = "leo") =>
        new(
            TaxonId: 1, AssessmentId: 1, RedlistCategory: "Least Concern", StatusCode: "LC",
            ScientificNameAssessments: null, ScientificNameTaxonomy: null,
            KingdomName: "ANIMALIA", PhylumName: "Chordata", ClassName: "Mammalia",
            OrderName: "Carnivora", FamilyName: "Felidae", GenusName: genus, SpeciesName: species,
            InfraType: infraType, InfraName: infraName, SubpopulationName: subpopulation, Scopes: scopes,
            Authority: null, InfraAuthority: null, PossiblyExtinct: null, PossiblyExtinctInTheWild: null,
            YearPublished: null);

    [Theory]
    [InlineData("ssp.", "persica", true)]
    [InlineData("subsp.", "persica", true)]
    [InlineData("var.", "alba", false)]
    [InlineData(null, null, false)]
    [InlineData("ssp.", null, false)] // rank present but no infra name
    public void IsSubspecies(string? infraType, string? infraName, bool expected) {
        Assert.Equal(expected, RecordClassification.IsSubspecies(Rec(infraType, infraName)));
    }

    [Theory]
    [InlineData("var.", "alba", true)]
    [InlineData("ssp.", "persica", false)]
    public void IsVariety(string? infraType, string? infraName, bool expected) {
        Assert.Equal(expected, RecordClassification.IsVariety(Rec(infraType, infraName)));
    }

    [Fact]
    public void IsInfraspecific_RequiresBothTypeAndName() {
        Assert.True(RecordClassification.IsInfraspecific(Rec("var.", "alba")));
        Assert.False(RecordClassification.IsInfraspecific(Rec("var.", null)));
        Assert.False(RecordClassification.IsInfraspecific(Rec(null, "alba")));
    }

    [Fact]
    public void IsRegionalAssessment_SubpopulationOrNonGlobalScope() {
        Assert.True(RecordClassification.IsRegionalAssessment(Rec(subpopulation: "Mediterranean")));
        Assert.True(RecordClassification.IsRegionalAssessment(Rec(scopes: "Europe")));
        Assert.False(RecordClassification.IsRegionalAssessment(Rec(scopes: "Global")));
        Assert.False(RecordClassification.IsRegionalAssessment(Rec())); // scopes null
    }

    [Fact]
    public void GetRegionalScopeLabel_DropsGlobalKeepsRest() {
        // A purely regional multi-scope assessment keeps all its parts.
        Assert.Equal("Europe, Mediterranean",
            RecordClassification.GetRegionalScopeLabel(Rec(scopes: "Europe, Mediterranean")));

        // When regionality comes from a subpopulation, a stray "Global" scope is dropped from the label.
        Assert.Equal("Europe",
            RecordClassification.GetRegionalScopeLabel(Rec(subpopulation: "Pop", scopes: "Europe, Global")));

        // A bare Global scope is not regional at all -> no label.
        Assert.Null(RecordClassification.GetRegionalScopeLabel(Rec(scopes: "Global")));
    }

    [Fact]
    public void GetParentSpeciesKey_LowercasesGenusSpecies() {
        Assert.Equal("panthera|leo", RecordClassification.GetParentSpeciesKey(Rec(genus: "Panthera", species: "Leo")));
    }
}
