using BeastieBot3.Audit.Producers;
using Xunit;

// Pins what the audit reads as a provisional name and which quoted tags it will turn into a
// candidate binomial. The conservative cases matter most: a "cf." tag is a comparison, and a real
// epithet that merely starts with "nov" is not a marker at all.

namespace BeastieBot3.Tests;

public class ProvisionalNameTests {
    private static ProvisionalName Read(string name, string? genus = null, string? species = null, bool infra = false) =>
        ProvisionalNames.Read(name, genus, species, infra);

    [Fact]
    public void AQuotedEpithetBecomesABinomial() {
        var result = Read("Notogomphus sp. nov. 'gorilla'", "Notogomphus");
        Assert.Equal(ProvisionalOutcome.Candidate, result.Outcome);
        Assert.Equal("Notogomphus gorilla", result.CandidateName);
    }

    [Fact]
    public void AnInfraspecificProvisionalNameBecomesATrinomial() {
        var result = Read("Heideella andreae ssp. nov. 'boulanouari'", "Heideella", "andreae", infra: true);
        Assert.Equal(ProvisionalOutcome.Candidate, result.Outcome);
        Assert.Equal("Heideella andreae boulanouari", result.CandidateName);
    }

    // "cf." and "aff." say the taxon resembles that species, not that it is that species.
    [Theory]
    [InlineData("Barbus sp. nov. 'cf. gurneyi'")]
    [InlineData("Trichoglossum sp. nov. 'c.f. walteri'")]
    [InlineData("Heliotropium sp. nov. 'aff. wagneri'")]
    public void AQualifiedTagIsNeverTurnedIntoACandidate(string name) {
        var result = Read(name, "Barbus");
        Assert.Equal(ProvisionalOutcome.Qualified, result.Outcome);
        Assert.Null(result.CandidateName);
    }

    // Localities, collector codes and descriptions are not epithets.
    [Theory]
    [InlineData("Oncorhynchus sp. nov. 'Bavispe Trout'")]
    [InlineData("Justicia sp. nov. 'B = Bester 11112'")]
    [InlineData("Acrocyrtus sp. nov. 'HC - blind'")]
    [InlineData("Callopanchax sp. nov. 'Guinea'")]
    public void ADescriptiveTagYieldsNothingToLookUp(string name) {
        var result = Read(name, "Genus");
        Assert.Equal(ProvisionalOutcome.TagNotAnEpithet, result.Outcome);
        Assert.Null(result.CandidateName);
    }

    [Theory]
    [InlineData("Labeo sp. nov.")]
    [InlineData("Notogomphus sp. nov. A")]
    [InlineData("Pupisoma sp. nov. 1")]
    public void AnUntaggedProvisionalNameIsStillProvisional(string name) {
        var result = Read(name, "Genus");
        Assert.Equal(ProvisionalOutcome.NoTag, result.Outcome);
        Assert.True(result.IsProvisional);
    }

    // The marker needs its trailing period: "novicium" is a real subspecies epithet.
    [Theory]
    [InlineData("Conophytum flavum subsp. novicium")]
    [InlineData("Panthera leo")]
    [InlineData("Hemicycla cf. themera")]
    public void AnOrdinaryNameIsNotProvisional(string name) {
        Assert.Equal(ProvisionalOutcome.NotProvisional, Read(name, "Genus", "species").Outcome);
        Assert.False(ProvisionalNames.IsProvisional(name));
    }

    [Fact]
    public void ADoubleQuotedTagIsReadTheSameWay() =>
        Assert.Equal(ProvisionalOutcome.TagNotAnEpithet, Read("Mercuria sp. nov. \"A\"", "Mercuria").Outcome);

    [Fact]
    public void WithoutAGenusThereIsNoBinomialToBuild() =>
        Assert.Equal(ProvisionalOutcome.TagNotAnEpithet, Read("sp. nov. 'gorilla'").Outcome);

    // Used to drop a synonym that is itself provisional, which answers nothing.
    [Fact]
    public void AProvisionalSynonymIsRecognisedAsProvisional() =>
        Assert.True(ProvisionalNames.IsProvisional("Schefflera sp. nov. 'nanocephala'"));

    [Theory]
    [InlineData("Notogomphus gorilla")]
    [InlineData("Heideella andreae boulanouari")]
    [InlineData("Rosa x damascena")]
    public void ABinomialCanStandInTheDescribedNameColumn(string name) =>
        Assert.True(ProvisionalNames.LooksLikeDescribedName(name));

    [Theory]
    [InlineData("Notogomphus")]
    [InlineData("Notogomphus gorilla Dijkstra, 2015")]
    [InlineData("Notogomphus sp. nov. 'gorilla'")]
    [InlineData("Gila spotted whiptail lizard of Arizona")]
    [InlineData("")]
    [InlineData(null)]
    public void AnythingElseIsNotADescribedName(string? name) =>
        Assert.False(ProvisionalNames.LooksLikeDescribedName(name));

    // Shape alone cannot separate a common-name article title from a trinomial, which is why the
    // producer also requires the leading word to be a genus.
    [Fact]
    public void ACommonNameTitleHasTheSameShapeAsATrinomial() {
        Assert.True(ProvisionalNames.LooksLikeDescribedName("Gila spotted whiptail"));
        Assert.Equal("Gila", ProvisionalNames.LeadingWord("Gila spotted whiptail"));
        Assert.Equal("Notogomphus", ProvisionalNames.LeadingWord("Notogomphus gorilla"));
        Assert.Null(ProvisionalNames.LeadingWord("  "));
    }
}
