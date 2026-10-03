using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// `common-names aggregate --source wikipedia`: a taxon matched to a page through a synonym or a
// Catalogue of Life name gets no names from it when another taxon is matched to it by name
// ("Hector's dolphin" is Cephalorhynchus hectori's page, not the subspecies C. hectori maui's).
public class WikipediaPageMatchTests {
    [Theory]
    [InlineData("iucn-synonym", true)]
    [InlineData("col-accepted", true)]
    [InlineData("col-synonym", true)]
    [InlineData("col-accepted-via-synonym", true)]
    [InlineData("iucn-taxonomy", false)]
    [InlineData("iucn-constructed", false)]
    [InlineData("iucn-infra-rank", false)]
    [InlineData("TaxonName", false)]
    [InlineData("Label", false)]
    [InlineData(null, false)]
    public void IsThroughAnotherName(string? method, bool expected) {
        Assert.Equal(expected, WikipediaPageMatch.IsThroughAnotherName(method));
    }

    [Fact]
    public void ASynonymMatch_GivesNoNames_WhenAnotherTaxonIsMatchedByName() {
        Assert.False(WikipediaPageMatch.GivesNames("iucn-synonym", pageMatchedByOwnName: true));
        Assert.False(WikipediaPageMatch.GivesNames("col-accepted", pageMatchedByOwnName: true));
    }

    [Fact]
    public void ASynonymMatch_GivesNames_WhenNoTaxonIsMatchedByName() {
        // "Ameiva polops" for Pholidoscelis polops: the page is about this species under an older name.
        Assert.True(WikipediaPageMatch.GivesNames("iucn-synonym", pageMatchedByOwnName: false));
    }

    [Fact]
    public void AMatchByOwnName_AlwaysGivesNames() {
        Assert.True(WikipediaPageMatch.GivesNames("iucn-taxonomy", pageMatchedByOwnName: true));
        Assert.True(WikipediaPageMatch.GivesNames("TaxonName", pageMatchedByOwnName: false));
    }
}
