using BeastieBot3.SiteBuild.ExtraSpecies;

namespace BeastieBot3.Tests;

// The name tests behind the possible-duplicate notices of the site's lists (extra_overlap).
public sealed class ExtraSpeciesNameRulesTests {
    [Theory]
    [InlineData("albus", "alba")]
    [InlineData("alba", "album")]
    [InlineData("niger", "nigra")]
    [InlineData("niger", "nigrum")]
    [InlineData("asper", "aspera")]
    [InlineData("brevis", "breve")]
    [InlineData("sinensis", "sinense")]
    [InlineData("maritimus", "maritima")]
    public void GenderVariants(string a, string b) {
        Assert.True(ExtraSpeciesNameRules.IsGenderVariant(a, b));
        Assert.True(ExtraSpeciesNameRules.IsGenderVariant(b, a));
    }

    [Theory]
    [InlineData("alba", "albida")]
    [InlineData("alba", "alba")]
    [InlineData("minor", "minus")]
    [InlineData("ab", "a")]
    public void NotGenderVariants(string a, string b) =>
        Assert.False(ExtraSpeciesNameRules.IsGenderVariant(a, b));

    [Theory]
    [InlineData("smithi", "smithii", true)]
    [InlineData("wallacei", "wallacii", true)]
    [InlineData("lybica", "libyca", false)]     // two letters apart; six letters allow one
    [InlineData("hemprichii", "hemprichi", true)]
    [InlineData("leo", "lea", false)]           // too short
    [InlineData("albus", "alba", false)]        // a gender variant, not a spelling variant
    [InlineData("pardus", "pardalis", false)]
    public void SpellingVariants(string a, string b, bool expected) =>
        Assert.Equal(expected, ExtraSpeciesNameRules.IsSpellingVariant(a, b));

    [Fact]
    public void SameAuthorityIgnoresBracketsAndPunctuation() {
        Assert.True(ExtraSpeciesNameRules.SameAuthority("(Linnaeus, 1758)", "Linnaeus 1758"));
        Assert.True(ExtraSpeciesNameRules.SameAuthority("(Smith & Jones, 1901)", "Smith and Jones, 1901"));
        Assert.True(ExtraSpeciesNameRules.SameAuthority("(Müller, 1776)", "Muller, 1776"));
        Assert.False(ExtraSpeciesNameRules.SameAuthority("Linnaeus, 1758", "Linnaeus, 1766"));
        // A year is required on both sides.
        Assert.False(ExtraSpeciesNameRules.SameAuthority("Linnaeus", "Linnaeus"));
        Assert.False(ExtraSpeciesNameRules.SameAuthority(null, "Linnaeus, 1758"));
    }

    [Theory]
    [InlineData("Panthera leo", "Panthera", "leo")]
    [InlineData("Dicranomyia (Dicranomyia) semicuneata", "Dicranomyia", "semicuneata")]
    [InlineData("Aedes aegypti-formosus", "Aedes", "aegypti-formosus")]
    public void SplitsBinomials(string name, string genus, string epithet) =>
        Assert.Equal((genus, epithet), ExtraSpeciesNameRules.SplitBinomial(name));

    [Theory]
    [InlineData("Panthera")]
    [InlineData("Panthera leo persica")]
    [InlineData("Citrus × limon")]
    [InlineData("panthera leo")]
    [InlineData("Panthera Leo")]
    [InlineData("Ficus sp. 3")]
    public void RejectsOtherNames(string name) =>
        Assert.Null(ExtraSpeciesNameRules.SplitBinomial(name));

    [Fact]
    public void LikelyReasons() {
        Assert.True(OverlapReason.IsLikely(OverlapReason.IucnSynonym));
        Assert.True(OverlapReason.IsLikely(OverlapReason.ColSynonym));
        Assert.True(OverlapReason.IsLikely(OverlapReason.GenderEnding));
        Assert.False(OverlapReason.IsLikely(OverlapReason.Spelling));
        Assert.False(OverlapReason.IsLikely(OverlapReason.OtherGenus));
    }
}
