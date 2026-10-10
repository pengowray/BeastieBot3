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

    // Pairs from the extra species of release 2026-1 (and the made-up last two), with whether one
    // epithet is used in no other genus (rare) and whether the authorities are the same; the branch
    // of SpellingMatch that matches, or null.
    [Theory]
    [InlineData("horhammeri", "hoerhammeri", false, false, "B")]   // Lophoterges
    [InlineData("coeruleum", "caeruleum", false, false, "B")]      // Zygocarpum
    [InlineData("hakeaeformis", "hakeiformis", true, false, "B")]  // Persoonia
    [InlineData("canescens", "x canescens", true, false, "A")]     // Crataegus
    [InlineData("joponensis", "yoponensis", true, false, "B")]     // Ficus
    [InlineData("kusumba", "kasumba", true, false, "E")]           // Harpactes
    [InlineData("lowei", "lowii", false, false, "D")]              // Sundasciurus
    [InlineData("richardsii", "richardsiae", false, false, "C")]   // Premna
    [InlineData("tnaculatus", "maculatus", true, true, "F")]       // Dasyurus
    [InlineData("unica", "uncia", false, true, "E")]               // Panthera
    [InlineData("smithi", "smithii", false, false, "B")]            // ii as i
    [InlineData("le-testui", "letestui", false, false, "A")]
    [InlineData("microcarpa", "macrocarpa", false, false, null)]   // Elegia
    [InlineData("pubescens", "rubescens", false, false, null)]     // Trichilia
    [InlineData("striata", "stricta", false, false, null)]         // Carex
    [InlineData("rugulosus", "rugosus", false, false, null)]       // Elaeocarpus
    [InlineData("angolensis", "ngomensis", false, true, null)]     // Zelotes, same author and year
    [InlineData("roosi", "rossii", true, false, null)]             // Erebia
    [InlineData("thomsonii", "thomsoniana", false, false, null)]   // Quercus
    [InlineData("densiflora", "densifolia", false, false, null)]   // Caraipa
    [InlineData("albus", "alba", true, true, null)]                // a gender variant, counted apart
    [InlineData("leo", "lea", true, true, null)]                   // too short
    public void SpellingMatches(string a, string b, bool rare, bool sameAuthor, string? branch) {
        Assert.Equal(branch, ExtraSpeciesNameRules.SpellingMatch(a, b, rare, sameAuthor));
        Assert.Equal(branch, ExtraSpeciesNameRules.SpellingMatch(b, a, rare, sameAuthor));
    }

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
