using BeastieBot3.Taxonomy;
using Xunit;

namespace BeastieBot3.Tests;

// Every case here is a real IUCN 2026-1 name paired with the closest Catalogue of Life name. The
// rejected half is the point: an edit-distance rule accepts all of them, and using one to pick a
// Wikipedia article title would have linked a shrew to a tree.
public class LatinNameVariantTests {
    [Theory]
    // Gender agreement after a genus transfer.
    [InlineData("Schistura striatus", "Schistura striata")]
    [InlineData("Tellia apodus", "Tellia apoda")]
    [InlineData("Amphelikturus dendritica", "Amphelikturus dendriticus")]
    [InlineData("Aquarana catesbeianus", "Aquarana catesbeiana")]
    [InlineData("Hylarana malabaricus", "Hylarana malabarica")]
    [InlineData("Kurochkinegramma hypogrammica", "Kurochkinegramma hypogrammicum")]
    [InlineData("Guyramemua affinis", "Guyramemua affine")]
    [InlineData("Gundlachia radiatus", "Gundlachia radiata")]
    [InlineData("Epistrophella euchroma", "Epistrophella euchromus")]
    // Patronyms formed with one -i or two.
    [InlineData("Ancistrocheirus lesueuri", "Ancistrocheirus lesueurii")]
    [InlineData("Palumbia bellierii", "Palumbia bellieri")]
    [InlineData("Petrocephalus haullevillei", "Petrocephalus haullevillii")]
    [InlineData("Matucana haynei", "Matucana haynii")]
    [InlineData("Mastigodryas pleei", "Mastigodryas pleii")]
    // Transliterated Greek, and a hyphen one catalogue keeps.
    [InlineData("Pteris lidgatei", "Pteris lydgatei")]
    [InlineData("Peltophorum dasyrachis", "Peltophorum dasyrhachis")]
    [InlineData("Xerocrassa rithymna", "Xerocrassa rhithymna")]
    [InlineData("Carex montis-everesti", "Carex montis-everestii")]
    // The rank label is a label, not part of the name.
    [InlineData("Vipera ursinii ssp. rakosiensis", "Vipera ursinii subsp. rakosiensi")]
    public void SameNameWrittenTwoWays(string iucn, string col) {
        Assert.True(LatinNameVariant.SameName(iucn, col), $"{iucn} / {col} should read as one name");
        Assert.True(LatinNameVariant.SameName(col, iucn), "the test must not depend on argument order");
    }

    [Theory]
    // A different genus, however close the spelling. The first three are different kingdoms.
    [InlineData("Cordia santacruzensis", "Cora santacruzensis")]
    [InlineData("Sorex monticola", "Shorea monticola")]
    [InlineData("Dacne pontica", "Daphne pontica")]
    [InlineData("Satyrus favonius", "Satyrium favonius")]
    [InlineData("Thaleropsis ionia", "Thaleropis ionia")]
    [InlineData("Oedemera marmorata", "Oedodera marmorata")]
    [InlineData("Scaergus unicirrhus", "Scaeurgus unicirrhus")]
    // Same genus, genuinely different species.
    [InlineData("Elater turcicus", "Elater suecicus")]
    [InlineData("Sinocyclocheilus xingyiensis", "Sinocyclocheilus jinxiensis")]
    [InlineData("Brachyhypopomus beebei", "Brachyhypopomus bennetti")]
    [InlineData("Pseudophilotes panope", "Pseudophilotes panoptes")]
    [InlineData("Dichagyris lutescens", "Dichagyris clarescens")]
    [InlineData("Dichagyris soror", "Dichagyris socorro")]
    [InlineData("Protaetia cuprina", "Protaetia cuprea")]
    [InlineData("Triplophysa posterodorsalus", "Triplophysa anterodorsalis")]
    // A working suffix left in the IUCN name is a data error, not a spelling of the name.
    [InlineData("Capparis spinosa_new", "Capparis spinosa")]
    // A doubled vowel is a typo, not a second spelling. Doubled consonants are folded; these are not.
    [InlineData("Panthera leoo", "Panthera leo")]
    [InlineData("Aquila chrysaetoos", "Aquila chrysaetos")]
    // Nothing to offer: identical, or not a binomial at all.
    [InlineData("Panthera leo", "Panthera leo")]
    [InlineData("Launaea", "Launaea")]
    [InlineData("Launaea sp. nov. A", "Launaea")]
    public void NotTheSameName(string iucn, string col) {
        Assert.False(LatinNameVariant.SameName(iucn, col), $"{iucn} / {col} must not read as one name");
        Assert.False(LatinNameVariant.SameName(col, iucn), "the test must not depend on argument order");
    }

    [Theory]
    [InlineData(null, "Panthera leo")]
    [InlineData("Panthera leo", "")]
    [InlineData("   ", "  ")]
    public void MissingNamesAreNotAMatch(string? a, string? b) => Assert.False(LatinNameVariant.SameName(a, b));
}
