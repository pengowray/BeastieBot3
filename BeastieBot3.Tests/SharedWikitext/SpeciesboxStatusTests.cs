using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins the taxobox status lines against what Module:Conservation status accepts (see the evidence
// comment in SpeciesboxStatus.cs).
public class SpeciesboxStatusTests {
    [Fact]
    public void Render_WritesThreeLines() {
        Assert.Equal("| status = EN\n| status_system = IUCN3.1\n| status_ref = <ref>x</ref>",
            SpeciesboxStatus.Render("EN", false, false, "3.1", "<ref>x</ref>"));
    }

    [Fact]
    public void Render_LeavesStatusRefOutWhenNull() {
        Assert.Equal("| status = LC\n| status_system = IUCN3.1", SpeciesboxStatus.Render("LC", false, false, "3.1", null));
        Assert.Equal("| status = LC\n| status_system = IUCN3.1", SpeciesboxStatus.Render("LC", false, false, "3.1", " "));
    }

    [Theory]
    [InlineData("CR", true, false, "3.1", "PE", "IUCN3.1")]
    [InlineData("CR", false, true, "3.1", "PEW", "IUCN3.1")]
    [InlineData("Critically Endangered", true, false, "3.1", "PE", "IUCN3.1")]
    [InlineData("CR", false, false, "3.1", "CR", "IUCN3.1")]
    [InlineData("CR", true, false, "2.3", "PE", "IUCN2.3")]
    [InlineData("EN", true, false, "3.1", "EN", "IUCN3.1")]
    [InlineData("LR/cd", false, false, "2.3", "LR/cd", "IUCN2.3")]
    [InlineData("Lower Risk/near threatened", false, false, "2.3", "LR/nt", "IUCN2.3")]
    [InlineData("LR/lc", false, false, null, "LR/lc", "IUCN2.3")]
    [InlineData("LR/lc", false, false, "3.1", "LR/lc", "IUCN2.3")]
    [InlineData("VU", false, false, "2.3", "VU", "IUCN2.3")]
    [InlineData("VU", false, false, null, "VU", "IUCN3.1")]
    [InlineData("EX", false, false, "2.3", "EX", "IUCN2.3")]
    [InlineData("DD", false, false, " 3.1 ", "DD", "IUCN3.1")]
    [InlineData("NA", false, false, "2.3", "NA", "IUCN3.1")]
    public void Render_CodeAndSystem(string category, bool pe, bool pew, string? criteria, string code, string system) {
        Assert.Equal($"| status = {code}\n| status_system = {system}", SpeciesboxStatus.Render(category, pe, pew, criteria, null));
    }
}
