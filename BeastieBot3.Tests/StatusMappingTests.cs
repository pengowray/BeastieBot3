using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins the IUCN status-code → descriptor mapping (code, template name, category, label) and the
// reverse DB resolution that splits CR into CR / CR(PE) / CR(PEW) via the possibly-extinct flags.
public class StatusMappingTests {
    [Theory]
    [InlineData("EX", "EX", "EX", "Extinct")]
    [InlineData("CR", "CR", "CR", "Critically Endangered")]
    [InlineData("EN", "EN", "EN", "Endangered")]
    [InlineData("VU", "VU", "VU", "Vulnerable")]
    [InlineData("NT", "NT", "NT", "Near Threatened")]
    [InlineData("LC", "LC", "LC", "Least Concern")]
    [InlineData("LR/nt", "LR/nt", "LR/nt", "Lower Risk/near threatened")]
    [InlineData("LR/lc", "LR/lc", "LR/lc", "Lower Risk/least concern")]
    public void Describe_KnownCodes(string code, string expectedCode, string expectedTemplate, string expectedCategory) {
        var d = IucnRedlistStatus.Describe(code);
        Assert.Equal(expectedCode, d.Code);
        Assert.Equal(expectedTemplate, d.TemplateName);
        Assert.Equal(expectedCategory, d.Category);
    }

    [Fact]
    public void Describe_PossiblyExtinct_KeepsParenCodeButCrTemplate() {
        var pe = IucnRedlistStatus.Describe("CR(PE)");
        Assert.Equal("CR(PE)", pe.Code);
        Assert.Equal("CR", pe.TemplateName);
        Assert.Equal("Possibly extinct", pe.Label);

        var pew = IucnRedlistStatus.Describe("CR(PEW)");
        Assert.Equal("CR(PEW)", pew.Code);
        Assert.Equal("CR", pew.TemplateName);
    }

    [Fact]
    public void Describe_IsCaseInsensitive() {
        Assert.Equal("VU", IucnRedlistStatus.Describe("vu").Code);
    }

    [Fact]
    public void Describe_StripsUnknownParenthetical() {
        // Normalize() drops "(...)" so an unrecognised qualifier still resolves to the base code.
        Assert.Equal("CR", IucnRedlistStatus.Describe("CR(weird)").TemplateName);
    }

    [Fact]
    public void Describe_UnknownCode_FallsBackToItself() {
        var d = IucnRedlistStatus.Describe("ZZ");
        Assert.Equal("ZZ", d.Code);
        Assert.Equal("ZZ", d.Category);
    }

    [Theory]
    [InlineData("Critically Endangered", "true", "false", "CR(PE)")]
    [InlineData("Critically Endangered", "false", "true", "CR(PEW)")]
    [InlineData("Critically Endangered", "false", "false", "CR")]
    [InlineData("Critically Endangered", null, null, "CR")]
    [InlineData("Endangered", null, null, "EN")]
    [InlineData("Least Concern", "false", "false", "LC")]
    public void ResolveFromDatabase_SplitsCrByFlags(string category, string? pe, string? pew, string expectedCode) {
        Assert.Equal(expectedCode, IucnRedlistStatus.ResolveFromDatabase(category, pe, pew).Code);
    }
}
