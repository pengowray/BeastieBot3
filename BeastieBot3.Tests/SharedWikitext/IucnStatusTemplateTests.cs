using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins the {{IUCN status}} badge now built in BeastieBot3.Shared. The CLI's lists reach it through
// IucnRedlistStatus.BuildStatusTemplate, whose output must not change: LegacyBuildStatusTemplate below
// is the code as it was before the move, and every combination in the grid must still match it.
public class IucnStatusTemplateTests {
    [Theory]
    [InlineData("CR", true, false, "CR(PE)")]
    [InlineData("CR", false, true, "CR(PEW)")]
    [InlineData("CR", true, true, "CR(PE)")]
    [InlineData("CR", false, false, "CR")]
    [InlineData("cr", true, false, "CR(PE)")]
    [InlineData("Critically Endangered", false, true, "CR(PEW)")]
    [InlineData("EN", true, false, "EN")]
    [InlineData("LR/cd", false, false, "LR/cd")]
    [InlineData("lr/nt", false, false, "LR/nt")]
    [InlineData("LR/LC", false, false, "LR/lc")]
    [InlineData("CD", false, false, "LR/cd")]
    [InlineData("PE", false, false, "CR(PE)")]
    [InlineData("CR(PEW)", false, false, "CR(PEW)")]
    [InlineData("ex", false, false, "EX")]
    public void ToTemplateCode_MapsCodes(string category, bool pe, bool pew, string expected) {
        Assert.Equal(expected, IucnStatusTemplate.ToTemplateCode(category, pe, pew));
    }

    [Theory]
    [InlineData("Extinct", "EX")]
    [InlineData("Extinct in the Wild", "EW")]
    [InlineData("Endangered", "EN")]
    [InlineData("Vulnerable", "VU")]
    [InlineData("Near Threatened", "NT")]
    [InlineData("Least Concern", "LC")]
    [InlineData("Data Deficient", "DD")]
    [InlineData("Regionally Extinct", "RE")]
    [InlineData("Not Applicable", "NA")]
    [InlineData("Lower Risk/conservation dependent", "LR/cd")]
    [InlineData("Lower Risk/near threatened", "LR/nt")]
    [InlineData("Lower Risk/least concern", "LR/lc")]
    public void ToTemplateCode_AcceptsIucnCategoryNames(string category, string expected) {
        Assert.Equal(expected, IucnStatusTemplate.ToTemplateCode(category, false, false));
    }

    [Fact]
    public void ToTemplateCode_UnknownCodeComesBackUpperCased() {
        Assert.Equal("ZZ", IucnStatusTemplate.ToTemplateCode("zz", false, false));
    }

    [Fact]
    public void Render_LinksTheAssessmentWithYear() {
        Assert.Equal("{{IUCN status|CR(PE)|5112/271898609|1|year=2025}}",
            IucnStatusTemplate.Render("CR", true, false, 5112, 271898609, "2025"));
    }

    [Fact]
    public void Render_BareLabelPutsTheYearInLabel() {
        Assert.Equal("{{IUCN status|VU|22823/14871490|1|label=2015}}",
            IucnStatusTemplate.Render("VU", false, false, 22823, 14871490, "2015", yearAsBareLabel: true));
    }

    [Theory]
    [InlineData("EX")]
    [InlineData("EW")]
    [InlineData("Extinct")]
    public void Render_ExtinctHasNoYear(string category) {
        var code = IucnStatusTemplate.ToTemplateCode(category, false, false);
        Assert.Equal($"{{{{IUCN status|{code}|1/2|1}}}}", IucnStatusTemplate.Render(category, false, false, 1, 2, "2020"));
    }

    [Fact]
    public void Render_NoYearWhenBlank() {
        Assert.Equal("{{IUCN status|LC|1/2|1}}", IucnStatusTemplate.Render("LC", false, false, 1, 2, " "));
    }

    [Fact]
    public void BuildStatusTemplate_MatchesThePreMoveOutput() {
        // Codes the CLI passes, odd spellings, and unknown values that fall through upper-cased. IUCN's
        // full category names other than "Critically Endangered" are left out: they used to come back
        // upper-cased ("ENDANGERED") and now map to their codes, and no caller passed them.
        string[] codes = ["EX", "EW", "CR", "cr", "CR(PE)", "CR(PEW)", "PE", "PEW", "EN", "VU", "NT", "LC", "DD",
            "LR/cd", "LR/CD", "lr/nt", "LR/lc", "CD", "NA", "RE", "NE", "Critically Endangered", "zz", "Weird code"];
        string?[] flags = [null, "true", "TRUE", "false", " true", ""];
        string?[] years = [null, "", "2019"];
        var checkedCount = 0;
        foreach (var code in codes) {
            foreach (var pe in flags) {
                foreach (var pew in flags) {
                    foreach (var year in years) {
                        foreach (var bareLabel in new[] { false, true }) {
                            var expected = LegacyBuildStatusTemplate(code, pe, pew, 22823, 14871490, year, bareLabel);
                            var actual = IucnRedlistStatus.BuildStatusTemplate(code, pe, pew, 22823, 14871490, year, bareLabel);
                            Assert.True(expected == actual, $"{code} pe={pe} pew={pew} year={year} label={bareLabel}: {actual} != {expected}");
                            checkedCount++;
                        }
                    }
                    Assert.Equal(LegacyToWikipediaTemplateCode(code, pe, pew), IucnRedlistStatus.ToWikipediaTemplateCode(code, pe, pew));
                }
            }
        }
        Assert.Equal(codes.Length * flags.Length * flags.Length * years.Length * 2, checkedCount);
    }

    // IucnRedlistStatus.BuildStatusTemplate and ToWikipediaTemplateCode as they were before the logic
    // moved to BeastieBot3.Shared (commit d490198). Do not edit.
    private static string LegacyBuildStatusTemplate(string baseCode, string? possiblyExtinct, string? possiblyExtinctInTheWild,
        long taxonId, long assessmentId, string? yearPublished, bool yearAsBareLabel = false) {
        var statusCode = LegacyToWikipediaTemplateCode(baseCode, possiblyExtinct, possiblyExtinctInTheWild);
        var sb = new StringBuilder();
        sb.Append("{{IUCN status|").Append(statusCode).Append('|')
          .Append(taxonId).Append('/').Append(assessmentId).Append("|1");
        if (statusCode.ToUpperInvariant() is not ("EX" or "EW") && !string.IsNullOrWhiteSpace(yearPublished)) {
            sb.Append(yearAsBareLabel ? "|label=" : "|year=").Append(yearPublished);
        }
        sb.Append("}}");
        return sb.ToString();
    }

    private static string LegacyToWikipediaTemplateCode(string code, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        var normalized = code.ToUpperInvariant();

        if (normalized == "CR" || normalized == "CRITICALLY ENDANGERED") {
            if (string.Equals(possiblyExtinct, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PE)";
            }
            if (string.Equals(possiblyExtinctInTheWild, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PEW)";
            }
            return "CR";
        }

        return normalized switch {
            "CR(PE)" or "PE" => "CR(PE)",
            "CR(PEW)" or "PEW" => "CR(PEW)",
            "LR/CD" or "CD" => "LR/cd",
            "LR/NT" => "LR/nt",
            "LR/LC" => "LR/lc",
            _ => normalized
        };
    }
}
