using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

namespace BeastieBot3.Tests;

// Pins how a species line shows a taxon's names and links its article. A taxon's article title is
// only ever a link target: with no common name the line shows the scientific name, so an article
// whose title is another scientific name ("Crenimugil buchanani" for Moolgarda buchanani) or a
// genus never reads as the taxon's common name.
public sealed class SpeciesLineFormatterTests : IDisposable {
    private readonly string _rulesPath = Path.GetTempFileName();

    public void Dispose() => File.Delete(_rulesPath);

    private SpeciesLineFormatter Formatter() =>
        new(new LegacyTaxaRuleList(_rulesPath), storeBackedProvider: null, commonNameProvider: null);

    private static DisplayPreferences Style(ListingStyle style) =>
        new() { ListingStyle = style, IncludeStatusTemplate = false };

    private static IucnSpeciesRecord Mullet(string? article = null, string? commonName = null,
        string? infraType = null, string? infraName = null, string kingdom = "ANIMALIA") {
        var scientific = infraName is null ? "Moolgarda buchanani" : $"Moolgarda buchanani {infraType} {infraName}";
        return new IucnSpeciesRecord(
            TaxonId: 1, AssessmentId: 1, RedlistCategory: "Least Concern", StatusCode: "LC",
            ScientificNameAssessments: scientific, ScientificNameTaxonomy: scientific,
            KingdomName: kingdom, PhylumName: "CHORDATA", ClassName: "ACTINOPTERYGII",
            OrderName: "MUGILIFORMES", FamilyName: "MUGILIDAE", GenusName: "Moolgarda", SpeciesName: "buchanani",
            InfraType: infraType, InfraName: infraName, SubpopulationName: null, Scopes: "Global",
            Authority: null, InfraAuthority: null, PossiblyExtinct: null, PossiblyExtinctInTheWild: null,
            YearPublished: "2024", CommonNameOverride: commonName, ArticleTitleOverride: article);
    }

    // The text a reader sees: each link replaced by its label.
    private static string VisibleText(string line) =>
        Regex.Replace(line, @"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]", m => m.Groups[2].Success ? m.Groups[2].Value : m.Groups[1].Value);

    [Fact]
    public void StyleB_NoCommonName_ShowsTheScientificNameLinkedToTheArticle() {
        // 2026 data: Wikipedia's article for Moolgarda buchanani is "Crenimugil buchanani", and
        // Wikipedia has no page "Moolgarda buchanani". The line used to read
        // [[Moolgarda buchanani|Crenimugil buchanani]] (''Moolgarda buchanani'').
        var line = Formatter().FormatSpeciesLine(Mullet(article: "Crenimugil buchanani"), Style(ListingStyle.CommonNameFocus), null);

        Assert.Equal("* [[Crenimugil buchanani|''Moolgarda buchanani'']]", line);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("Moolgarda buchanani")]
    public void StyleB_NoCommonName_ArticleWithTheScientificNameOrNone_LinksTheScientificName(string? article) {
        var line = Formatter().FormatSpeciesLine(Mullet(article: article), Style(ListingStyle.CommonNameFocus), null);

        Assert.Equal("* ''[[Moolgarda buchanani]]''", line);
    }

    [Fact]
    public void StyleB_ArticleTitleDifferingOnlyInCase_IsALinkTargetNotTheLinkText() {
        var line = Formatter().FormatSpeciesLine(Mullet(article: "Moolgarda Buchanani"), Style(ListingStyle.CommonNameFocus), null);

        Assert.Equal("* [[Moolgarda Buchanani|''Moolgarda buchanani'']]", line);
    }

    [Fact]
    public void StyleB_WithCommonName_LinksTheCommonNameToTheArticle() {
        var line = Formatter().FormatSpeciesLine(Mullet(article: "Crenimugil buchanani", commonName: "Bluetail mullet"),
            Style(ListingStyle.CommonNameFocus), null);

        Assert.Equal("* [[Crenimugil buchanani|Bluetail mullet]] (''Moolgarda buchanani'')", line);
    }

    [Fact]
    public void StylesBAndC_NoCommonName_WriteTheSameNameFragment() {
        var formatter = Formatter();
        var record = Mullet(article: "Crenimugil buchanani");

        Assert.Equal(
            formatter.FormatSpeciesLine(record, Style(ListingStyle.CommonNameOnly), null),
            formatter.FormatSpeciesLine(record, Style(ListingStyle.CommonNameFocus), null));
    }

    public static IEnumerable<object?[]> AllLineShapes() {
        foreach (var style in new[] { nameof(ListingStyle.ScientificNameFocus), nameof(ListingStyle.CommonNameFocus), nameof(ListingStyle.CommonNameOnly) }) {
            foreach (var commonName in new[] { null, "Bluetail mullet" }) {
                yield return new object?[] { style, commonName, null, null, "ANIMALIA" };
                yield return new object?[] { style, commonName, "ssp.", "pacifica", "ANIMALIA" };
                yield return new object?[] { style, commonName, "var.", "pacifica", "PLANTAE" };
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllLineShapes))]
    public void ArticleTitle_IsNeverShownOnTheLine(string styleName, string? commonName, string? infraType, string? infraName, string kingdom) {
        var style = Enum.Parse<ListingStyle>(styleName);
        var formatter = Formatter();
        var record = Mullet(article: "Crenimugil buchanani", commonName: commonName, infraType: infraType, infraName: infraName, kingdom: kingdom);

        var species = formatter.FormatSpeciesLine(record, Style(style), null);
        var infraspecific = formatter.FormatInfraspecificLine(record, Style(style), null);

        Assert.Contains("[[Crenimugil buchanani|", species);
        Assert.DoesNotContain("Crenimugil", VisibleText(species));
        Assert.DoesNotContain("Crenimugil", VisibleText(infraspecific));
    }
}
