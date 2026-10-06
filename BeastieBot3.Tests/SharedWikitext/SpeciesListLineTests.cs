using System.IO;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins the list line renderer shared by the generated Wikipedia lists and the public site.
public sealed class SpeciesListLineTests {
    private static SpeciesListLineOptions Style(SpeciesListStyle style, string? context = null, bool template = false) =>
        new() { Style = style, IncludeStatusTemplate = template, StatusContext = context };

    private static SpeciesListEntry Species(string genus, string epithet, string? commonName = null,
        string? article = null, string kingdom = "ANIMALIA") => new() {
        ScientificName = $"{genus} {epithet}",
        Genus = genus,
        SpeciesEpithet = epithet,
        Kingdom = kingdom,
        CommonName = commonName,
        ArticleTitle = article,
        StatusCode = "LC",
        TaxonId = 1,
        AssessmentId = 2,
        YearPublished = "2020",
    };

    private static SpeciesListEntry Infra(string genus, string epithet, string infraType, string infraName,
        string kingdom, string? commonName = null, string? article = null, string? parentArticle = null) =>
        Species(genus, epithet, commonName, article, kingdom) with {
            ScientificName = $"{genus} {epithet} {infraType} {infraName}",
            InfraType = infraType,
            InfraName = infraName,
            ParentSpeciesArticleTitle = parentArticle,
        };

    [Fact]
    public void StyleA_ScientificNameFirst_CommonNameAfterComma() {
        var line = SpeciesListLine.Format(
            Species("Pinus", "radiata", "Monterey pine", "Pinus radiata", "PLANTAE") with { YearPublished = "2013", TaxonId = 42, AssessmentId = 99 },
            Style(SpeciesListStyle.ScientificNameFirst, template: true));

        Assert.Equal("* ''[[Pinus radiata]]'', Monterey pine {{IUCN status|LC|42/99|1|year=2013}}", line);
    }

    [Fact]
    public void StyleA_ArticleWithAnotherTitle_PipesTheScientificName() {
        var line = SpeciesListLine.Format(Species("Pinus", "radiata", "Monterey pine", "Monterey pine", "PLANTAE"),
            Style(SpeciesListStyle.ScientificNameFirst));

        Assert.Equal("* [[Monterey pine|''Pinus radiata'']], Monterey pine", line);
    }

    [Theory]
    [InlineData("Western gorilla", "* [[Western gorilla]] (''Gorilla gorilla'')")]
    [InlineData(null, "* [[Gorilla gorilla|Western gorilla]] (''Gorilla gorilla'')")]
    public void StyleB_CommonNameFirst_ScientificNameInBrackets(string? article, string expected) {
        var line = SpeciesListLine.Format(Species("Gorilla", "gorilla", "Western gorilla", article),
            Style(SpeciesListStyle.CommonNameFirst));

        Assert.Equal(expected, line);
    }

    [Fact]
    public void StyleC_CommonNameOnly() {
        var line = SpeciesListLine.Format(Species("Gorilla", "gorilla", "Western gorilla", "Western gorilla"),
            Style(SpeciesListStyle.CommonNameOnly));

        Assert.Equal("* [[Western gorilla]]", line);
    }

    [Theory]
    [InlineData(null, "* ''[[Moolgarda buchanani]]''")]
    [InlineData("Crenimugil buchanani", "* [[Crenimugil buchanani|''Moolgarda buchanani'']]")]
    public void StyleC_NoCommonName_FallsBackToTheScientificName(string? article, string expected) {
        var line = SpeciesListLine.Format(Species("Moolgarda", "buchanani", article: article),
            Style(SpeciesListStyle.CommonNameOnly));

        Assert.Equal(expected, line);
    }

    [Theory]
    [InlineData("subsp.")]
    [InlineData("ssp.")]
    public void PlantSubspecies_KeepsSubsp(string infraType) {
        var line = SpeciesListLine.Format(Infra("Abies", "pinsapo", infraType, "marocana", "PLANTAE", "Moroccan fir"),
            Style(SpeciesListStyle.ScientificNameFirst));

        Assert.Equal("* ''Abies pinsapo'' subsp. ''marocana'', Moroccan fir", line);
    }

    [Fact]
    public void AnimalSubspecies_HidesTheRankMarker() {
        var line = SpeciesListLine.Format(Infra("Panthera", "tigris", "ssp.", "sondaica", "ANIMALIA", "Javan tiger", "Javan tiger"),
            Style(SpeciesListStyle.CommonNameFirst));

        Assert.Equal("* [[Javan tiger]] (''Panthera tigris sondaica'')", line);
    }

    [Fact]
    public void Variety_ShowsVar() {
        var noCommonName = SpeciesListLine.Format(Infra("Pinus", "nigra", "var.", "pallasiana", "PLANTAE"),
            Style(SpeciesListStyle.CommonNameFirst));
        var withCommonName = SpeciesListLine.Format(Infra("Pinus", "nigra", "var.", "pallasiana", "PLANTAE", "Crimean pine"),
            Style(SpeciesListStyle.CommonNameFirst));

        Assert.Equal("* ''Pinus nigra'' var. ''pallasiana''", noCommonName);
        Assert.Equal("* [[Pinus nigra var. pallasiana|Crimean pine]] (''Pinus nigra'' var. ''pallasiana'')", withCommonName);
    }

    [Fact]
    public void Subspecies_WithNoArticle_LinksTheParentSpeciesArticle() {
        var entry = Infra("Balaenoptera", "musculus", "ssp.", "intermedia", "ANIMALIA", parentArticle: "Blue whale");

        Assert.Equal("* [[Blue whale|''Balaenoptera musculus intermedia'']]",
            SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly)));
        Assert.Equal("* [[Blue whale|''B. musculus intermedia'']], Antarctic blue whale",
            SpeciesListLine.FormatInfraspecificUnderSpecies(entry with { CommonName = "Antarctic blue whale" }, Style(SpeciesListStyle.CommonNameOnly)));
    }

    [Fact]
    public void UndescribedName_IsNotLinked() {
        // An epithet ending in an apostrophe uses <i></i>: ''X'' would leave a stray ''' that renders bold.
        var line = SpeciesListLine.Format(Species("Galaxias", "sp. nov. 'Pool'"), Style(SpeciesListStyle.ScientificNameFirst));

        Assert.Equal("* <i>Galaxias sp. nov. 'Pool'</i>", line);
    }

    [Fact]
    public void PossiblyExtinct_LabelLeftOutOnAPossiblyExtinctList() {
        var entry = Species("Gorilla", "gorilla", "Western gorilla", "Western gorilla") with { StatusCode = "CR(PE)" };

        Assert.Equal("* [[Western gorilla]] (possibly extinct) {{IUCN status|CR(PE)|1/2|1|year=2020}}",
            SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly, context: "CR", template: true)));
        Assert.Equal("* [[Western gorilla]]",
            SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly, context: "CR(PE)")));
    }

    [Fact]
    public void ExtinctInTheWild_LabelLeftOutOnAnExtinctInTheWildList() {
        var entry = Species("Gorilla", "gorilla", "Western gorilla", "Western gorilla") with { StatusCode = "EW" };

        Assert.Equal("* [[Western gorilla]] (extinct in the wild)", SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly)));
        Assert.Equal("* [[Western gorilla]]", SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly, context: "EW")));
    }

    [Fact]
    public void CrWithPossiblyExtinctFlag_TemplateCodeIsCrPe() {
        var entry = Species("Gorilla", "gorilla", "Western gorilla", "Western gorilla") with { StatusCode = "CR", PossiblyExtinct = true };

        Assert.Equal("{{IUCN status|CR(PE)|1/2|1|year=2020}}", SpeciesListLine.StatusTemplate(entry));
    }

    [Fact]
    public void Subpopulation_AndScope_InBrackets() {
        var entry = Species("Thunnus", "thynnus", "Atlantic bluefin tuna", "Atlantic bluefin tuna") with {
            SubpopulationName = "Mediterranean subpopulation",
            RegionalScopeLabel = "Europe",
        };

        Assert.Equal("* [[Atlantic bluefin tuna]] (Mediterranean subpopulation; scope: Europe)",
            SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly)));
    }

    [Fact]
    public void Extinct_TemplateHasNoYear() {
        var entry = Species("Hydrodamalis", "gigas", "Steller's sea cow", "Steller's sea cow") with { StatusCode = "EX", YearPublished = "2008" };

        Assert.Equal("* [[Steller's sea cow]] {{IUCN status|EX|1/2|1}}",
            SpeciesListLine.Format(entry, Style(SpeciesListStyle.CommonNameOnly, template: true)));
    }

    // ------------------------------------------------------------ authorities

    private static SpeciesListLineOptions WithAuthority(SpeciesListStyle style, SpeciesListAuthority authority = SpeciesListAuthority.Small) =>
        Style(style) with { Authority = authority };

    [Fact]
    public void Authority_DefaultOptions_LeaveTheLineUnchanged() {
        var entry = Species("Panthera", "leo", "Lion", "Lion");
        var withAuthority = entry with { Authority = "(Linnaeus, 1758)" };

        foreach (var style in Enum.GetValues<SpeciesListStyle>()) {
            Assert.Equal(SpeciesListLine.Format(entry, Style(style, template: true)),
                SpeciesListLine.Format(withAuthority, Style(style, template: true)));
        }
    }

    [Theory]
    [InlineData(SpeciesListStyle.ScientificNameFirst, "Panthera leo", "* ''[[Panthera leo]]'' <small>(Linnaeus, 1758)</small>, Lion")]
    [InlineData(SpeciesListStyle.ScientificNameFirst, "Lion", "* [[Lion|''Panthera leo'']] <small>(Linnaeus, 1758)</small>, Lion")]
    [InlineData(SpeciesListStyle.CommonNameFirst, "Lion", "* [[Lion]] (''Panthera leo'' <small>(Linnaeus, 1758)</small>)")]
    // Style C shows no scientific name, so no authority.
    [InlineData(SpeciesListStyle.CommonNameOnly, "Lion", "* [[Lion]]")]
    public void Authority_SmallText_AfterTheScientificName(SpeciesListStyle style, string article, string expected) {
        var entry = Species("Panthera", "leo", "Lion", article) with { Authority = "(Linnaeus, 1758)" };

        Assert.Equal(expected, SpeciesListLine.Format(entry, WithAuthority(style)));
    }

    [Theory]
    [InlineData(SpeciesListStyle.ScientificNameFirst)]
    [InlineData(SpeciesListStyle.CommonNameFirst)]
    [InlineData(SpeciesListStyle.CommonNameOnly)]
    public void Authority_NoCommonName_AfterTheScientificName(SpeciesListStyle style) {
        var entry = Species("Panthera", "antiquorum") with { Authority = "(Cuvier, 1824)" };

        Assert.Equal("* ''[[Panthera antiquorum]]'' (Cuvier, 1824)",
            SpeciesListLine.Format(entry, WithAuthority(style, SpeciesListAuthority.Plain)));
    }

    [Fact]
    public void Authority_Infraspecific_AfterTheFullNameAndBeforeTheCommonName() {
        var entry = Infra("Abies", "pinsapo", "subsp.", "marocana", "PLANTAE", "Moroccan fir") with { Authority = "(Trab.) Emb. & Maire" };

        Assert.Equal("* ''Abies pinsapo'' subsp. ''marocana'' <small>(Trab.) Emb. & Maire</small>, Moroccan fir",
            SpeciesListLine.Format(entry, WithAuthority(SpeciesListStyle.ScientificNameFirst)));
        Assert.Equal("** ''A. pinsapo'' subsp. ''marocana'' <small>(Trab.) Emb. & Maire</small>, Moroccan fir",
            "*" + SpeciesListLine.FormatInfraspecificUnderSpecies(entry, WithAuthority(SpeciesListStyle.CommonNameFirst)));
    }

    [Fact]
    public void Authority_WithMarkupCharacters_IsWrappedInNowiki() {
        var entry = Species("Ficus", "x") with { Authority = "Smith [unpublished]" };

        Assert.Equal("* ''[[Ficus x]]'' <small><nowiki>Smith [unpublished]</nowiki></small>",
            SpeciesListLine.Format(entry, WithAuthority(SpeciesListStyle.ScientificNameFirst)));
    }

    [Fact]
    public void SpeciesLineFormatter_RendersTheSharedLine() {
        var rulesPath = Path.GetTempFileName();
        try {
            var formatter = new SpeciesLineFormatter(new LegacyTaxaRuleList(rulesPath), storeBackedProvider: null, commonNameProvider: null);
            var record = new IucnSpeciesRecord(
                TaxonId: 7, AssessmentId: 8, RedlistCategory: "Critically Endangered", StatusCode: "CR(PE)",
                ScientificNameAssessments: "Abies pinsapo subsp. marocana", ScientificNameTaxonomy: "Abies pinsapo subsp. marocana",
                KingdomName: "PLANTAE", PhylumName: null, ClassName: null, OrderName: null, FamilyName: null,
                GenusName: "Abies", SpeciesName: "pinsapo", InfraType: "subsp.", InfraName: "marocana",
                SubpopulationName: null, Scopes: "Europe", Authority: null, InfraAuthority: null,
                PossiblyExtinct: "true", PossiblyExtinctInTheWild: "false", YearPublished: "2011",
                CommonNameOverride: "Moroccan fir");
            var display = new DisplayPreferences { ListingStyle = ListingStyle.ScientificNameFocus };

            var line = formatter.FormatSpeciesLine(record, display, "CR");

            Assert.Equal("* ''Abies pinsapo'' subsp. ''marocana'', Moroccan fir (possibly extinct) (scope: Europe) {{IUCN status|CR(PE)|7/8|1|year=2011}}", line);
            Assert.Equal(line, SpeciesListLine.Format(formatter.ToEntry(record), SpeciesLineFormatter.ToLineOptions(display, "CR")));
        } finally {
            File.Delete(rulesPath);
        }
    }
}
