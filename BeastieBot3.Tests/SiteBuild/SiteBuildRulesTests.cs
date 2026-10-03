using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// The single-value decisions of `site build-db` (SiteBuildRules, IucnLanguageCodes, SiteNameSet).
public class SiteBuildRulesTests {
    // ------------------------------------------------------------ kind and rank

    [Theory]
    [InlineData(null, null, "species")]
    [InlineData("", "", "species")]
    [InlineData("subspecies", null, "subspecies")]
    [InlineData("subspecies (plantae)", null, "subspecies")]
    [InlineData("variety", null, "variety")]
    [InlineData(null, "Adriatic Sea subpopulation", "subpopulation")]
    // Four Zea mays rows are a subpopulation of a subspecies: subpopulation wins.
    [InlineData("subspecies (plantae)", "Nobogame subpopulation", "subpopulation")]
    public void KindOf_MapsTheCsvColumns(string? infraType, string? subpopulation, string expected) =>
        Assert.Equal(expected, SiteBuildRules.KindOf(infraType, subpopulation));

    [Theory]
    [InlineData("Panthera pardus ssp. kotiya", "ssp.")]
    [InlineData("Impatiens engleri subsp. pubescens", "subsp.")]
    [InlineData("Cassia afrofistula var. afrofistula", "var.")]
    [InlineData("Ursus maritimus", null)]
    [InlineData("Huso huso Adriatic Sea subpopulation", null)]
    public void InfraRankMarker_IsTheMarkerAsWritten(string name, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.InfraRankMarker(name));

    [Fact]
    public void SubpopulationParentName_TakesTheSubpopulationOffTheEnd() {
        Assert.Equal("Zea mays subsp. mexicana",
            SiteBuildRules.SubpopulationParentName("Zea mays subsp. mexicana Nobogame subpopulation", "Nobogame subpopulation"));
        Assert.Equal("Huso huso", SiteBuildRules.SubpopulationParentName("Huso huso Adriatic Sea subpopulation", "Adriatic Sea subpopulation"));
        Assert.Null(SiteBuildRules.SubpopulationParentName("Huso huso", "Adriatic Sea subpopulation"));
    }

    // ------------------------------------------------------------ scope

    [Theory]
    [InlineData("Global", "Global")]
    [InlineData("Global & Europe", "Global")]
    [InlineData("Europe, Global", "Global")]
    [InlineData("Global, Europe & Mediterranean", "Global")]
    [InlineData("Europe", "Europe")]
    [InlineData("Europe & Mediterranean", "Europe")]
    [InlineData("Pan-Africa & S. Africa FW", "Pan-Africa")]
    [InlineData("Gulf of Mexico", "Gulf of Mexico")]
    [InlineData(null, null)]
    [InlineData("  ", null)]
    public void ScopeFromCsv_IsGlobalWhenGlobalIsAmongTheScopes_ElseTheFirstRegion(string? scopes, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.ScopeFromCsv(scopes));

    [Fact]
    public void AssignCsvScopes_GivesNoRegionTwoLatestRows() {
        // Taxon 155717: a 2010 "Europe & Mediterranean" row, a 2014 global row and a 2026 Europe row.
        var scopes = SiteBuildRules.AssignCsvScopes(new[] {
            SiteBuildRules.CsvRegions("Europe & Mediterranean"),
            SiteBuildRules.CsvRegions("Global"),
            SiteBuildRules.CsvRegions("Europe"),
        });
        Assert.Equal(new[] { "Mediterranean", "Global", "Europe" }, scopes);
    }

    [Fact]
    public void AssignCsvScopes_KeepsTheFirstRegionWhenEveryRegionIsTaken_AndNullForNoScope() {
        var scopes = SiteBuildRules.AssignCsvScopes(new[] {
            SiteBuildRules.CsvRegions("Europe"),
            SiteBuildRules.CsvRegions("Europe"),
            SiteBuildRules.CsvRegions(null),
            SiteBuildRules.CsvRegions("Global & Europe"),
        });
        Assert.Equal(new[] { "Europe", "Europe", null, "Global" }, scopes);
    }

    [Fact]
    public void ScopeFromApi_IsGlobalForCode1_ElseTheFirstDescription() {
        Assert.Equal("Global", SiteBuildRules.ScopeFromApi(new[] { ((string?)"2", (string?)"Europe"), ("1", "Global") }));
        Assert.Equal("Europe", SiteBuildRules.ScopeFromApi(new[] { ((string?)"2", (string?)"Europe"), ("3", "Mediterranean") }));
        Assert.Null(SiteBuildRules.ScopeFromApi(Array.Empty<(string?, string?)>()));
    }

    // ------------------------------------------------------------ category and values

    [Theory]
    [InlineData("Critically Endangered", "CR")]
    [InlineData("Endangered", "EN")]
    [InlineData("Vulnerable", "VU")]
    [InlineData("Near Threatened", "NT")]
    [InlineData("Least Concern", "LC")]
    [InlineData("Data Deficient", "DD")]
    [InlineData("Extinct", "EX")]
    [InlineData("Extinct in the Wild", "EW")]
    [InlineData("Lower Risk/conservation dependent", "LR/cd")]
    [InlineData("Lower Risk/near threatened", "LR/nt")]
    [InlineData("Lower Risk/least concern", "LR/lc")]
    [InlineData("Not Applicable", "NA")]
    [InlineData("Regionally Extinct", "RE")]
    [InlineData("Something new", null)]
    [InlineData(null, null)]
    public void CategoryCodeFromCsv_GivesIucnsCode(string? text, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.CategoryCodeFromCsv(text));

    [Theory]
    [InlineData("3.1", "3.1")]
    [InlineData("2.3", "2.3")]
    [InlineData("Earlier Version", null)]
    [InlineData(null, null)]
    public void CriteriaVersion_KeepsVersionNumbersOnly(string? text, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.CriteriaVersion(text));

    [Theory]
    [InlineData("2015-08-27 00:00:00 UTC", "2015-08-27")]
    [InlineData("2015-08-27T01:00:00.000+01:00", "2015-08-27")]
    [InlineData("2022-12-09T00:00:00.000+00:00", "2022-12-09")]
    [InlineData("1994-01-01", "1994-01-01")]
    [InlineData("", null)]
    public void UtcDate_GivesTheCsvAndTheApiTheSameDate(string? text, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.UtcDate(text));

    // ------------------------------------------------------------ links

    [Theory]
    [InlineData("Extinct", "EX")]
    [InlineData("Extinct in the wild", "EW")]
    [InlineData("Critically Endangered", "CR")]
    [InlineData("Endangered", "EN")]
    [InlineData("Vulnerable", "VU")]
    [InlineData("Conservation Dependent", "CD")]
    [InlineData(null, null)]
    [InlineData("Least Concern", null)]
    public void EpbcCode_MapsTheSixEpbcCategories(string? status, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.EpbcCode(status));

    [Fact]
    public void ChooseP627Item_PrefersTheItemNamedLikeTheTaxon() {
        var candidates = new[] {
            new WikidataCandidate(100, "Felis leo", Array.Empty<string>()),
            new WikidataCandidate(140, "lion", new[] { "Panthera leo" }),
            new WikidataCandidate(900, "Panthera leo", Array.Empty<string>()),
        };
        Assert.Equal(140, SiteBuildRules.ChooseP627Item(candidates, "Panthera leo"));
    }

    [Fact]
    public void ChooseP627Item_AnItemWithTheIdAtDeprecatedRank_IsChosenOnlyWhenEveryItemHasIt() {
        // Taxon 96251644: Q3008560 is named like the taxon but keeps the id at deprecated rank.
        var candidates = new[] {
            new WikidataCandidate(3008560, "Ptyas semicarinatus", Array.Empty<string>(), TaxonIdDeprecated: true),
            new WikidataCandidate(122932761, "Ptyas semicarinata", Array.Empty<string>()),
        };
        Assert.Equal(122932761, SiteBuildRules.ChooseP627Item(candidates, "Ptyas semicarinatus"));
        var allDeprecated = candidates.Select(c => c with { TaxonIdDeprecated = true }).ToArray();
        Assert.Equal(3008560, SiteBuildRules.ChooseP627Item(allDeprecated, "Ptyas semicarinatus"));
    }

    [Fact]
    public void ChooseP627Item_ComparesWithoutTheRankMarker_AndFallsBackToTheLowestItem() {
        var named = new[] {
            new WikidataCandidate(5, "something else", Array.Empty<string>()),
            new WikidataCandidate(77, null, new[] { "Panthera pardus kotiya" }),
        };
        Assert.Equal(77, SiteBuildRules.ChooseP627Item(named, "Panthera pardus ssp. kotiya"));

        var unnamed = new[] {
            new WikidataCandidate(500, "a", Array.Empty<string>()),
            new WikidataCandidate(20, "b", Array.Empty<string>()),
        };
        Assert.Equal(20, SiteBuildRules.ChooseP627Item(unnamed, "Panthera leo"));
    }

    [Theory]
    [InlineData("/data/col_coldp_COL26.7_XR.sqlite", "COL26.7 XR")]
    [InlineData("col_coldp_COL25.10_XR.sqlite", "COL25.10 XR")]
    [InlineData("/data/col_coldp_COL26.7.sqlite", "COL26.7")]
    [InlineData("/data/other.sqlite", null)]
    public void ColReleaseFromPath_ReadsTheFileName(string path, string? expected) =>
        Assert.Equal(expected, SiteBuildRules.ColReleaseFromPath(path));

    // ------------------------------------------------------------ names

    [Theory]
    [InlineData("eng", "en")]
    [InlineData("fre", "fr")]
    [InlineData("fra", "fr")]
    [InlineData("ger", "de")]
    [InlineData("spa", "es")]
    [InlineData("chi", "zh")]
    [InlineData("may", "ms")]
    [InlineData("dut", "nl")]
    [InlineData("scr", "hr")]
    [InlineData("scc", "sr")]
    [InlineData("\uFEFFaar", "aa")]
    [InlineData("haw", "haw")]
    [InlineData("phi", "phi")]
    [InlineData("fil", "fil")]
    [InlineData("und", null)]
    [InlineData("zxx", null)]
    [InlineData("mul", null)]
    [InlineData("qaa", null)]
    [InlineData("qtz", null)]
    [InlineData("QMB", null)]
    [InlineData("que", "qu")]
    [InlineData("quz", "quz")]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void LanguageCodes_UseIso6391WhereThereIsOne(string? code, string? expected) =>
        Assert.Equal(expected, IucnLanguageCodes.Normalise(code));

    [Theory]
    [InlineData("Thalarctos", "maritimus", null, null, null, "Thalarctos maritimus (Phipps, 1774)", "Thalarctos maritimus")]
    [InlineData("Felis", "pardus", "subspecies", "orientalis", null, "Felis pardus ssp. orientalis Schlegel, 1857", "Felis pardus ssp. orientalis")]
    [InlineData("Alburnus", "lucidus", "variety", "elata", null, "Alburnus lucidus var. elata Fatio, 1882", "Alburnus lucidus var. elata")]
    [InlineData("Vachellia", "etbaica", "subspecies (plantae)", "australis", null, "Vachellia etbaica australis (Brenan) Kyal.", "Vachellia etbaica subsp. australis")]
    [InlineData("Serapias ", "latifolia", null, null, "atrorubens ", "Serapias  latifolia atrorubens", "Serapias latifolia atrorubens")]
    [InlineData("Squalus", "acanthias", null, null, "(Northwest Pacific subpopulation)", "x", "Squalus acanthias Northwest Pacific subpopulation")]
    [InlineData(null, null, null, null, null, "<i>Emys</i> <i>depressa</i> Spix, 1824 [non Emys depressa Merrem, 1820]", "Emys depressa Spix, 1824")]
    public void IucnSynonymName_IsBuiltFromItsPartsWithoutTheAuthority(string? genus, string? species, string? infraType,
        string? infraName, string? subpopulation, string? fullName, string expected) =>
        Assert.Equal(expected, SiteBuildRules.IucnSynonymName(genus, species, infraType, infraName, subpopulation, fullName));

    [Fact]
    public void CleanName_DecodesEntities_AndDropsLeadingBackslashes() {
        Assert.Equal("Woolly Akodont", SiteBuildRules.CleanName("\\\\ Woolly Akodont"));
        Assert.Equal("Grey-Wilson & Frim.", SiteBuildRules.CleanName("Grey-Wilson &amp;  Frim."));
        Assert.Equal(string.Empty, SiteBuildRules.CleanName("   "));
    }

    [Fact]
    public void NameSet_KeepsOneRowPerSourceForCommonNames_AndOnePerNameForSynonyms() {
        var names = new SiteNameSet();
        Assert.True(names.Add("Ursus maritimus", "scientific", null, "iucn", isPreferred: true));
        Assert.True(names.Add("Polar Bear", "common", "en", "iucn", isPreferred: true));
        Assert.False(names.Add("Polar bear", "common", "en", "iucn"));       // same name, same source
        Assert.True(names.Add("polar bear", "common", "en", "wikidata"));    // same name, another source
        Assert.True(names.Add("Ours polaire", "common", "fr", "iucn"));
        Assert.True(names.Add("Ours polaire", "common", null, "iucn"));      // another language
        Assert.True(names.Add("Thalarctos maritimus", "synonym", null, "iucn"));
        Assert.False(names.Add("Thalarctos  maritimus", "synonym", null, "col")); // synonyms: one per name
        Assert.False(names.Add("Ursus Maritimus", "synonym", null, "col"));  // the taxon's own name
        Assert.False(names.Add("  ", "common", "en", "col"));

        Assert.Equal(new[] { "Ursus maritimus", "Polar Bear", "polar bear", "Ours polaire", "Ours polaire", "Thalarctos maritimus" },
            names.Names.Select(n => n.Name));
    }

    [Fact]
    public void NameSet_ALaterCopyCanOnlyTurnPreferredOn() {
        var names = new SiteNameSet();
        names.Add("Giant Panda", "common", "en", "iucn", isPreferred: false);
        names.Add("giant panda", "common", "en", "iucn", isPreferred: true);
        names.Add("GIANT PANDA", "common", "en", "iucn", isPreferred: false);
        var only = Assert.Single(names.Names);
        Assert.Equal("Giant Panda", only.Name);
        Assert.True(only.IsPreferred);
    }

    [Fact]
    public void NameSet_ASynonymAddedBeforeTheScientificNameIsDropped() {
        var names = new SiteNameSet();
        names.Add("Panthera leo", "synonym", null, "col");
        names.Add("Felis leo", "synonym", null, "col");
        names.Add("Panthera leo", "scientific", null, "iucn", isPreferred: true);
        Assert.Equal(new[] { "Felis leo", "Panthera leo" }, names.Names.Select(n => n.Name));
        Assert.Equal("scientific", names.Names[1].NameType);
        Assert.False(names.Add("Felis leo", "synonym", null, "iucn"));
    }

    // ------------------------------------------------------------ name sources

    [Theory]
    [InlineData("wikipedia_title", "wikipedia")]
    [InlineData("wikipedia_taxobox", "wikipedia-taxobox")]
    [InlineData("wikidata_label", "wikidata")]
    [InlineData("wikidata", "wikidata")]
    [InlineData("col", "col")]
    [InlineData("iucn", "iucn")]
    [InlineData(" IUCN ", "iucn")]
    [InlineData("gbif", null)]
    public void SiteSource_MapsTheStoresSources(string storeSource, string? expected) =>
        Assert.Equal(expected, SiteCommonNamesReader.SiteSource(storeSource));

    [Fact]
    public void SiteNameSet_KeepsATaxoboxNameBesideTheSameTitleName() {
        // The species page lists every source of a name, so one row per source is kept.
        var names = new SiteNameSet();
        Assert.True(names.Add("Sea bear", "common", "en", SiteNameSource.Wikipedia));
        Assert.True(names.Add("Sea bear", "common", "en", SiteNameSource.WikipediaTaxobox));
        Assert.Equal(new[] { "wikipedia", "wikipedia-taxobox" }, names.Names.Select(n => n.Source));
    }
}
