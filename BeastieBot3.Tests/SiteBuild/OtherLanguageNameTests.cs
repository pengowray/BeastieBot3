using System.Linq;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// The rules site build-db applies to common names in other languages from the Catalogue of Life,
// Wikidata and Wikipedia: language codes (SiteLanguageCodes), which names are scientific names
// (OtherLanguageNameRules), the names on a Wikidata item (WikidataItemNames), and how SiteNameSet
// merges them (SiteNameKey.CaseFold).
public class OtherLanguageNameTests {
    [Theory]
    // ISO 639-1 when the language has one, else ISO 639-3.
    [InlineData("fra", "fr")]
    [InlineData("deu", "de")]
    [InlineData("zho", "zh")]
    [InlineData("nob", "nb")]
    [InlineData("sme", "se")]
    [InlineData("ceb", "ceb")]
    [InlineData("yue", "yue")]
    [InlineData("hbs", "hbs")]
    // Wikimedia's codes.
    [InlineData("zh-hans", "zh")]
    [InlineData("zh-hant", "zh")]
    [InlineData("zh-tw", "zh")]
    [InlineData("zh-yue", "yue")]
    [InlineData("zh_min_nan", "nan")]
    [InlineData("zh-classical", "lzh")]
    [InlineData("pt-br", "pt")]
    [InlineData("sr-ec", "sr")]
    [InlineData("sr-el", "sr")]
    [InlineData("be-tarask", "be")]
    [InlineData("be-x-old", "be")]
    [InlineData("kk-cyrl", "kk")]
    [InlineData("nan-latn-pehoeji", "nan")]
    [InlineData("bat-smg", "sgs")]
    [InlineData("als", "gsw")]
    [InlineData("sh", "hbs")]
    [InlineData("bh", "bho")]
    [InlineData("ike-cans", "iu")]
    // The Catalogue of Life's wrong codes, and individual languages as their macrolanguage.
    [InlineData("dnj", "da")]
    [InlineData("thy", "th")]
    [InlineData("mlf", "ml")]
    [InlineData("fqs", "fa")]
    [InlineData("cmn", "zh")]
    [InlineData("zlm", "ms")]
    [InlineData("swh", "sw")]
    public void LanguageCodes_GoIntoTheStoredForm(string code, string expected) =>
        Assert.Equal(expected, SiteLanguageCodes.Normalise(code));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("en")]
    [InlineData("eng")]
    [InlineData("en-gb")]
    [InlineData("simple")]
    [InlineData("mul")]
    [InlineData("und")]
    [InlineData("zxx")]
    [InlineData("qaa")]
    [InlineData("nrm")]
    [InlineData("roa-tara")]
    [InlineData("map-bms")]
    [InlineData("avk")]
    [InlineData("isv")]
    [InlineData("xx-unknown")]
    public void LanguageCodes_LeftOut(string? code) => Assert.Null(SiteLanguageCodes.Normalise(code));

    [Fact]
    public void EveryLanguageKeptHasAName_AndNoTwoCodesShareOne() {
        foreach (var code in new[] { "fr", "zh", "yue", "nan", "lzh", "sgs", "vro", "rup", "cbk", "gsw", "bho", "hbs", "iu", "da", "th", "ml", "fa" }) {
            Assert.NotNull(LanguageNameTable.Name(code));
        }
        Assert.Equal(LanguageNameTable.Count, LanguageNameTable.All.Select(n => n.Name).Distinct().Count());
    }

    private static readonly TaxonScientificKeys Tiger =
        new("Panthera", "tigris", ["Panthera tigris", "Felis tigris", "Tigris regalis"]);

    [Theory]
    [InlineData("Panthera tigris", "ScientificName")]
    [InlineData("panthera  TIGRIS", "ScientificName")]
    [InlineData("Felis tigris", "ScientificName")]
    [InlineData("Panthera", "Genus")]
    [InlineData("Panthera tigris altaica", "Binomial")]
    [InlineData("Panthera tygris", "Binomial")]
    [InlineData("P. tigris", "AbbreviatedBinomial")]
    [InlineData("P. tigris altaica", "AbbreviatedBinomial")]
    [InlineData("Tigre", "None")]
    [InlineData("Tiger", "None")]
    [InlineData("Panthera Tigre", "None")]
    [InlineData("P. tigrisoides", "None")]
    [InlineData("老虎", "None")]
    public void ScientificNames_AreNotCommonNames(string name, string expected) =>
        Assert.Equal(expected, OtherLanguageNameRules.Check(name, Tiger).ToString());

    [Fact]
    public void BinomialRule_WithoutAWordListTakesOutAnyNameThatBeginsWithTheGenusAndALowerCaseWord() {
        var tragopan = new TaxonScientificKeys("Tragopan", "caboti", ["Tragopan caboti"]);
        Assert.Equal(OtherNameDrop.Binomial, OtherLanguageNameRules.Check("Tragopan de Cabot", tragopan));
    }

    [Theory]
    // Real names that begin with the genus: the next word is no epithet.
    [InlineData("Tragopan de Cabot", "None")]
    [InlineData("Veronica delle paludi", "None")]
    // Scientific names: the taxon's own epithet, or an epithet of another name.
    [InlineData("Tragopan caboti guangxiensis", "Binomial")]
    [InlineData("Tragopan temminckii", "Binomial")]
    public void BinomialRule_WithAWordListNeedsAnEpithetAfterTheGenus(string name, string expected) {
        var genus = name.Split(' ')[0];
        var keys = new TaxonScientificKeys(genus, genus == "Tragopan" ? "caboti" : "scutellata", [$"{genus} x"]);
        string[] epithets = ["caboti", "temminckii", "scutellata"];
        Assert.Equal(expected, OtherLanguageNameRules.Check(name, keys, epithets.Contains).ToString());
    }

    [Fact]
    public void ScientificKeysOfASiteTaxon_IncludeTheNameWithoutTheRankMarkerAndEverySynonym() {
        var taxon = new SiteTaxon {
            TaxonId = 1, ScientificName = "Panthera tigris ssp. sumatrae", Kind = "subspecies", Genus = "Panthera", SpeciesEpithet = "tigris",
            InfraName = "sumatrae",
        };
        taxon.ColSynonyms.Add(new SiteSynonym("Panthera sumatrae"));
        taxon.ChecklistNames.Add(("Felis sumatrae", "synonym", null, "mdd"));
        taxon.ChecklistNames.Add(("Sumatran tiger", "common", null, "mdd"));
        var keys = TaxonScientificKeys.For(taxon).With(["Tigris sondaica"]);
        Assert.True(keys.Contains("panthera tigris sumatrae"));
        Assert.True(keys.Contains("panthera tigris ssp. sumatrae"));
        Assert.True(keys.Contains("panthera sumatrae"));
        Assert.True(keys.Contains("felis sumatrae"));
        Assert.True(keys.Contains("tigris sondaica"));
        Assert.False(keys.Contains("sumatran tiger"));
    }

    [Theory]
    [InlineData("‎Pedaria ovata‎", "Pedaria ovata")]
    [InlineData(" ‏‫تاس‌ماهی ایرانی‬ ", "تاس‌ماهی ایرانی")]
    public void DirectionMarks_AreTrimmedFromTheEnds(string name, string expected) =>
        Assert.Equal(expected, OtherLanguageNameRules.TrimDirectionMarks(name));

    [Fact]
    public void ZeroWidthNonJoiner_IsKept() =>
        Assert.Equal("تاس‌ماهی", OtherLanguageNameRules.TrimDirectionMarks("تاس‌ماهی"));

    [Theory]
    [InlineData("Tigre (animal)", "Tigre")]
    [InlineData("Kea (Vogelart)", "Kea")]
    [InlineData("Tiger", "Tiger")]
    [InlineData("(Panthera)", "(Panthera)")]
    public void WikipediaTitles_LoseTheirDisambiguation(string title, string expected) =>
        Assert.Equal(expected, OtherLanguageNameRules.WithoutDisambiguation(title));

    [Theory]
    [InlineData("frwiki", "fr")]
    [InlineData("cebwiki", "ceb")]
    [InlineData("zh_yuewiki", "zh-yue")]
    [InlineData("be_x_oldwiki", "be-x-old")]
    [InlineData("simplewiki", null)]
    [InlineData("commonswiki", null)]
    [InlineData("specieswiki", null)]
    [InlineData("frwikiquote", null)]
    [InlineData("enwiktionary", null)]
    public void WikipediaSites_GiveTheirLanguage(string site, string? expected) =>
        Assert.Equal(expected, OtherLanguageNameRules.WikipediaLanguage(site));

    [Fact]
    public void WikidataItem_GivesLabelsAliasesCommonNamesSitelinksAndTaxonNames() {
        const string json = """
            {"entities":{"Q19939":{"id":"Q19939",
              "labels":{"fr":{"language":"fr","value":"tigre"},"ast":{"language":"ast","value":"Panthera tigris"}},
              "aliases":{"de":[{"language":"de","value":"Königstiger"}]},
              "claims":{
                "P1843":[
                  {"mainsnak":{"datavalue":{"value":{"text":"Tigre","language":"fr"},"type":"monolingualtext"}},"rank":"normal"},
                  {"mainsnak":{"datavalue":{"value":{"text":"Tigger","language":"nl"},"type":"monolingualtext"}},"rank":"deprecated"}],
                "P225":[{"mainsnak":{"datavalue":{"value":"Panthera tigris","type":"string"}},"rank":"normal"}]},
              "sitelinks":{"frwiki":{"site":"frwiki","title":"Tigre"},"commonswiki":{"site":"commonswiki","title":"Panthera tigris"}}}}}
            """;
        var item = WikidataItemNames.Read(json)!;
        Assert.Equal([("tigre", "fr"), ("Panthera tigris", "ast"), ("Königstiger", "de"), ("Tigre", "fr")], item.Names);
        Assert.Equal([("frwiki", "Tigre"), ("commonswiki", "Panthera tigris")], item.Sitelinks);
        Assert.Equal(["Panthera tigris"], item.ScientificNames);
        Assert.Null(WikidataItemNames.Read("not json"));
    }

    [Fact]
    public void NonEnglishNamesInOneSource_AreOneNameOnlyWhenTheyDifferInCase() {
        var names = new SiteNameSet();
        Assert.True(names.Add("Ñandú", "common", "es", "col"));
        Assert.False(names.Add("ñandú", "common", "es", "col"));
        Assert.True(names.Add("Nandu", "common", "es", "col"));
        Assert.True(names.Add("ガエル", "common", "ja", "col"));
        Assert.True(names.Add("カエル", "common", "ja", "col"));
        // Full-width letters are the same name (Unicode compatibility normalisation).
        Assert.False(names.Add("ｎａｎｄｕ", "common", "es", "col"));
        // Another source's copy is its own row.
        Assert.True(names.Add("ñandú", "common", "es", "wikidata"));
        // English names still ignore accents, as before.
        Assert.True(names.Add("Rhea", "common", "en", "col"));
        Assert.False(names.Add("Rhéa", "common", "en", "col"));
        Assert.Equal(["Ñandú", "Nandu", "ガエル", "カエル", "ñandú", "Rhea"], names.Names.Select(n => n.Name));
    }
}
