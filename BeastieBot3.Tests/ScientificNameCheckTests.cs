using System.Collections.Generic;
using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// ScientificNameCheck decides which Wikipedia titles, taxobox names and Wikidata labels
// `common-names aggregate` stores as common names. The examples are titles of articles matched to
// IUCN taxa in the Wikipedia cache (September 2026).
public class ScientificNameCheckTests {
    private static TaxonScientificNames Taxon(string canonical, params string[] synonyms) => new(canonical, synonyms);

    // english: words used in 100 English names; rareEnglish: in 5, enough for a word but not for an
    // epithet (NameWordSets.MinCommonNamesForEnglishEpithet).
    private static NameWordSets Words(string[]? genera = null, string[]? epithets = null, string[]? english = null,
        string[]? rareEnglish = null) {
        var counts = new Dictionary<string, int>();
        foreach (var word in rareEnglish ?? []) counts[word] = 5;
        foreach (var word in english ?? []) counts[word] = 100;
        return new(new HashSet<string>(genera ?? []), new HashSet<string>(epithets ?? []), counts);
    }

    private static bool IsScientific(string name, TaxonScientificNames taxon, NameWordSets? words = null) =>
        ScientificNameCheck.IsScientificName(name, taxon, words ?? NameWordSets.Empty);

    // Common names that the old shape test took for scientific names.

    [Theory]
    [InlineData("Pygmy hippopotamus", "choeropsis liberiensis")]
    [InlineData("Bluntnose sixgill shark", "hexanchus griseus")]
    [InlineData("Sunda slow loris", "nycticebus coucang")]
    [InlineData("Common dolphin", "delphinus delphis")]
    public void SentenceCaseCommonName_IsNotScientific_EvenWithNoWordSets(string title, string canonical) {
        Assert.False(IsScientific(title, Taxon(canonical)));
    }

    [Fact]
    public void LowerCaseWikidataLabel_IsNotScientific() {
        Assert.False(IsScientific("pygmy hippopotamus", Taxon("choeropsis liberiensis")));
    }

    [Theory]
    [InlineData("Grey's mudsnake")]
    [InlineData("Rüppell's vulture")]
    [InlineData("Golden-lined spinefoot")]
    [InlineData("Kinkajou")]
    [InlineData("Narrow-barred Spanish mackerel")]
    public void NameNotShapedLikeAScientificName_IsNotScientific(string title) {
        Assert.False(IsScientific(title, Taxon("panthera leo")));
    }

    // The taxon's own names.

    [Fact]
    public void TheTaxonsOwnName_IsScientific() {
        // The old shape test let this through: "donax" has no Latin ending it knew.
        Assert.True(IsScientific("Arundo donax", Taxon("arundo donax")));
    }

    [Fact]
    public void ASynonym_IsScientific() {
        Assert.True(IsScientific("Hemidactylus tolampyae", Taxon("lygodactylus tolampyae", "hemidactylus tolampyae")));
    }

    [Fact]
    public void ANameWithASubgenus_IsScientific() {
        Assert.True(IsScientific("Holothuria (Metriatyla) lessoni", Taxon("holothuria lessoni")));
    }

    [Fact]
    public void TheGenusOfAMonotypicGenus_IsScientific() {
        Assert.True(IsScientific("Techmarscincus", Taxon("techmarscincus jigurru")));
    }

    [Fact]
    public void TheAcceptedGenus_IsScientific_EvenWhenItIsAnEnglishWord() {
        Assert.True(IsScientific("Hippopotamus", Taxon("hippopotamus amphibius"), Words(english: ["hippopotamus"])));
    }

    [Fact]
    public void TheGenusOfASynonym_IsScientific_UnlessItIsAnEnglishWord() {
        Assert.True(IsScientific("Geomalia", Taxon("zoothera heinrichi", "geomalia heinrichi")));
        Assert.False(IsScientific("Orca", Taxon("orcinus orca", "orca gladiator"), Words(english: ["orca"])));
    }

    [Fact]
    public void TheSpeciesOfASubspecies_IsScientific() {
        Assert.True(IsScientific("Aloeides molomo", Taxon("aloeides molomo krooni")));
    }

    // Scientific names the store does not have for the taxon.

    [Theory]
    [InlineData("Gobio gobio", "gobio latus")]
    [InlineData("Psychotria srilankensis", "psychotria stenophylla")]
    [InlineData("Gonichthys cocco", "gonichthys coccoi")]
    public void NameStartingWithTheTaxonsGenus_IsScientific(string title, string canonical) {
        Assert.True(IsScientific(title, Taxon(canonical)));
    }

    [Fact]
    public void NameStartingWithTheTaxonsGenus_IsScientific_EvenWithAnEnglishWord() {
        Assert.True(IsScientific("Medicago murex", Taxon("medicago heterocarpa"), Words(english: ["murex"])));
        // So a common name that starts with a synonym's genus is lost; IUCN has the same name.
        Assert.True(IsScientific("Hamadryas baboon", Taxon("papio hamadryas", "hamadryas hamadryas"), Words(english: ["baboon"])));
    }

    [Theory]
    [InlineData("Rubroshorea ovata", "shorea ovata")]
    [InlineData("Tliltocatl vagans", "brachypelma vagans")]
    [InlineData("Hyaloglanis pulex", "ammoglanis pulex")]
    public void AnotherGenusWithTheTaxonsEpithet_IsScientific(string title, string canonical) {
        Assert.True(IsScientific(title, Taxon(canonical)));
    }

    [Fact]
    public void AnEpithetThatIsAlsoTheTaxonsGenus_IsNotEvidence() {
        Assert.False(IsScientific("Western gorilla", Taxon("gorilla gorilla")));
        Assert.False(IsScientific("Eurasian lynx", Taxon("lynx lynx")));
    }

    [Fact]
    public void GenusAndEpithetKnownToTheStore_IsScientific() {
        var words = Words(genera: ["camphora", "cinnamomum"], epithets: ["officinarum", "camphora"]);
        Assert.True(IsScientific("Camphora officinarum", Taxon("cinnamomum camphora"), words));
    }

    [Fact]
    public void AnEnglishWordThatIsNotAGenusOrEpithet_MakesItACommonName() {
        var words = Words(genera: ["magnolia", "alligator", "turbo"], english: ["warbler", "gar", "shrew", "andean", "condor"],
            epithets: ["condor"]);
        Assert.False(IsScientific("Magnolia warbler", Taxon("setophaga magnolia"), words));
        Assert.False(IsScientific("Alligator gar", Taxon("atractosteus spatula"), words));
        Assert.False(IsScientific("Turbo shrew", Taxon("crocidura turba"), words));
        Assert.False(IsScientific("Andean condor", Taxon("vultur gryphus", "sarcoramphus condor"), words));
    }

    [Fact]
    public void AHyphenatedWord_IsEnglishOnlyWhenEveryPartIs() {
        var words = Words(genera: ["tillandsia", "popondetta"], english: ["walter", "blue", "eye"]);
        Assert.True(IsScientific("Tillandsia walter-tillii", Taxon("vriesea tillii"), words));
        Assert.False(IsScientific("Popondetta blue-eye", Taxon("pseudomugil connieae"), words));
    }

    [Fact]
    public void AnEpithetCountsAsEnglishOnlyWhenManyEnglishNamesUseIt() {
        var words = Words(genera: ["gazella", "aiouea", "ocotea"], epithets: ["gazelle", "dorcas", "montana", "falcata"],
            english: ["gazelle"], rareEnglish: ["montana"]);
        Assert.False(IsScientific("Dorcas gazelle", Taxon("gazella dorcas"), words));
        Assert.True(IsScientific("Aiouea montana", Taxon("ocotea falcata"), words));
    }

    [Fact]
    public void AGenusThatIsAlsoAnEnglishWord_CountsAsAGenus() {
        // "Bulbophyllum" and "deshmukhii" are both words of English names in CoL.
        var words = Words(genera: ["bulbophyllum"], epithets: ["deshmukhii"], rareEnglish: ["bulbophyllum", "deshmukhii"]);
        Assert.True(IsScientific("Bulbophyllum deshmukhii", Taxon("genyorchis macrantha"), words));
    }

    // A taxon with no names known.

    [Fact]
    public void UnknownTaxon_ShapeDecides() {
        Assert.True(IsScientific("Panthera leo", TaxonScientificNames.None));
        Assert.False(IsScientific("Pygmy hippopotamus", TaxonScientificNames.None, Words(english: ["pygmy"])));
        Assert.False(IsScientific("Grey's mudsnake", TaxonScientificNames.None));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void Blank_IsNotScientific(string? name) {
        Assert.False(ScientificNameCheck.IsScientificName(name, Taxon("panthera leo"), NameWordSets.Empty));
    }

    // The word sets.

    [Fact]
    public void Build_AWordInThreeEnglishNamesIsEnglish_InTwoItIsNot() {
        var words = NameWordSets.Build([], [
            "Red-backed Shrike", "Lesser Grey Shrike", "Red-tailed Shrike", "Red Fox",
            "Gobio gobio", "Gudgeon gobio",
        ]);
        Assert.True(words.IsEnglish("shrike"));
        Assert.True(words.IsEnglish("red"));
        Assert.True(words.IsEnglish("red-shrike"));
        Assert.False(words.IsEnglish("red-necked")); // no name has "necked"
        Assert.False(words.IsEnglish("gobio"));
        Assert.Equal(3, NameWordSets.MinCommonNamesForEnglishWord);
        Assert.Equal(10, NameWordSets.MinCommonNamesForEnglishEpithet);
    }

    [Fact]
    public void Store_GivesTheTaxonsNames_AndReadsWordsFromIucnAndCatalogueOfLifeNamesOnly() {
        var conn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        conn.Open();
        using var store = CommonNameStore.OpenFromConnection(conn);
        long AddTaxon(string canonical, string id) => store.InsertOrUpdateTaxon(canonical, canonical, "species", "PLANTAE",
            isExtinct: false, isFossil: false, validityStatus: "valid", primarySource: "iucn", primarySourceId: id);
        var shorea = AddTaxon("shorea ovata", "1");
        store.InsertSynonym(shorea, "rubroshorea ovata", "Rubroshorea ovata", "col");
        var others = new[] { AddTaxon("shorea alba", "2"), AddTaxon("shorea robusta", "3"), AddTaxon("shorea faguetiana", "4") };
        for (var i = 0; i < others.Length; i++) {
            store.InsertCommonName(others[i], $"Pygmy meranti {i}", $"pygmymeranti{i}", "en", i == 0 ? "col" : "iucn", null, false);
            store.InsertCommonName(others[i], $"Sixgill meranti {i}", $"sixgillmeranti{i}", "en", "wikipedia_title", null, false);
        }

        var names = store.GetTaxonScientificNames(shorea);
        Assert.Equal("shorea ovata", names.Canonical);
        Assert.Equal(["rubroshorea ovata"], names.Synonyms);

        var words = store.LoadNameWordSets();
        Assert.True(words.IsGenus("rubroshorea"));
        Assert.True(words.IsEpithet("faguetiana"));
        Assert.True(words.IsEnglish("pygmy"));
        Assert.False(words.IsEnglish("sixgill"));
    }

    [Fact]
    public void Build_TakesGeneraAndEpithetsOnlyFromPlainScientificNames() {
        var words = NameWordSets.Build([
            "panthera leo",
            "panthera leo persica",
            "salmo salar eastern cape breton subpopulation",
            "haplochromis sp. nov. 'yellow'",
            "abies grandis var. idahoensis",
        ], []);
        Assert.True(words.IsGenus("panthera"));
        Assert.True(words.IsEpithet("leo"));
        Assert.True(words.IsEpithet("persica"));
        Assert.True(words.IsEpithet("idahoensis"));
        Assert.False(words.IsEpithet("eastern"));
        Assert.False(words.IsEpithet("salar"));
        Assert.False(words.IsGenus("haplochromis"));
    }
}
