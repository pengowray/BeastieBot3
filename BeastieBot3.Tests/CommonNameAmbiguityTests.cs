using System.Linq;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the one ambiguity rule (AmbiguousNames, built by CommonNameStore.QueryAmbiguousNames):
// a name that two or more taxa have is kept by the taxon with the best source priority for it,
// a species beats its own subspecies, varieties and subpopulations at the same priority, and
// every other taxon skips the name. `wikipedia generate-lists`, `site build-db` and
// `common-names report --report ambiguous` all read these verdicts; the report test below fails
// if the report and the lists ever disagree on its fixture.
public class CommonNameAmbiguityTests {
    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string sourceId, string kingdom = "ANIMALIA",
        string rank = "species") =>
        store.InsertOrUpdateTaxon(canonical, canonical, rank, kingdom,
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "iucn", primarySourceId: sourceId);

    private static void AddName(CommonNameStore store, long taxon, string raw, string source, bool preferred = false) =>
        store.InsertCommonName(taxon, raw, raw.ToLowerInvariant().Replace(" ", ""), "en", source, null, preferred);

    private static string? Best(CommonNameStore store, long taxon) => store.GetBestCommonNameForTaxon(taxon)?.RawName;

    [Fact]
    public void Lion_IsKeptByTheSpecies_WhoseWikipediaTitleItIs() {
        // 2026 data: "Lion" is Panthera leo's Wikipedia title and IUCN main name, and one of the
        // IUCN names (not the main one) of Panthera leo ssp. leo. The species fell back to
        // "Lioness" while any second taxon made a name ambiguous.
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "15951");
        var nominate = AddTaxon(store, "panthera leo ssp. leo", "280668607", rank: "subspecies");
        AddName(store, lion, "Lion", "wikipedia_title", preferred: true);
        AddName(store, lion, "Lion", "iucn", preferred: true);
        AddName(store, lion, "Lioness", "col");
        AddName(store, nominate, "Lion", "iucn");
        AddName(store, nominate, "Barbary lion", "wikipedia_title", preferred: true);

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Equal(lion, verdicts.KeptBy("lion"));
        Assert.False(verdicts.IsAmbiguousFor(lion, "lion"));
        Assert.True(verdicts.IsAmbiguousFor(nominate, "lion"));
        Assert.Equal("Lion", Best(store, lion));
        Assert.Equal("Barbary lion", Best(store, nominate));
    }

    [Fact]
    public void Tiger_IsKeptByTheSpecies_OverAnUnrelatedTaxonWithOnlyACatalogueOfLifeName() {
        // 2026 data: Plectropomus oligacanthus has "Tiger" from the Catalogue of Life only;
        // Panthera tigris fell back to "Malayan tiger".
        using var store = OpenInMemory();
        var tiger = AddTaxon(store, "panthera tigris", "15955");
        var grouper = AddTaxon(store, "plectropomus oligacanthus", "132776");
        AddName(store, tiger, "Tiger", "wikipedia_title", preferred: true);
        AddName(store, tiger, "Malayan tiger", "wikidata_label");
        AddName(store, grouper, "Tiger", "col");
        AddName(store, grouper, "Highfin Coral Grouper", "iucn", preferred: true);

        Assert.Equal("Tiger", Best(store, tiger));
        Assert.Equal("Highfin Coral Grouper", Best(store, grouper));
        Assert.True(store.GetAmbiguousNames("en").IsAmbiguousFor(grouper, "tiger"));
    }

    [Fact]
    public void TwoUnrelatedTaxa_AtTheSamePriority_BothSkipTheName() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "sclerophrys regularis", "1");
        var b = AddTaxon(store, "sclerophrys gutturalis", "2");
        AddName(store, a, "African common toad", "iucn");
        AddName(store, b, "African common toad", "iucn");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Null(verdicts.KeptBy("africancommontoad"));
        Assert.True(verdicts.IsAmbiguousFor(a, "africancommontoad"));
        Assert.True(verdicts.IsAmbiguousFor(b, "africancommontoad"));
        Assert.Null(Best(store, a));
        Assert.Equal("African common toad", store.GetBestCommonNameForTaxon(a, allowAmbiguous: true)!.RawName);
    }

    [Fact]
    public void AnOldIucnIdAndItsCurrentId_BothKeepTheTitleTheyShare() {
        // 2026 data: the store has Arthroleptella bicolor under its old IUCN id 58057 and its
        // current id 121376651, and both have the Wikipedia title "Bainskloof moss frog". The tie
        // made the name ambiguous for both: the current taxon showed no common name and the old
        // id showed "Bainskloof chirping frog".
        using var store = OpenInMemory();
        var old = AddTaxon(store, "arthroleptella bicolor", "58057");
        var current = AddTaxon(store, "arthroleptella bicolor", "121376651");
        AddName(store, old, "Bainskloof moss frog", "wikipedia_title", preferred: true);
        AddName(store, old, "Bainskloof chirping frog", "col");
        AddName(store, current, "Bainskloof moss frog", "wikipedia_title", preferred: true);

        var verdicts = store.GetAmbiguousNames("en");

        Assert.False(verdicts.IsShared("bainskloofmossfrog"));
        Assert.False(verdicts.IsAmbiguousFor(old, "bainskloofmossfrog"));
        Assert.False(verdicts.IsAmbiguousFor(current, "bainskloofmossfrog"));
        Assert.Equal("Bainskloof moss frog", Best(store, current));
        Assert.Equal("Bainskloof moss frog", Best(store, old));
    }

    [Fact]
    public void AnOldIucnIdAndItsCurrentId_KeepAName_TogetherAgainstAnotherTaxon() {
        // Both ids of Arthroleptella bicolor keep the name over an unrelated taxon with a
        // lower-priority source; an unrelated taxon at the same priority still makes it ambiguous.
        using var store = OpenInMemory();
        var old = AddTaxon(store, "arthroleptella bicolor", "58057");
        var current = AddTaxon(store, "arthroleptella bicolor", "121376651");
        var other = AddTaxon(store, "arthroleptella landdrosia", "121377639");
        var rival = AddTaxon(store, "arthroleptella drewesii", "58058");
        AddName(store, old, "Moss frog", "wikipedia_title", preferred: true);
        AddName(store, current, "Moss frog", "wikipedia_title", preferred: true);
        AddName(store, other, "Moss frog", "col");
        AddName(store, old, "Chirping frog", "iucn", preferred: true);
        AddName(store, current, "Chirping frog", "iucn", preferred: true);
        AddName(store, rival, "Chirping frog", "iucn", preferred: true);

        var verdicts = store.GetAmbiguousNames("en");

        Assert.True(verdicts.Keeps(old, "mossfrog"));
        Assert.True(verdicts.Keeps(current, "mossfrog"));
        Assert.Equal(old, verdicts.KeptBy("mossfrog"));
        Assert.True(verdicts.IsAmbiguousFor(other, "mossfrog"));
        Assert.Null(verdicts.KeptBy("chirpingfrog"));
        Assert.True(verdicts.IsAmbiguousFor(old, "chirpingfrog"));
        Assert.True(verdicts.IsAmbiguousFor(current, "chirpingfrog"));
        Assert.True(verdicts.IsAmbiguousFor(rival, "chirpingfrog"));
    }

    [Fact]
    public void ThePreferredIucnName_BeatsAnotherTaxonsOtherIucnName() {
        using var store = OpenInMemory();
        var purple = AddTaxon(store, "porphyrio martinicus", "1");
        var swamphen = AddTaxon(store, "porphyrio porphyrio", "2");
        AddName(store, purple, "Purple Gallinule", "iucn", preferred: true);
        AddName(store, swamphen, "Purple Gallinule", "iucn");
        AddName(store, swamphen, "Purple Swamphen", "iucn", preferred: true);

        Assert.Equal(purple, store.GetAmbiguousNames("en").KeptBy("purplegallinule"));
        Assert.Equal("Purple Gallinule", Best(store, purple));
    }

    [Fact]
    public void AtTheSamePriority_ASpecies_BeatsItsOwnSubspecies() {
        // 2026 data: IUCN gives the nominate subspecies the species' main name.
        using var store = OpenInMemory();
        var species = AddTaxon(store, "lagothrix lagothricha", "1");
        var nominate = AddTaxon(store, "lagothrix lagothricha ssp. lagothricha", "2", rank: "subspecies");
        AddName(store, species, "Common Woolly Monkey", "iucn", preferred: true);
        AddName(store, species, "lugens", "col");
        AddName(store, nominate, "Common Woolly Monkey", "iucn", preferred: true);

        Assert.Equal(species, store.GetAmbiguousNames("en").KeptBy("commonwoollymonkey"));
        Assert.Equal("Common Woolly Monkey", Best(store, species));
        Assert.Null(Best(store, nominate));
    }

    [Fact]
    public void AtTheSamePriority_ASpecies_BeatsItsOwnVarietyAndSubpopulation() {
        // The store ranks a subpopulation as a species; its name begins with the species' name.
        using var store = OpenInMemory();
        var pine = AddTaxon(store, "pinus nigra", "1", "PLANTAE");
        var variety = AddTaxon(store, "pinus nigra var. caramanica", "2", "PLANTAE", rank: "variety");
        var whale = AddTaxon(store, "megaptera novaeangliae", "3");
        var subpopulation = AddTaxon(store, "megaptera novaeangliae arabian sea subpopulation", "4");
        AddName(store, pine, "Black pine", "iucn", preferred: true);
        AddName(store, variety, "Black pine", "iucn", preferred: true);
        AddName(store, whale, "Humpback Whale", "iucn", preferred: true);
        AddName(store, subpopulation, "Humpback Whale", "iucn", preferred: true);

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Equal(pine, verdicts.KeptBy("blackpine"));
        Assert.Equal(whale, verdicts.KeptBy("humpbackwhale"));
    }

    [Fact]
    public void ASubspecies_KeepsTheName_WhenItHasItFromABetterSourceThanItsSpecies() {
        // 2026 data: "Austrian pine" is the Wikidata label of Pinus nigra subsp. nigra and an IUCN
        // name of Pinus nigra. The species only wins ties.
        using var store = OpenInMemory();
        var pine = AddTaxon(store, "pinus nigra", "1", "PLANTAE");
        var austrian = AddTaxon(store, "pinus nigra subsp. nigra", "2", "PLANTAE", rank: "subspecies");
        AddName(store, pine, "Austrian Pine", "iucn", preferred: true);
        AddName(store, pine, "European black pine", "iucn");
        AddName(store, austrian, "Austrian pine", "wikidata_label");

        Assert.Equal(austrian, store.GetAmbiguousNames("en").KeptBy("austrianpine"));
        Assert.Equal("European black pine", Best(store, pine));
    }

    [Fact]
    public void ASpeciesTiedWithItsSubspecies_AndAnUnrelatedTaxon_DoesNotKeepTheName() {
        using var store = OpenInMemory();
        var species = AddTaxon(store, "oreochromis placidus", "1");
        var subspecies = AddTaxon(store, "oreochromis placidus ssp. rovumae", "2", rank: "subspecies");
        var other = AddTaxon(store, "oreochromis mossambicus", "3");
        AddName(store, species, "Black Tilapia", "iucn", preferred: true);
        AddName(store, subspecies, "Black Tilapia", "iucn", preferred: true);
        AddName(store, other, "Black Tilapia", "iucn", preferred: true);

        Assert.Null(store.GetAmbiguousNames("en").KeptBy("blacktilapia"));
    }

    [Theory]
    [InlineData("panthera leo", "panthera leo ssp. leo", true)]
    [InlineData("pinus nigra", "pinus nigra subsp. nigra", true)]
    [InlineData("pinus nigra", "pinus nigra var. caramanica", true)]
    [InlineData("megaptera novaeangliae", "megaptera novaeangliae arabian sea subpopulation", true)]
    [InlineData("panthera leo", "panthera leo", false)]
    [InlineData("panthera leo", "panthera leonis", false)]
    [InlineData("panthera leo ssp. leo", "panthera leo ssp. leo x", false)]
    [InlineData("panthera leo ssp. leo", "panthera leo", false)]
    [InlineData("panthera", "panthera leo", false)]
    public void IsSpeciesOf_MatchesATwoWordNameThatTheOtherNameBeginsWith(string species, string other, bool expected) {
        Assert.Equal(expected, AmbiguousNames.IsSpeciesOf(species, other));
    }

    [Fact]
    public void NameOnTwoTaxa_InDifferentKingdoms_IsShared() {
        using var store = OpenInMemory();
        var tree = AddTaxon(store, "pochota fendleri", "1", "PLANTAE");
        var moth = AddTaxon(store, "conistra vaccinii", "2", "ANIMALIA");
        AddName(store, tree, "Chestnut", "iucn");
        AddName(store, moth, "Chestnut", "col");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Contains("chestnut", verdicts.Names);
        Assert.Equal(tree, verdicts.KeptBy("chestnut"));
    }

    [Fact]
    public void NameOnTwoTaxa_ThatShareASynonym_IsShared() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "sclerophrys regularis", "1");
        var b = AddTaxon(store, "sclerophrys gutturalis", "2");
        store.InsertSynonym(a, "bufo regularis", "Bufo regularis", "col");
        store.InsertSynonym(b, "bufo regularis", "Bufo regularis", "col");
        AddName(store, a, "African common toad", "iucn");
        AddName(store, b, "African common toad", "iucn");

        Assert.True(store.AreSynonyms(a, b));
        Assert.Contains("africancommontoad", store.GetAmbiguousNames("en").Names);
    }

    [Fact]
    public void NameOnOneTaxon_FromSeveralSources_IsNotShared() {
        using var store = OpenInMemory();
        var adder = AddTaxon(store, "vipera berus", "1");
        AddName(store, adder, "Adder", "iucn");
        AddName(store, adder, "Adder", "wikidata");
        AddName(store, adder, "Adder", "col");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Empty(verdicts.Names);
        Assert.False(verdicts.IsShared("adder"));
        Assert.False(verdicts.IsAmbiguousFor(adder, "adder"));
    }

    [Fact]
    public void AmbiguousReport_GivesTheSameVerdictsTheListsUse() {
        using var store = OpenInMemory();
        var tree = AddTaxon(store, "pochota fendleri", "1", "PLANTAE");
        var moth = AddTaxon(store, "conistra vaccinii", "2");
        var lion = AddTaxon(store, "panthera leo", "3");
        var cougar = AddTaxon(store, "puma concolor", "4");
        AddName(store, tree, "Chestnut", "iucn");
        AddName(store, moth, "Chestnut", "col");
        AddName(store, lion, "Lion", "iucn");
        AddName(store, lion, "Mountain lion", "wikidata");
        AddName(store, cougar, "Mountain lion", "iucn");
        AddName(store, cougar, "Cougar", "wikipedia_title");

        var usedByLists = store.GetAmbiguousNames("en");
        var listedByReport = store.GetAmbiguousCommonNames();

        Assert.Equal(new[] { "chestnut", "mountainlion" }, usedByLists.Names.OrderBy(n => n));
        Assert.Equal(usedByLists.Names.OrderBy(n => n), listedByReport.Names.OrderBy(n => n));
        foreach (var name in usedByLists.Names) {
            Assert.Equal(usedByLists.KeptBy(name), listedByReport.KeptBy(name));
        }
        Assert.Equal(cougar, listedByReport.KeptBy("mountainlion"));
    }

    [Fact]
    public void BestName_SkipsANameAnotherTaxonKeeps_AndTakesTheNextOne() {
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        // The IUCN main name is the cougar's first name in priority order, but the lion has
        // "Mountain lion" from a better source, so the lion keeps it.
        AddName(store, cougar, "Mountain lion", "iucn", preferred: true);
        AddName(store, cougar, "Cougar", "iucn");
        AddName(store, lion, "Mountain lion", "wikipedia_taxobox");

        var best = store.GetBestCommonNameForTaxon(cougar);

        Assert.NotNull(best);
        Assert.Equal("Cougar", best!.RawName);
        Assert.False(best.IsAmbiguous);
    }

    [Fact]
    public void ChooseBest_ReadsTheVerdictForTheTaxonItIsGiven() {
        // site build-db calls the chooser with the store's taxa.id; a different id gets the other
        // taxon's verdict.
        using var store = OpenInMemory();
        var tiger = AddTaxon(store, "panthera tigris", "15955");
        var grouper = AddTaxon(store, "plectropomus oligacanthus", "132776");
        AddName(store, tiger, "Tiger", "wikipedia_title", preferred: true);
        AddName(store, grouper, "Tiger", "col");
        var verdicts = store.GetAmbiguousNames("en");
        var candidates = new[] {
            new CommonNameCandidate("Tiger", "tiger", "wikipedia_title", true),
            new CommonNameCandidate("Malayan tiger", "malayantiger", "wikidata_label", false),
        };

        Assert.Equal("Tiger", CommonNameChooser.ChooseBest(tiger, candidates, verdicts)!.RawName);
        Assert.Equal("Malayan tiger", CommonNameChooser.ChooseBest(grouper, candidates, verdicts)!.RawName);
    }

    [Fact]
    public void AmbiguousReport_WithAKingdom_CountsOnlyThatKingdomsTaxa() {
        // A name shared by a plant and an animal is shared for the lists, but is not shared
        // within either kingdom, so `--kingdom` leaves it out of the report.
        using var store = OpenInMemory();
        var tree = AddTaxon(store, "pochota fendleri", "1", "PLANTAE");
        var moth = AddTaxon(store, "conistra vaccinii", "2", "ANIMALIA");
        var oak = AddTaxon(store, "quercus robur", "3", "PLANTAE");
        var holmOak = AddTaxon(store, "quercus ilex", "4", "PLANTAE");
        AddName(store, tree, "Chestnut", "iucn");
        AddName(store, moth, "Chestnut", "col");
        AddName(store, oak, "Oak", "iucn");
        AddName(store, holmOak, "Oak", "col");

        Assert.Equal(new[] { "oak" }, store.GetAmbiguousCommonNames(kingdom: "PLANTAE").Names);
        // The --kingdom help gives "Plantae"; the store holds kingdoms in upper case.
        Assert.Equal(new[] { "oak" }, store.GetAmbiguousCommonNames(kingdom: "Plantae").Names);
        Assert.Empty(store.GetAmbiguousCommonNames(kingdom: "ANIMALIA").Names);
        Assert.Equal(new[] { "chestnut", "oak" }, store.GetAmbiguousNames("en").Names.OrderBy(n => n));
    }

    [Fact]
    public void AmbiguousReport_WithAKingdom_ShowsTheVerdictTheListsUse() {
        // An animal with "Oak" as its Wikipedia title keeps the name, so neither plant uses it, even
        // though the plants alone would leave the IUCN holder as the keeper.
        using var store = OpenInMemory();
        var oak = AddTaxon(store, "quercus robur", "1", "PLANTAE");
        var holmOak = AddTaxon(store, "quercus ilex", "2", "PLANTAE");
        var moth = AddTaxon(store, "oakmothus testus", "3", "ANIMALIA");
        AddName(store, oak, "Oak", "iucn");
        AddName(store, holmOak, "Oak", "col");
        AddName(store, moth, "Oak", "wikipedia_title", preferred: true);

        var (names, verdicts) = CommonNameReportCommand.AmbiguousReportScope(store, "Plantae");

        Assert.Equal(new[] { "oak" }, names);
        Assert.Equal(moth, verdicts.KeptBy("oak"));
        Assert.Equal(oak, store.GetAmbiguousCommonNames(kingdom: "Plantae").KeptBy("oak"));
    }

    [Fact]
    public void AmbiguousReport_ListsTheMostSharedNamesFirst() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "puma concolor", "2");
        var c = AddTaxon(store, "panthera onca", "3");
        AddName(store, a, "Mountain lion", "col");
        AddName(store, b, "Mountain lion", "iucn");
        AddName(store, a, "Big cat", "col");
        AddName(store, b, "Big cat", "col");
        AddName(store, c, "Big cat", "col");

        Assert.Equal(new[] { "bigcat", "mountainlion" }, store.GetAmbiguousCommonNames().Names);
    }

    [Fact]
    public void AmbiguousNames_AreWorkedOutAgain_AfterASourceIsReplaced() {
        // `common-names aggregate --replace` purges a source in the same store instance that then
        // prints the shared-name count, so the cached verdicts must not outlive the purge.
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        AddName(store, lion, "Mountain lion", "col");
        AddName(store, cougar, "Mountain lion", "iucn");
        Assert.Equal(1, store.GetAmbiguousNames("en").Count);

        store.PurgeSource("col");

        Assert.Equal(0, store.GetAmbiguousNames("en").Count);
    }

    [Fact]
    public void ListsProvider_CountsTheSharedNames() {
        // generate-lists prints this count under the store path.
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        AddName(store, lion, "Mountain lion", "col");
        AddName(store, cougar, "Mountain lion", "iucn");
        AddName(store, cougar, "Cougar", "iucn");

        using var provider = new StoreBackedCommonNameProvider(store);
        using var allowing = new StoreBackedCommonNameProvider(store, allowAmbiguous: true);

        Assert.Equal(1, provider.AmbiguousNameCount);
        Assert.Equal(0, allowing.AmbiguousNameCount);
        Assert.Null(WikipediaListCommand.AmbiguousNamesLine(0));
        Assert.Contains("common-names report --report ambiguous", WikipediaListCommand.AmbiguousNamesLine(1));
    }

    [Fact]
    public void JunkName_DoesNotMakeAGoodNameAmbiguous() {
        // "Rooiberg girdled lizard)" is cut off at a bracket; its key is the good name's key, but a
        // junk name is not counted as having the name, so the other taxon keeps it.
        using var store = OpenInMemory();
        var junkHolder = AddTaxon(store, "cordylus imkeae", "1");
        var other = AddTaxon(store, "cordylus otherus", "2");
        store.InsertCommonName(junkHolder, "Rooiberg girdled lizard)", "rooiberggirdledlizard", "en", "wikidata", null, false);
        store.InsertCommonName(other, "Rooiberg girdled lizard", "rooiberggirdledlizard", "en", "col", null, false);

        Assert.Empty(store.GetAmbiguousNames("en").Names);
        Assert.Equal("Rooiberg girdled lizard", Best(store, other));
        Assert.Null(Best(store, junkHolder));
    }

    [Fact]
    public void RepairedName_CountsUnderItsRepairedKey() {
        // The taxobox name repairs to "Sunda slow loris", so it is the same name as the other
        // taxon's, and the taxobox (priority 2) beats the Catalogue of Life (priority 7).
        using var store = OpenInMemory();
        var loris = AddTaxon(store, "nycticebus coucang", "1");
        var other = AddTaxon(store, "nycticebus otherus", "2");
        store.InsertCommonName(loris, "Sunda slow loris{sfn|Groves|2005|p=122}", "sundaslowlorissfngroves2005p122",
            "en", "wikipedia_taxobox", null, false);
        AddName(store, other, "Sunda slow loris", "col");
        AddName(store, other, "Other loris", "col");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Equal(loris, verdicts.KeptBy("sundaslowloris"));
        Assert.Equal("Other loris", Best(store, other));
    }
}
