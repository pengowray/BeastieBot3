using System.Linq;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the one ambiguity rule (AmbiguousNames, built by CommonNameStore.QueryAmbiguousNames):
// a name that two or more taxa have is kept by the taxon with the best source priority for it
// (AmbiguousNames.KeeperPriority: Wikipedia title, IUCN main name, taxobox, Wikidata label, other
// IUCN names, other Wikidata names, Catalogue of Life), a taxobox name and then a Wikidata label
// decide between two IUCN main names, a species beats its own subspecies, varieties and
// subpopulations at the same priority, and every other taxon skips the name. `wikipedia generate-lists`, `site build-db` and
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
        store.InsertCommonName(taxon, raw, CommonNameNormalizer.NormalizeForMatching(raw)!, "en", source, null, preferred);

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
        // 2026 data: "Hartmann's mountain zebra" is the Wikipedia title and IUCN main name of
        // Equus zebra ssp. hartmannae, and one of the other IUCN names of Equus zebra. The species
        // only wins ties.
        using var store = OpenInMemory();
        var zebra = AddTaxon(store, "equus zebra", "7960");
        var hartmann = AddTaxon(store, "equus zebra ssp. hartmannae", "7958", rank: "subspecies");
        AddName(store, zebra, "Mountain Zebra", "iucn", preferred: true);
        AddName(store, zebra, "Hartmann's Mountain Zebra", "iucn");
        AddName(store, zebra, "Hartmann's Mountain Zebra", "wikidata");
        AddName(store, hartmann, "Hartmann's mountain zebra", "wikipedia_title", preferred: true);
        AddName(store, hartmann, "Hartmann's Mountain Zebra", "iucn", preferred: true);

        Assert.Equal(hartmann, store.GetAmbiguousNames("en").KeptBy("hartmannsmountainzebra"));
        Assert.Equal("Mountain Zebra", Best(store, zebra));
    }

    [Fact]
    public void AustrianPine_AWikidataLabel_DecidesBetweenTwoIucnMainNames() {
        // 2026 data: "Austrian pine" is IUCN's main name for both Pinus nigra and Pinus nigra subsp.
        // nigra, and the Wikidata label of the subspecies. The label decides between the two IUCN
        // main names, so the subspecies keeps the name, as it did before October 2026, when the
        // label beat IUCN's main name outright. A species beats its own subspecies only when
        // neither has a taxobox name or a Wikidata label to decide.
        using var store = OpenInMemory();
        var pine = AddTaxon(store, "pinus nigra", "1", "PLANTAE");
        var austrian = AddTaxon(store, "pinus nigra subsp. nigra", "2", "PLANTAE", rank: "subspecies");
        AddName(store, pine, "Austrian Pine", "iucn", preferred: true);
        AddName(store, pine, "Austrian pine", "col");
        AddName(store, austrian, "Austrian Pine", "iucn", preferred: true);
        AddName(store, austrian, "Austrian pine", "wikidata_label");

        Assert.Equal(austrian, store.GetAmbiguousNames("en").KeptBy("austrianpine"));
    }

    // Real cases from the store of 3 October 2026 for the keeper order: a Wikipedia title first,
    // then IUCN's main name, then a taxobox name. Each fixture has every taxon that had the name.

    [Fact]
    public void WoodFrog_IsKeptByTheWikipediaTitle_OverAnotherTaxonsIucnMainName() {
        using var store = OpenInMemory();
        var sylvaticus = AddTaxon(store, "lithobates sylvaticus", "58728");
        var daemeli = AddTaxon(store, "papurana daemeli", "41202");
        var asiatica = AddTaxon(store, "rana asiatica", "58549");
        AddName(store, sylvaticus, "Wood frog", "wikipedia_title", preferred: true);
        AddName(store, sylvaticus, "Wood Frog", "iucn", preferred: true);
        AddName(store, sylvaticus, "Wood Frog", "wikidata");
        AddName(store, daemeli, "Wood Frog", "iucn", preferred: true);
        AddName(store, daemeli, "Wood Frog", "wikidata");
        AddName(store, asiatica, "Wood Frog", "iucn");
        AddName(store, asiatica, "Wood Frog", "col");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Equal(sylvaticus, verdicts.KeptBy("woodfrog"));
        Assert.True(verdicts.IsAmbiguousFor(daemeli, "woodfrog"));
    }

    [Fact]
    public void DwarfWhipray_IsKeptByIucnsMainName_OverAnotherTaxonsTaxoboxName() {
        // Brevitrygon walga kept the name from its taxobox until October 2026.
        using var store = OpenInMemory();
        var heterura = AddTaxon(store, "brevitrygon heterura", "104179262");
        var imbricata = AddTaxon(store, "brevitrygon imbricata", "1");
        var walga = AddTaxon(store, "brevitrygon walga", "104176764");
        AddName(store, heterura, "Dwarf Whipray", "iucn", preferred: true);
        AddName(store, heterura, "Dwarf whipray", "col");
        AddName(store, imbricata, "Dwarf Whipray", "col");
        AddName(store, walga, "Dwarf whipray", "wikipedia_taxobox");
        AddName(store, walga, "Dwarf Whipray", "iucn");
        AddName(store, walga, "Dwarf Whipray", "col");
        AddName(store, walga, "Scaly Whipray", "iucn", preferred: true);

        Assert.Equal(heterura, store.GetAmbiguousNames("en").KeptBy("dwarfwhipray"));
        Assert.Equal("Dwarf Whipray", Best(store, heterura));
        Assert.Equal("Scaly Whipray", Best(store, walga));
    }

    [Fact]
    public void YellowfinBream_IsKeptByIucnsMainName_OverAnotherTaxonsWikidataLabel() {
        // Rhabdosargus sarba kept the name from its Wikidata label until October 2026.
        using var store = OpenInMemory();
        var australis = AddTaxon(store, "acanthopagrus australis", "170257");
        var latus = AddTaxon(store, "acanthopagrus latus", "170263");
        var sarba = AddTaxon(store, "rhabdosargus sarba", "170198");
        AddName(store, australis, "Yellowfin Bream", "iucn", preferred: true);
        AddName(store, australis, "Yellowfin Bream", "wikidata");
        AddName(store, latus, "Yellowfin Bream", "iucn");
        AddName(store, sarba, "Yellowfin Bream", "wikidata_label");
        AddName(store, sarba, "Yellow Fin Bream", "iucn");

        Assert.Equal(australis, store.GetAmbiguousNames("en").KeptBy("yellowfinbream"));
    }

    [Fact]
    public void SilverWattle_TwoIucnMainNames_Tie_SoATaxoboxNameNoLongerKeepsIt() {
        // Acacia rivalis kept "Silver wattle" from its taxobox until October 2026. Now Acacia
        // dealbata and Acacia neriifolia both have it as IUCN's main name, neither has it from a
        // taxobox or a Wikidata label, so no taxon keeps it.
        using var store = OpenInMemory();
        var dealbata = AddTaxon(store, "acacia dealbata", "49841387", "PLANTAE");
        var neriifolia = AddTaxon(store, "acacia neriifolia", "200142937", "PLANTAE");
        var oshanesii = AddTaxon(store, "acacia oshanesii", "1", "PLANTAE");
        var rivalis = AddTaxon(store, "acacia rivalis", "198819055", "PLANTAE");
        AddName(store, dealbata, "Silver Wattle", "iucn", preferred: true);
        AddName(store, dealbata, "silver wattle", "wikidata");
        AddName(store, dealbata, "Silver Wattle", "col");
        AddName(store, neriifolia, "Silver Wattle", "iucn", preferred: true);
        AddName(store, neriifolia, "Silver Wattle", "col");
        AddName(store, oshanesii, "silver wattle", "col");
        AddName(store, rivalis, "Silver wattle", "wikipedia_taxobox");
        AddName(store, rivalis, "Silver Wattle", "iucn");
        AddName(store, rivalis, "Creek Wattle", "iucn", preferred: true);

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Null(verdicts.KeptBy("silverwattle"));
        Assert.True(verdicts.IsAmbiguousFor(rivalis, "silverwattle"));
        Assert.Equal("Creek Wattle", Best(store, rivalis));
    }

    [Fact]
    public void Swallowtail_ATaxoboxName_DecidesBetweenTwoIucnMainNames() {
        // Papilio machaon and Centroberyx lineatus both have "Swallowtail" as IUCN's main name;
        // only the fish also has it from its taxobox ("Swallow-tail"), so the fish keeps it, as it
        // did before October 2026.
        using var store = OpenInMemory();
        var machaon = AddTaxon(store, "papilio machaon", "160213");
        var esperanza = AddTaxon(store, "papilio esperanza", "1");
        var lineatus = AddTaxon(store, "centroberyx lineatus", "123356374");
        var botla = AddTaxon(store, "trachinotus botla", "2");
        AddName(store, machaon, "Swallowtail", "iucn", preferred: true);
        AddName(store, machaon, "swallowtail", "wikidata");
        AddName(store, machaon, "Swallowtail", "col");
        AddName(store, esperanza, "Swallowtail", "wikidata");
        AddName(store, esperanza, "Swallowtail", "col");
        AddName(store, lineatus, "Swallowtail", "iucn", preferred: true);
        AddName(store, lineatus, "Swallow-tail", "wikipedia_taxobox");
        AddName(store, lineatus, "Swallow-tail", "col");
        AddName(store, botla, "swallowtail", "col");

        var verdicts = store.GetAmbiguousNames("en");

        Assert.Equal(lineatus, verdicts.KeptBy("swallowtail"));
        Assert.True(verdicts.IsAmbiguousFor(machaon, "swallowtail"));
    }

    [Fact]
    public void MountainAsh_ATaxoboxName_DecidesBetweenTwoIucnMainNames() {
        // Sorbus umbellata and Eucalyptus regnans both have "Mountain ash" as IUCN's main name;
        // only Eucalyptus regnans also has it from its taxobox.
        using var store = OpenInMemory();
        var umbellata = AddTaxon(store, "sorbus umbellata", "79925699", "PLANTAE");
        var regnans = AddTaxon(store, "eucalyptus regnans", "61915636", "PLANTAE");
        var aucuparia = AddTaxon(store, "sorbus aucuparia", "61957558", "PLANTAE");
        var tetracentron = AddTaxon(store, "tetracentron sinense", "114848476", "PLANTAE");
        var alphitonia = AddTaxon(store, "alphitonia excelsa", "1", "PLANTAE");
        AddName(store, umbellata, "Mountain ash", "iucn", preferred: true);
        AddName(store, regnans, "Mountain Ash", "iucn", preferred: true);
        AddName(store, regnans, "Mountain ash", "wikipedia_taxobox");
        AddName(store, regnans, "Mountain ash", "wikidata");
        AddName(store, regnans, "mountain-ash", "col");
        AddName(store, aucuparia, "Mountain ash", "iucn");
        AddName(store, aucuparia, "Mountain Ash", "col");
        AddName(store, tetracentron, "Mountain Ash", "iucn");
        AddName(store, alphitonia, "mountain ash", "wikidata");

        Assert.Equal(regnans, store.GetAmbiguousNames("en").KeptBy("mountainash"));
    }

    [Fact]
    public void WhiteOak_ATaxoboxName_DecidesBetweenTwoIucnMainNames() {
        // Quercus alba and Grevillea baileyana both have "White oak" as IUCN's main name; only
        // Quercus alba also has it from its taxobox.
        using var store = OpenInMemory();
        var alba = AddTaxon(store, "quercus alba", "194051", "PLANTAE");
        var baileyana = AddTaxon(store, "grevillea baileyana", "112646635", "PLANTAE");
        var musgravea = AddTaxon(store, "musgravea heterophylla", "1", "PLANTAE");
        var oleoides = AddTaxon(store, "quercus oleoides", "2", "PLANTAE");
        AddName(store, alba, "White Oak", "iucn", preferred: true);
        AddName(store, alba, "White oak", "wikipedia_taxobox");
        AddName(store, alba, "white oak", "wikidata");
        AddName(store, baileyana, "White Oak", "iucn", preferred: true);
        AddName(store, baileyana, "white-oak", "col");
        AddName(store, musgravea, "White Oak", "wikidata");
        AddName(store, oleoides, "white oak", "col");

        Assert.Equal(alba, store.GetAmbiguousNames("en").KeptBy("whiteoak"));
    }

    [Fact]
    public void WaterOpal_IsKeptByTheNominateSubspeciesIucnMainName_OverTheSpeciesTaxoboxName() {
        // 2026 data: IUCN gives "Water Opal" to Chrysoritis palmus ssp. palmus and no English name
        // to the species, whose taxobox has it. A species beats its own subspecies only at the
        // same priority, so since October 2026 the subspecies keeps the name and the species has
        // no English name left.
        using var store = OpenInMemory();
        var species = AddTaxon(store, "chrysoritis palmus", "1");
        var nominate = AddTaxon(store, "chrysoritis palmus ssp. palmus", "180447782", rank: "subspecies");
        AddName(store, species, "Water opal", "wikipedia_taxobox");
        AddName(store, species, "Water Opal", "col");
        AddName(store, nominate, "Water Opal", "iucn", preferred: true);

        Assert.Equal(nominate, store.GetAmbiguousNames("en").KeptBy("wateropal"));
        Assert.Null(Best(store, species));
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
        // "Mountain lion" from a better source (a Wikipedia title), so the lion keeps it.
        AddName(store, cougar, "Mountain lion", "iucn", preferred: true);
        AddName(store, cougar, "Cougar", "iucn");
        AddName(store, lion, "Mountain lion", "wikipedia_title", preferred: true);

        var best = store.GetBestCommonNameForTaxon(cougar);

        Assert.NotNull(best);
        Assert.Equal("Cougar", best!.RawName);
        Assert.False(best.IsAmbiguous);
    }

    [Fact]
    public void ChooseBest_ForOneTaxonsOwnNames_StillTriesATaxoboxNameAndAWikidataLabelBeforeIucnsMainName() {
        // The keeper order (AmbiguousNames.KeeperPriority) puts IUCN's main name above a taxobox
        // name; the order the chooser tries one taxon's own names (CommonNameStore.GetSourcePriority)
        // does not change.
        var candidates = new[] {
            new CommonNameCandidate("Clouded Rock Iguana", "cloudedrockiguana", "iucn", true),
            new CommonNameCandidate("Cuban rock iguana", "cubanrockiguana", "wikipedia_taxobox", false),
            new CommonNameCandidate("Cuban ground iguana", "cubangroundiguana", "wikidata_label", false),
        };

        Assert.Equal("Cuban rock iguana", CommonNameChooser.ChooseBest(1, candidates, AmbiguousNames.None)!.RawName);
        Assert.Equal("Cuban ground iguana", CommonNameChooser.ChooseBest(1, candidates[0..1].Append(candidates[2]), AmbiguousNames.None)!.RawName);
        Assert.True(CommonNameStore.GetSourcePriority("wikipedia_taxobox", false) < CommonNameStore.GetSourcePriority("iucn", true));
        Assert.True(AmbiguousNames.KeeperPriority("iucn", true) < AmbiguousNames.KeeperPriority("wikipedia_taxobox", false));
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
        // taxon's, and a taxobox name beats a Catalogue of Life name.
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
