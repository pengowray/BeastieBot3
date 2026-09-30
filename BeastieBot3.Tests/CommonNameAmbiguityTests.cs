using System.Linq;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the rule `wikipedia generate-lists` uses to skip ambiguous common names, and that
// `common-names report --report ambiguous` lists the same names. The workflow page and the
// generate-lists help say both of these. Both read CommonNameStore.QueryAmbiguousNames; the
// report test below fails if the two sets ever differ on its fixture.
public class CommonNameAmbiguityTests {
    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string sourceId, string kingdom = "ANIMALIA") =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", kingdom,
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "iucn", primarySourceId: sourceId);

    private static void AddName(CommonNameStore store, long taxon, string raw, string source, bool preferred = false) =>
        store.InsertCommonName(taxon, raw, raw.ToLowerInvariant().Replace(" ", ""), "en", source, null, preferred);

    [Fact]
    public void NameOnTwoTaxa_InDifferentKingdoms_IsAmbiguous() {
        using var store = OpenInMemory();
        var tree = AddTaxon(store, "pochota fendleri", "1", "PLANTAE");
        var moth = AddTaxon(store, "conistra vaccinii", "2", "ANIMALIA");
        AddName(store, tree, "Chestnut", "iucn");
        AddName(store, moth, "Chestnut", "col");

        Assert.Contains("chestnut", store.GetAmbiguousNames("en"));
    }

    [Fact]
    public void NameOnTwoTaxa_ThatShareASynonym_IsAmbiguous() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "sclerophrys regularis", "1");
        var b = AddTaxon(store, "sclerophrys gutturalis", "2");
        store.InsertSynonym(a, "bufo regularis", "Bufo regularis", "col");
        store.InsertSynonym(b, "bufo regularis", "Bufo regularis", "col");
        AddName(store, a, "African common toad", "iucn");
        AddName(store, b, "African common toad", "iucn");

        Assert.True(store.AreSynonyms(a, b));
        Assert.Contains("africancommontoad", store.GetAmbiguousNames("en"));
    }

    [Fact]
    public void NameOnOneTaxon_FromSeveralSources_IsNotAmbiguous() {
        using var store = OpenInMemory();
        var adder = AddTaxon(store, "vipera berus", "1");
        AddName(store, adder, "Adder", "iucn");
        AddName(store, adder, "Adder", "wikidata");
        AddName(store, adder, "Adder", "col");

        Assert.Empty(store.GetAmbiguousNames("en"));
    }

    [Fact]
    public void AmbiguousReport_ListsTheSameNamesTheListsSkip() {
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

        var skippedByLists = store.GetAmbiguousNames("en").OrderBy(n => n).ToList();
        var listedByReport = store.GetAmbiguousCommonNames(limit: null).OrderBy(n => n).ToList();

        Assert.Equal(new[] { "chestnut", "mountainlion" }, skippedByLists);
        Assert.Equal(skippedByLists, listedByReport);
    }

    [Fact]
    public void BestName_SkipsAnAmbiguousName_AndTakesTheNextOne() {
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        // wikipedia_title is the first source in priority order, so "Mountain lion" would win
        // for the cougar if it were not ambiguous.
        AddName(store, cougar, "Mountain lion", "wikipedia_title");
        AddName(store, cougar, "Cougar", "iucn", preferred: true);
        AddName(store, lion, "Mountain lion", "col");

        var best = store.GetBestCommonNameForTaxon(cougar);

        Assert.NotNull(best);
        Assert.Equal("Cougar", best!.RawName);
        Assert.False(best.IsAmbiguous);
    }

    [Fact]
    public void BestName_IsNull_WhenEveryNameIsAmbiguous() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "sclerophrys regularis", "1");
        var b = AddTaxon(store, "sclerophrys gutturalis", "2");
        AddName(store, a, "African common toad", "iucn");
        AddName(store, b, "African common toad", "iucn");

        Assert.Null(store.GetBestCommonNameForTaxon(a));
        Assert.Equal("African common toad", store.GetBestCommonNameForTaxon(a, allowAmbiguous: true)!.RawName);
    }

    [Fact]
    public void AmbiguousReport_WithAKingdom_CountsOnlyThatKingdomsTaxa() {
        // A name shared by a plant and an animal is skipped by the lists, but is not ambiguous
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

        Assert.Equal(new[] { "oak" }, store.GetAmbiguousCommonNames(limit: null, kingdom: "PLANTAE"));
        // The --kingdom help gives "Plantae"; the store holds kingdoms in upper case.
        Assert.Equal(new[] { "oak" }, store.GetAmbiguousCommonNames(limit: null, kingdom: "Plantae"));
        Assert.Empty(store.GetAmbiguousCommonNames(limit: null, kingdom: "ANIMALIA"));
        Assert.Equal(new[] { "chestnut", "oak" }, store.GetAmbiguousNames("en").OrderBy(n => n));
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

        Assert.Equal(new[] { "bigcat", "mountainlion" }, store.GetAmbiguousCommonNames(limit: null));
        Assert.Equal(new[] { "bigcat" }, store.GetAmbiguousCommonNames(limit: 1));
    }

    [Fact]
    public void AmbiguousNames_AreWorkedOutAgain_AfterASourceIsReplaced() {
        // `common-names aggregate --replace` purges a source in the same store instance that then
        // prints the ambiguous-name count, so the cached set must not outlive the purge.
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        AddName(store, lion, "Mountain lion", "col");
        AddName(store, cougar, "Mountain lion", "iucn");
        Assert.Single(store.GetAmbiguousNames("en"));

        store.PurgeSource("col");

        Assert.Empty(store.GetAmbiguousNames("en"));
    }

    [Fact]
    public void ListsProvider_CountsTheNamesItSkips() {
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
}
