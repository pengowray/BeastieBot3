using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins CommonNameStore.AreSynonyms (CN3): two taxa that share a scientific name — via one's
// canonical name matching the other's recorded synonym — must be recognised as synonyms so
// conflict detection does not record them as an ambiguous-name conflict. Both canonical_name
// and synonyms.normalized_name are stored pre-normalized, so the comparison is a plain equality.
public class CommonNameSynonymTests {
    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string source, string sourceId) =>
        store.InsertOrUpdateTaxon(
            canonicalName: canonical,
            originalName: canonical,
            rank: "species",
            kingdom: "ANIMALIA",
            isExtinct: false,
            isFossil: false,
            validityStatus: "valid",
            primarySource: source,
            primarySourceId: sourceId);

    [Fact]
    public void AreSynonyms_True_WhenCanonicalMatchesOtherSynonym() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "iucn", "1");
        var b = AddTaxon(store, "felis leo", "iucn", "2");
        // b carries "panthera leo" as a synonym → a and b name the same animal.
        store.InsertSynonym(b, "panthera leo", "Panthera leo", "col");

        Assert.True(store.AreSynonyms(a, b));
        Assert.True(store.AreSynonyms(b, a)); // symmetric
    }

    [Fact]
    public void AreSynonyms_True_WhenTheyShareASynonym() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "aaa one", "iucn", "1");
        var b = AddTaxon(store, "bbb two", "iucn", "2");
        store.InsertSynonym(a, "shared name", "Shared name", "col");
        store.InsertSynonym(b, "shared name", "Shared name", "wikidata");

        Assert.True(store.AreSynonyms(a, b));
    }

    [Fact]
    public void AreSynonyms_False_WhenUnrelated() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "iucn", "1");
        var c = AddTaxon(store, "canis lupus", "iucn", "3");

        Assert.False(store.AreSynonyms(a, c));
    }

    [Fact]
    public void AreSynonyms_True_ForSameTaxon() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "iucn", "1");

        Assert.True(store.AreSynonyms(a, a));
    }
}
