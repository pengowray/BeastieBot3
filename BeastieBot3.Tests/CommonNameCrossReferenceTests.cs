using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the CN2 cross-reference seam: a recorded (source, sourceIdentifier) -> taxon link
// round-trips via FindTaxonByCrossReference (the cheapest dedup probe), is idempotent, and
// doesn't collide across sources.
public class CommonNameCrossReferenceTests {
    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string sourceId) =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", "ANIMALIA",
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "iucn", primarySourceId: sourceId);

    [Fact]
    public void CrossReference_RoundTrips_AndIsIdempotent() {
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "panthera leo", "1");

        store.InsertCrossReference(taxon, "wikidata", "Q140");
        store.InsertCrossReference(taxon, "wikidata", "Q140"); // duplicate -> OR IGNORE

        Assert.Equal(taxon, store.FindTaxonByCrossReference("wikidata", "Q140"));
        Assert.Equal(1, store.GetCrossReferenceCount());
    }

    [Fact]
    public void FindTaxonByCrossReference_Null_WhenUnknown() {
        using var store = OpenInMemory();
        AddTaxon(store, "panthera leo", "1");

        Assert.Null(store.FindTaxonByCrossReference("wikidata", "Q999"));
        Assert.Null(store.FindTaxonByCrossReference("col", "Q140"));
    }

    [Fact]
    public void CreateMissingTaxon_IsDedupedBySubsequentSources() {
        // Simulates --create-missing: CoL mints a union taxon for a species absent from IUCN,
        // recording its cross-reference. A later source naming the same species must resolve
        // onto it (by canonical name) and by the recorded cross-reference -- never duplicate it.
        using var store = OpenInMemory();
        var created = store.InsertOrUpdateTaxon(
            "abrocoma boliviensis", "Abrocoma boliviensis", "species", null,
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "col", primarySourceId: "COL-123");
        store.InsertCrossReference(created, "col", "COL-123");

        // A second source (Wikidata) naming the same species resolves by canonical name...
        Assert.Equal(created, store.FindTaxonByScientificName("Abrocoma boliviensis"));
        // ...and the recorded CoL cross-reference still points back to the one taxon.
        Assert.Equal(created, store.FindTaxonByCrossReference("col", "COL-123"));
        Assert.Equal(1, store.GetCrossReferenceCount());
    }

    [Fact]
    public void CrossReference_DistinctPerSource() {
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "panthera tigris", "2");

        store.InsertCrossReference(a, "wikidata", "Q140");
        store.InsertCrossReference(b, "col", "Q140"); // same id string, different source

        Assert.Equal(a, store.FindTaxonByCrossReference("wikidata", "Q140"));
        Assert.Equal(b, store.FindTaxonByCrossReference("col", "Q140"));
    }
}
