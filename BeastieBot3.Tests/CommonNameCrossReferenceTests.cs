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
    public void GetCommonNamesByNormalized_ReadsEveryColumnAtTheRightOrdinal() {
        // Guards the display_name removal: the SELECT/reader ordinals were shifted by hand, so
        // assert every field round-trips (a wrong ordinal would surface as a mismatched value).
        using var store = OpenInMemory();
        var taxon = store.InsertOrUpdateTaxon(
            canonicalName: "panthera leo", originalName: "Panthera leo", rank: "species", kingdom: "ANIMALIA",
            isExtinct: true, isFossil: false, validityStatus: "valid", primarySource: "iucn", primarySourceId: "1");
        store.InsertCommonName(taxon, "Lion", "lion", "en", "iucn", "src-1", isPreferred: true);

        var records = store.GetCommonNamesByNormalized("lion", "en");
        var r = Assert.Single(records);
        Assert.Equal(taxon, r.TaxonId);
        Assert.Equal("Lion", r.RawName);
        Assert.Equal("lion", r.NormalizedName);
        Assert.Equal("en", r.Language);
        Assert.Equal("iucn", r.Source);
        Assert.Equal("src-1", r.SourceIdentifier);
        Assert.True(r.IsPreferred);
        Assert.Equal("panthera leo", r.TaxonCanonicalName);
        Assert.Equal("ANIMALIA", r.TaxonKingdom);
        Assert.Equal("valid", r.TaxonValidityStatus);
        Assert.True(r.TaxonIsExtinct);
        Assert.False(r.TaxonIsFossil);
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

    private static string WikidataItem(string id, string scientificName) =>
        "{\"entities\":{\"" + id + "\":{\"claims\":{\"P225\":[{\"mainsnak\":{\"datavalue\":{\"value\":\"" + scientificName + "\"}}}]}}}}";

    [Fact]
    public void WikidataItem_GoesToTheTaxonOfItsCurrentIucnId_NotAnEarlierRunsTaxon() {
        // An earlier aggregate run without --replace recorded Q1 for the lumped taxon; the item's
        // P627 now names the split species.
        using var store = OpenInMemory();
        var lumped = AddTaxon(store, "aus bus", "100");
        var split = AddTaxon(store, "aus cus", "200");
        store.InsertCrossReference(lumped, "wikidata", "Q1");

        var (taxonId, matchType, _) = CommonNameAggregateCommand.ResolveWikidataTaxon(store, "Q1", "999,200", WikidataItem("Q1", "Aus bus"));

        Assert.Equal(split, taxonId);
        Assert.Equal("exact", matchType);
    }

    [Fact]
    public void WikidataItem_WithoutAKnownIucnId_UsesTheEarlierRunsTaxon_ThenItsScientificName() {
        using var store = OpenInMemory();
        var recorded = AddTaxon(store, "aus bus", "100");
        var named = AddTaxon(store, "aus cus", "200");
        var synonymised = AddTaxon(store, "aus dus", "300");
        store.InsertCrossReference(recorded, "wikidata", "Q1");
        store.InsertSynonym(synonymised, "aus eus", "Aus eus", "iucn");

        Assert.Equal((recorded, "exact"), Found(CommonNameAggregateCommand.ResolveWikidataTaxon(store, "Q1", "999", WikidataItem("Q1", "Aus cus"))));
        Assert.Equal((named, "exact"), Found(CommonNameAggregateCommand.ResolveWikidataTaxon(store, "Q2", null, WikidataItem("Q2", "Aus cus"))));
        Assert.Equal((synonymised, "synonym"), Found(CommonNameAggregateCommand.ResolveWikidataTaxon(store, "Q3", null, WikidataItem("Q3", "Aus eus"))));

        var (missing, _, createName) = CommonNameAggregateCommand.ResolveWikidataTaxon(store, "Q4", null, WikidataItem("Q4", "Aus fus"));
        Assert.Null(missing);
        Assert.Equal("Aus fus", createName);
    }

    private static (long?, string) Found((long? TaxonId, string MatchType, string? CreateName) result) => (result.TaxonId, result.MatchType);
}
