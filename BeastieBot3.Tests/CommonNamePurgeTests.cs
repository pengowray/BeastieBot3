using System;
using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins `common-names aggregate --replace`: a purge removes exactly what one source contributed,
// leaves every other source's rows alone, and only deletes taxa that source minted once nothing
// refers to them any more.
public class CommonNamePurgeTests {
    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string source, string sourceId) =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", "ANIMALIA",
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: source, primarySourceId: sourceId);

    private static void AddName(CommonNameStore store, long taxonId, string name, string source) =>
        store.InsertCommonName(taxonId, name, name.ToLowerInvariant(), "en", source, "id-" + name, false);

    [Fact]
    public void PurgeSource_RemovesOnlyThatSourcesRows() {
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "panthera leo", "iucn", "15951");
        AddName(store, taxon, "Lion", "iucn");
        AddName(store, taxon, "African Lion", "col");
        store.InsertSynonym(taxon, "felis leo", "Felis leo", "col");
        store.InsertSynonym(taxon, "panthera leo melanochaita", "Panthera leo melanochaita", "iucn");
        store.InsertCrossReference(taxon, "col", "OLD-COL-ID");
        store.InsertCrossReference(taxon, "wikidata", "Q140");

        var removed = store.PurgeSource("col");

        Assert.Equal(1, removed.CommonNames);
        Assert.Equal(1, removed.Synonyms);
        Assert.Equal(1, removed.CrossReferences);
        Assert.Equal(0, removed.Taxa);

        // The stale CoL id no longer resolves, so a re-import cannot attach a new release's
        // vernacular to whatever taxon that id used to mean.
        Assert.Null(store.FindTaxonByCrossReference("col", "OLD-COL-ID"));
        Assert.Equal(taxon, store.FindTaxonByCrossReference("wikidata", "Q140"));
        Assert.Null(store.FindTaxonBySynonym("felis leo"));
        Assert.Equal(taxon, store.FindTaxonBySynonym("panthera leo melanochaita"));

        var names = store.GetCommonNamesForTaxon(taxon);
        Assert.Single(names);
        Assert.Equal("iucn", names[0].Source);
    }

    [Fact]
    public void PurgeSource_DropsTaxaTheSourceMintedAndNothingElseReferences() {
        using var store = OpenInMemory();
        var minted = AddTaxon(store, "abrocoma boliviensis", "col", "COL-123");
        AddName(store, minted, "Bolivian Chinchilla Rat", "col");
        store.InsertCrossReference(minted, "col", "COL-123");

        var shared = AddTaxon(store, "puma concolor", "col", "COL-456");
        AddName(store, shared, "Cougar", "col");
        AddName(store, shared, "Puma", "wikidata");

        var removed = store.PurgeSource("col");

        Assert.Equal(1, removed.Taxa);
        Assert.Null(store.FindTaxonByCanonicalName("abrocoma boliviensis"));
        // Still named by Wikidata, so it stays.
        Assert.Equal(shared, store.FindTaxonByCanonicalName("puma concolor"));
    }

    [Fact]
    public void PurgeSource_KeepsIucnSkeletonTaxaThatHaveNoNamesYet() {
        // `common-names init` seeds hub taxa from IUCN; a species with no common name anywhere
        // has no child rows at all, and must survive a purge of any other source.
        using var store = OpenInMemory();
        var bare = AddTaxon(store, "hypothetical species", "iucn", "999");

        store.PurgeSource("col");

        Assert.Equal(bare, store.FindTaxonByCanonicalName("hypothetical species"));
    }

    [Fact]
    public void PurgeSource_CanKeepSynonyms() {
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "panthera leo", "iucn", "15951");
        AddName(store, taxon, "Lion", "iucn");
        store.InsertSynonym(taxon, "felis leo", "Felis leo", "iucn");

        var removed = store.PurgeSource("iucn", includeSynonyms: false);

        Assert.Equal(1, removed.CommonNames);
        Assert.Equal(0, removed.Synonyms);
        Assert.Equal(taxon, store.FindTaxonBySynonym("felis leo"));
    }

    [Fact]
    public void PurgeSource_LeavesConstructedSynonymsAlone() {
        // `init` mints these from the hub's own names; no aggregation source owns them.
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "panthera leo", "iucn", "15951");
        store.InsertSynonym(taxon, "panthera (panthera) leo", "Panthera (Panthera) leo", "constructed", "subgenus_variant");

        store.PurgeSource("iucn");
        store.PurgeSource("col");

        Assert.Equal(taxon, store.FindTaxonBySynonym("panthera (panthera) leo"));
    }

    [Fact]
    public void PurgeSource_ClearsTheConflictList() {
        // Conflict rows name specific common-name rows, so a purge leaves them describing names
        // that may no longer exist; detect-conflicts rebuilds them.
        using var store = OpenInMemory();
        var a = AddTaxon(store, "panthera leo", "iucn", "15951");
        var b = AddTaxon(store, "panthera onca", "iucn", "15953");
        var nameA = store.InsertCommonName(a, "Lion", "lion", "en", "col", "c1", false);
        var nameB = store.InsertCommonName(b, "Lion", "lion", "en", "iucn", "i1", false);
        store.InsertConflict("lion", "ambiguous", a, nameA, b, nameB);

        var removed = store.PurgeSource("col");

        Assert.Equal(1, removed.Conflicts);
        Assert.Equal(0, store.GetStatistics().ConflictCount);
    }

    [Fact]
    public void PurgeSource_RecordsWhenTheSourceWasReplaced() {
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "panthera leo", "iucn", "15951");
        AddName(store, taxon, "African Lion", "col");

        Assert.Empty(store.GetSourceReplacements());
        var before = DateTime.UtcNow.AddSeconds(-1);

        store.PurgeSource("col");

        var replacements = store.GetSourceReplacements();
        Assert.True(replacements.ContainsKey("col"));
        Assert.False(replacements.ContainsKey("iucn"));
        var col = replacements["col"];
        Assert.Equal(DateTimeKind.Utc, col.ReplacedAt.Kind);
        Assert.True(col.ReplacedAt >= before);
        Assert.Equal(1, col.Removed.CommonNames);

        // A second replacement overwrites the first rather than accumulating rows.
        store.PurgeSource("col");
        Assert.Single(store.GetSourceReplacements());
        Assert.Equal(0, store.GetSourceReplacements()["col"].Removed.CommonNames);
    }

    [Fact]
    public void PurgeSource_RejectsUnknownSource() {
        using var store = OpenInMemory();
        Assert.Throws<ArgumentException>(() => store.PurgeSource("sprat"));
    }
}
