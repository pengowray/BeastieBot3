using BeastieBot3.CommonNames;
using BeastieBot3.SiteBuild;
using BeastieBot3.Wikidata;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s subspecies and varieties of each species from the Catalogue of Life and Wikidata
// (infraspecific_name, SiteSubspecies), for a lion cut down to a few subspecies. IUCN has the lion, its
// subspecies persica and the wildcat; the lion's CoL ID comes from the common names store and its
// Wikidata item from the cache's P627 values.
public sealed class SiteDbBuildSubspeciesTests : IDisposable {
    private const long Lion = 15951;
    private const long AsiaticLion = 15952;
    private const long Wildcat = 60354;
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_StoresAcceptedColSubspeciesWithTheirAuthorities() {
        using var db = OpenReadOnly(Build(out _));
        var rows = Rows(db, "SELECT source_id, rank, name, authority FROM infraspecific_name WHERE taxon_id = @t AND source = 'col' ORDER BY source_id",
            ("@t", Lion)).Select(r => string.Join(" | ", r)).ToList();
        Assert.Equal([
            "5K5L8 | subspecies | Panthera leo leo | (Linnaeus, 1758)",
            "7KGW9 | subspecies | Panthera leo melanochaita | (C. E. H. Smith, 1858)",
            "P9 | subspecies | Panthera leo azandica | ",
        ], rows);
        // Left out: the synonym nubica, the form, the unranked BOLD entry, and the species-rank synonym.
    }

    [Fact]
    public void Build_StoresWikidataSubspeciesAndVarietiesUnderTheSpeciesItem() {
        using var db = OpenReadOnly(Build(out _));
        var rows = Rows(db, "SELECT source_id, rank, name, authority FROM infraspecific_name WHERE taxon_id = @t AND source = 'wikidata' ORDER BY source_id",
            ("@t", Lion)).Select(r => string.Join(" | ", r)).ToList();
        Assert.Equal([
            "Q20907143 | subspecies | Panthera leo melanochaita | ",
            // A population item with the rank subspecies, and an extinct subspecies: kept.
            "Q221094 | subspecies | Panthera leo leo | ",
            "Q221247 | subspecies | Panthera leo melanochaitus | ",
            "Q900 | variety | Panthera leo var. testvar | ",
        ], rows);
        // Left out: a fossil taxon, two items that name each other as a taxon synonym, a name that is
        // not genus, species and one more epithet, and an item whose parent is not a site species.
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM infraspecific_name WHERE taxon_id <> 15951"));
    }

    [Fact]
    public void Build_CountsEachSourceAndTheSpeciesWithAList() {
        Build(out var stats);
        var s = stats.Subspecies;
        Assert.Equal(1, s.IucnRows);          // persica
        Assert.Equal(3, s.ColRows);
        Assert.Equal(1, s.ColSpeciesRead);    // the wildcat has no CoL ID
        Assert.Equal(4, s.WikidataRows);
        Assert.Equal(1, s.WikidataLeftOutByInstance);
        Assert.Equal(2, s.WikidataLeftOutAsSynonym);
        Assert.Equal(1, s.WikidataUnreadable);
        Assert.Equal(1, s.SpeciesWithList);
        Assert.Equal(1, s.SpeciesWithSeveralSources);
    }

    // ------------------------------------------------------------ sources

    private string Build(out SiteBuildStats stats) {
        var output = _sources.PathOf("site.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var names = _sources.PathOf("common_names.sqlite");
        var col = _sources.PathOf("col.sqlite");
        var wikidata = _sources.PathOf("wikidata.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", """
                    (1, 15951, 'Panthera leo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, 15952, 'Panthera leo ssp. persica', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', 'subspecies', 'persica', NULL, NULL, '(Meyer, 1826)', NULL),
                    (1, 60354, 'Felis silvestris', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Felis', 'silvestris', NULL, NULL, NULL, NULL, 'Schreber, 1777', NULL)
                """, """
                    (1, 1, 15951, 'Panthera leo', 'Vulnerable', NULL, '2025', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 2, 15952, 'Panthera leo ssp. persica', 'Endangered', NULL, '2023', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 3, 60354, 'Felis silvestris', 'Least Concern', NULL, '2022', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteCommonNames(names);
            WriteCol(col);
            WriteWikidata(wikidata);
        }
        stats = new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, CommonNames = names, ColDatabase = col, WikidataCache = wikidata, Output = output,
            ExtraSpecies = BeastieBot3.SiteBuild.ExtraSpecies.ExtraPlacement.None,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    // The lion with its CoL ID as a cross-reference.
    private static void WriteCommonNames(string path) {
        using (CommonNameStore.Open(path)) {
            // Creates the schema.
        }
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO taxa (id, canonical_name, original_name, rank, kingdom, validity_status, primary_source, primary_source_id, created_at, updated_at) VALUES
                (1, 'panthera leo', 'Panthera leo', 'species', 'ANIMALIA', 'valid', 'iucn', '15951', 'x', 'x');
            INSERT INTO taxon_cross_references (taxon_id, source, source_identifier, match_type, created_at) VALUES
                (1, 'col', 'L1', 'exact', 'x');
            """);
    }

    private static void WriteCol(string path) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE nameusage (ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, authorship TEXT, rank TEXT,
                genericName TEXT, specificEpithet TEXT, genus TEXT, family TEXT, kingdom TEXT, extinct TEXT);
            CREATE INDEX idx_nameusage_ID ON nameusage(ID);
            CREATE INDEX idx_nameusage_parentID ON nameusage(parentID);
            CREATE INDEX idx_nameusage_rank ON nameusage(rank);
            INSERT INTO nameusage VALUES
              ('L1', 'P0', 'accepted', 'Panthera leo', '(Linnaeus, 1758)', 'species', 'Panthera', 'leo', 'Panthera', 'Felidae', 'Animalia', 'false'),
              ('7KGW9', 'L1', 'accepted', 'Panthera leo melanochaita', '(C. E. H. Smith, 1858)', 'subspecies', 'Panthera', 'leo', NULL, NULL, 'Animalia', 'true'),
              ('5K5L8', 'L1', 'accepted', 'Panthera leo leo', '(Linnaeus, 1758)', 'subspecies', 'Panthera', 'leo', NULL, NULL, 'Animalia', 'true'),
              ('P9', 'L1', 'provisionally accepted', 'Panthera leo azandica', '', 'subspecies', 'Panthera', 'leo', NULL, NULL, 'Animalia', NULL),
              ('5K5LB', 'L1', 'synonym', 'Panthera leo nubica', '(de Blainville, 1843)', 'subspecies', 'Panthera', 'leo', NULL, NULL, NULL, NULL),
              ('F1', 'L1', 'accepted', 'Panthera leo f. alba', NULL, 'form', 'Panthera', 'leo', NULL, NULL, NULL, NULL),
              ('CFSCR', 'L1', 'synonym', 'Felis leo', 'Linnaeus, 1758', 'species', 'Felis', 'leo', NULL, NULL, NULL, NULL),
              ('BOLD.AEW2394', 'L1', 'accepted', 'BOLD:AEW2394', NULL, 'unranked', NULL, NULL, NULL, NULL, NULL, NULL);
            """);
    }

    private static void WriteWikidata(string path) {
        using (var store = WikidataCacheStore.Open(path)) {
            WikidataSweptTaxon Item(long qid, string name, long rank, IReadOnlyList<long> parents, IReadOnlyList<long>? instance = null,
                IReadOnlyList<long>? synonymOf = null) =>
                new(qid, name, [], rank, parents, [], [], null, null, instance ?? [], synonymOf ?? []);
            store.StoreTaxonSweepPage([
                Item(140, "Panthera leo", WikidataTaxonSweep.SpeciesRank, [3]),
                Item(900, "Panthera leo var. testvar", SiteSubspecies.VarietyRankQid, [140]),
                Item(901, "Panthera leo 'Barbary'", SiteSubspecies.SubspeciesRankQid, [140]),
                Item(902, "Felis silvestris lybica", SiteSubspecies.SubspeciesRankQid, [999]),
                Item(182347, "Panthera leo persica", SiteSubspecies.SubspeciesRankQid, [140], synonymOf: [56289810]),
                Item(192492, "Panthera leo spelaea", SiteSubspecies.SubspeciesRankQid, [140], instance: [23038290]),
                Item(221094, "Panthera leo leo", SiteSubspecies.SubspeciesRankQid, [140], instance: [2625603]),
                Item(221247, "Panthera leo melanochaitus", SiteSubspecies.SubspeciesRankQid, [140], instance: [98961713]),
                Item(20907143, "Panthera leo melanochaita", SiteSubspecies.SubspeciesRankQid, [140]),
                Item(56289810, "Panthera leo leo", SiteSubspecies.SubspeciesRankQid, [140], synonymOf: [182347]),
            ], DateTime.UtcNow);
        }
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO wikidata_entities (entity_numeric_id, entity_id, discovered_at, last_seen_at, json_downloaded, downloaded_at, label_en, json)
            VALUES (140, 'Q140', 'x', 'x', 1, '2026-10-01T00:00:00Z', 'lion', '{"entities":{"Q140":{"id":"Q140"}}}');
            INSERT INTO wikidata_p627_values (entity_numeric_id, source, value) VALUES (140, 'claim', '15951');
            """);
    }
}
