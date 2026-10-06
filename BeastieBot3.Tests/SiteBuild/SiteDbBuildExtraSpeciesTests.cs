using BeastieBot3.SiteBuild;
using BeastieBot3.SiteBuild.ExtraSpecies;
using BeastieBot3.Wikidata;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s species from the Catalogue of Life and Wikidata that IUCN does not have
// (extra_species, extra_overlap, higher_taxon_extra), over a CoL database and a Wikidata taxon sweep
// cut down to a few cats. IUCN has Panthera leo, Panthera pardus and Felis silvestris.
public sealed class SiteDbBuildExtraSpeciesTests : IDisposable {
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_AddsSpeciesOfIucnGeneraFromColAndWikidata() {
        using var db = OpenReadOnly(Build(ExtraPlacement.Genus));
        var rows = Rows(db, """
            SELECT e.scientific_name, e.sources, e.col_id, e.wikidata_qid, e.common_name_en, e.enwiki_title, h.name
            FROM extra_species e JOIN higher_taxon h ON h.node_id = e.node_id ORDER BY e.extra_id
            """);
        var byName = rows.ToDictionary(r => (string)r[0]!);

        // In CoL and on Wikidata (the item states the CoL ID): one row, with the item's label and article.
        Assert.Equal(3L, byName["Felis lybica"][1]);
        Assert.Equal("F2", byName["Felis lybica"][2]);
        Assert.Equal(100L, byName["Felis lybica"][3]);
        Assert.Equal("African wildcat", byName["Felis lybica"][4]);
        Assert.Equal("African wildcat", byName["Felis lybica"][5]);
        Assert.Equal("Felis", byName["Felis lybica"][6]);
        // Only in CoL; only on Wikidata.
        Assert.Equal(1L, byName["Panthera parda"][1]);
        Assert.Equal(2L, byName["Felis libyca"][1]);
        Assert.Equal(2L, byName["Panthera zdanskyi"][1]);
        // Left out: the same as an IUCN species (by name, or by the item's IUCN taxon ID), fossils (CoL
        // extinct, a Wikidata fossil taxon item, and a CoL species Wikidata marks as fossil), and a
        // species whose genus IUCN does not have.
        Assert.DoesNotContain("Panthera leo", byName.Keys);
        Assert.DoesNotContain("Panthera palaeosinensis", byName.Keys);
        Assert.DoesNotContain("Panthera fossilis", byName.Keys);
        Assert.DoesNotContain("Panthera spelaea", byName.Keys);
        Assert.DoesNotContain("Acinonyx jubatus", byName.Keys);
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public void Build_RecordsPossibleDuplicates() {
        using var db = OpenReadOnly(Build(ExtraPlacement.Genus));
        var overlaps = Rows(db, """
            SELECT e.scientific_name, o.taxon_id, o2.scientific_name, o.reason, o.likely
            FROM extra_overlap o JOIN extra_species e ON e.extra_id = o.extra_id
            LEFT JOIN extra_species o2 ON o2.extra_id = o.other_extra_id
            ORDER BY e.scientific_name
            """);

        // Wikidata's "Felis libyca" is a CoL synonym of CoL's Felis lybica.
        Assert.Contains(overlaps, o => (string)o[0]! == "Felis libyca" && (string?)o[2] == "Felis lybica" && (string)o[3]! == OverlapReason.ColSynonym
            && (long)o[4]! == 1);
        // Wikidata's Felis lybica item names "Felis ornata" as a taxon synonym (P1420).
        Assert.Contains(overlaps, o => (string)o[0]! == "Felis ornata" && (string?)o[2] == "Felis lybica" && (string)o[3]! == OverlapReason.WikidataSynonym);
        // "parda" is "pardus" with another gender ending.
        Assert.Contains(overlaps, o => (string)o[0]! == "Panthera parda" && (long?)o[1] == 15954 && (string)o[3]! == OverlapReason.GenderEnding);
    }

    [Fact]
    public void Build_CountsExtraSpeciesPerGroup() {
        using var db = OpenReadOnly(Build(ExtraPlacement.Genus));
        var panthera = Rows(db, """
            SELECT x.col_count, x.wikidata_count, x.both_count, x.last_node_id >= h.node_id
            FROM higher_taxon_extra x JOIN higher_taxon h ON h.node_id = x.node_id WHERE h.rank = 'genus' AND h.name = 'Panthera'
            """).Single();
        Assert.Equal(new object?[] { 1L, 1L, 0L, 1L }, panthera);
        var family = Rows(db, """
            SELECT x.col_count, x.wikidata_count, x.both_count
            FROM higher_taxon_extra x JOIN higher_taxon h ON h.node_id = x.node_id WHERE h.rank = 'family'
            """).Single();
        Assert.Equal(new object?[] { 1L, 3L, 1L }, family);
    }

    [Fact]
    public void Build_WithFamilyPlacement_AddsSpeciesOfIucnFamilies() {
        using var db = OpenReadOnly(Build(ExtraPlacement.Family));
        var row = Rows(db, """
            SELECT h.rank, h.name FROM extra_species e JOIN higher_taxon h ON h.node_id = e.node_id WHERE e.scientific_name = 'Acinonyx jubatus'
            """).Single();
        Assert.Equal(new object?[] { "family", "Felidae" }, row);
    }

    [Fact]
    public void Build_WithNoExtraSpecies_LeavesTheTablesEmpty() {
        using var db = OpenReadOnly(Build(ExtraPlacement.None));
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM extra_species"));
    }

    // ------------------------------------------------------------ sources

    private string Build(ExtraPlacement placement) {
        var output = _sources.PathOf($"site-{placement}.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var col = _sources.PathOf("col.sqlite");
        var wikidata = _sources.PathOf("wikidata.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", """
                    (1, 15951, 'Panthera leo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, 15954, 'Panthera pardus', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'pardus', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, 60354, 'Felis silvestris', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Felis', 'silvestris', NULL, NULL, NULL, NULL, 'Schreber, 1777', NULL)
                """, """
                    (1, 1, 15951, 'Panthera leo', 'Vulnerable', NULL, '2025', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 2, 15954, 'Panthera pardus', 'Vulnerable', NULL, '2025', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 3, 60354, 'Felis silvestris', 'Least Concern', NULL, '2022', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteCol(col);
            WriteWikidata(wikidata);
        }
        new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, ColDatabase = col, WikidataCache = wikidata, Output = output, ExtraSpecies = placement,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    private static void WriteCol(string path) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE nameusage (ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, authorship TEXT, rank TEXT,
                genericName TEXT, specificEpithet TEXT, genus TEXT, family TEXT, kingdom TEXT, extinct TEXT);
            CREATE INDEX idx_nameusage_ID ON nameusage(ID);
            CREATE INDEX idx_nameusage_parentID ON nameusage(parentID);
            CREATE INDEX idx_nameusage_rank ON nameusage(rank);
            CREATE INDEX idx_nameusage_genus ON nameusage(genus);
            CREATE INDEX idx_nameusage_family ON nameusage(family);
            INSERT INTO nameusage VALUES
              ('L1', 'P0', 'accepted', 'Panthera leo', '(Linnaeus, 1758)', 'species', 'Panthera', 'leo', 'Panthera', 'Felidae', 'Animalia', 'false'),
              ('P1', 'P0', 'accepted', 'Panthera palaeosinensis', 'Zdansky, 1924', 'species', 'Panthera', 'palaeosinensis', 'Panthera', 'Felidae', 'Animalia', 'true'),
              ('P2', 'P0', 'provisionally accepted', 'Panthera spelaea', 'Goldfuss, 1810', 'species', 'Panthera', 'spelaea', 'Panthera', 'Felidae', 'Animalia', NULL),
              ('P3', 'P0', 'accepted', 'Panthera parda', 'Test, 1900', 'species', 'Panthera', 'parda', 'Panthera', 'Felidae', 'Animalia', NULL),
              ('F2', 'F0', 'accepted', 'Felis lybica', 'Forster, 1780', 'species', 'Felis', 'lybica', 'Felis', 'Felidae', 'Animalia', 'false'),
              ('S1', 'F2', 'synonym', 'Felis libyca', NULL, 'species', 'Felis', 'libyca', NULL, NULL, NULL, NULL),
              ('A1', 'A0', 'accepted', 'Acinonyx jubatus', '(Schreber, 1775)', 'species', 'Acinonyx', 'jubatus', 'Acinonyx', 'Felidae', 'Animalia', 'false');
            """);
    }

    private static void WriteWikidata(string path) {
        using (var store = WikidataCacheStore.Open(path)) {
            WikidataSweptTaxon Item(long qid, string name, IReadOnlyList<string>? col = null, IReadOnlyList<string>? iucn = null,
                string? enwiki = null, string? label = null, IReadOnlyList<long>? instance = null, IReadOnlyList<long>? synonymOf = null) =>
                new(qid, name, [], WikidataTaxonSweep.SpeciesRank, [], col ?? [], iucn ?? [], enwiki, label, instance ?? [], synonymOf ?? []);
            store.StoreTaxonSweepPage([
                Item(100, "Felis lybica", col: ["F2"], enwiki: "African wildcat", label: "African wildcat"),
                Item(101, "Felis libyca"),
                Item(102, "Felis ornata", synonymOf: [100]),
                Item(140, "Panthera leo", iucn: ["15951"]),
                Item(200, "Panthera fossilis", instance: [23038290]),
                Item(201, "Panthera spelaea", instance: [23038290]),
                Item(202, "Panthera zdanskyi"),
            ], DateTime.UtcNow);
            store.SetSyncText(WikidataCacheStore.TaxonSweepCompletedKey, DateTime.UtcNow.ToString("O"));
        }
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    }
}
