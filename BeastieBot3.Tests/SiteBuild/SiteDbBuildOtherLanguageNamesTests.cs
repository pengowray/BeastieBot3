using BeastieBot3.CommonNames;
using BeastieBot3.SiteBuild;
using BeastieBot3.Wikidata;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s common names in other languages from the Catalogue of Life (vernacularname of the
// taxon's col_id, here from the common names store's cross-reference) and from the taxon's Wikidata
// item (labels, aliases, P1843 and Wikipedia sitelink titles), for a tiger cut down to a few names.
public sealed class SiteDbBuildOtherLanguageNamesTests : IDisposable {
    private const long Tiger = 15955;
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_StoresNamesInOtherLanguagesAndLeavesOutScientificNames() {
        using var db = OpenReadOnly(Build());
        var names = Rows(db, """
            SELECT source, language, name FROM name
            WHERE taxon_id = @t AND name_type = 'common' AND language <> 'en'
            ORDER BY source, language, name
            """, ("@t", Tiger)).Select(r => $"{r[0]} {r[1]} {r[2]}").ToList();
        Assert.Equal([
            "col da Tiger",            // CoL's "dnj" (Dan) for a Danish name
            "col fr tigre",
            "col zh 老虎",              // "cmn" stored as Chinese
            "wikidata de Königstiger",  // an alias
            "wikidata de Tiger",        // P1843, also the German label: one row
            "wikidata fr Tigre",        // the label; "tigre" from P1843 differs only in case
            "wikidata zh 虎",           // zh-hans and zh-hant labels: one row
            "wikipedia fr Tigre",       // "Tigre (animal)"
            "wikipedia ja トラ",
        ], names);
    }

    [Fact]
    public void Build_KeepsEnglishNamesAsTheyWere() {
        using var db = OpenReadOnly(Build());
        var english = Rows(db, "SELECT source, name FROM name WHERE taxon_id = @t AND name_type = 'common' AND language = 'en' ORDER BY source",
            ("@t", Tiger)).Select(r => $"{r[0]} {r[1]}").ToList();
        // Only the common names store's English names: none of the CoL, Wikidata or Wikipedia English names read here.
        Assert.Equal(["iucn Tiger"], english);
    }

    // ------------------------------------------------------------ sources

    private string Build() {
        var output = _sources.PathOf("site.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var names = _sources.PathOf("common_names.sqlite");
        var col = _sources.PathOf("col.sqlite");
        var wikidata = _sources.PathOf("wikidata.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", """
                    (1, 15955, 'Panthera tigris', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'tigris', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL)
                """, """
                    (1, 1, 15955, 'Panthera tigris', 'Endangered', NULL, '2022', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteCommonNames(names);
            WriteCol(col);
            WriteWikidata(wikidata);
        }
        new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, CommonNames = names, ColDatabase = col, WikidataCache = wikidata, Output = output,
            ExtraSpecies = BeastieBot3.SiteBuild.ExtraSpecies.ExtraPlacement.None,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    // The tiger with its CoL id as a cross-reference, an English name and a CoL synonym.
    private static void WriteCommonNames(string path) {
        using (CommonNameStore.Open(path)) {
            // Creates the schema.
        }
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO taxa (id, canonical_name, original_name, rank, kingdom, validity_status, primary_source, primary_source_id, created_at, updated_at) VALUES
                (1, 'panthera tigris', 'Panthera tigris', 'species', 'ANIMALIA', 'valid', 'iucn', '15955', 'x', 'x');
            INSERT INTO common_names (taxon_id, raw_name, normalized_name, language, source, source_identifier, is_preferred, created_at) VALUES
                (1, 'Tiger', 'tiger', 'en', 'iucn', '15955', 1, 'x');
            INSERT INTO scientific_name_synonyms (taxon_id, normalized_name, original_name, source, synonym_type, created_at) VALUES
                (1, 'felis tigris', 'Felis tigris', 'col', 'synonym', 'x');
            INSERT INTO taxon_cross_references (taxon_id, source, source_identifier, match_type, created_at) VALUES
                (1, 'col', 'T1', 'exact', 'x');
            """);
    }

    private static void WriteCol(string path) {
        using var c = OpenWritable(path);
        Execute(c, """
            CREATE TABLE nameusage (ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, authorship TEXT, rank TEXT,
                genericName TEXT, specificEpithet TEXT, genus TEXT, family TEXT, kingdom TEXT, extinct TEXT);
            INSERT INTO nameusage VALUES
              ('T1', 'P0', 'accepted', 'Panthera tigris', '(Linnaeus, 1758)', 'species', 'Panthera', 'tigris', 'Panthera', 'Felidae', 'Animalia', 'false');
            CREATE TABLE vernacularname (taxonID TEXT, name TEXT, language TEXT);
            INSERT INTO vernacularname VALUES
              ('T1', 'Tiger', 'eng'),
              ('T1', 'tigre', 'fra'),
              ('T1', 'Tiger', 'dnj'),
              ('T1', '老虎', 'cmn'),
              ('T1', 'Felis tigris', 'ita'),
              ('T1', 'Panthera tigris altaica', 'rus'),
              ('T1', 'harimau', ''),
              ('T1', 'harimau', 'und'),
              ('T2', 'Löwe', 'deu');
            """);
    }

    private static void WriteWikidata(string path) {
        using (WikidataCacheStore.Open(path)) {
            // Creates the schema.
        }
        const string json = """
            {"entities":{"Q19939":{"id":"Q19939",
              "labels":{
                "en":{"language":"en","value":"tiger"},
                "fr":{"language":"fr","value":"Tigre"},
                "de":{"language":"de","value":"Tiger"},
                "zh-hans":{"language":"zh-hans","value":"虎"},
                "zh-hant":{"language":"zh-hant","value":"虎"},
                "ast":{"language":"ast","value":"‎Panthera tigris‎"},
                "nl":{"language":"nl","value":"Panthera tigris"},
                "mul":{"language":"mul","value":"Panthera tigris"},
                "avk":{"language":"avk","value":"Karvol (Panthera tigris)"}},
              "aliases":{"de":[{"language":"de","value":"Königstiger"}],"es":[{"language":"es","value":"P. tigris"}]},
              "claims":{
                "P1843":[
                  {"mainsnak":{"datavalue":{"value":{"text":"tigre","language":"fr"},"type":"monolingualtext"}},"rank":"normal"},
                  {"mainsnak":{"datavalue":{"value":{"text":"Tiger","language":"de"},"type":"monolingualtext"}},"rank":"normal"},
                  {"mainsnak":{"datavalue":{"value":{"text":"Tijgertje","language":"nl"},"type":"monolingualtext"}},"rank":"deprecated"}],
                "P225":[{"mainsnak":{"datavalue":{"value":"Panthera tigris","type":"string"}},"rank":"normal"}]},
              "sitelinks":{
                "enwiki":{"site":"enwiki","title":"Tiger"},
                "frwiki":{"site":"frwiki","title":"Tigre (animal)"},
                "jawiki":{"site":"jawiki","title":"トラ"},
                "cebwiki":{"site":"cebwiki","title":"Panthera tigris"},
                "commonswiki":{"site":"commonswiki","title":"Panthera tigris"},
                "specieswiki":{"site":"specieswiki","title":"Panthera tigris"}}}}}
            """;
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO wikidata_entities (entity_numeric_id, entity_id, discovered_at, last_seen_at, json_downloaded, downloaded_at, label_en, json)
            VALUES (19939, 'Q19939', 'x', 'x', 1, '2026-10-01T00:00:00Z', 'tiger', @json);
            INSERT INTO wikidata_p627_values (entity_numeric_id, source, value) VALUES (19939, 'claim', '15955');
            """, ("@json", json));
    }
}
