using System.IO.Compression;
using System.Text;
using BeastieBot3.CommonNames;
using BeastieBot3.SiteBuild;
using BeastieBot3.Wikidata;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s names and synonyms from the GBIF Backbone Taxonomy, read from a cut-down backbone
// zip: the tiger matched by the GBIF id on its Wikidata item (P846), the lion by its name.
public sealed class SiteDbBuildGbifNamesTests : IDisposable {
    private const long Tiger = 15955;
    private const long Lion = 15951;
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_StoresGbifCommonNamesWithTheirDatasets() {
        using var db = OpenReadOnly(Build());
        var names = Rows(db, """
            SELECT taxon_id, language, name, source_detail FROM name
            WHERE source = 'gbif' AND name_type = 'common' ORDER BY taxon_id, language, name
            """).Select(r => $"{r[0]} {r[1]} {r[2]} [{r[3]}]").ToList();
        Assert.Equal([
            "15951 en Lion [EUNIS Biodiversity Database]",
            "15951 nl Leeuw [Checklist Dutch Species Register - Nederlands Soortenregister]",
            "15955 en Bengal tiger [EUNIS Biodiversity Database]",
            // Two datasets give "Tigre" and "tigre": one row, both datasets.
            "15955 es Tigre [EUNIS Biodiversity Database\nBelgian Species List]",
            "15955 nl Tijger [Belgian Species List]",
            // Left out: "Tiger" from GBIF's copy of the Catalogue of Life, "Tigre" (English) from a
            // Handbook of the Mammals of the World chapter, "Tijger" labelled English, "Panthera
            // tigris" labelled Latin, "harimau" with no language, "Tigre royal" labelled English with
            // no dataset, and the lion's "Lion d'Afrique" from a dataset titled with a family name.
        ], names);
    }

    [Fact]
    public void Build_StoresGbifSynonymsWithTheirAuthority() {
        using var db = OpenReadOnly(Build());
        var synonyms = Rows(db, "SELECT taxon_id, name, authority FROM name WHERE source = 'gbif' AND name_type = 'synonym' ORDER BY taxon_id")
            .Select(r => $"{r[0]} {r[1]} ({r[2]})").ToList();
        // Not the pro parte synonym Tigris regalis, nor the plant Panthera leo's synonym.
        Assert.Equal(["15951 Felis leo (Linnaeus, 1758)", "15955 Felis tigris (Linnaeus, 1758)"], synonyms);
    }

    [Fact]
    public void Build_KeepsTheChosenEnglishNamesAndStoresTheBackboneVersion() {
        using var db = OpenReadOnly(Build());
        Assert.Equal("Tiger", Scalar(db, $"SELECT common_name_en FROM taxon WHERE taxon_id = {Tiger}"));
        Assert.Equal("Lion", Scalar(db, $"SELECT common_name_en FROM taxon WHERE taxon_id = {Lion}"));
        Assert.Equal("2023-08-28", Scalar(db, "SELECT value FROM meta WHERE key = 'gbif_backbone_version'"));
        Assert.Equal("10.15468/39omei", Scalar(db, "SELECT value FROM meta WHERE key = 'gbif_backbone_doi'"));
        // The lion's English name from GBIF repeats IUCN's: one row each, merged on the page.
        Assert.Equal("2", Scalar(db, $"SELECT COUNT(*) FROM name WHERE taxon_id = {Lion} AND name = 'Lion'"));
    }

    // ------------------------------------------------------------ sources

    private string Build() {
        var output = _sources.PathOf("site.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var names = _sources.PathOf("common_names.sqlite");
        var wikidata = _sources.PathOf("wikidata.sqlite");
        var backbone = _sources.PathOf("backbone-2023-08-28.zip");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", """
                    (1, 15955, 'Panthera tigris', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'tigris', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, 15951, 'Panthera leo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL)
                """, """
                    (1, 1, 15955, 'Panthera tigris', 'Endangered', NULL, '2022', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 2, 15951, 'Panthera leo', 'Vulnerable', NULL, '2023', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteCommonNames(names);
            WriteWikidata(wikidata);
            WriteBackbone(backbone);
        }
        new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, CommonNames = names, WikidataCache = wikidata, GbifBackbone = backbone, Output = output,
            ExtraSpecies = BeastieBot3.SiteBuild.ExtraSpecies.ExtraPlacement.None,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    private static void WriteCommonNames(string path) {
        using (CommonNameStore.Open(path)) {
            // Creates the schema.
        }
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO taxa (id, canonical_name, original_name, rank, kingdom, validity_status, primary_source, primary_source_id, created_at, updated_at) VALUES
                (1, 'panthera tigris', 'Panthera tigris', 'species', 'ANIMALIA', 'valid', 'iucn', '15955', 'x', 'x'),
                (2, 'panthera leo', 'Panthera leo', 'species', 'ANIMALIA', 'valid', 'iucn', '15951', 'x', 'x');
            INSERT INTO common_names (taxon_id, raw_name, normalized_name, language, source, source_identifier, is_preferred, created_at) VALUES
                (1, 'Tiger', 'tiger', 'en', 'iucn', '15955', 1, 'x'),
                (2, 'Lion', 'lion', 'en', 'iucn', '15951', 1, 'x');
            """);
    }

    // The tiger's item, with its IUCN taxon id and its GBIF id (P846). The lion has no item.
    private static void WriteWikidata(string path) {
        using (WikidataCacheStore.Open(path)) {
            // Creates the schema.
        }
        const string json = """
            {"entities":{"Q19939":{"id":"Q19939",
              "labels":{"en":{"language":"en","value":"tiger"}},
              "claims":{
                "P846":[{"mainsnak":{"datavalue":{"value":"5219416","type":"string"}},"rank":"normal"}],
                "P225":[{"mainsnak":{"datavalue":{"value":"Panthera tigris","type":"string"}},"rank":"normal"}]},
              "sitelinks":{}}}}
            """;
        using var c = OpenWritable(path);
        Execute(c, """
            INSERT INTO wikidata_entities (entity_numeric_id, entity_id, discovered_at, last_seen_at, json_downloaded, downloaded_at, label_en, json)
            VALUES (19939, 'Q19939', 'x', 'x', 1, '2026-10-01T00:00:00Z', 'tiger', @json);
            INSERT INTO wikidata_p627_values (entity_numeric_id, source, value) VALUES (19939, 'claim', '15955');
            """, ("@json", json));
    }

    // Columns in another order than GBIF's, so the reader must find them through meta.xml.
    internal static void WriteBackbone(string path) {
        const string meta = """
            <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
              <core encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <files><location>Taxon.tsv</location></files>
                <id index="0" />
                <field index="1" term="http://rs.tdwg.org/dwc/terms/taxonomicStatus"/>
                <field index="2" term="http://rs.tdwg.org/dwc/terms/acceptedNameUsageID"/>
                <field index="3" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
                <field index="4" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
                <field index="5" term="http://rs.gbif.org/terms/1.0/canonicalName"/>
                <field index="6" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
                <field index="7" term="http://rs.tdwg.org/dwc/terms/kingdom"/>
              </core>
              <extension encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/VernacularName">
                <files><location>VernacularName.tsv</location></files>
                <coreid index="0" />
                <field index="1" term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
                <field index="2" term="http://purl.org/dc/terms/language"/>
                <field index="3" term="http://purl.org/dc/terms/source"/>
              </extension>
            </archive>
            """;
        const string eml = """
            <eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"><dataset><title>GBIF Backbone Taxonomy</title>
              <intellectualRights><para><ulink url="http://creativecommons.org/licenses/by/4.0/legalcode"><citetitle>CC BY 4.0</citetitle></ulink></para></intellectualRights>
            </dataset><additionalMetadata><metadata><gbif><dateStamp>2023-08-28T13:45:33Z</dateStamp>
              <citation>GBIF Secretariat. GBIF Backbone Taxonomy. Checklist dataset https://doi.org/10.15468/39omei accessed via GBIF.org on 2023-08-28.</citation>
            </gbif></metadata></additionalMetadata></eml:eml>
            """;
        const string chapter = """
            <eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"><dataset><title>Felidae</title>
              <abstract><para>This dataset contains the digitized treatments in Plazi based on the original book chapter Felidae. In: Handbook of the Mammals of the World.</para></abstract>
            </dataset></eml:eml>
            """;
        string[] taxa = [
            "taxonID\ttaxonomicStatus\tacceptedNameUsageID\tscientificName\tscientificNameAuthorship\tcanonicalName\ttaxonRank\tkingdom",
            "5219416\taccepted\t\tPanthera tigris (Linnaeus, 1758)\t(Linnaeus, 1758)\tPanthera tigris\tspecies\tAnimalia",
            "4969819\tsynonym\t5219416\tFelis tigris Linnaeus, 1758\tLinnaeus, 1758\tFelis tigris\tspecies\tAnimalia",
            "4969820\tproparte synonym\t5219416\tTigris regalis Gray, 1867\tGray, 1867\tTigris regalis\tspecies\tAnimalia",
            "5219404\taccepted\t\tPanthera leo (Linnaeus, 1758)\t(Linnaeus, 1758)\tPanthera leo\tspecies\tAnimalia",
            "5219405\tsynonym\t5219404\tFelis leo Linnaeus, 1758\tLinnaeus, 1758\tFelis leo\tspecies\tAnimalia",
            "9000001\taccepted\t\tPanthera leo Hort.\tHort.\tPanthera leo\tspecies\tPlantae",
            "9000002\tsynonym\t9000001\tLeonia panthera Hort.\tHort.\tLeonia panthera\tspecies\tPlantae",
        ];
        string[] vernacular = [
            "taxonID\tvernacularName\tlanguage\tsource",
            "5219416\tTiger\ten\tCatalogue of Life Checklist",
            "5219416\tTigre\tes\tEUNIS Biodiversity Database",
            "5219416\ttigre\tes\tBelgian Species List",
            "5219416\tBengal tiger\ten\tEUNIS Biodiversity Database",
            "5219416\tTigre\ten\tFelidae",
            "5219416\tTijger\tnl\tBelgian Species List",
            "5219416\tTijger\ten\tBelgian Species List",
            "5219416\tPanthera tigris\tla\tBelgian Species List",
            "5219416\tharimau\t\t",
            "5219416\tTigre royal\ten\t",
            "5219404\tLion d'Afrique\ten\tLeonidae",
            "5219404\tLion\ten\tEUNIS Biodiversity Database",
            "5219404\tLeeuw\tnl\tChecklist Dutch Species Register - Nederlands Soortenregister",
            "9000001\tLeo plant\ten\tGRIN Taxonomy",
        ];
        using var zip = ZipFile.Open(path, ZipArchiveMode.Create);
        void Add(string name, string text) {
            using var writer = new StreamWriter(zip.CreateEntry(name).Open(), new UTF8Encoding(false));
            writer.Write(text);
        }
        Add("meta.xml", meta);
        Add("eml.xml", eml);
        Add("dataset/0d000000-0000-0000-0000-000000000001.xml", chapter);
        Add("Taxon.tsv", string.Join("\n", taxa) + "\n");
        Add("VernacularName.tsv", string.Join("\n", vernacular) + "\n");
    }
}
