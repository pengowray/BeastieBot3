using System.IO.Compression;
using System.Text;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `statuses red-lists-import`: meta.xml mapping, reading a small Darwin Core Archive built here, the
// IUCN code of a threatStatus, canonical names, the GBIF registry answer, when an archive is
// downloaded again, the store's replace, and the shipped list of red lists.
public sealed class RedListImportTests {
    private static readonly DateTime Now = new(2026, 10, 9, 1, 2, 3, DateTimeKind.Utc);

    private static RedListDataset Dataset(string key = "xx-test", string? kingdom = null, IReadOnlyDictionary<string, string>? categories = null) =>
        new(key, "00000000-0000-0000-0000-000000000001", "XX", null, null, "Test list", null, 2026, "Publisher", "CC0 1.0", "Citation",
            kingdom, null, null, categories ?? new Dictionary<string, string>());

    // ---- meta.xml ----

    // The core lists its fields out of order and gives one by default only; the extension has no
    // enclosure, no header line and the IUCN namespace for threatStatus.
    private const string Meta = """
        <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
          <core encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy='"' ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
            <files><location>taxon.txt</location></files>
            <id index="0" />
            <field index="3" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
            <field index="1" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
            <field index="2" term="http://rs.tdwg.org/dwc/terms/acceptedNameUsageID"/>
            <field index="4" term="http://rs.tdwg.org/dwc/terms/taxonomicStatus"/>
            <field index="5" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
            <field index="6" term="http://purl.org/dc/terms/references"/>
            <field index="0" term="http://rs.tdwg.org/dwc/terms/taxonID"/>
            <field term="http://rs.tdwg.org/dwc/terms/kingdom" default="Animalia"/>
          </core>
          <extension encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy='' ignoreHeaderLines="0" rowType="http://rs.gbif.org/terms/1.0/Distribution">
            <files><location>distribution.txt</location></files>
            <coreid index="0" />
            <field index="2" term="http://rs.tdwg.org/dwc/terms/locality"/>
            <field index="1" term="http://iucn.org/terms/threatStatus"/>
            <field index="3" term="http://rs.tdwg.org/dwc/terms/countryCode"/>
          </extension>
        </archive>
        """;

    [Fact]
    public void Meta_maps_terms_by_local_name_and_index() {
        var meta = DwcArchive.ReadMeta(Meta);
        Assert.Equal("Taxon", meta.Core.RowType);
        Assert.Equal("taxon.txt", meta.Core.Location);
        Assert.Equal("\t", meta.Core.Delimiter);
        Assert.Equal('"', meta.Core.Quote);
        Assert.Equal(1, meta.Core.HeaderLines);
        Assert.Equal(3, meta.Core.Fields["scientificName"]);
        Assert.Equal(0, meta.Core.Fields["taxonID"]);
        Assert.Equal("Animalia", meta.Core.Defaults["kingdom"]);

        var distribution = Assert.IsType<DwcFile>(meta.Extension("Distribution"));
        Assert.Null(distribution.Quote);
        Assert.Equal(0, distribution.HeaderLines);
        Assert.Equal(1, distribution.Fields["threatStatus"]);
        Assert.Equal(0, distribution.IdIndex);
    }

    [Theory]
    [InlineData("http://iucn.org/terms/threatStatus", "threatStatus")]
    [InlineData("http://rs.tdwg.org/dwc/terms/taxonID", "taxonID")]
    [InlineData("http://purl.org/dc/terms/references/", "references")]
    [InlineData("http://example.org/terms#status", "status")]
    public void Local_name_of_a_term(string uri, string name) => Assert.Equal(name, DwcArchive.LocalName(uri));

    // ---- a whole archive ----

    // Taxon 1 has two statuses (mainland and islands); 2 is a synonym of 1; 3 is a misapplied name
    // for 1; 4 is a hybrid formula; 5 has a Ukrainian status with a no-break space; 6 is NE; 7 has no
    // status; row 99 has no taxon. The distribution file has no header line and no enclosure, and one
    // of its fields starts with a quote that is never closed.
    private const string Taxa = "id\trank\taccepted\tname\tstatus\tauthor\treferences\n"
        + "1\tspecies\t1\tAlpha beta (Smith, 1900)\taccepted\t(Smith, 1900)\thttps://example.org/taxa/1\n"
        + "2\tspecies\t1\tAlpha gamma Jones\tsynonym\tJones\t\n"
        + "3\tspecies\t1\tAlpha delta auct.\tmisapplied\t\t\n"
        + "4\thybrid\t4\tAlpha beta × Omega zeta\taccepted\t\t\n"
        + "5\tspecies\t5\tBeta epsilon\taccepted\t\t\n"
        + "6\tspecies\t6\tGamma eta\taccepted\t\t\n"
        + "7\tspecies\t7\tDelta theta\taccepted\t\t\n";

    private const string Distribution = "1\tLC\tMainland\tXX\n"
        + "1\tCR(PE)\t\"Islands\tXX\n"
        + "4\tVU\t\tXX\n"
        + "5\tзниклий в природі\t\tXX\n"
        + "6\tNE\t\tXX\n"
        + "7\tNULL\t\tXX\n"
        + "7\t\t\tXX\n"
        + "99\tEN\t\tXX\n";

    private static ZipArchive Archive() {
        var memory = new MemoryStream();
        using (var zip = new ZipArchive(memory, ZipArchiveMode.Create, leaveOpen: true)) {
            void Add(string name, string text) {
                using var writer = new StreamWriter(zip.CreateEntry(name).Open(), new UTF8Encoding(false));
                writer.Write(text);
            }
            Add("meta.xml", Meta);
            Add("taxon.txt", Taxa);
            Add("distribution.txt", Distribution);
        }
        memory.Position = 0;
        return new ZipArchive(memory, ZipArchiveMode.Read);
    }

    private static RedListParse Parse() {
        using var zip = Archive();
        return RedListArchiveReader.Read(zip, Dataset(categories: new Dictionary<string, string> { ["зниклий в природі"] = "Extinct in the wild" }));
    }

    [Fact]
    public void Archive_rows_are_joined_to_their_taxa() {
        var parse = Parse();
        Assert.Equal(new[] { ("1", 0), ("1", 1), ("4", 0), ("5", 0) }, parse.Taxa.Select(t => (t.TaxonId, t.Seq)).ToArray());
        Assert.Equal(3, parse.TaxaWithStatus);
        Assert.Equal(new RedListSkips(NoStatus: 2, NotEvaluated: 1, NoTaxon: 1, Misapplied: 1), parse.Skipped);

        var mainland = parse.Taxa[0];
        Assert.Equal("Alpha beta (Smith, 1900)", mainland.ScientificName);
        Assert.Equal("Alpha beta", mainland.CanonicalName);
        Assert.Equal("(Smith, 1900)", mainland.Authorship);
        Assert.Equal(("LC", "LC", "Mainland", "XX"), (mainland.ThreatStatus, mainland.IucnCode, mainland.Locality, mainland.CountryCode));
        Assert.Equal("Animalia", mainland.Kingdom);
        Assert.Equal("https://example.org/taxa/1", mainland.Url);
        Assert.Null(mainland.AcceptedTaxonId);
        // The quote is part of the value: an empty enclosure means no quoting.
        Assert.Equal(("CR(PE)", "CR", "\"Islands", "XX"),
            (parse.Taxa[1].ThreatStatus, parse.Taxa[1].IucnCode, parse.Taxa[1].Locality, parse.Taxa[1].CountryCode));

        Assert.Null(parse.Taxa[2].CanonicalName);
        Assert.Equal("Beta epsilon", parse.Taxa[3].CanonicalName);
        Assert.Equal("зниклий в природі", parse.Taxa[3].ThreatStatus);
        Assert.Null(parse.Taxa[3].IucnCode);
        Assert.Equal("Extinct in the wild", parse.Taxa[3].StatusLabel);

        var synonym = Assert.Single(parse.Synonyms);
        Assert.Equal(("2", "Alpha gamma Jones", "Alpha gamma", "1"), (synonym.TaxonId, synonym.Name, synonym.CanonicalName, synonym.AcceptedTaxonId));
    }

    [Fact]
    public void Archive_without_a_distribution_extension_is_refused() {
        var memory = new MemoryStream();
        using (var zip = new ZipArchive(memory, ZipArchiveMode.Create, leaveOpen: true)) {
            using var writer = new StreamWriter(zip.CreateEntry("meta.xml").Open());
            writer.Write(Meta[..Meta.IndexOf("<extension", StringComparison.Ordinal)] + "</archive>");
        }
        memory.Position = 0;
        using var read = new ZipArchive(memory, ZipArchiveMode.Read);
        var ex = Assert.Throws<InvalidDataException>(() => RedListArchiveReader.Read(read, Dataset()));
        Assert.Contains("Distribution", ex.Message);
    }

    // ---- threatStatus ----

    [Theory]
    [InlineData("VU", "VU")]
    [InlineData("En", "EN")]
    [InlineData(" lc ", "LC")]
    [InlineData("RE", "RE")]
    [InlineData("NA", "NA")]
    [InlineData("NE", "NE")]
    [InlineData("Least Concern", "LC")]
    [InlineData("Not Applicable", "NA")]
    [InlineData("Extinct in the Wild", "EW")]
    [InlineData("Least Concern (LC)", "LC")]
    [InlineData("Regionally Extinct (RE)", "RE")]
    [InlineData("Lower Risk/near threatened", "NT")]
    [InlineData("CR(PE)", "CR")]
    [InlineData("CR (PEW)", "CR")]
    [InlineData("CR-PE", "CR")]
    [InlineData("CR*", "CR")]
    [InlineData("VU°", "VU")]
    [InlineData("NTº", "NT")]
    [InlineData("NAa", "NA")]
    [InlineData("NAb", "NA")]
    [InlineData("REW", null)]
    [InlineData("R", null)]
    [InlineData("V", null)]
    [InlineData("3", null)]
    [InlineData("*", null)]
    [InlineData("nb", null)]
    [InlineData("вразливий", null)]
    [InlineData("iucnStatus=vulnerable", null)]
    [InlineData("unknown", null)]
    [InlineData("Least Concern (VU)", null)]
    [InlineData("", null)]
    public void Iucn_code_of_a_status(string status, string? code) => Assert.Equal(code, RedListCategories.ToIucnCode(status));

    // ---- canonical names ----

    [Theory]
    [InlineData("Dendrocopos minor (Linnaeus, 1758)", "(Linnaeus, 1758)", "SPECIES", "Dendrocopos minor")]
    [InlineData("Huperzia selago  (L.) Bernh. Ex Schrank et Mart.", null, "species", "Huperzia selago")]
    [InlineData("Taraxacum dilatatum Lindb. f.", null, "species", "Taraxacum dilatatum")]
    [InlineData("Papaver radicatum subsp. laestadianum", null, "subspecies", "Papaver radicatum subsp. laestadianum")]
    [InlineData("Agonopterix quadripunctata s.lat.", null, "speciesAggregate", "Agonopterix quadripunctata")]
    [InlineData("Abies alba Mill.", null, null, "Abies alba")]
    [InlineData("Mentha × gracilis", null, "species", "Mentha × gracilis")]
    [InlineData("Salix x rubens Schrank", null, null, "Salix × rubens")]
    [InlineData("Geum ×heldreichii hort. ex Bergmans", null, null, "Geum × heldreichii")]
    [InlineData("Salix × rubens nothosubsp. basfordiana", null, null, null)]
    [InlineData("Elytrigia repens × Hordeum secalinum", null, "HYBRID", null)]
    [InlineData("Phocoena phocoena (Baltic population)", null, "unranked", null)]
    [InlineData("Sphagnum sect. Sphagnum", null, "SECTION", null)]
    [InlineData("Hamatocaulis vernicosus, southern cryptic species", null, null, null)]
    [InlineData("Atelopus", null, "genus", "Atelopus")]
    [InlineData("Tritomaria quinquedentata(Huds.) Buch", null, "Species", "Tritomaria quinquedentata")]
    [InlineData("Oenothera biennis-Gruppe", null, null, null)]
    [InlineData("Rubus sect. Rubus", null, null, null)]
    public void Canonical_name(string name, string? authorship, string? rank, string? canonical) =>
        Assert.Equal(canonical, RedListArchiveReader.CanonicalName(name, authorship, rank));

    [Theory]
    [InlineData("https://arter.dk//taxa/57859", "https://arter.dk//taxa/57859")]
    [InlineData("Lorgé P. 2019. Die Rote Liste. https://example.org/a.pdf", null)]
    [InlineData("urn:lsid:dyntaxa.se:Taxon:1", null)]
    public void Only_a_whole_url_is_a_url(string value, string? url) => Assert.Equal(url, RedListArchiveReader.WholeUrl(value));

    // ---- GBIF registry ----

    [Fact]
    public void Registry_answer_gives_licence_date_and_archive() {
        const string json = """
            {"key":"87e639cc-30a9-4007-bd2c-b0cab60326b9","title":"The Swedish Red List 2025","doi":"10.15468/zbbyqv",
             "license":"http://creativecommons.org/publicdomain/zero/1.0/legalcode","pubDate":"2026-04-30T00:00:00.000+00:00",
             "citation":{"text":"SLU Artdatabanken (2026). The Swedish Red List 2025."},
             "endpoints":[{"type":"EML","url":"https://www.gbif.se/ipt/eml.do?r=swedishredlist2025"},
                          {"type":"DWC_ARCHIVE","url":"https://www.gbif.se/ipt/archive.do?r=swedishredlist2025"}]}
            """;
        var info = GbifRegistry.Parse(json);
        Assert.Equal(("The Swedish Red List 2025", "CC0 1.0", "2026-04-30", "10.15468/zbbyqv"), (info.Title, info.Licence, info.PubDate, info.Doi));
        Assert.Equal("https://www.gbif.se/ipt/archive.do?r=swedishredlist2025", info.ArchiveUrl);
        Assert.False(info.Deleted);
    }

    [Theory]
    [InlineData("http://creativecommons.org/licenses/by/4.0/legalcode", "CC BY 4.0")]
    [InlineData("https://creativecommons.org/licenses/by-nc/4.0/legalcode", "CC BY-NC 4.0")]
    [InlineData("http://creativecommons.org/publicdomain/zero/1.0/legalcode", "CC0 1.0")]
    [InlineData("UNSPECIFIED", "UNSPECIFIED")]
    public void Licence_names(string url, string name) => Assert.Equal(name, GbifRegistry.LicenceName(url));

    // ---- downloads ----

    private static RedListDatasetRecord Record(RedListParse parse, string key = "xx-test", string? pubDate = "2026-04-30", string sha = "aa") =>
        new(key, "00000000-0000-0000-0000-000000000001", "GBIF title", "Test list", "Test list in English", 2026, "Publisher", "XX", null, null,
            "CC0 1.0", "Citation", "GBIF citation", "10.1/x", pubDate, "https://example.org/archive.zip", key + "-2026-10-09.zip", sha, 1234,
            null, Now, Now, parse.Taxa.Count, parse.TaxaWithStatus, parse.Synonyms.Count, RedListArchiveReader.Version);

    [Fact]
    public void Archive_is_downloaded_again_only_when_something_changed() {
        var previous = Record(Parse());
        const string url = "https://example.org/archive.zip";
        Assert.False(RedListPlan.NeedsDownload(previous, "2026-04-30", url, previousFileExists: true, force: false));
        Assert.True(RedListPlan.NeedsDownload(previous, "2026-04-30", url, previousFileExists: true, force: true));
        Assert.True(RedListPlan.NeedsDownload(previous, "2026-05-01", url, previousFileExists: true, force: false));
        Assert.True(RedListPlan.NeedsDownload(previous, null, url, previousFileExists: true, force: false));
        Assert.True(RedListPlan.NeedsDownload(previous, "2026-04-30", url, previousFileExists: false, force: false));
        Assert.True(RedListPlan.NeedsDownload(previous, "2026-04-30", "https://example.org/other.zip", previousFileExists: true, force: false));
        Assert.True(RedListPlan.NeedsDownload(null, "2026-04-30", url, previousFileExists: false, force: false));
    }

    [Fact]
    public void Archive_is_read_again_when_it_or_the_reader_changed() {
        var previous = Record(Parse(), sha: "aa");
        Assert.False(RedListPlan.NeedsImport(previous, "aa", force: false));
        Assert.True(RedListPlan.NeedsImport(previous, "aa", force: true));
        Assert.True(RedListPlan.NeedsImport(previous, "bb", force: false));
        Assert.True(RedListPlan.NeedsImport(previous with { ReaderVersion = RedListArchiveReader.Version - 1 }, "aa", force: false));
        Assert.True(RedListPlan.NeedsImport(null, "aa", force: false));
    }

    // ---- the store ----

    [Fact]
    public void Store_replaces_a_dataset_and_its_source_row() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = StatusListStore.OpenFromConnection(connection);
        var parse = Parse();
        store.ReplaceRedList(Record(parse), parse);
        store.ReplaceRedList(Record(parse, "yy-other"), parse);

        // Replaced with fewer rows: the old rows go, the other dataset stays.
        var fewer = parse with { Taxa = parse.Taxa.Take(1).ToList(), Synonyms = [] };
        store.ReplaceRedList(Record(fewer, sha: "bb"), fewer);
        Assert.Equal(1 + 4, store.CountRedListTaxa());
        var stored = Assert.IsType<RedListDatasetRecord>(store.GetRedListDataset("xx-test"));
        Assert.Equal(("bb", 1L, 1L, 0L), (stored.ArchiveSha256, stored.RowCount, stored.TaxonCount, stored.SynonymCount));
        Assert.Equal(new (string, string?, long)[] { ("LC", "LC", 1L) }, store.RedListStatusCounts("xx-test"));

        var source = Assert.Single(store.Sources(), s => s.Source == "redlist:xx-test");
        Assert.Equal(("Test list in English", "https://www.gbif.org/dataset/00000000-0000-0000-0000-000000000001", "CC0 1.0", 1L),
            (source.Title, source.Url, source.Licence, source.RowCount));

        store.MarkRedListChecked("xx-test", Now.AddDays(1), "2026-05-01");
        stored = store.GetRedListDataset("xx-test")!;
        Assert.Equal(("2026-05-01", Now.AddDays(1), Now), (stored.PubDate, stored.FetchedAtUtc, stored.ImportedAtUtc));

        Assert.True(store.DeleteRedList("yy-other"));
        Assert.Equal(1, store.CountRedListTaxa());
        Assert.DoesNotContain(store.Sources(), s => s.Source == "redlist:yy-other");
        using var synonyms = connection.CreateCommand();
        synonyms.CommandText = "SELECT COUNT(*) FROM red_list_synonym";
        Assert.Equal(0L, synonyms.ExecuteScalar());
    }

    // ---- the shipped list ----

    [Fact]
    public void Shipped_list_of_red_lists_loads() {
        var datasets = RedListManifest.Load(Path.Combine(AppContext.BaseDirectory, "rules", RedListManifest.Folder, RedListManifest.FileName));
        Assert.True(datasets.Count >= 20);
        Assert.All(datasets, d => Assert.Contains(d.Licence, RedListManifest.Licences));
        var norway = Assert.Single(datasets, d => d.Key == "no-plants-2021");
        Assert.Equal(("NO", "Plantae"), (norway.Country, norway.Kingdom));
        var flanders = Assert.Single(datasets, d => d.Key == "be-vlg-validated");
        Assert.Equal(("Flanders", "BE-VLG"), (flanders.Region, flanders.RegionCode));
        var germany = Assert.Single(datasets, d => d.Key == "de-plants-2018");
        Assert.Equal("Threatened (Gefährdet)", RedListCategories.Label(germany.Categories, "3"));
    }

    [Theory]
    [InlineData("licence: CC BY-ND 4.0", "licence")]
    [InlineData("country: Sweden", "country")]
    [InlineData("key: Bad_Key", "key")]
    public void List_entries_are_checked(string replace, string complaint) {
        var entry = new Dictionary<string, string> {
            ["key"] = "key: se-test",
            ["gbif"] = "gbif: 87e639cc-30a9-4007-bd2c-b0cab60326b9",
            ["country"] = "country: SE",
            ["name"] = "name: Test",
            ["publisher"] = "publisher: Test",
            ["licence"] = "licence: CC0 1.0",
            ["citation"] = "citation: Test",
        };
        entry[replace.Split(':')[0]] = replace;
        var yaml = "datasets:\n  - " + string.Join("\n    ", entry.Values) + "\n";
        var ex = Assert.Throws<InvalidOperationException>(() => RedListManifest.Parse(yaml, "test.yml"));
        Assert.Contains(complaint, ex.Message);
    }
}
