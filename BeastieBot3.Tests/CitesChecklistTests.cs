using System.Text.Json;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `statuses cites-import`: reading a page of the CITES Checklist's taxon_concepts endpoint, splitting
// synonyms from their authors, the store's replace, and an import of a kept gzip file. The page is
// cut down from the endpoint's answers (October 2026): long notes are shortened and fields the reader
// does not use are left out.
public sealed class CitesChecklistTests : IDisposable {
    private const string Page = """
        [{"result_cnt":6,"total_cnt":6,
          "animalia":[
            {"id":3734,"full_name":"Abeillia abeillei","rank_name":"SPECIES","cites_accepted":true,"genus_name":"Abeillia",
             "family_name":"Trochilidae","order_name":"Apodiformes","class_name":"Aves","phylum_name":"Chordata","kingdom_name":"Animalia",
             "current_listing":"II","author_year":"(Lesson & DeLattre, 1839)","synonyms_with_authors":["Ornismya abeillei Lesson & DeLattre, 1839"],
             "current_additions":[{"id":4661,"change_type_name":"ADDITION","species_listing_name":"II","party_iso_code":null,"party_full_name":null,
               "is_current":true,"hash_ann_symbol":null,"auto_note":"FAMILY listing Trochilidae spp.","full_note":null,"hash_full_note":null,
               "short_note":null,"inherited_short_note":null,"inherited_full_note":null,"effective_at_formatted":"22/10/1987","nomenclature_note":null}]},
            {"id":7925,"full_name":"Acrocephalus rodericanus","rank_name":"SPECIES","cites_accepted":true,"genus_name":"Acrocephalus",
             "family_name":"Muscicapidae","order_name":"Passeriformes","class_name":"Aves","phylum_name":"Chordata","kingdom_name":"Animalia",
             "current_listing":"III","author_year":"(Newton, 1865)","synonyms_with_authors":["Bebrornis rodericanus (Newton, 1865)"],
             "current_additions":[{"id":2279,"change_type_name":"ADDITION","species_listing_name":"III","party_iso_code":"MU","party_full_name":"Mauritius",
               "is_current":true,"hash_ann_symbol":null,"auto_note":null,"full_note":null,"hash_full_note":null,"short_note":null,
               "inherited_short_note":null,"inherited_full_note":null,"effective_at_formatted":"04/12/1975","nomenclature_note":null}]},
            {"id":7767,"full_name":"Agapornis roseicollis","rank_name":"SPECIES","cites_accepted":true,"genus_name":"Agapornis",
             "family_name":"Psittacidae","order_name":"Psittaciformes","class_name":"Aves","phylum_name":"Chordata","kingdom_name":"Animalia",
             "current_listing":"NC","author_year":"(Vieillot, 1818)","synonyms_with_authors":["Psittacus roseicollis Vieillot, 1818"],"current_additions":[]},
            {"id":1708,"full_name":"Loxodonta","rank_name":"GENUS","cites_accepted":true,"genus_name":"Loxodonta","family_name":"Elephantidae",
             "order_name":"Proboscidea","class_name":"Mammalia","phylum_name":"Chordata","kingdom_name":"Animalia","current_listing":"I/II",
             "author_year":null,"synonyms_with_authors":[],
             "current_additions":[{"id":39734,"change_type_name":"ADDITION","species_listing_name":"I","party_iso_code":null,"party_full_name":null,
               "is_current":true,"hash_ann_symbol":null,"auto_note":null,
               "full_note":"Except the populations of <i>Loxodonta africana</i> of Botswana, Namibia, South Africa and Zimbabwe, which are included in Appendix II subject to ...",
               "hash_full_note":null,
               "short_note":"Except the populations of <i>Loxodonta africana</i> of Botswana, Namibia, South Africa and Zimbabwe, which are included in Appendix II subject to annotation A11.",
               "inherited_short_note":null,"inherited_full_note":null,"effective_at_formatted":"05/03/2026","nomenclature_note":null}]},
            {"id":4521,"full_name":"Loxodonta africana","rank_name":"SPECIES","cites_accepted":true,"genus_name":"Loxodonta",
             "family_name":"Elephantidae","order_name":"Proboscidea","class_name":"Mammalia","phylum_name":"Chordata","kingdom_name":"Animalia",
             "current_listing":"I/II","author_year":"(Blumenbach, 1797)","synonyms_with_authors":[],
             "current_additions":[
               {"id":40054,"change_type_name":"ADDITION","species_listing_name":"I","party_iso_code":null,"party_full_name":null,
                "is_current":true,"hash_ann_symbol":null,"auto_note":"GENUS listing Loxodonta spp.","full_note":null,"hash_full_note":null,
                "short_note":null,
                "inherited_short_note":"Except the populations of <i>Loxodonta africana</i> of Botswana, Namibia, South Africa and Zimbabwe, which are included in Appendix II subject to annotation A11.",
                "inherited_full_note":"Except the populations of <i>Loxodonta africana</i> of Botswana, Namibia, South Africa and Zimbabwe, which are included in Appendix II subject to ...",
                "effective_at_formatted":"05/03/2026","nomenclature_note":null},
               {"id":39737,"change_type_name":"ADDITION","species_listing_name":"II","party_iso_code":null,"party_full_name":null,
                "is_current":true,"hash_ann_symbol":null,"auto_note":null,
                "full_note":"The populations of Botswana, Namibia, South Africa and Zimbabwe are listed in Appendix II for the exclusive purpose of allowing:<p><p>a) trade in ...",
                "hash_full_note":null,
                "short_note":"Populations of Botswana, Namibia, South Africa and Zimbabwe are included in Appendix II subject to annotation A11 (see full note); all other populations are included in Appendix I",
                "inherited_short_note":null,"inherited_full_note":null,"effective_at_formatted":"05/03/2026","nomenclature_note":null}]}],
          "plantae":[
            {"id":71501,"full_name":"Lepanthes tenuis","rank_name":"SPECIES","cites_accepted":true,"genus_name":"Lepanthes",
             "family_name":"Orchidaceae","order_name":"Orchidales","class_name":null,"phylum_name":null,"kingdom_name":"Plantae",
             "current_listing":"II","author_year":"Schltr., 1913","synonyms_with_authors":["Lepanthes glacensis Dod"],
             "current_additions":[{"id":39958,"change_type_name":"ADDITION","species_listing_name":"II","party_iso_code":null,"party_full_name":null,
               "is_current":true,"hash_ann_symbol":"#4","auto_note":"FAMILY listing Orchidaceae spp.",
               "full_note":"Included in Appendix II, except for the species included in Appendix I.<p>\r\nAdditionally, artificially propagated hybrids of the following genera are ...",
               "hash_full_note":"All parts and derivatives, except:<p> a) seeds (including seedpods of Orchidaceae), spores and pollen (including pollinia). The exemption does not ...",
               "short_note":"Except the species included in Appendix I","inherited_short_note":null,"inherited_full_note":null,
               "effective_at_formatted":"05/03/2026","nomenclature_note":null}]}]}]
        """;

    private static readonly DateTime Now = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-cites-" + Guid.NewGuid().ToString("N"));

    public CitesChecklistTests() => Directory.CreateDirectory(_dir);

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    private static IReadOnlyList<CitesTaxon> Taxa() => CitesChecklist.ReadTaxa(CitesChecklist.ReadPage(Page).Rows);

    private static CitesTaxon Taxon(string name) => Taxa().Single(t => t.FullName == name);

    [Fact]
    public void Page_gives_its_total_and_the_animals_then_the_plants() {
        var (total, rows) = CitesChecklist.ReadPage(Page);
        Assert.Equal(6, total);
        Assert.Equal(new long[] { 3734, 7925, 7767, 1708, 4521, 71501 }, rows.Select(r => r.GetProperty("id").GetInt64()).ToArray());
    }

    [Fact]
    public void Taxon_is_read_with_its_ranks_and_listing_summary() {
        var taxon = Taxon("Loxodonta africana");
        Assert.Equal((4521L, "(Blumenbach, 1797)", "SPECIES", true, "I/II"),
            (taxon.TaxonConceptId, taxon.AuthorYear, taxon.Rank, taxon.CitesAccepted, taxon.CurrentListing));
        Assert.Equal(("Animalia", "Chordata", "Mammalia", "Proboscidea", "Elephantidae", "Loxodonta"),
            (taxon.Kingdom, taxon.Phylum, taxon.TaxClass, taxon.TaxOrder, taxon.Family, taxon.Genus));
    }

    [Fact]
    public void Split_listing_has_a_row_for_each_appendix_with_its_own_note() {
        var listings = Taxon("Loxodonta africana").Listings;
        Assert.Equal(new[] { "I", "II" }, listings.Select(l => l.Appendix).ToArray());

        // Appendix I comes from the genus listing, with the genus's note on the populations it leaves out.
        var inherited = listings[0];
        Assert.Equal(("GENUS", "Loxodonta", (long?)1708), (inherited.InheritedRank, inherited.InheritedName, inherited.InheritedFromId));
        Assert.Null(inherited.ShortNote);
        Assert.StartsWith("Except the populations of <i>Loxodonta africana</i> of Botswana", inherited.InheritedShortNote);
        Assert.Equal("2026-03-05", inherited.EffectiveOn);

        // Appendix II is the species' own listing; its note names the populations.
        var own = listings[1];
        Assert.Equal(39737, own.ListingChangeId);
        Assert.Null(own.InheritedName);
        Assert.StartsWith("Populations of Botswana, Namibia, South Africa and Zimbabwe are included in Appendix II", own.ShortNote);
        Assert.StartsWith("The populations of Botswana", own.FullNote);
    }

    [Fact]
    public void Appendix_III_listing_names_its_party() {
        var listing = Assert.Single(Taxon("Acrocephalus rodericanus").Listings);
        Assert.Equal(("III", "MU", "Mauritius", "1975-12-04"), (listing.Appendix, listing.PartyIsoCode, listing.PartyName, listing.EffectiveOn));
        Assert.Null(listing.InheritedName);
    }

    [Fact]
    public void Inherited_listing_of_a_higher_taxon_not_in_the_file_keeps_its_rank_and_name() {
        var listing = Assert.Single(Taxon("Abeillia abeillei").Listings);
        Assert.Equal(("II", "FAMILY", "Trochilidae"), (listing.Appendix, listing.InheritedRank, listing.InheritedName));
        Assert.Null(listing.InheritedFromId);
        Assert.Equal(4661, listing.ListingChangeId);
    }

    [Fact]
    public void Excluded_species_is_NC_with_no_listing() {
        var taxon = Taxon("Agapornis roseicollis");
        Assert.Equal("NC", taxon.CurrentListing);
        Assert.Empty(taxon.Listings);
    }

    [Fact]
    public void Annotation_symbol_and_text_are_read() {
        var listing = Assert.Single(Taxon("Lepanthes tenuis").Listings);
        Assert.Equal("#4", listing.AnnotationSymbol);
        Assert.StartsWith("All parts and derivatives, except:<p> a) seeds", listing.AnnotationNote);
        Assert.Equal(("FAMILY", "Orchidaceae"), (listing.InheritedRank, listing.InheritedName));
    }

    [Fact]
    public void Synonyms_are_split_from_their_authors() {
        Assert.Equal(new CitesSynonym("Ornismya abeillei Lesson & DeLattre, 1839", "Ornismya abeillei", "Lesson & DeLattre, 1839"),
            Assert.Single(Taxon("Abeillia abeillei").Synonyms));
        Assert.Equal(new CitesSynonym("Bebrornis rodericanus (Newton, 1865)", "Bebrornis rodericanus", "(Newton, 1865)"),
            Assert.Single(Taxon("Acrocephalus rodericanus").Synonyms));
    }

    // Synonyms as the endpoint writes them (October 2026), and the names it gives with show_author=0.
    [Theory]
    [InlineData("Abronia aurita Köhler, 2008", "Abronia aurita", "Köhler, 2008")]
    [InlineData("Auriculabronia aurita", "Auriculabronia aurita", null)]
    [InlineData("Heteropora", "Heteropora", null)]
    [InlineData("Trochilus tzacatl de la Llave, 1833", "Trochilus tzacatl", "de la Llave, 1833")]
    [InlineData("Hemitriton (siredon) mexicanum Van der Hoeven, 1833", "Hemitriton (siredon) mexicanum", "Van der Hoeven, 1833")]
    [InlineData("Phyllomedusa (agalychnis) callidryas Lutz, 1950", "Phyllomedusa (agalychnis) callidryas", "Lutz, 1950")]
    [InlineData("Lockhartia antioquiensis hort. ex Gard. Chron.", "Lockhartia antioquiensis", "hort. ex Gard. Chron.")]
    [InlineData("Lycaste jamesiana auct. 1889", "Lycaste jamesiana", "auct. 1889")]
    [InlineData("Gerrhonotus auritus O’Shaughnessy, 1873", "Gerrhonotus auritus", "O’Shaughnessy, 1873")]
    [InlineData("Papilio chikae Igarashi, 1965", "Papilio chikae", "Igarashi, 1965")]
    [InlineData("Laelia anceps subsp. dawsonii (J.Anderson) Rolfe", "Laelia anceps subsp. dawsonii", "(J.Anderson) Rolfe")]
    [InlineData("Gerrhonotus deppii var. digueti Mocquard, 1905 (fide Smith & Taylor, 1950)", "Gerrhonotus deppii var. digueti",
        "Mocquard, 1905 (fide Smith & Taylor, 1950)")]
    public void Synonym_split(string text, string name, string? author) => Assert.Equal((name, author), CitesChecklist.SplitSynonym(text));

    [Theory]
    [InlineData("22/10/1987", "1987-10-22")]
    [InlineData("05/03/2026", "2026-03-05")]
    [InlineData("", null)]
    [InlineData("1987-10-22", null)]
    public void Effective_dates_are_day_first(string text, string? iso) => Assert.Equal(iso, CitesChecklist.IsoDate(text));

    [Fact]
    public void Citation_is_the_checklists_own_form() =>
        Assert.Equal("UNEP-WCMC (Comps.) 2026. The Checklist of CITES Species Website. CITES Secretariat, Geneva, Switzerland. Compiled by UNEP-WCMC, Cambridge, UK. Available at: http://checklist.cites.org. [Accessed 08/10/2026].",
            CitesChecklist.Citation(Now));

    [Fact]
    public void Taxon_links_go_to_species_plus() =>
        Assert.Equal("https://speciesplus.net/species#/taxon_concepts/4521/legal", CitesChecklist.SpeciesPlusUrl(4521));

    [Fact]
    public void Page_url_asks_for_synonyms_with_authors_and_no_common_names() {
        var url = CitesChecklist.PageUrl(3);
        Assert.StartsWith("https://www.speciesplus.net/checklist/taxon_concepts?", url);
        Assert.Contains("show_synonyms=1&show_author=1&show_english=0&show_spanish=0&show_french=0", url);
        Assert.EndsWith("&page=3&per_page=1000", url);
    }

    // ---- the store ----

    private static StatusListStore MemoryStore(out SqliteConnection connection) {
        connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return StatusListStore.OpenFromConnection(connection);
    }

    private static object? Scalar(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static StatusSourceInfo Source(int rows) =>
        new(StatusSources.Cites, CitesChecklist.Title, CitesChecklist.SiteUrl, CitesChecklist.Licence, CitesChecklist.Citation(Now),
            "cites-2026-10-08.json.gz", Now, rows);

    [Fact]
    public void Store_replaces_every_cites_row() {
        using var store = MemoryStore(out var conn);
        using var connection = conn;
        var taxa = Taxa();
        store.ReplaceCites(taxa, Now, Source(taxa.Count));
        Assert.Equal((6L, 6L), (store.CountCitesTaxa(), store.CountCitesListings()));
        Assert.Equal(4L, Scalar(conn, "SELECT COUNT(*) FROM cites_synonym"));
        // Five long notes, four of them different: the genus's full note is also the species' inherited full note.
        Assert.Equal(4L, Scalar(conn, "SELECT COUNT(*) FROM cites_note"));
        Assert.Equal(1L, Scalar(conn, """
            SELECT COUNT(*) FROM cites_listing g JOIN cites_listing s ON s.inherited_full_note_id = g.full_note_id
            WHERE g.taxon_concept_id = 1708 AND s.taxon_concept_id = 4521
            """));
        Assert.Equal(1708L, Scalar(conn, "SELECT inherited_from_id FROM cites_listing WHERE taxon_concept_id = 4521 AND appendix = 'I'"));
        Assert.Equal("MU", Scalar(conn, "SELECT party_iso_code FROM cites_listing WHERE taxon_concept_id = 7925"));
        Assert.Equal("Ornismya abeillei", Scalar(conn, "SELECT name FROM cites_synonym WHERE taxon_concept_id = 3734"));
        Assert.Equal("https://speciesplus.net/species#/taxon_concepts/4521/legal", Scalar(conn, "SELECT url FROM cites_taxon WHERE taxon_concept_id = 4521"));
        Assert.Equal("#4", Scalar(conn, """
            SELECT l.annotation_symbol FROM cites_listing l JOIN cites_note n ON n.note_id = l.annotation_note_id
            WHERE l.taxon_concept_id = 71501 AND n.html LIKE 'All parts and derivatives%'
            """));

        // A second import replaces the first: only its taxa, listings, notes and synonyms are left.
        var two = taxa.Where(t => t.FullName.StartsWith("Loxodonta", StringComparison.Ordinal)).ToList();
        store.ReplaceCites(two, Now, Source(two.Count));
        Assert.Equal((2L, 3L), (store.CountCitesTaxa(), store.CountCitesListings()));
        Assert.Equal(0L, Scalar(conn, "SELECT COUNT(*) FROM cites_synonym"));
        Assert.Equal(2L, Scalar(conn, "SELECT COUNT(*) FROM cites_note"));
        var source = Assert.Single(store.Sources());
        Assert.Equal((StatusSources.Cites, 2L), (source.Source, source.RowCount));
    }

    [Fact]
    public async Task Kept_gzip_file_is_imported() {
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), "[Datastore]\n");
        var file = Path.Combine(_dir, "cites-2026-10-08.json.gz");
        await StatusListDownload.WriteGzipJsonArrayAsync(file, write => {
            var rows = CitesChecklist.ReadPage(Page).Rows;
            write(rows.Take(3));
            write(rows.Skip(3));
            return Task.CompletedTask;
        });
        Assert.False(File.Exists(file + ".part"));
        Assert.Equal(6, StatusListDownload.ReadJsonArray(file).Count());

        var storePath = Path.Combine(_dir, "status_lists.sqlite");
        var result = await StatusListImport.RunAsync(new CitesImportCommand.Settings {
            File = file,
            StorePath = storePath,
            IniFile = Path.Combine(_dir, "paths.ini"),
            SettingsDir = _dir,
        }, CitesImportCommand.Spec(), CancellationToken.None);
        Assert.Equal(0, result);

        using var store = StatusListStore.OpenReadOnly(storePath)!;
        Assert.Equal(6, store.CountCitesTaxa());
        var source = Assert.Single(store.Sources());
        Assert.Equal((StatusSources.Cites, "cites-2026-10-08.json.gz", 6L), (source.Source, source.Version, source.RowCount));
        Assert.Equal(CitesChecklist.Citation(source.FetchedAtUtc), source.Citation);
    }

    [Fact]
    public void Plain_json_file_is_read_too() {
        var file = Path.Combine(_dir, "rows.json");
        File.WriteAllText(file, """[{"id":1,"full_name":"Aa"},{"id":2,"full_name":"Ab"}]""");
        Assert.Equal(new long[] { 1, 2 }, StatusListDownload.ReadJsonArray(file).Select(r => r.GetProperty("id").GetInt64()).ToArray());
    }
}
