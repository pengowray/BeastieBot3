using BeastieBot3.StatusLists;
using BeastieBot3.Web.Flows;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The status lists store: reading the ECOS export and NatureServe's search results, what a
// natureserve-fetch run does, the store's writes, and the public-site workflow lights.
public sealed class StatusListsTests {
    // ---- ECOS export ----

    private const string EcosCsv = """
        "Common Name","Scientific Name","Scientific Name_url","ESA Listing Status","Entity Description","ESA Listing Date","ECOS Listed Species ID","ECOS Species ID","Distinct Population Segment?","Is Foreign?","Foreign or Domestic","Species Group","Taxonomic Serial Number","Taxonomic Serial Number_url","Taxonomic Kingdom","Taxonomic Family","Taxonomic Group"
        "Abbott's booby","Papasula (=Sula) abbotti","https://ecos.fws.gov/ecp/species/1470","Endangered","Wherever found","06-14-1976","3907","1470","false","true","Foreign","Birds","561804","https://www.itis.gov/","Animal","Sulidae","Birds"
        "Pink mucket","Lampsilis abrupta","https://ecos.fws.gov/ecp/species/7829","Experimental Population, Non-Essential","U.S.A. (AL)","","1680","7829","true","false","Domestic","Clams","79939","https://www.itis.gov/","Animal","Unionidae","Clams"
        "Pink mucket","Lampsilis abrupta","https://ecos.fws.gov/ecp/species/7829","Endangered","Wherever found","06-14-1976","326","7829","false","false","Domestic","Clams","79939","https://www.itis.gov/","Animal","Unionidae","Clams"
        "No common name","Kadua st.-johnii","https://ecos.fws.gov/ecp/species/1234","Endangered","","05-13-2013","999","1234","false","false","Domestic","Flowering Plants","","","Plant","Rubiaceae","Flowering Plants"
        """;

    [Fact]
    public void Ecos_rows_are_read_by_listing_id() {
        var rows = EcosListedSpecies.Read(new StringReader(EcosCsv), out var skipped);
        Assert.Equal(0, skipped);
        Assert.Equal(4, rows.Count);

        var booby = rows[0];
        Assert.Equal(3907, booby.EntityId);
        Assert.Equal(1470, booby.SpeciesId);
        Assert.Equal("Papasula (=Sula) abbotti", booby.ScientificNameRaw);
        Assert.Equal("Papasula abbotti", booby.ScientificName);
        Assert.Equal(new[] { "Papasula abbotti", "Sula abbotti" }, booby.Names.ToArray());
        Assert.Equal("1976-06-14", booby.ListingDate);
        Assert.True(booby.IsForeign);
        Assert.False(booby.IsDps);
        Assert.Equal(561804, booby.ItisTsn);
        Assert.Equal("Animal", booby.Kingdom);
        Assert.Equal("Birds", booby.SpeciesGroup);

        // One species, two listings: the comma inside the quoted status is not a column break.
        var mucket = rows.Where(r => r.SpeciesId == 7829).ToList();
        Assert.Equal(new long[] { 1680, 326 }, mucket.Select(r => r.EntityId).ToArray());
        Assert.Equal("Experimental Population, Non-Essential", mucket[0].Status);
        Assert.Null(mucket[0].ListingDate);

        Assert.Null(rows[3].CommonName);
        Assert.Null(rows[3].ItisTsn);
    }

    [Fact]
    public void Ecos_export_without_the_listing_id_column_is_refused() {
        const string csv = """
            "Common Name","Scientific Name","Scientific Name_url","ESA Listing Status","Entity Description","ESA Listing Date"
            "Abbott's booby","Papasula (=Sula) abbotti","https://ecos.fws.gov/ecp/species/1470","Endangered","Wherever found","06-14-1976"
            """;
        var ex = Assert.Throws<InvalidDataException>(() => EcosListedSpecies.Read(new StringReader(csv), out _));
        Assert.Contains("ECOS Listed Species ID", ex.Message);
    }

    [Theory]
    [InlineData("06-14-1976", "1976-06-14")]
    [InlineData("", null)]
    [InlineData("not a date", null)]
    public void Ecos_dates_are_month_first(string text, string? iso) => Assert.Equal(iso, EcosListedSpecies.IsoDate(text));

    // ---- NatureServe search results ----

    private const string Page = """
        {"results":[
          {"recordType":"SPECIES","elementGlobalId":828458,"uniqueId":"ELEMENT_GLOBAL.2.828458","nsxUrl":"/Taxon/ELEMENT_GLOBAL.2.828458/Acris_blanchardi",
           "elcode":"AAABC01040","scientificName":"Acris blanchardi","primaryCommonName":"Blanchard's Cricket Frog","primaryCommonNameLanguage":"EN",
           "roundedGRank":"G5","gRank":"G5","classificationStatus":"Standard","lastModified":"2026-10-02T22:49:27.8318Z",
           "nations":[{"nationCode":"US","roundedNRank":"N5","subnations":[{"subnationCode":"MN","roundedSRank":"S1"},{"subnationCode":"MN","roundedSRank":"S5"},
                                                                         {"roundedSRank":"S2"},{"subnationCode":"WI","roundedSRank":"S2","native":true,"exotic":false}]},
                      {"nationCode":"CA","roundedNRank":"NX","subnations":[]},
                      {"nationCode":"US","roundedNRank":"N1","subnations":[]}],
           "speciesGlobal":{"usesaCode":null,"cosewicCode":"XT","saraCode":"Endangered/En voie de disparition","synonyms":["Acris crepitans blanchardi"],
             "otherCommonNames":["Rainette grillon de Blanchard"],"kingdom":"Animalia","phylum":"Craniata","taxclass":"Amphibia","taxorder":"Anura",
             "family":"Hylidae","genus":"Acris","taxonomicComments":"Gamble et al. (2008) revised ...","informalTaxonomy":"Animals | Vertebrates | Amphibians",
             "infraspecies":false,"completeDistribution":true}},
          {"recordType":"SPECIES","elementGlobalId":1169377,"uniqueId":"ELEMENT_GLOBAL.2.1169377","nsxUrl":"/Taxon/ELEMENT_GLOBAL.2.1169377/Ambystoma_californiense_pop_1",
           "scientificName":"Ambystoma californiense pop. 1","roundedGRank":"T2","gRank":"G3TNRQ","nations":[],
           "speciesGlobal":{"usesaCode":"LE","synonyms":[],"infraspecies":true}},
          {"recordType":"ECOSYSTEM","elementGlobalId":5,"scientificName":"Not a species"}
        ],"resultsSummary":{"page":0,"recordsPerPage":100,"totalPages":1136,"totalResults":113530}}
        """;

    [Fact]
    public void NatureServe_page_is_read_without_narrative_text() {
        var page = NatureServeSearch.ParsePage(Page);
        Assert.Equal(113530, page.TotalResults);
        Assert.Equal(3, page.ResultCount);
        Assert.Equal(2, page.Species.Count);

        var frog = page.Species[0];
        Assert.Equal(828458, frog.ElementGlobalId);
        Assert.Equal("https://explorer.natureserve.org/Taxon/ELEMENT_GLOBAL.2.828458/Acris_blanchardi", frog.NsxUrl);
        Assert.Equal("Endangered", frog.SaraCode);
        Assert.Equal("Endangered/En voie de disparition", frog.SaraCodeRaw);
        Assert.Equal("XT", frog.CosewicCode);
        Assert.Null(frog.UsesaCode);
        Assert.Equal("N5", frog.UsNRank);
        Assert.Equal("NX", frog.CaNRank);
        Assert.Equal("Amphibia", frog.TaxClass);
        Assert.False(frog.Infraspecies);
        Assert.Equal(new[] { "Acris crepitans blanchardi" }, frog.Synonyms.ToArray());

        var population = page.Species[1];
        Assert.True(population.Infraspecies);
        Assert.Equal("G3TNRQ", population.GRank);
        Assert.Equal("LE", population.UsesaCode);
        Assert.Null(population.UsNRank);
        Assert.Empty(population.Nations);
    }

    [Fact]
    public void NatureServe_nations_keep_the_first_of_each_code_and_need_a_code() {
        var frog = NatureServeSearch.ParsePage(Page).Species[0];
        Assert.Equal(new[] { "US", "CA" }, frog.Nations.Select(n => n.NationCode).ToArray());
        var us = frog.Nations[0];
        Assert.Equal("N5", us.RoundedNRank);
        Assert.Null(us.Native);
        Assert.Null(us.Exotic);
        Assert.Equal(new (string, string?)[] { ("MN", "S1"), ("WI", "S2") }, us.Subnations.Select(s => (s.SubnationCode, s.RoundedSRank)).ToArray());
        Assert.Null(us.Subnations[0].Native);
        Assert.Equal((true, false), (us.Subnations[1].Native, us.Subnations[1].Exotic));
        Assert.Empty(frog.Nations[1].Subnations);
    }

    // Two records of a real speciesSearch page (2026-10-08), trimmed to a few subnations: the bald
    // eagle, native in both nations with breeding (B), nonbreeding (N) and migrant (M) ranks, and
    // the common myna, introduced in the US.
    private const string RanksPage = """
        {"results":[
          {"recordType":"SPECIES","elementGlobalId":104470,"uniqueId":"ELEMENT_GLOBAL.2.104470","nsxUrl":"/Taxon/ELEMENT_GLOBAL.2.104470/Haliaeetus_leucocephalus",
           "elcode":"ABNKC10010","scientificName":"Haliaeetus leucocephalus","formattedScientificName":"<i>Haliaeetus leucocephalus</i>",
           "primaryCommonName":"Bald Eagle","primaryCommonNameLanguage":"EN","roundedGRank":"G5","lastModified":"2026-10-02T21:44:11.345092Z",
           "classificationStatus":"Standard",
           "nations":[
             {"nationCode":"US","roundedNRank":"N5B,N5N","subnations":[
               {"subnationCode":"TX","roundedSRank":"S3B,S3N","exotic":false,"native":true},
               {"subnationCode":"AZ","roundedSRank":"S2,S4N","exotic":false,"native":true},
               {"subnationCode":"PA","roundedSRank":"S4B,S5N,S4M","exotic":false,"native":true},
               {"subnationCode":"NN","roundedSRank":"S2N","exotic":false,"native":true},
               {"subnationCode":"DC","roundedSRank":"S2N,SXB","exotic":false,"native":true}],"exotic":false,"native":true},
             {"nationCode":"CA","roundedNRank":"N5B,N5N","subnations":[
               {"subnationCode":"NF","roundedSRank":"S4","exotic":false,"native":true},
               {"subnationCode":"LB","roundedSRank":"S4B,SUM","exotic":false,"native":true}],"exotic":false,"native":true}],
           "speciesGlobal":{"usesaCode":"DL","cosewicCode":"NAR","saraCode":null,"synonyms":[],"kingdom":"Animalia","taxclass":"Aves","infraspecies":false},
           "gRank":"G5"},
          {"recordType":"SPECIES","elementGlobalId":105264,"uniqueId":"ELEMENT_GLOBAL.2.105264","scientificName":"Acridotheres tristis",
           "roundedGRank":"G5","gRank":"G5",
           "nations":[
             {"nationCode":"US","roundedNRank":"NNA","subnations":[
               {"subnationCode":"FL","roundedSRank":"SNA","exotic":true,"native":false},
               {"subnationCode":"HI","roundedSRank":"SNA","exotic":true,"native":false}],"exotic":true,"native":false}],
           "speciesGlobal":{"synonyms":[],"infraspecies":false}}
        ],"resultsSummary":{"page":0,"recordsPerPage":100,"totalPages":1,"totalResults":2}}
        """;

    [Fact]
    public void NatureServe_national_and_subnational_ranks_are_read() {
        var page = NatureServeSearch.ParsePage(RanksPage);
        var eagle = page.Species[0];
        Assert.Equal(new[] { "US", "CA" }, eagle.Nations.Select(n => n.NationCode).ToArray());
        Assert.Equal("N5B,N5N", eagle.UsNRank);
        Assert.Equal("N5B,N5N", eagle.CaNRank);

        var us = eagle.Nations[0];
        Assert.Equal(("N5B,N5N", (bool?)true, (bool?)false), (us.RoundedNRank, us.Native, us.Exotic));
        Assert.Equal(new[] { "TX", "AZ", "PA", "NN", "DC" }, us.Subnations.Select(s => s.SubnationCode).ToArray());
        Assert.Equal("S4B,S5N,S4M", us.Subnations[2].RoundedSRank);
        Assert.Equal("S2N,SXB", us.Subnations[4].RoundedSRank);
        Assert.All(us.Subnations, s => Assert.Equal((true, false), (s.Native, s.Exotic)));
        Assert.Equal(new (string, string?)[] { ("NF", "S4"), ("LB", "S4B,SUM") }, eagle.Nations[1].Subnations.Select(s => (s.SubnationCode, s.RoundedSRank)).ToArray());

        var myna = page.Species[1];
        var introduced = Assert.Single(myna.Nations);
        Assert.Equal(("US", "NNA", (bool?)false, (bool?)true), (introduced.NationCode, introduced.RoundedNRank, introduced.Native, introduced.Exotic));
        Assert.Equal(2, introduced.Subnations.Count);
        Assert.All(introduced.Subnations, s => Assert.Equal(("SNA", (bool?)false, (bool?)true), (s.RoundedSRank, s.Native, s.Exotic)));
        Assert.Null(myna.CaNRank);
    }

    [Fact]
    public void NatureServe_request_asks_for_a_name_prefix() {
        var body = NatureServeSearch.BuildRequest("Ab", 3, new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc));
        Assert.Equal("""{"criteriaType":"species","textCriteria":[{"paramType":"textSearch","searchToken":"Ab","matchAgainst":"scientificName","operator":"startsWith"}],"pagingOptions":{"page":3,"recordsPerPage":100},"modifiedSince":"2026-10-01T00:00:00Z"}""", body);
        Assert.Equal("""{"criteriaType":"species","textCriteria":[],"pagingOptions":{"page":0,"recordsPerPage":100}}""", NatureServeSearch.BuildRequest("", 0, null));
    }

    [Fact]
    public void NatureServe_prefixes_split_into_letters() {
        Assert.Equal(26, NatureServeSearch.Split("").Count);
        Assert.Equal("A", NatureServeSearch.Split("")[0]);
        Assert.Equal(new[] { "Aa", "Ab" }, NatureServeSearch.Split("A").Take(2).ToArray());
        Assert.Equal("Az", NatureServeSearch.Split("A")[^1]);
        Assert.Equal(100, NatureServeSearch.PageCount(10_000));
        Assert.Equal(1, NatureServeSearch.PageCount(1));
        Assert.Equal(0, NatureServeSearch.PageCount(0));
    }

    [Theory]
    [InlineData("Endangered/En voie de disparition", "Endangered")]
    [InlineData("Special Concern/Préoccupante", "Special Concern")]
    [InlineData("No Status", "No Status")]
    [InlineData(null, null)]
    public void Sara_english_part(string? bilingual, string? english) => Assert.Equal(english, NatureServeSearch.EnglishPart(bilingual));

    // ---- what a run does ----

    private static readonly DateTime Now = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private static NatureServePassState State(DateTime? started = null, DateTime? completed = null, DateTime? fullCompleted = null) =>
        new(started, false, null, null, completed, completed?.AddHours(-1), null, null, null, fullCompleted);

    [Fact]
    public void NatureServe_plan() {
        Assert.Equal(NatureServePlan.Action.StartFull, NatureServePlan.Decide(State(), false, null, Now));
        Assert.Equal(NatureServePlan.Action.Continue, NatureServePlan.Decide(State(started: Now.AddHours(-2)), false, 30, Now));
        Assert.Equal(NatureServePlan.Action.StartFull, NatureServePlan.Decide(State(started: Now.AddHours(-2)), true, null, Now));

        var finished = State(completed: Now.AddDays(-10), fullCompleted: Now.AddDays(-10));
        Assert.Equal(NatureServePlan.Action.UpToDate, NatureServePlan.Decide(finished, false, null, Now));
        Assert.Equal(NatureServePlan.Action.UpToDate, NatureServePlan.Decide(finished, false, 30, Now));
        Assert.Equal(NatureServePlan.Action.StartRefresh, NatureServePlan.Decide(finished, false, 7, Now));
        // A refresh asks from the start of the last download, less an hour.
        Assert.Equal(Now.AddDays(-10).AddHours(-2), NatureServePlan.RefreshSince(finished));
    }

    [Theory]
    [InlineData(113530, 113530L, true)]
    [InlineData(113531, 113530L, true)]
    [InlineData(113529, 113530L, false)]
    [InlineData(5, null, false)]
    public void NatureServe_deletes_unseen_records_only_after_a_complete_download(long seen, long? total, bool delete) =>
        Assert.Equal(delete, NatureServePlan.MayDeleteUnseen(seen, total));

    [Fact]
    public void NatureServe_full_download_is_needed_once_for_new_fields() {
        var finished = State(completed: Now.AddDays(-1), fullCompleted: Now.AddDays(-1));
        // A store whose last full download was before the version was recorded.
        Assert.True(NatureServePlan.NeedsFullDownloadForFields(finished, 113_530));
        Assert.False(NatureServePlan.NeedsFullDownloadForFields(finished, 0));
        Assert.False(NatureServePlan.NeedsFullDownloadForFields(finished with { FullCompletedFieldsVersion = NatureServePlan.FieldsVersion }, 113_530));
        Assert.True(NatureServePlan.NeedsFullDownloadForFields(finished with { FullCompletedFieldsVersion = 1 }, 113_530));

        // A full download of the current version under way will add them; a refresh, or a full
        // download started before the version was recorded, will not.
        var full = finished with { PassStartedUtc = Now, PassFieldsVersion = NatureServePlan.FieldsVersion };
        Assert.False(NatureServePlan.NeedsFullDownloadForFields(full, 113_530));
        Assert.True(NatureServePlan.NeedsFullDownloadForFields(full with { PassIsRefresh = true }, 113_530));
        Assert.True(NatureServePlan.NeedsFullDownloadForFields(full with { PassFieldsVersion = null }, 113_530));

        var read = NatureServePassState.Read(key => key switch {
            NatureServePassKeys.PassFieldsVersion => "2",
            NatureServePassKeys.FullCompletedFieldsVersion => "2",
            _ => null,
        });
        Assert.Equal((2L, 2L), (read.PassFieldsVersion, read.FullCompletedFieldsVersion));
    }

    [Fact]
    public void NatureServe_citation_has_the_access_date() =>
        Assert.Equal("NatureServe. 2026. NatureServe Explorer [web application]. NatureServe, Arlington, Virginia. Available https://explorer.natureserve.org/. (Accessed: October 8, 2026).",
            NatureServePlan.Citation(Now));

    // ---- the store ----

    private static StatusListStore MemoryStore(out SqliteConnection connection) {
        connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return StatusListStore.OpenFromConnection(connection);
    }

    [Fact]
    public void Store_pages_split_and_finish_a_pass() {
        using var store = MemoryStore(out var conn);
        using var connection = conn;
        var page = NatureServeSearch.ParsePage(Page);
        var start = Now;
        store.StartNatureServePass(new Dictionary<string, string?> { [NatureServePassKeys.Started] = StatusListStore.Stamp(start) },
            NatureServePassKeys.PassKeys, "");

        // The root has more than 10,000 records: its first page is stored and it becomes A..Z.
        store.StoreNatureServePage(store.GetPartitions().Single(), 113530, false, page.Species, start.AddMinutes(1), NatureServeSearch.Split(""));
        var partitions = store.GetPartitions();
        Assert.Equal(26, partitions.Count);
        Assert.Equal("A", partitions[0].Prefix);
        Assert.Equal(2, store.CountNatureServe());

        // Old record not seen by the pass, and a record seen again (synonyms replaced).
        store.StoreNatureServePage(partitions[0], 2, false, new[] { page.Species[0] with { ElementGlobalId = 1, Synonyms = Array.Empty<string>() } },
            start.AddDays(-30));
        store.StoreNatureServePage(store.GetPartitions()[0], 2, true, page.Species, start.AddMinutes(2));
        Assert.Equal(3, store.CountNatureServe());
        Assert.Equal(2, store.CountNatureServeFetchedSince(start));
        Assert.True(store.GetPartitions()[0].Done);

        var deleted = store.CompleteNatureServePass(Array.Empty<long>(), start,
            new Dictionary<string, string?> { [NatureServePassKeys.Completed] = StatusListStore.Stamp(start.AddHours(1)) },
            NatureServePassKeys.PassKeys,
            rows => new StatusSourceInfo(StatusSources.NatureServe, "NatureServe Explorer", "https://explorer.natureserve.org/", "CC BY 4.0", null, null, start.AddHours(1), rows));
        Assert.Equal(1, deleted);
        Assert.Equal(2, store.CountNatureServe());
        // The record the pass did not see goes with its national and subnational ranks.
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation WHERE element_global_id = 1"));
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation WHERE element_global_id = 1"));
        Assert.Equal(2L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation WHERE element_global_id = 828458"));
        Assert.Empty(store.GetPartitions());
        Assert.Null(store.GetState(NatureServePassKeys.Started));
        Assert.Equal(2, store.Sources().Single().RowCount);

        using var synonyms = conn.CreateCommand();
        synonyms.CommandText = "SELECT name FROM natureserve_synonym WHERE element_global_id = 828458";
        Assert.Equal("Acris crepitans blanchardi", synonyms.ExecuteScalar());
        using var types = conn.CreateCommand();
        types.CommandText = "SELECT typeof(element_global_id), typeof(infraspecies) FROM natureserve_species LIMIT 1";
        using var reader = types.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(("integer", "integer"), (reader.GetString(0), reader.GetString(1)));
    }

    [Fact]
    public void Store_replaces_national_and_subnational_ranks_with_the_record_and_deletes_them_with_it() {
        using var store = MemoryStore(out var conn);
        using var connection = conn;
        var page = NatureServeSearch.ParsePage(RanksPage);
        var eagle = page.Species[0];
        var myna = page.Species[1];
        var start = Now;
        store.StartNatureServePass(new Dictionary<string, string?> { [NatureServePassKeys.Started] = StatusListStore.Stamp(start) },
            NatureServePassKeys.PassKeys, "");
        var partition = store.GetPartitions().Single();
        store.StoreNatureServePage(partition, 2, false, page.Species, start.AddMinutes(1));

        Assert.Equal(3L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation"));
        Assert.Equal(9L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation"));
        Assert.Equal((2L, 2L), store.CountNatureServeWithRanks());
        using (var row = conn.CreateCommand()) {
            row.CommandText = """
                SELECT rounded_s_rank, native, exotic, typeof(native) FROM natureserve_subnation
                WHERE element_global_id = 104470 AND nation_code = 'CA' AND subnation_code = 'LB'
                """;
            using var reader = row.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(("S4B,SUM", 1L, 0L, "integer"), (reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetString(3)));
        }

        // Stored again with other ranks: the old rows go. A nation with no flags stores NULL.
        var changed = eagle with {
            Nations = new[] { new NatureServeNation("US", "N4", null, null, new[] { new NatureServeSubnation("TX", "S2", true, false) }) },
        };
        store.StoreNatureServePage(store.GetPartitions().Single(), 2, false, new[] { changed }, start.AddMinutes(2));
        Assert.Equal(1L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation WHERE element_global_id = 104470"));
        Assert.Equal(1L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation WHERE element_global_id = 104470 AND rounded_n_rank = 'N4' AND native IS NULL"));
        Assert.Equal(1L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation WHERE element_global_id = 104470"));
        Assert.Equal(2L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation WHERE element_global_id = 105264"));

        // Stored with no nations: no rows. Deleted with the record: none left.
        store.StoreNatureServePage(store.GetPartitions().Single(), 2, false, new[] { myna with { Nations = Array.Empty<NatureServeNation>() } }, start.AddMinutes(3));
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation WHERE element_global_id = 105264"));
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation WHERE element_global_id = 105264"));
        store.CompleteNatureServePass(new long[] { 104470 }, null, new Dictionary<string, string?>(), NatureServePassKeys.PassKeys,
            rows => new StatusSourceInfo(StatusSources.NatureServe, "NatureServe Explorer", "https://explorer.natureserve.org/", "CC BY 4.0", null, null, start, rows));
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_nation"));
        Assert.Equal(0L, Count(conn, "SELECT COUNT(*) FROM natureserve_subnation"));
        Assert.Equal((0L, 0L), store.CountNatureServeWithRanks());
    }

    private static long Count(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    [Fact]
    public void Store_replaces_ecos_listings() {
        using var store = MemoryStore(out var conn);
        using var connection = conn;
        var rows = EcosListedSpecies.Read(new StringReader(EcosCsv), out _);
        var source = new StatusSourceInfo(StatusSources.Ecos, EcosListedSpecies.Title, EcosListedSpecies.SiteUrl, EcosListedSpecies.Licence, null, "file.csv", Now, rows.Count);
        store.ReplaceEcos(rows, Now, source);
        store.ReplaceEcos(rows.Take(2).ToList(), Now, source with { RowCount = 2 });
        Assert.Equal(2, store.CountEcos());

        using var names = conn.CreateCommand();
        names.CommandText = "SELECT COUNT(*) FROM ecos_name";
        Assert.Equal(3L, names.ExecuteScalar());
        using var types = conn.CreateCommand();
        types.CommandText = "SELECT typeof(species_id), typeof(itis_tsn), typeof(is_dps) FROM ecos_listing WHERE entity_id = 3907";
        using var reader = types.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(("integer", "integer", "integer"), (reader.GetString(0), reader.GetString(1), reader.GetString(2)));
    }

    // ---- workflow lights ----

    private static PublicSiteState Site() => new() { StatusListsPath = "/data/status_lists.sqlite", ReadAtUtc = Now };

    [Fact]
    public void NatureServe_light() {
        Assert.Equal(("todo", "Not downloaded yet."), Result(PublicSiteProbes.NatureServeStep(Site())));

        var running = Site() with { NatureServePassStartedUtc = Now.AddHours(-1), NatureServePassStored = 40_000, NatureServePassTotal = 113_530 };
        Assert.Equal(("backlog", "A download started 2026-10-08 and has stored 40,000 of 113,530 records. Run the step again to finish it."),
            Result(PublicSiteProbes.NatureServeStep(running)));

        var recent = Site() with { NatureServe = new StatusListSourceState(Now.AddDays(-3), 113_530) };
        Assert.Equal(("ok", "113,530 records. The last download finished 2026-10-05."), Result(PublicSiteProbes.NatureServeStep(recent)));

        var old = Site() with { NatureServe = new StatusListSourceState(Now.AddDays(-40), 113_530) };
        Assert.Equal("todo", PublicSiteProbes.NatureServeStep(old).Status);

        Assert.Equal("todo", PublicSiteProbes.NatureServeStep(new PublicSiteState()).Status);
    }

    [Fact]
    public void Ecos_light() {
        Assert.Equal(("todo", "Not downloaded yet."), Result(PublicSiteProbes.EcosStep(Site())));
        var recent = Site() with { Ecos = new StatusListSourceState(Now.AddDays(-1), 2478) };
        Assert.Equal(("ok", "2,478 listings, downloaded 2026-10-07."), Result(PublicSiteProbes.EcosStep(recent)));
        var old = Site() with { Ecos = new StatusListSourceState(Now.AddDays(-31), 2478) };
        Assert.Equal(("todo", "2,478 listings, downloaded 2026-09-07, 31 days ago."), Result(PublicSiteProbes.EcosStep(old)));
    }

    private static (string, string?) Result(FlowProbeResult r) => (r.Status, r.Detail);
}
