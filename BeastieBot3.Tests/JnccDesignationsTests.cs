using System.Globalization;
using System.IO.Compression;
using System.Security;
using System.Text;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `statuses jncc-import`: finding the workbook's link on JNCC's resource page, reading the Master
// List sheet of a small workbook built here, where each designation applies, the codes, kingdoms
// and ranks read from the rows, and the run with --file.
public sealed class JnccDesignationsTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-jncc-" + Guid.NewGuid().ToString("N"));

    public JnccDesignationsTests() {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), "[Datastore]\n");
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    // ---- the resource page ----

    // The resource list of https://jncc.gov.uk/resources/478f7160-967b-4366-acdf-8941fd33850b on
    // 2026-10-08, cut down to its links.
    private const string ResourcePage = """
        <div class="tabs-panel is-active" id="resource-list"><ul>
          <li class="pb-2" id="conservation-designations-20260609.zip">
            <a href="https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/conservation-designations-20260609.zip"
               class="space-after" data-event="download" data-size="8093030" target="_blank">Conservation Designations for UK Taxa &#x2013; zipped spreadsheet and guidance</a>
          </li>
          <li class="pb-2" id="conservation-designations-uktaxa-spreadsheet-guidance.pdf">
            <a href="https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/conservation-designations-uktaxa-spreadsheet-guidance.pdf"
               class="space-after" data-event="download" data-size="105874" target="_blank">Conservation Designations for UK Taxa &#x2013; guidance</a>
          </li>
          <li class="pb-2" id="taxon-designations-20260609.xlsx">
            <a href="https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20260609.xlsx"
               class="space-after" data-event="download" data-size="8495468" target="_blank">Conservation Designations for UK Taxa &#x2013; designations spreadsheet</a>
          </li>
          <li class="pb-2" id="b0714797-4481-43a9-8275-8bfa0e1d1aba">
            <a href="https://jncc.gov.uk/resources/b0714797-4481-43a9-8275-8bfa0e1d1aba" class="space-after" data-event="external">GB Red List Dataset</a>
          </li>
        </ul></div>
        """;

    [Fact]
    public void Workbook_link_is_found_on_the_resource_page() {
        var link = JnccDesignations.FindSpreadsheetLink(ResourcePage);
        Assert.Equal(new JnccSpreadsheetLink(
            "https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20260609.xlsx",
            "taxon-designations-20260609.xlsx"), link);
    }

    [Fact]
    public void Newest_dated_workbook_is_taken() {
        var page = ResourcePage + """
            <a href="/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20270115.xlsx?x=1&amp;y=2">new</a>
            """;
        var link = JnccDesignations.FindSpreadsheetLink(page)!;
        Assert.Equal("taxon-designations-20270115.xlsx", link.FileName);
        Assert.Equal("https://jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20270115.xlsx?x=1&y=2", link.Url);
    }

    [Fact]
    public void Workbook_under_another_name_in_the_data_folder_is_taken_when_no_name_has_a_date() {
        const string page = """
            <a href="https://example.org/other.xlsx">elsewhere</a>
            <a href="https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/designations.xlsx">workbook</a>
            """;
        Assert.Equal("designations.xlsx", JnccDesignations.FindSpreadsheetLink(page)!.FileName);
        Assert.Null(JnccDesignations.FindSpreadsheetLink("""<a href="https://data.jncc.gov.uk/data/x/guidance.pdf">pdf</a>"""));
    }

    [Fact]
    public void Release_date_and_attribution_come_from_the_file_name() {
        Assert.Equal(new DateOnly(2026, 6, 9), JnccDesignations.DateInFileName("/data/taxon-designations-20260609.xlsx"));
        Assert.Null(JnccDesignations.DateInFileName("taxon-designations.xlsx"));
        Assert.Null(JnccDesignations.DateInFileName("taxon-designations-20261399.xlsx"));

        var fetched = new DateTime(2027, 2, 1, 0, 0, 0, DateTimeKind.Utc);
        var source = new StatusSourceInfo(StatusSources.Jncc, JnccDesignations.Title, JnccDesignations.ResourcePageUrl, JnccDesignations.Licence,
            JnccDesignations.Attribution(fetched.Year), "x", fetched, 1);

        var kept = JnccDesignations.SourceFor("/kept/taxon-designations-20260609.xlsx", null, source);
        Assert.Equal("https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20260609.xlsx", kept.Url);
        Assert.Equal("Contains JNCC/NE/NRW/NatureScot/NIEA data © copyright and database right 2026", kept.Citation);

        var downloaded = JnccDesignations.SourceFor("/folder/taxon-designations-20260609.xlsx",
            new JnccSpreadsheetLink("https://mirror.example/taxon-designations-20260609.xlsx", "taxon-designations-20260609.xlsx"), source);
        Assert.Equal("https://mirror.example/taxon-designations-20260609.xlsx", downloaded.Url);

        var renamed = JnccDesignations.SourceFor("/kept/designations.xlsx", null, source);
        Assert.Equal((JnccDesignations.ResourcePageUrl, "Contains JNCC/NE/NRW/NatureScot/NIEA data © copyright and database right 2027"),
            (renamed.Url, renamed.Citation));
    }

    // ---- reading the workbook ----

    private static readonly string[] Headings = [
        "Category", "Taxon group", "Recommended taxon name", "Recommended authority", "Recommended qualifier", "Recommended taxon version",
        "Designated name", "Common name", "Source", "Source description", "URL source", "Date designated", "Reporting category",
        "Designation", "Designation abbreviation", "designation description", "IUCN version", "Criteria description", "Comments",
        "Reporting category sort order",
    ];

    // Rows in the form of the 2026-06-09 file, with their values cut down.
    private static object?[][] MasterList() => [
        ["The Master List contains one row for each designation applied to each taxa."],
        Headings,
        ["Bird", "bird", "Anas crecca", "Linnaeus, 1758", null, "NBNSYS0000000131", "Anas crecca crecca", "Teal",
            "The risk of extinction for birds in Great Britain Sep 2017 British Birds 110: 502-517.", "A long description.",
            "https://www.bto.org/", new DateTime(2017, 9, 1), "Red listing based on 2001 IUCN guidelines", "Least Concern",
            "Bird_RedList_GB_post2001-LC_NonBreeding", "A long description.", 2001.0, null, "The status of non-breeding Teal was assessed ...", "Fc"],
        ["Invertebrate", "mollusc", "Vertigo (Vertigo) moulinsiana", "(Dupuy, 1849)", null, "NBNSYS0000000999", "Vertigo moulinsiana", null,
            "Wildlife and Countryside Act 1981", null, null, new DateTime(1988, 1, 1), "Wildlife and Countryside Act 1981",
            "Schedule 5 Section 9.4b", "WACA-Sch5_sect9.4b", "Protection.", null, null, "Designation does not apply in Scotland since 2007", "I"],
        [],
        ["Vascular plant", "flowering plant", "Bromus interruptus", "(Hack.) Druce", null, "NHMSYS0000456674", "Bromus interruptus", "Interrupted Brome",
            "A Vascular Plant Red List for England", null, "https://www.bsbi.org.uk/england", 41961.0,
            "Red listing based on 2001 IUCN guidelines", "Extinct in the Wild", "RedList_GB_post2001-EW", "Extinct in the wild.", 2001.0, 7.0, null, "Fc"],
        ["Invertebrate", "insect - beetle (Coleoptera)", "Carabus intricatus", "Linnaeus, 1761", "s.l.", "NBNSYS0000024000", "Carabus intricatus", null,
            "British Red Data Books: 2. Insects", null, null, "1987-01-01", "Red Listing based on pre 1994 IUCN guidelines",
            "IUCN (pre 1994) - Insufficiently known", "RedList_GB_Pre94-Insu", "Insufficiently known.", "pre 1994", null, "pre 1994 IUCN criteria", "Fa"],
        ["Mammal", "marine mammal", "Phocoena phocoena", "(Linnaeus, 1758)", null, "NHMSYS0000080190", "Phocoena phocoena", "Harbour Porpoise",
            "Agreement on the Conservation of Small Cetaceans", null, "https://www.ascobans.org/", new DateTime(2008, 2, 3),
            "Convention on Migratory Species", "ASCOBANS", "CMS_ASCOBANS", "Small cetaceans.", null, "All species ...", null, "C1"],
        ["Fungi", "fungus", null, null, null, null, null, null, null, null, null, null, "Bern Convention", "Appendix 1", "Bern-A1", null, null, null, null, "A"],
    ];

    private IReadOnlyList<JnccDesignation> ReadWorkbook(byte[] workbook, out int skipped) {
        using var stream = new MemoryStream(workbook);
        return JnccDesignations.Read(stream, out skipped);
    }

    [Fact]
    public void Master_list_rows_are_read() {
        var workbook = Xlsx(("Spreadsheet guidance", [["BASIC GUIDANCE FOR SPREADSHEET USE"]]), ("Master List", MasterList()));
        var rows = ReadWorkbook(workbook, out var skipped);

        Assert.Equal(1, skipped);
        Assert.Equal([3, 4, 6, 7, 8], rows.Select(r => r.RowNumber));

        Assert.Equal(new JnccDesignation(3, "NBNSYS0000000131", "Anas crecca", "Linnaeus, 1758", null, "species", "Anas crecca crecca", "Teal",
            "Bird", "bird", "ANIMALIA", "Red listing based on 2001 IUCN guidelines", "Fc", "Least Concern", "Bird_RedList_GB_post2001-LC_NonBreeding",
            "LC", "non-breeding", "2001", JnccClassification.Uk, JnccClassification.GreatBritain,
            "The risk of extinction for birds in Great Britain Sep 2017 British Birds 110: 502-517.", "https://www.bto.org/", "2017-09-01"), rows[0]);

        // The comment narrows the Wildlife and Countryside Act row to England and Wales; the subgenus does not change the rank.
        Assert.Equal(("species", JnccClassification.Country, JnccClassification.EnglandAndWales, "1988-01-01", null),
            (rows[1].Rank, rows[1].Scope, rows[1].Area, rows[1].DesignatedOn, rows[1].StatusCode));

        // A date as a serial number, and an England red list row with a GB code.
        Assert.Equal(("2014-11-18", "EW", JnccClassification.Country, JnccClassification.England, "PLANTAE"),
            (rows[2].DesignatedOn, rows[2].StatusCode, rows[2].Scope, rows[2].Area, rows[2].Kingdom));

        // A date as text and the IUCN version "pre 1994".
        Assert.Equal(("1987-01-01", "pre 1994", "Insu", "s.l."), (rows[3].DesignatedOn, rows[3].IucnVersion, rows[3].StatusCode, rows[3].Qualifier));

        Assert.Equal((JnccClassification.International, "North-East Atlantic and Baltic", (string?)null),
            (rows[4].Scope, rows[4].Area, rows[4].IucnVersion));
    }

    [Fact]
    public void Workbook_without_the_master_list_or_its_columns_is_refused() {
        Assert.Throws<InvalidDataException>(() => ReadWorkbook(Xlsx(("Summary for each taxon", MasterList())), out _));

        var noCode = MasterList().Select(row => row.Select(v => Equals(v, "Designation abbreviation") ? "Code" : v).ToArray()).ToArray();
        var ex = Assert.Throws<InvalidDataException>(() => ReadWorkbook(Xlsx(("Master List", noCode)), out _));
        Assert.Contains("Designation abbreviation", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Kept_workbook_is_stored_with_its_source_row() {
        var file = Path.Combine(_dir, "taxon-designations-20260609.xlsx");
        File.WriteAllBytes(file, Xlsx(("Master List", MasterList())));
        var storePath = Path.Combine(_dir, "status_lists.sqlite");

        var code = await StatusListImport.RunAsync(new JnccImportCommand.Settings {
            File = file, StorePath = storePath, IniFile = Path.Combine(_dir, "paths.ini"), SettingsDir = _dir,
        }, JnccImportCommand.Spec(), CancellationToken.None);

        Assert.Equal(0, code);
        using var store = StatusListStore.OpenReadOnly(storePath)!;
        Assert.Equal(5, store.CountJncc());
        var source = Assert.Single(store.Sources());
        Assert.Equal((StatusSources.Jncc, "taxon-designations-20260609.xlsx", 5L,
                "https://data.jncc.gov.uk/data/478f7160-967b-4366-acdf-8941fd33850b/taxon-designations-20260609.xlsx",
                "Contains JNCC/NE/NRW/NatureScot/NIEA data © copyright and database right 2026"),
            (source.Source, source.Version, source.RowCount, source.Url, source.Citation));
    }

    // ---- what is read from the rows ----

    // Every designation code in the 2026-06-09 file.
    private static readonly string[] Codes2026 = [
        "BAP-2007", "Bern-A1", "Bern-A2", "Bern-A3", "Bird-Amber", "Bird-Red", "Bird_RedList_GB_post2001-CR(PE)_Breeding",
        "Bird_RedList_GB_post2001-CR_Breeding", "Bird_RedList_GB_post2001-CR_NonBreeding", "Bird_RedList_GB_post2001-DD_Breeding",
        "Bird_RedList_GB_post2001-DD_NonBreeding", "Bird_RedList_GB_post2001-EN_Breeding", "Bird_RedList_GB_post2001-EN_NonBreeding",
        "Bird_RedList_GB_post2001-EX_Breeding", "Bird_RedList_GB_post2001-LC_Breeding", "Bird_RedList_GB_post2001-LC_NonBreeding",
        "Bird_RedList_GB_post2001-NT_Breeding", "Bird_RedList_GB_post2001-NT_NonBreeding", "Bird_RedList_GB_post2001-RE_Breeding",
        "Bird_RedList_GB_post2001-VU_Breeding", "Bird_RedList_GB_post2001-VU_NonBreeding", "BirdsDir-A1", "BirdsDir-A2.1",
        "BirdsDir-A2.2", "CMS_A1", "CMS_A2", "CMS_AEWA-A2", "CMS_ASCOBANS", "CMS_EUROBATS-A1", "ConsRegsNI-Sch2", "ConsRegsNI-Sch3",
        "ConsRegsNI-Sch4", "ECCITES-A", "ECCITES-B", "ECCITES-C", "ECCITES-D", "England_NERC_S.41", "Env (Wales) Act S7", "HabDir-A2",
        "HabDir-A2*", "HabDir-A4", "HabDir-A5", "HabReg-Sch2", "HabReg-Sch4", "HabReg-Sch5", "Marine-NR", "Marine-NS", "NI_Priority",
        "NR-excludes", "NR-includes", "NS-excludes", "NS-includes", "Notable", "Notable-A", "Notable-B", "OSPAR",
        "Protection_of_Badgers_Act_1992", "RedList_ENG_post2001-CR", "RedList_ENG_post2001-DD", "RedList_ENG_post2001-EN",
        "RedList_ENG_post2001-LC", "RedList_ENG_post2001-NT", "RedList_ENG_post2001-RE", "RedList_ENG_post2001-VU",
        "RedList_Europe_post2001-LC", "RedList_Europe_post2001-NT", "RedList_GB_Pre94-EN", "RedList_GB_Pre94-EX",
        "RedList_GB_Pre94-Inde", "RedList_GB_Pre94-Insu", "RedList_GB_Pre94-R", "RedList_GB_Pre94-VU", "RedList_GB_post2001-CR",
        "RedList_GB_post2001-CR(PE)", "RedList_GB_post2001-DD", "RedList_GB_post2001-EN", "RedList_GB_post2001-EW",
        "RedList_GB_post2001-EX", "RedList_GB_post2001-LC", "RedList_GB_post2001-NA", "RedList_GB_post2001-NE", "RedList_GB_post2001-NT",
        "RedList_GB_post2001-RE", "RedList_GB_post2001-VU", "RedList_GB_post94-CR", "RedList_GB_post94-DD", "RedList_GB_post94-EN",
        "RedList_GB_post94-EX", "RedList_GB_post94-NT", "RedList_GB_post94-VU", "RedList_Global_post2001-CR",
        "RedList_Global_post2001-DD", "RedList_Global_post2001-EN", "RedList_Global_post2001-EX", "RedList_Global_post2001-LC",
        "RedList_Global_post2001-NT", "RedList_Global_post2001-VU", "RedList_Global_post94-CR", "RedList_Global_post94-DD",
        "RedList_Global_post94-EN", "RedList_Global_post94-LC", "RedList_Global_post94-LR(cd)", "RedList_Global_post94-NT",
        "RedList_Global_post94-VU", "Scottish_Biodiversity_List", "Spider-Amber", "W(NI)O-Sch1_part1", "W(NI)O-Sch1_part2",
        "W(NI)O-Sch5", "W(NI)O-Sch8_part1", "W(NI)O-Sch8_part2", "WACA-Sch1_part1", "WACA-Sch1_part2", "WACA-Sch5", "WACA-Sch5Sect9.4c",
        "WACA-Sch5_sect9.1(kill/injuring)", "WACA-Sch5_sect9.1(taking)", "WACA-Sch5_sect9.2", "WACA-Sch5_sect9.4.a",
        "WACA-Sch5_sect9.4A", "WACA-Sch5_sect9.4b", "WACA-Sch5_sect9.5a", "WACA-Sch8", "WL",
    ];

    [Fact]
    public void Every_code_of_the_2026_file_has_a_scope() {
        Assert.Equal(124, Codes2026.Length);
        Assert.DoesNotContain(Codes2026, c => JnccClassification.ScopeOf(c, null, null) is null);
        Assert.Null(JnccClassification.ScopeOf("NewList-A1", null, null));
    }

    [Theory]
    [InlineData("RedList_GB_post2001-VU", "uk", "Great Britain")]
    [InlineData("Bird_RedList_GB_post2001-CR_Breeding", "uk", "Great Britain")]
    [InlineData("Bird-Red", "uk", "United Kingdom")]
    [InlineData("BAP-2007", "uk", "United Kingdom")]
    [InlineData("NS-includes", "uk", "Great Britain")]
    [InlineData("WACA-Sch1_part1", "uk", "Great Britain")]
    [InlineData("RedList_ENG_post2001-CR", "country", "England")]
    [InlineData("WL", "country", "England")]
    [InlineData("England_NERC_S.41", "country", "England")]
    [InlineData("Scottish_Biodiversity_List", "country", "Scotland")]
    [InlineData("Env (Wales) Act S7", "country", "Wales")]
    [InlineData("NI_Priority", "country", "Northern Ireland")]
    [InlineData("W(NI)O-Sch5", "country", "Northern Ireland")]
    [InlineData("HabReg-Sch2", "country", "England and Wales")]
    [InlineData("RedList_Global_post94-LR(cd)", "international", "World")]
    [InlineData("RedList_Europe_post2001-LC", "international", "Europe")]
    [InlineData("Bern-A2", "international", "Europe")]
    [InlineData("HabDir-A2*", "international", "European Union")]
    [InlineData("ECCITES-A", "international", "European Union")]
    [InlineData("CMS_A1", "international", "World")]
    [InlineData("CMS_AEWA-A2", "international", "Africa-Eurasia")]
    [InlineData("CMS_EUROBATS-A1", "international", "Europe")]
    [InlineData("OSPAR", "international", "North-East Atlantic")]
    public void Scope_and_area_of_a_code(string code, string scope, string area) =>
        Assert.Equal((scope, area), JnccClassification.ScopeOf(code, null, null));

    [Fact]
    public void Source_and_comments_narrow_a_great_britain_designation() {
        Assert.Equal(("country", "England"), JnccClassification.ScopeOf("RedList_GB_post2001-EX", "A Vascular Plant Red List for England", null));
        Assert.Equal(("country", "England and Wales"), JnccClassification.ScopeOf("WACA-Sch5_sect9.4b", null, "Does not apply to Scotland since 2007"));
        Assert.Equal(("country", "England"), JnccClassification.ScopeOf("WACA-Sch5", null, "England only. Uncommon and vulnerable."));
        Assert.Equal(("uk", "Great Britain"), JnccClassification.ScopeOf("WACA-Sch5_sect9.4b", null, "Wales? Most recent ammendment 2008"));
        // Only a Great Britain designation is narrowed.
        Assert.Equal(("country", "Scotland"), JnccClassification.ScopeOf("Scottish_Biodiversity_List", null, "Does not apply to Scotland since 2007"));
    }

    [Theory]
    [InlineData("RedList_GB_post2001-CR(PE)", "CR(PE)", null)]
    [InlineData("RedList_Global_post94-LR(cd)", "LR(cd)", null)]
    [InlineData("RedList_GB_Pre94-Insu", "Insu", null)]
    [InlineData("Bird_RedList_GB_post2001-VU_Breeding", "VU", "breeding")]
    [InlineData("Bird_RedList_GB_post2001-CR(PE)_Breeding", "CR(PE)", "breeding")]
    [InlineData("Bird_RedList_GB_post2001-LC_NonBreeding", "LC", "non-breeding")]
    [InlineData("WL", "WL", null)]
    [InlineData("Bird-Amber", "Amber", null)]
    [InlineData("Spider-Amber", "Amber", null)]
    [InlineData("WACA-Sch5_sect9.4b", null, null)]
    [InlineData("NR-includes", null, null)]
    public void Status_code_and_population_of_a_code(string code, string? status, string? population) =>
        Assert.Equal((status, population), JnccClassification.StatusOf(code));

    [Theory]
    [InlineData("Bird", "bird", "ANIMALIA")]
    [InlineData("Invertebrate", "spider (Araneae)", "ANIMALIA")]
    [InlineData("Vascular plant", "fern", "PLANTAE")]
    [InlineData("Non-vascular plant", "moss", "PLANTAE")]
    [InlineData("Non-vascular plant", "lichen", "FUNGI")]
    [InlineData("Non-vascular plant", "alga", null)]
    [InlineData("Fungi", "fungus", "FUNGI")]
    [InlineData("Algae", "chromist", "CHROMISTA")]
    [InlineData("Slime mould", "slime mould", null)]
    public void Kingdom_of_a_group(string category, string group, string? kingdom) =>
        Assert.Equal(kingdom, JnccClassification.KingdomOf(category, group));

    [Theory]
    [InlineData("Glossobalanus sarniensis", "ANIMALIA", "species")]
    [InlineData("Lithobius (Monotarsobius) crassipes", "ANIMALIA", "species")]
    [InlineData("Arion (Carinarion) circumscriptus silvaticus", "ANIMALIA", "subspecies")]
    [InlineData("Acanthis flammea cabaret", "ANIMALIA", "subspecies")]
    [InlineData("Mine site community", "FUNGI", null)]
    [InlineData("Alosa fallax subsp. fallax", "ANIMALIA", "subspecies")]
    [InlineData("Euplectus bonvouloiri ssp. rosae", "ANIMALIA", "subspecies")]
    [InlineData("Dactylorhiza traunsteinerioides subsp. francis-drucei var. ebudensis", "PLANTAE", "variety")]
    [InlineData("Neoboletus praestigiator f. pseudosulphureus", "FUNGI", "form")]
    [InlineData("Euphydryas aurinia form aurinia", "ANIMALIA", "form")]
    [InlineData("Cladonia chlorophaea s. lat.", "FUNGI", "species")]
    [InlineData("Polytrichum commune s.l.", "PLANTAE", "species")]
    [InlineData("Taraxacum agg.", "PLANTAE", "aggregate")]
    [InlineData("Anser fabalis/serrirostris", "ANIMALIA", "aggregate")]
    [InlineData("Hieracium sect. Alpestria", "PLANTAE", "section")]
    [InlineData("Salix alba x euxina = S. x fragilis", "PLANTAE", "hybrid")]
    [InlineData("Cetacea", "ANIMALIA", "above species")]
    [InlineData("Aphodius (Nialus) ?varians", "ANIMALIA", null)]
    [InlineData("Mycetoporus 'species A'", "ANIMALIA", null)]
    [InlineData("Cantharis nigra (=thoracica)", "ANIMALIA", null)]
    [InlineData("Echinogammarus incertae sedis planicrurus", "ANIMALIA", null)]
    public void Rank_of_a_name(string name, string kingdom, string? rank) =>
        Assert.Equal(rank, JnccClassification.RankOf(name, kingdom));

    // ---- a small workbook ----

    // An .xlsx with the given sheets: text as inline strings, numbers as numbers, dates as serial
    // numbers in the built-in date format 14.
    private static byte[] Xlsx(params (string Name, object?[][] Rows)[] sheets) {
        using var output = new MemoryStream();
        using (var zip = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true)) {
            void Add(string path, string xml) {
                using var writer = new StreamWriter(zip.CreateEntry(path).Open(), new UTF8Encoding(false));
                writer.Write(xml);
            }
            const string Main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
            const string Rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
            var overrides = string.Concat(sheets.Select((_, i) =>
                $"""<Override PartName="/xl/worksheets/sheet{i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>"""));
            Add("[Content_Types].xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
                <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
                <Default Extension="xml" ContentType="application/xml"/>
                <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
                <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
                {overrides}
                </Types>
                """);
            Add("_rels/.rels", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                <Relationship Id="rId1" Type="{Rel}/officeDocument" Target="xl/workbook.xml"/>
                </Relationships>
                """);
            Add("xl/workbook.xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <workbook xmlns="{Main}" xmlns:r="{Rel}"><sheets>
                {string.Concat(sheets.Select((s, i) => $"""<sheet name="{SecurityElement.Escape(s.Name)}" sheetId="{i + 1}" r:id="rId{i + 1}"/>"""))}
                </sheets></workbook>
                """);
            Add("xl/_rels/workbook.xml.rels", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                {string.Concat(sheets.Select((_, i) => $"""<Relationship Id="rId{i + 1}" Type="{Rel}/worksheet" Target="worksheets/sheet{i + 1}.xml"/>"""))}
                <Relationship Id="rId{sheets.Length + 1}" Type="{Rel}/styles" Target="styles.xml"/>
                </Relationships>
                """);
            Add("xl/styles.xml", $"""
                <?xml version="1.0" encoding="UTF-8"?>
                <styleSheet xmlns="{Main}"><cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/></cellXfs></styleSheet>
                """);
            for (var i = 0; i < sheets.Length; i++) {
                var rows = new StringBuilder();
                for (var r = 0; r < sheets[i].Rows.Length; r++) {
                    rows.Append(CultureInfo.InvariantCulture, $"""<row r="{r + 1}">""");
                    for (var c = 0; c < sheets[i].Rows[r].Length; c++) {
                        var cell = $"{(char)('A' + c % 26)}".Insert(0, c >= 26 ? "A" : "") + (r + 1).ToString(CultureInfo.InvariantCulture);
                        rows.Append(sheets[i].Rows[r][c] switch {
                            null => "",
                            string s => $"""<c r="{cell}" t="inlineStr"><is><t>{SecurityElement.Escape(s)}</t></is></c>""",
                            DateTime d => $"""<c r="{cell}" s="1"><v>{d.ToOADate().ToString(CultureInfo.InvariantCulture)}</v></c>""",
                            double n => $"""<c r="{cell}"><v>{n.ToString(CultureInfo.InvariantCulture)}</v></c>""",
                            var other => throw new ArgumentException($"No cell for {other.GetType()}"),
                        });
                    }
                    rows.Append("</row>");
                }
                Add($"xl/worksheets/sheet{i + 1}.xml", $"""
                    <?xml version="1.0" encoding="UTF-8"?>
                    <worksheet xmlns="{Main}"><sheetData>{rows}</sheetData></worksheet>
                    """);
            }
        }
        return output.ToArray();
    }
}
