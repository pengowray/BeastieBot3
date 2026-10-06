using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;
using Author = BeastieBot3.Shared.Wikitext.CitationAuthor;

namespace BeastieBot3.Site.Tests;

/// A small site database built from SiteDbSchema.Ddl, with one taxon for each case the pages
/// handle. Ids follow IUCN's where they are known; the rest are made up.
public static class FixtureDb {
    public const long PolarBear = 22823;
    public const long PolarBearLatest = 14871490;
    public const long PolarBear2008 = 13045100;
    public const long PolarBear1996 = 13045101;
    public const long PolarBear1988Nt = 13045102;

    public const long HouseSparrow = 103818789;
    public const long HouseSparrowLatest = 155522130;
    public const long HouseSparrowEurope = 166245544;

    public const long Baiji = 12119;
    public const long BaijiLatest = 50358152;
    public const long Baiji1986Ex = 12119001;

    public const long Tiger = 15955;
    public const long TigerLatest = 214862019;
    /// Made-up Wikidata items for assessments.
    public const string TigerLatestItem = "Q900000001";
    public const string WestAfricanLionLatestItem = "Q900000002";
    public const long SumatranTiger = 15966;
    public const long SumatranTigerLatest = 136285;

    public const long Lion = 15951;
    public const long LionLatest = 280792135;
    public const long WestAfricanLion = 68933833;
    public const long WestAfricanLionLatest = 68933837;

    public const long RegionalOnly = 135570;
    public const long RegionalOnlyEurope = 135570001;
    public const long RegionalOnlyMediterranean = 135570002;
    /// An earlier Europe assessment of RegionalOnly, which its page does not list.
    public const long RegionalOnlyEurope2006 = 135570000;

    public const long Variety = 34010;

    // An errata pair: the latest assessment is the errata version of an assessment published the
    // same year, which is still listed. The Europe assessment has an errata version too.
    public const long Micropyropsis = 162107;
    public const long MicropyropsisLatest = 85140439;
    public const long MicropyropsisReplaced = 5539282;
    public const long MicropyropsisEurope = 85140183;
    public const long MicropyropsisEuropeReplaced = 5539452;

    // A plant subspecies (its species is not in the database), with an "NT" from 1998 that has no
    // criteria version.
    public const long PlantSubspecies = 32277;
    public const long PlantSubspeciesSibling = 32278;
    public const long NoScopeOnly = 155555;
    public const long NoScopeOnlyLatest = 155555001;
    public const long PlantSubspeciesLatest = 2812588;
    public const long PlantSubspecies1998Nt = 9692717;
    public const long PlantSubspecies1998Vu = 9692643;

    // Extinct in the Wild, with an "EX" from 1998 that has no criteria version.
    public const long Bromus = 165247;
    public const long BromusLatest = 5995954;
    public const long Bromus1998Ex = 5996068;

    // The house sparrow's 2018 assessment, replaced by the amended version published in 2019.
    public const long HouseSparrow2018 = 129643357;

    // Koala: SPRAT profile 197 (no EPBC listing) and 85104, the Endangered listing of the combined
    // populations of Queensland, New South Wales and the ACT.
    public const long Koala = 16892;
    public const long KoalaLatest = 166496779;
    public const long KoalaSprat = 197;
    public const long KoalaPopulationSprat = 85104;

    // Southern cassowary: listed under the EPBC Act as Casuarius casuarius johnsonii.
    public const long Cassowary = 22678108;
    public const long CassowaryLatest = 155429591;
    public const long CassowarySprat = 1096;

    // Leopard (in the release) and the Amur leopard, a subspecies IUCN no longer assesses: not in the
    // release, its newest assessment is Not Evaluated (2016).
    public const long Leopard = 15954;
    public const long LeopardLatest = 50659089;
    public const long AmurLeopard = 15957;
    public const long AmurLeopard2016Ne = 96947390;
    public const long AmurLeopard2008 = 5333757;
    public const long AmurLeopard1996 = 5333803;
    // Bombus pyrrhopygus: not in the release, with three Europe assessments and no global one.
    public const long BombusPyrrhopygus = 88120770;
    public const long BombusEurope2013 = 13357670;
    public const long BombusEurope2015 = 57368180;
    public const long BombusEurope2016 = 95860837;

    // The woylie: taxon 2790 in the release, whose latest citation has a DOI found by checking
    // doi.org, and an old id, 2785, with the same name, not in the release.
    public const long Woylie = 2790;
    public const long WoylieLatest = 2790001;
    public const string WoylieDoi = "10.2305/IUCN.UK.2015-4.RLTS.T2790A2790001.en";
    public const long WoylieOld = 2785;
    public const long WoylieOld2008 = 6143;

    // Acropora palmerae, in the release, and Acropora minuta, an old id whose name IUCN lists as a
    // synonym of it. Only the newest assessment of Acropora palmerae has taxonomic notes.
    public const long Palmerae = 133531;
    public const long PalmeraeLatest = 133531001;
    public const long Palmerae2008 = 133531000;
    public const long Minuta = 133018;
    public const long Minuta2008 = 133018001;

    // An old id with two links, as after a split: Platanista gangetica (41758) has the name of
    // 41756 and is a synonym of Platanista minor (41757).
    public const long Gangetica = 41756;
    public const long GangeticaLatest = 41756001;
    public const long Minor = 41757;
    public const long MinorLatest = 41757001;
    public const long GangeticaOld = 41758;
    public const long GangeticaOld2012 = 41758001;
    public const long GangeticaOld1996 = 41758002;
    public const long GangeticaOldAsia = 41758003;

    // Ids with no global assessments: Clessiniola variabilis (in the release, a Europe assessment
    // only) and Turricaspia trivialis, an old id whose name it lists as a synonym; and Pupilla
    // bigranata, an old id with a Europe assessment only, whose name Pupilla muscorum lists.
    public const long Clessiniola = 212620635;
    public const long ClessiniolaEurope = 212620635001;
    public const long Turricaspia = 189519;
    public const long Turricaspia2011 = 189519001;
    public const long PupillaMuscorum = 215033857;
    public const long PupillaMuscorumLatest = 215033857001;
    public const long PupillaBigranata = 156831;
    public const long PupillaBigranataEurope = 156831001;

    // Artemia monica (taxon 2117): Lower Risk/conservation dependent, which has no P141 value; its
    // item Q4560564 says least concern.
    public const long ArtemiaMonica = 2117;
    public const long ArtemiaMonicaLatest = 9254479;

    // Made-up P141 statement ids on the fixture's taxon items.
    public const string TigerP141Statement = "q132186$1A2B3C4D-0000-4000-8000-000000000001";
    public const string PolarBearP141Statement = "Q33609$1A2B3C4D-0000-4000-8000-000000000002";
    public const string PlantSubspeciesItem = "Q900000010";
    public const string KoalaLatestItem = "Q900000003";
    public const string KoalaP141Endangered = "Q36101$1A2B3C4D-0000-4000-8000-000000000005";
    public const string KoalaP141Vulnerable = "Q36101$1A2B3C4D-0000-4000-8000-000000000006";
    /// The tiger's assessment item's title and label, as SourceMD wrote them.
    public const string TigerItemOldTitle = "Panthera tigris: Goodrich, J. & Wibisono, H.";
    /// Made up: the item of Micropyropsis tuberosa's 2010 assessment, which its errata version shares.
    public const string MicropyropsisItem = "Q900000020";
    public const string MicropyropsisItemOldTitle = "Micropyropsis tuberosa: Rhazi, L., Grillas, P., Rhazi, M. & Flanagan, D.";
    /// Made up: the name Crossref registered for the polar bear's 2008 assessment, an older name.
    public const string PolarBear2008RegisteredName = "Thalarctos maritimus";
    /// The leopard's item, and a made-up second item that states its IUCN taxon ID too.
    public const string LeopardItem = "Q35694";
    public const string LeopardOtherItem = "Q900000030";
    /// The cassowary's item, which states its IUCN taxon ID at deprecated rank (made up).
    public const string CassowaryItem = "Q190722";
    public const string CassowaryLatestItem = "Q900000031";
    /// The woylie's item (made up): critically endangered with a reference that has its IUCN taxon
    /// ID, and endangered with a reference to another source.
    public const string WoylieItem = "Q900000040";
    public const string WoylieP141Critically = "Q900000040$1A2B3C4D-0000-4000-8000-000000000007";
    public const string WoylieP141Endangered = "Q900000040$1A2B3C4D-0000-4000-8000-000000000008";

    public const int FillerCount = 55;
    public const long FillerFirstId = 900000;

    /// Text placed in a citation_json as an unknown property, the way a narrative field would
    /// arrive if the builder ever wrote one. No response may contain it.
    public const string NarrativeMarker = "SECRET-NARRATIVE-MARKER";

    public const string VerbatimAuthor = "Jon Aars";

    private static readonly Lazy<string> Default = new(() => Create("fixture"));

    /// The shared fixture database (created once per test run).
    public static string Path => Default.Value;

    /// Creates a fixture database in a new temporary folder and returns its path. The folder is
    /// deleted when the test run ends. schemaVersion and dropTable make the broken variants;
    /// release sets meta iucn_release; withSourceCitations=false leaves out the GBIF and Catalogue
    /// of Life citation and DOI meta keys, and the date DOIs were last checked; wikidataItemModelJson
    /// replaces the stored Wikidata assessment item model (the defaults).
    public static string Create(string name, string? schemaVersion = null, string? dropTable = null, string release = "2026-1",
        bool withSourceCitations = true, string? wikidataItemModelJson = null) {
        var dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "beastiebot-site-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        AppDomain.CurrentDomain.ProcessExit += (_, _) => {
            try {
                SqliteConnection.ClearAllPools();
                Directory.Delete(dir, recursive: true);
            } catch (IOException) {
                // Left for the system's temp cleanup.
            } catch (UnauthorizedAccessException) {
            }
        };
        var path = System.IO.Path.Combine(dir, name + ".sqlite");

        var builder = new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false };
        using (var connection = new SqliteConnection(builder.ToString())) {
            connection.Open();
            Exec(connection, SiteDbSchema.Ddl);
            using var tx = connection.BeginTransaction();
            var writer = new Writer(connection, tx);
            Populate(writer, schemaVersion ?? SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture), release, withSourceCitations,
                wikidataItemModelJson ?? new WikidataItemModel().ToJson());
            tx.Commit();
            Exec(connection, "INSERT INTO name_fts(name_fts) VALUES('rebuild')");
            if (dropTable is not null) {
                Exec(connection, $"DROP TABLE {dropTable}");
            }
        }
        return path;
    }

    private static void Populate(Writer w, string schemaVersion, string release, bool withSourceCitations, string wikidataItemModelJson) {
        // Polar bear: a species with a global history, a citation with a DOI from GBIF, an
        // unsplit author, common names in three languages and synonyms.
        w.Taxon(PolarBear, "Ursus maritimus", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "URSIDAE", "Ursus",
            authority: "Phipps, 1774", commonEn: "Polar bear", enwiki: "Polar bear", qid: "Q33609", colId: "4QHKG", latest: PolarBearLatest,
            qidSource: "p627", p141: P141((PolarBearP141Statement, "Q278113", "normal", "Q115962546")), itemDownloaded: "2026-09-13");
        w.Assessment(PolarBearLatest, PolarBear, "Global", true, "VU", criteria: "A3c", criteriaVersion: "3.1", year: 2015,
            date: "2015-03-21", trend: "Unknown",
            citation: Citation(PolarBear, PolarBearLatest, 2015, "Ursus maritimus",
                [Person("Wiig", "Ø."), Person("Amstrup", "S."), Person("Atwood", "T."), Person("Laidre", "K."), Verbatim(VerbatimAuthor), Person("Thiemann", "G.")],
                doi: "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", doiSource: DoiSource.Gbif,
                text: "Wiig, Ø., Amstrup, S., Atwood, T., Laidre, K., Jon Aars & Thiemann, G. 2015. Ursus maritimus. The IUCN Red List of Threatened Species 2015: e.T22823A14871490.",
                narrative: true));
        w.Assessment(PolarBear2008, PolarBear, "Global", false, "VU", criteria: "A3c", criteriaVersion: "3.1", year: 2008,
            date: "2008-06-30", trend: "Decreasing",
            citation: Citation(PolarBear, PolarBear2008, 2008, "Ursus maritimus", [Person("Schliebe", "S.")], etAl: true,
                doi: null, doiSource: DoiSource.None, text: null, registeredName: PolarBear2008RegisteredName));
        w.Assessment(PolarBear1996, PolarBear, "Global", false, "LR/cd", criteriaVersion: "2.3", year: 1996, date: "1996-06-30");
        // site build-db stores IUCN's "Earlier Version" as a NULL criteria version.
        w.Assessment(PolarBear1988Nt, PolarBear, "Global", false, "nt", year: 1988, apiNotFound: true);
        w.Name(PolarBear, "Ursus maritimus", "scientific", null, "iucn");
        w.Name(PolarBear, "Polar bear", "common", "en", "iucn", preferred: true);
        w.Name(PolarBear, "Polar Bear", "common", "en", "wikidata");
        w.Name(PolarBear, "White bear", "common", "en", "col");
        w.Name(PolarBear, "Thalassic bear", "common", "en", "wikipedia-taxobox");
        w.Name(PolarBear, "Ours polaire", "common", "fr", "iucn");
        w.Name(PolarBear, "Ours blanc", "common", "fr", "wikidata");
        w.Name(PolarBear, "Oso polar", "common", "es", "iucn");
        // An ISO 639-2 code for English, and the code for an undetermined language.
        w.Name(PolarBear, "Ice bear", "common", "eng", "iucn");
        w.Name(PolarBear, "Nanuq", "common", "und", "iucn");
        // A synonym from two sources whose authorities differ, one from one source, and one with no authority.
        w.Name(PolarBear, "Thalarctos maritimus", "synonym", null, "iucn", authority: "(Phipps, 1774)");
        w.Name(PolarBear, "Ursus marinus", "synonym", null, "col", authority: "Pallas, 1776");
        w.Name(PolarBear, "Thalarctos maritimus", "synonym", null, "col", authority: "Phipps, 1774");
        w.Name(PolarBear, "Ursus polaris", "synonym", null, "wikidata");

        // House sparrow: a BirdLife assessment with an organisation as author and a regional one.
        w.Taxon(HouseSparrow, "Passer domesticus", "species", "ANIMALIA", "CHORDATA", "AVES", "PASSERIFORMES", "PASSERIDAE", "Passer",
            authority: "(Linnaeus, 1758)", commonEn: "House sparrow", enwiki: "House sparrow", qid: "Q28922", latest: HouseSparrowLatest,
            qidSource: "p627");
        w.Assessment(HouseSparrowLatest, HouseSparrow, "Global", true, "LC", criteriaVersion: "3.1", year: 2019, date: "2018-08-07", trend: "Decreasing",
            citation: Citation(HouseSparrow, HouseSparrowLatest, 2019, "Passer domesticus", [Organisation("BirdLife International")],
                doi: "10.2305/IUCN.UK.2019-3.RLTS.T103818789A155522130.en", doiSource: DoiSource.Citation, text: "BirdLife International. 2019. Passer domesticus.",
                amendsYear: 2018));
        w.Assessment(HouseSparrow2018, HouseSparrow, "Global", false, "LC", criteriaVersion: "3.1", year: 2018, date: "2016-10-01",
            replacedBy: HouseSparrowLatest,
            citation: Citation(HouseSparrow, HouseSparrow2018, 2018, "Passer domesticus", [Organisation("BirdLife International")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(HouseSparrowEurope, HouseSparrow, "Europe", true, "LC", criteriaVersion: "3.1", year: 2021, date: "2021-03-01",
            citation: Citation(HouseSparrow, HouseSparrowEurope, 2021, "Passer domesticus", [Organisation("BirdLife International")],
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe"));
        w.Name(HouseSparrow, "Passer domesticus", "scientific", null, "iucn");
        w.Name(HouseSparrow, "House sparrow", "common", "en", "iucn", preferred: true);

        // Baiji: CR with possibly extinct, a DOI from Wikidata, and a pre-1994 "Ex" in its history.
        w.Taxon(Baiji, "Lipotes vexillifer", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CETARTIODACTYLA", "LIPOTIDAE", "Lipotes",
            authority: "Miller, 1918", commonEn: "Baiji", enwiki: "Baiji", qid: "Q190826", latest: BaijiLatest,
            qidSource: "p627", p141: "[]", itemDownloaded: "2026-09-14");
        w.Assessment(BaijiLatest, Baiji, "Global", true, "CR", possiblyExtinct: true, criteria: "A2cd; C2a(ii); D", criteriaVersion: "3.1",
            year: 2017, date: "2017-07-01", trend: "Unknown",
            citation: Citation(Baiji, BaijiLatest, 2017, "Lipotes vexillifer",
                [Person("Smith", "B.D."), Person("Wang", "D."), Person("Braulik", "G.T."), Person("Reeves", "R.")],
                doi: "10.2305/IUCN.UK.2017-3.RLTS.T12119A50358152.en", doiSource: DoiSource.Wikidata, text: null));
        w.Assessment(Baiji1986Ex, Baiji, "Global", false, "Ex", year: 1986);
        w.Name(Baiji, "Lipotes vexillifer", "scientific", null, "iucn");
        w.Name(Baiji, "Baiji", "common", "en", "iucn", preferred: true);

        // Tiger and the Sumatran tiger: a species with a subspecies. The tiger's latest assessment has
        // a Wikidata item that lacks some statements, and full given names for one of its two authors.
        w.Taxon(Tiger, "Panthera tigris", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "(Linnaeus, 1758)", commonEn: "Tiger", enwiki: "Tiger", qid: "Q132186", latest: TigerLatest,
            qidSource: "p627", p141: P141((TigerP141Statement, "Q278113", "normal", "Q115962546")), itemDownloaded: "2026-09-13");
        w.Assessment(TigerLatest, Tiger, "Global", true, "EN", criteria: "A2abcd+4abcd", criteriaVersion: "3.1", year: 2022, date: "2021-12-01",
            trend: "Stable",
            citation: Citation(Tiger, TigerLatest, 2022, "Panthera tigris", [Person("Goodrich", "J.", "John"), Person("Wibisono", "H.")],
                doi: "10.2305/IUCN.UK.2022-1.RLTS.T15955A214862019.en", doiSource: DoiSource.Gbif, text: null),
            wikidataItem: TigerLatestItem, wikidataItemProperties: "P31 P1476 P1433 P921 Len",
            wikidataItemTitles: WikidataTitle.ListToJson([new WikidataTitle(TigerItemOldTitle, "en")]), wikidataItemLabelEn: TigerItemOldTitle);
        w.Name(Tiger, "Panthera tigris", "scientific", null, "iucn");
        w.Name(Tiger, "Tiger", "common", "en", "iucn", preferred: true);
        w.Name(Tiger, "Big cat", "common", "en", "wikidata");
        w.Name(Tiger, "Felis tigris", "synonym", null, "iucn");
        // IUCN's collective code for Austronesian languages.
        w.Name(Tiger, "Harimau", "common", "map", "iucn");

        w.Taxon(SumatranTiger, "Panthera tigris ssp. sumatrae", "subspecies", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "Pocock, 1929", commonEn: "Sumatran tiger", enwiki: "Sumatran tiger", parent: Tiger, latest: SumatranTigerLatest,
            infraRank: "ssp.", infraName: "sumatrae");
        w.Assessment(SumatranTigerLatest, SumatranTiger, "Global", true, "CR", criteria: "C1a(ii)", criteriaVersion: "3.1", year: 2008,
            date: "2008-06-30", trend: "Decreasing",
            citation: Citation(SumatranTiger, SumatranTigerLatest, 2008, "Panthera tigris ssp. sumatrae", [Person("Linkie", "M.")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Name(SumatranTiger, "Panthera tigris ssp. sumatrae", "scientific", null, "iucn");
        w.Name(SumatranTiger, "Sumatran tiger", "common", "en", "iucn", preferred: true);

        // Lion and its West Africa subpopulation, whose assessment has no citation.
        w.Taxon(Lion, "Panthera leo", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "(Linnaeus, 1758)", commonEn: "Lion", enwiki: "Lion", qid: "Q140", latest: LionLatest, qidSource: "name-match");
        w.Assessment(LionLatest, Lion, "Global", true, "VU", criteria: "A2abcd", criteriaVersion: "3.1", year: 2025, date: "2024-09-01",
            trend: "Decreasing",
            citation: Citation(Lion, LionLatest, 2025, "Panthera leo", [Person("Nicholson", "S.", "Samantha"), Person("Bauer", "H.", "Hans")],
                doi: "10.2305/IUCN.UK.2025-2.RLTS.T15951A280792135.en", doiSource: DoiSource.Citation, text: null));
        w.Name(Lion, "Panthera leo", "scientific", null, "iucn");
        w.Name(Lion, "Lion", "common", "en", "iucn", preferred: true);
        w.Name(Lion, "Big cat", "common", "en", "wikidata");

        w.Taxon(WestAfricanLion, "Panthera leo West Africa subpopulation", "subpopulation", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            subpopulation: "West Africa subpopulation", commonEn: "West African lion", parent: Lion, latest: WestAfricanLionLatest);
        // A Wikidata item, but no citation parts.
        w.Assessment(WestAfricanLionLatest, WestAfricanLion, "Global", true, "CR", criteria: "C2a(i)", criteriaVersion: "3.1", year: 2015,
            date: "2015-01-01", trend: "Decreasing", wikidataItem: WestAfricanLionLatestItem, wikidataItemProperties: "P31 P1476 P1433 P123 P921");
        w.Name(WestAfricanLion, "Panthera leo West Africa subpopulation", "scientific", null, "iucn");
        w.Name(WestAfricanLion, "West African lion", "common", "en", "iucn", preferred: true);

        // A taxon with regional assessments only.
        w.Taxon(RegionalOnly, "Gobio kovatschevi", "species", "ANIMALIA", "CHORDATA", "ACTINOPTERYGII", "CYPRINIFORMES", "GOBIONIDAE", "Gobio",
            authority: "Chichkoff, 1937");
        w.Assessment(RegionalOnlyEurope, RegionalOnly, "Europe", true, "LC", criteriaVersion: "3.1", year: 2008, date: "2008-01-01",
            citation: Citation(RegionalOnly, RegionalOnlyEurope, 2008, "Gobio kovatschevi", [Person("Freyhof", "J.")],
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe"));
        w.Assessment(RegionalOnlyMediterranean, RegionalOnly, "Mediterranean", true, "DD", criteriaVersion: "3.1", year: 2010, date: "2010-01-01");
        w.Assessment(RegionalOnlyEurope2006, RegionalOnly, "Europe", false, "VU", criteriaVersion: "3.1", year: 2006, date: "2006-01-01");
        w.Name(RegionalOnly, "Gobio kovatschevi", "scientific", null, "iucn");

        // Micropyropsis tuberosa: a plant whose latest global and Europe assessments are errata versions
        // of assessments published the same year.
        w.Taxon(Micropyropsis, "Micropyropsis tuberosa", "species", "PLANTAE", "TRACHEOPHYTA", "LILIOPSIDA", "POALES", "POACEAE", "Micropyropsis",
            authority: "Romero-Zarco & Cabezudo", latest: MicropyropsisLatest);
        Author[] rhazi = [Person("Rhazi", "L."), Person("Grillas", "P."), Person("Rhazi", "M."), Person("Flanagan", "D.")];
        w.Assessment(MicropyropsisLatest, Micropyropsis, "Global", true, "EN", criteria: "B1ab(i,ii,iii,v)+2ab(i,ii,iii,v)", criteriaVersion: "3.1",
            year: 2010, date: "2009-02-12",
            citation: Citation(Micropyropsis, MicropyropsisLatest, 2010, "Micropyropsis tuberosa", rhazi,
                doi: "10.2305/IUCN.UK.2010-2.RLTS.T162107A5539282.en", doiSource: DoiSource.Gbif, text: null, errataYear: 2016),
            // The errata version has the DOI of the assessment it corrects, so it shares that assessment's item.
            wikidataItem: MicropyropsisItem, wikidataItemProperties: "P31 P1476 P1433 P577 P356 Len",
            wikidataItemTitles: WikidataTitle.ListToJson([new WikidataTitle(MicropyropsisItemOldTitle, "en")]),
            wikidataItemLabelEn: MicropyropsisItemOldTitle, wikidataItemAssessment: MicropyropsisReplaced);
        w.Assessment(MicropyropsisReplaced, Micropyropsis, "Global", false, "EN", criteria: "B1ab(i,ii,iii,v)+2ab(i,ii,iii,v)", criteriaVersion: "3.1",
            year: 2010, date: "2009-02-12", replacedBy: MicropyropsisLatest,
            citation: Citation(Micropyropsis, MicropyropsisReplaced, 2010, "Micropyropsis tuberosa", rhazi,
                doi: "10.2305/IUCN.UK.2010-2.RLTS.T162107A5539282.en", doiSource: DoiSource.Wikidata, text: null),
            wikidataItem: MicropyropsisItem, wikidataItemProperties: "P31 P1476 P1433 P577 P356 Len",
            wikidataItemTitles: WikidataTitle.ListToJson([new WikidataTitle(MicropyropsisItemOldTitle, "en")]),
            wikidataItemLabelEn: MicropyropsisItemOldTitle);
        Author[] deVega = [Person("de Vega Durán", "C."), Person("Berjano Pérez", "R.")];
        w.Assessment(MicropyropsisEurope, Micropyropsis, "Europe", true, "EN", criteria: "B1ab(iii)+2ab(iii)", criteriaVersion: "3.1",
            year: 2011, date: "2011-03-23",
            citation: Citation(Micropyropsis, MicropyropsisEurope, 2011, "Micropyropsis tuberosa", deVega,
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe", errataYear: 2016));
        w.Assessment(MicropyropsisEuropeReplaced, Micropyropsis, "Europe", false, "EN", criteria: "B1ab(iii)+2ab(iii)", criteriaVersion: "3.1",
            year: 2011, date: "2011-03-23", replacedBy: MicropyropsisEurope,
            citation: Citation(Micropyropsis, MicropyropsisEuropeReplaced, 2011, "Micropyropsis tuberosa", deVega,
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe"));
        w.Name(Micropyropsis, "Micropyropsis tuberosa", "scientific", null, "iucn");

        // A plant subspecies with an "NT" from 1998 that has no criteria version.
        w.Taxon(PlantSubspecies, "Hirtella zanzibarica subsp. megacarpa", "subspecies", "PLANTAE", "TRACHEOPHYTA", "MAGNOLIOPSIDA", "MALPIGHIALES",
            "CHRYSOBALANACEAE", "Hirtella", authority: "(R.A.Graham) Prance", latest: PlantSubspeciesLatest, infraRank: "subsp.", infraName: "megacarpa",
            qid: PlantSubspeciesItem, qidSource: "p627", itemDownloaded: "2026-09-13",
            p141: P141(("Q900000010$1A2B3C4D-0000-4000-8000-000000000003", "Q719675", "deprecated", null),
                ("Q900000010$1A2B3C4D-0000-4000-8000-000000000004", "Q211005", "normal", null)));
        w.Assessment(PlantSubspeciesLatest, PlantSubspecies, "Global", true, "NT", criteria: "B1a+2a", criteriaVersion: "3.1", year: 2020, date: "2007-03-07",
            citation: Citation(PlantSubspecies, PlantSubspeciesLatest, 2020, "Hirtella zanzibarica subsp. megacarpa", [Person("Lovett", "J.")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(PlantSubspecies1998Nt, PlantSubspecies, "Global", false, "NT", year: 1998, date: "1997-01-01",
            citation: Citation(PlantSubspecies, PlantSubspecies1998Nt, 1998, "Hirtella zanzibarica subsp. megacarpa",
                [Organisation("World Conservation Monitoring Centre")], doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(PlantSubspecies1998Vu, PlantSubspecies, "Global", false, "VU", criteria: "B1+2b", criteriaVersion: "2.3", year: 1998, date: "1998-01-01");
        w.Name(PlantSubspecies, "Hirtella zanzibarica subsp. megacarpa", "scientific", null, "iucn");
        w.Name(PlantSubspecies, "Hirtella megacarpa", "synonym", null, "iucn");
        // IUCN has not assessed Hirtella zanzibarica itself, only these two subspecies.
        w.Taxon(PlantSubspeciesSibling, "Hirtella zanzibarica subsp. cryptadenia", "subspecies", "PLANTAE", "TRACHEOPHYTA", "MAGNOLIOPSIDA",
            "MALPIGHIALES", "CHRYSOBALANACEAE", "Hirtella", infraRank: "subsp.", infraName: "cryptadenia");

        // Bromus interruptus: Extinct in the Wild, with an "EX" from 1998 that has no criteria version.
        w.Taxon(Bromus, "Bromus interruptus", "species", "PLANTAE", "TRACHEOPHYTA", "LILIOPSIDA", "POALES", "POACEAE", "Bromus",
            authority: "(Hack.) Druce", commonEn: "Interrupted brome", enwiki: "Bromus interruptus", latest: BromusLatest);
        w.Assessment(BromusLatest, Bromus, "Global", true, "EW", criteriaVersion: "3.1", year: 2011, date: "2011-01-10",
            citation: Citation(Bromus, BromusLatest, 2011, "Bromus interruptus", [Person("Rumsey", "F.")], doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(Bromus1998Ex, Bromus, "Global", false, "EX", year: 1998, date: "1997-01-01",
            citation: Citation(Bromus, Bromus1998Ex, 1998, "Bromus interruptus", [Organisation("World Conservation Monitoring Centre")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Name(Bromus, "Bromus interruptus", "scientific", null, "iucn");
        w.Name(Bromus, "Interrupted brome", "common", "en", "iucn", preferred: true);

        // Koala: a SPRAT profile with no listing, and the listing of some of its populations.
        w.Taxon(Koala, "Phascolarctos cinereus", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "DIPROTODONTIA", "PHASCOLARCTIDAE", "Phascolarctos",
            authority: "(Goldfuss, 1817)", commonEn: "Koala", enwiki: "Koala", qid: "Q36101", latest: KoalaLatest,
            // Its item gives endangered at preferred rank, and the assessment's vulnerable at normal rank
            // with a reference to the assessment's item: the commands only remove endangered.
            itemDownloaded: "2026-09-13",
            p141: P141((KoalaP141Endangered, "Q96377276", "preferred", null), (KoalaP141Vulnerable, "Q278113", "normal", KoalaLatestItem)));
        w.EpbcListing(Koala, KoalaSprat, "Phascolarctos cinereus", null, "taxon", null);
        w.EpbcListing(Koala, KoalaPopulationSprat, "Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)", "EN", "population",
            "combined populations of Qld, NSW and the ACT");
        w.Assessment(KoalaLatest, Koala, "Global", true, "VU", criteria: "A2bc", criteriaVersion: "3.1", year: 2016, date: "2014-07-08",
            trend: "Decreasing", wikidataItem: KoalaLatestItem, wikidataItemProperties: "P31 P1476 P1433 P921 P953 P577 P356 P2093 Len");
        w.Name(Koala, "Phascolarctos cinereus", "scientific", null, "iucn");
        w.Name(Koala, "Koala", "common", "en", "iucn", preferred: true);
        w.Name(Koala, "Bear", "common", "en", "col");

        // Southern cassowary: the EPBC Act lists the whole species under another name.
        w.Taxon(Cassowary, "Casuarius casuarius", "species", "ANIMALIA", "CHORDATA", "AVES", "CASUARIIFORMES", "CASUARIIDAE", "Casuarius",
            authority: "(Linnaeus, 1758)", commonEn: "Southern cassowary", latest: CassowaryLatest,
            qid: CassowaryItem, itemDownloaded: "2026-09-13", p627Deprecated: true,
            p141: P141(("Q190722$1A2B3C4D-0000-4000-8000-000000000009", "Q211005", "normal", "Q136547248")));
        // Its assessment item lacks main subject (P921) and publication date (P577); the add commands
        // leave out P921, since the taxon's item states the IUCN taxon ID only at deprecated rank.
        w.Assessment(CassowaryLatest, Cassowary, "Global", true, "LC", criteriaVersion: "3.1", year: 2016, date: "2016-10-01",
            citation: Citation(Cassowary, CassowaryLatest, 2016, "Casuarius casuarius", [Organisation("BirdLife International")],
                doi: "10.2305/IUCN.UK.2016-3.RLTS.T22678108A155429591.en", doiSource: DoiSource.Gbif, text: null),
            wikidataItem: CassowaryLatestItem, wikidataItemProperties: "P31 P1476 P1433 P953 P356 P2093 Len");
        w.EpbcListing(Cassowary, CassowarySprat, "Casuarius casuarius johnsonii", "EN", "taxon", null);
        w.Name(Cassowary, "Casuarius casuarius", "scientific", null, "iucn");

        // Leopard and the Amur leopard, which is not in the release.
        w.Taxon(Leopard, "Panthera pardus", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "(Linnaeus, 1758)", commonEn: "Leopard", latest: LeopardLatest,
            qid: LeopardItem, itemDownloaded: "2026-09-13", p141: "[]",
            otherItems: WikidataOtherTaxonItem.ListToJson([new WikidataOtherTaxonItem(LeopardOtherItem, false,
                [new WikidataStatusStatement(LeopardOtherItem + "$1A2B3C4D-0000-4000-8000-00000000000A", "Q278113", "normal", ["Q136547248"], [], 1, true)])]));
        w.Assessment(LeopardLatest, Leopard, "Global", true, "VU", criteria: "A2cd", criteriaVersion: "3.1", year: 2024, date: "2023-01-01",
            citation: Citation(Leopard, LeopardLatest, 2024, "Panthera pardus", [Person("Stein", "A.B.")],
                doi: "10.2305/IUCN.UK.2024-1.RLTS.T15954A50659089.en", doiSource: DoiSource.Gbif, text: null));
        w.Name(Leopard, "Panthera pardus", "scientific", null, "iucn");
        w.Name(Leopard, "Leopard", "common", "en", "iucn", preferred: true);

        w.Taxon(AmurLeopard, "Panthera pardus ssp. orientalis", "subspecies", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "(Schlegel, 1857)", parent: Leopard, infraRank: "ssp.", infraName: "orientalis", inRelease: false);
        w.Assessment(AmurLeopard2016Ne, AmurLeopard, "Global", false, "NE", criteriaVersion: "3.1", year: 2016, date: "2016-06-05",
            citation: Citation(AmurLeopard, AmurLeopard2016Ne, 2016, "Panthera pardus ssp. orientalis", [Person("Stein", "A.B.")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(AmurLeopard2008, AmurLeopard, "Global", false, "CR", criteria: "C2a(ii); D", criteriaVersion: "3.1", year: 2008,
            date: "2008-06-30",
            citation: Citation(AmurLeopard, AmurLeopard2008, 2008, "Panthera pardus ssp. orientalis", [Person("Jackson", "P.")],
                doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(AmurLeopard1996, AmurLeopard, "Global", false, "CR", criteria: "A2c; D", criteriaVersion: "2.3", year: 1996,
            date: "1996-09-01");
        w.Name(AmurLeopard, "Panthera pardus ssp. orientalis", "scientific", null, "iucn");
        w.Name(AmurLeopard, "Amur Leopard", "common", "en", "iucn", preferred: true);
        w.Name(AmurLeopard, "Bars", "common", "ru", "iucn");

        w.Taxon(BombusPyrrhopygus, "Bombus pyrrhopygus", "species", "ANIMALIA", "ARTHROPODA", "INSECTA", "HYMENOPTERA", "APIDAE", "Bombus",
            inRelease: false);
        w.Assessment(BombusEurope2015, BombusPyrrhopygus, "Europe", false, "VU", criteriaVersion: "3.1", year: 2015, date: "2015-01-01");
        w.Assessment(BombusEurope2013, BombusPyrrhopygus, "Europe", false, "LC", criteriaVersion: "3.1", year: 2013, date: "2013-01-01");
        w.Assessment(BombusEurope2016, BombusPyrrhopygus, "Europe", false, "VU", criteriaVersion: "3.1", year: 2016, date: "2016-01-01");
        w.Name(BombusPyrrhopygus, "Bombus pyrrhopygus", "scientific", null, "iucn");

        // The woylie, and an old id with its name.
        w.Taxon(Woylie, "Bettongia penicillata", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "DIPROTODONTIA", "POTOROIDAE", "Bettongia",
            authority: "Gray, 1837", commonEn: "Woylie", latest: WoylieLatest, qid: WoylieItem, itemDownloaded: "2026-09-13",
            p141: P141(
                new WikidataStatusStatement(WoylieP141Critically, "Q219127", "normal", ["Q136547248"], [Woylie.ToString()], 1, true),
                new WikidataStatusStatement(WoylieP141Endangered, "Q96377276", "normal", [], [], 1, false)));
        w.Assessment(WoylieLatest, Woylie, "Global", true, "CR", criteria: "A3e", criteriaVersion: "3.1", year: 2015, date: "2014-01-01",
            citation: Citation(Woylie, WoylieLatest, 2015, "Bettongia penicillata", [Person("Woinarski", "J.")],
                doi: WoylieDoi, doiSource: DoiSource.Resolved, text: null), taxonomicNotes: true);
        w.Name(Woylie, "Bettongia penicillata", "scientific", null, "iucn");
        w.Name(Woylie, "Woylie", "common", "en", "iucn", preferred: true);

        w.Taxon(WoylieOld, "Bettongia penicillata", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "DIPROTODONTIA", "POTOROIDAE", "Bettongia",
            authority: "Gray, 1837", commonEn: "Woylie", inRelease: false, currentTaxon: Woylie);
        w.Assessment(WoylieOld2008, WoylieOld, "Global", false, "CR", criteria: "A2ce", criteriaVersion: "3.1", year: 2008, date: "2008-06-30",
            citation: Citation(WoylieOld, WoylieOld2008, 2008, "Bettongia penicillata", [Person("Woinarski", "J.")],
                doi: null, doiSource: DoiSource.None, text: null), taxonomicNotes: true);
        w.Name(WoylieOld, "Bettongia penicillata", "scientific", null, "iucn");
        w.Name(WoylieOld, "Woylie", "common", "en", "iucn", preferred: true);
        w.TaxonLink(WoylieOld, Woylie, "same-name");

        // Acropora palmerae and Acropora minuta, an old id whose name is a synonym of it.
        w.Taxon(Palmerae, "Acropora palmerae", "species", "ANIMALIA", "CNIDARIA", "ANTHOZOA", "SCLERACTINIA", "ACROPORIDAE", "Acropora",
            authority: "Wells, 1954", latest: PalmeraeLatest);
        w.Assessment(PalmeraeLatest, Palmerae, "Global", true, "EN", criteria: "A4c", criteriaVersion: "3.1", year: 2024, date: "2022-01-01",
            citation: Citation(Palmerae, PalmeraeLatest, 2024, "Acropora palmerae", [Person("Aeby", "G.")], doi: null, doiSource: DoiSource.None, text: null),
            taxonomicNotes: true);
        w.Assessment(Palmerae2008, Palmerae, "Global", false, "VU", criteria: "A4c", criteriaVersion: "3.1", year: 2008, date: "2008-01-01",
            taxonomicNotes: false);
        w.Name(Palmerae, "Acropora palmerae", "scientific", null, "iucn");
        w.Name(Palmerae, "Acropora minuta", "synonym", null, "iucn");

        w.Taxon(Minuta, "Acropora minuta", "species", "ANIMALIA", "CNIDARIA", "ANTHOZOA", "SCLERACTINIA", "ACROPORIDAE", "Acropora",
            authority: "Veron, 2000", inRelease: false);
        w.Assessment(Minuta2008, Minuta, "Global", false, "VU", criteria: "A4c", criteriaVersion: "3.1", year: 2008, date: "2008-01-01",
            citation: Citation(Minuta, Minuta2008, 2008, "Acropora minuta", [Person("Aeby", "G.")], doi: null, doiSource: DoiSource.None, text: null),
            taxonomicNotes: false);
        w.Name(Minuta, "Acropora minuta", "scientific", null, "iucn");
        w.TaxonLink(Minuta, Palmerae, "iucn-synonym");

        // Platanista: an old id linked to two taxa in the release.
        w.Taxon(Gangetica, "Platanista gangetica", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CETARTIODACTYLA", "PLATANISTIDAE", "Platanista",
            authority: "(Roxburgh, 1801)", latest: GangeticaLatest);
        w.Assessment(GangeticaLatest, Gangetica, "Global", true, "EN", criteria: "A2abc", criteriaVersion: "3.1", year: 2022, date: "2021-03-01",
            taxonomicNotes: true);
        w.Name(Gangetica, "Platanista gangetica", "scientific", null, "iucn");
        w.Taxon(Minor, "Platanista minor", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CETARTIODACTYLA", "PLATANISTIDAE", "Platanista",
            authority: "Owen, 1853", latest: MinorLatest);
        w.Assessment(MinorLatest, Minor, "Global", true, "EN", criteria: "A2abc", criteriaVersion: "3.1", year: 2022, date: "2021-03-02",
            taxonomicNotes: true);
        w.Name(Minor, "Platanista minor", "scientific", null, "iucn");
        w.Name(Minor, "Platanista gangetica", "synonym", null, "iucn");
        w.Taxon(GangeticaOld, "Platanista gangetica", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CETARTIODACTYLA", "PLATANISTIDAE", "Platanista",
            authority: "(Roxburgh, 1801)", inRelease: false, currentTaxon: Gangetica);
        w.Assessment(GangeticaOld2012, GangeticaOld, "Global", false, "EN", criteria: "A2abcd", criteriaVersion: "3.1", year: 2012, date: "2012-01-01",
            taxonomicNotes: true);
        w.Assessment(GangeticaOld1996, GangeticaOld, "Global", false, "EN", criteria: "A1acd", criteriaVersion: "2.3", year: 1996, date: "1996-01-01");
        w.Assessment(GangeticaOldAsia, GangeticaOld, "Asia", false, "EN", criteriaVersion: "3.1", year: 2010, date: "2010-01-01");
        w.Name(GangeticaOld, "Platanista gangetica", "scientific", null, "iucn");
        w.TaxonLink(GangeticaOld, Gangetica, "same-name");
        w.TaxonLink(GangeticaOld, Minor, "iucn-synonym");

        // Ids with no global assessments.
        w.Taxon(Clessiniola, "Clessiniola variabilis", "species", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "LITTORINIMORPHA", "HYDROBIIDAE", "Clessiniola",
            authority: "(Eichwald, 1838)");
        w.Assessment(ClessiniolaEurope, Clessiniola, "Europe", true, "LC", criteriaVersion: "3.1", year: 2011, date: "2010-06-01");
        w.Name(Clessiniola, "Clessiniola variabilis", "scientific", null, "iucn");
        w.Name(Clessiniola, "Turricaspia trivialis", "synonym", null, "iucn");
        w.Taxon(Turricaspia, "Turricaspia trivialis", "species", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "LITTORINIMORPHA", "HYDROBIIDAE", "Turricaspia",
            inRelease: false);
        w.Assessment(Turricaspia2011, Turricaspia, "Global", false, "DD", criteriaVersion: "3.1", year: 2011, date: "2010-06-01");
        w.Name(Turricaspia, "Turricaspia trivialis", "scientific", null, "iucn");
        w.TaxonLink(Turricaspia, Clessiniola, "iucn-synonym");

        w.Taxon(PupillaMuscorum, "Pupilla muscorum", "species", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "STYLOMMATOPHORA", "PUPILLIDAE", "Pupilla",
            authority: "(Linnaeus, 1758)", latest: PupillaMuscorumLatest);
        w.Assessment(PupillaMuscorumLatest, PupillaMuscorum, "Global", true, "LC", criteriaVersion: "3.1", year: 2017, date: "2016-08-01");
        w.Name(PupillaMuscorum, "Pupilla muscorum", "scientific", null, "iucn");
        w.Name(PupillaMuscorum, "Pupilla bigranata", "synonym", null, "iucn");
        w.Taxon(PupillaBigranata, "Pupilla bigranata", "species", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "STYLOMMATOPHORA", "PUPILLIDAE", "Pupilla",
            inRelease: false);
        w.Assessment(PupillaBigranataEurope, PupillaBigranata, "Europe", false, "LC", criteriaVersion: "3.1", year: 2011, date: "2010-06-01");
        w.Name(PupillaBigranata, "Pupilla bigranata", "scientific", null, "iucn");
        w.TaxonLink(PupillaBigranata, PupillaMuscorum, "iucn-synonym");

        // A provisional name whose only assessment IUCN published with no scope.
        w.Taxon(NoScopeOnly, "Hauffenia sp. nov.", "species", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "LITTORINIMORPHA", "HYDROBIIDAE", "Hauffenia");
        w.Assessment(NoScopeOnlyLatest, NoScopeOnly, "", true, "DD", criteriaVersion: "3.1", year: 2011, date: "2010-06-01");
        w.Name(NoScopeOnly, "Hauffenia sp. nov.", "scientific", null, "iucn");

        // A variety, for the kind label.
        w.Taxon(Variety, "Cupressus arizonica var. glabra", "variety", "PLANTAE", "TRACHEOPHYTA", "PINOPSIDA", "PINALES", "CUPRESSACEAE", "Cupressus",
            authority: "(Sudw.) Little", latest: 34010001, infraRank: "var.", infraName: "glabra");
        w.Assessment(34010001, Variety, "Global", true, "VU", criteria: "B2ab(iii)", criteriaVersion: "3.1", year: 2013, date: "2012-05-08");
        w.Name(Variety, "Cupressus arizonica var. glabra", "scientific", null, "iucn");

        // Artemia monica: LR/cd, which has no IUCN conservation status value on Wikidata.
        w.Taxon(ArtemiaMonica, "Artemia monica", "species", "ANIMALIA", "ARTHROPODA", "BRANCHIOPODA", "ANOSTRACA", "ARTEMIIDAE", "Artemia",
            authority: "Verrill, 1869", latest: ArtemiaMonicaLatest, qid: "Q4560564", itemDownloaded: "2026-09-14",
            p141: P141(("Q4560564$B47E2AA1-46AB-47D6-84B9-E8384DD52550", "Q211005", "normal", "Q115962546")));
        w.Assessment(ArtemiaMonicaLatest, ArtemiaMonica, "Global", true, "LR/cd", criteriaVersion: "2.3", year: 1996, date: "1996-08-01");
        w.Name(ArtemiaMonica, "Artemia monica", "scientific", null, "iucn");

        // Enough taxa sharing a genus name to fill more than one page of search results.
        for (var i = 0; i < FillerCount; i++) {
            var id = FillerFirstId + i;
            var epithet = "filler" + i.ToString("D3", CultureInfo.InvariantCulture);
            w.Taxon(id, $"Fillerus {epithet}", "species", "ANIMALIA", "ARTHROPODA", "INSECTA", "COLEOPTERA", "FILLERIDAE", "Fillerus",
                latest: id * 10);
            w.Assessment(id * 10, id, "Global", true, "LC", criteriaVersion: "3.1", year: 2020);
            w.Name(id, $"Fillerus {epithet}", "scientific", null, "iucn");
        }

        // The groups of the polar bear, with Catalogue of Life suborder Caniformia, and two genera
        // named Abronia in two kingdoms (no taxa in this fixture).
        w.Group(1, null, 0, "kingdom", "Animalia", "iucn", "ANIMALIA", 1, 1, species: 1);
        w.Group(2, 1, 1, "phylum", "Chordata", "iucn", "ANIMALIA", 1, 1, species: 1, common: "chordates");
        w.Group(3, 2, 2, "class", "Mammalia", "iucn", "ANIMALIA", 1, 1, species: 1, common: "mammals");
        w.Group(4, 3, 3, "order", "Carnivora", "iucn", "ANIMALIA", 1, 1, species: 1);
        w.Group(5, 4, 4, "suborder", "Caniformia", "col", "ANIMALIA", 1, 1, species: 1, colId: "6224H");
        w.Group(6, 5, 5, "family", "Ursidae", "iucn", "ANIMALIA", 1, 1, species: 1, common: "bears", enwiki: "Bear");
        w.Group(7, 6, 6, "genus", "Ursus", "iucn", "ANIMALIA", 1, 1, species: 1);
        foreach (var id in Enumerable.Range(1, 7)) {
            w.GroupCount(id, "VU", 1);
        }
        w.GroupName(6, "Bears");
        w.Place(PolarBear, 7, 1, "Polar bear");
        w.Group(8, null, 0, "kingdom", "Plantae", "iucn", "PLANTAE", 2, 1);
        w.Group(9, 4, 4, "genus", "Abronia", "iucn", "ANIMALIA", 2, 1, linkQuery: "kingdom=animalia");
        w.Group(10, 8, 1, "genus", "Abronia", "iucn", "PLANTAE", 2, 1, linkQuery: "kingdom=plantae");
        w.Group(11, null, 0, "genus", "Hirtella", "iucn", "PLANTAE", 20, 21);
        w.Place(PlantSubspecies, 11, 20, null);
        w.Place(PlantSubspeciesSibling, 11, 21, null);
        // Names from English Wikipedia (the article title and redirects). "Bear" is also a Catalogue
        // of Life name of the koala, which is not the koala's English name, so a search for "bear"
        // goes to Ursidae. "Baiji" is the baiji's own English name, so a search for it lists both.
        w.GroupName(6, "Bear", "wikipedia");
        w.GroupName(6, "Bears", "wikipedia");
        w.Group(12, null, 0, "genus", "Lipotes", "iucn", "ANIMALIA", 30, 30, species: 1);
        w.GroupName(12, "Baiji", "wikipedia");

        w.Meta(SiteDbSchema.MetaKeys.SchemaVersion, schemaVersion);
        w.Meta(SiteDbSchema.MetaKeys.BuiltAtUtc, "2026-10-02T09:00:00Z");
        w.Meta(SiteDbSchema.MetaKeys.IucnRelease, release);
        w.Meta(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom, "2026-08-18");
        w.Meta(SiteDbSchema.MetaKeys.IucnApiDownloadedTo, "2026-09-01");
        w.Meta(SiteDbSchema.MetaKeys.GbifChecklistVersion, "2026-1");
        w.Meta(SiteDbSchema.MetaKeys.GbifChecklistPublished, "2026-07-28");
        if (withSourceCitations) {
            w.Meta(SiteDbSchema.MetaKeys.GbifChecklistCitation, GbifCitation);
            w.Meta(SiteDbSchema.MetaKeys.GbifChecklistDoi, "10.15468/0qnb58");
            w.Meta(SiteDbSchema.MetaKeys.ColCitation, ColCitation);
            w.Meta(SiteDbSchema.MetaKeys.ColDoi, "10.48580/dgykv");
            w.Meta(SiteDbSchema.MetaKeys.IucnDoiCheckedTo, "2026-09-30");
        }
        w.Meta(SiteDbSchema.MetaKeys.ColRelease, "COL26.7 XR");
        w.Meta(SiteDbSchema.MetaKeys.SpratReport, "01102026-023504-report.csv");
        w.Meta(SiteDbSchema.MetaKeys.WikidataItemModel, wikidataItemModelJson);
        w.Meta(SiteDbSchema.MetaKeys.TaxonCount, w.TaxonCount.ToString(CultureInfo.InvariantCulture));
        w.Meta(SiteDbSchema.MetaKeys.AssessmentCount, w.AssessmentCount.ToString(CultureInfo.InvariantCulture));
    }

    public static int GlobalTaxonCount => 20 + FillerCount;

    public const string GbifCitation =
        "IUCN (2026). The IUCN Red List of Threatened Species. Version 2026-1. https://www.iucnredlist.org. Downloaded on 2026-07-28. https://doi.org/10.15468/0qnb58";

    public const string ColCitation =
        "Bánki, O., Roskov, Y., Döring, M. et al. (2026). Catalogue of Life (Version 2026-07-14 XR). Catalogue of Life, Amsterdam, Netherlands. https://doi.org/10.48580/dgykv";

    /// taxon.wikidata_p141 JSON: (statement id, value, rank, stated in item or null). Each statement
    /// has one reference that cites IUCN, with no IUCN taxon ID in it.
    private static string P141(params (string Id, string Value, string Rank, string? StatedIn)[] statements) =>
        WikidataStatusStatement.ListToJson(statements
            .Select(s => new WikidataStatusStatement(s.Id, s.Value, s.Rank, s.StatedIn is null ? [] : [s.StatedIn],
                TaxonIds: [], References: 1, CitesIucn: true))
            .ToList());

    private static string P141(params WikidataStatusStatement[] statements) => WikidataStatusStatement.ListToJson(statements);

    private static Author Person(string last, string initials, string? givenNames = null) =>
        new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials, givenNames);

    private static Author Organisation(string name) => new(CitationAuthorKind.Organisation, name);

    private static Author Verbatim(string name) => new(CitationAuthorKind.Verbatim, name);

    private static string Citation(long taxonId, long assessmentId, int year, string name, IReadOnlyList<Author> authors,
        string? doi, DoiSource doiSource, string? text, bool etAl = false, string? region = null, bool narrative = false,
        int? errataYear = null, int? amendsYear = null, string? registeredName = null) {
        var parts = new IucnCitationParts {
            TaxonId = taxonId,
            AssessmentId = assessmentId,
            Year = year,
            ScientificName = name,
            RegionalScope = region,
            ErrataYear = errataYear,
            AmendsYear = amendsYear,
            Authors = authors,
            AuthorsEtAl = etAl,
            Doi = doi,
            DoiSource = doiSource,
            RegisteredName = registeredName,
            IucnCitationText = text,
            DownloadedAtUtc = new DateTime(2026, 8, 18, 10, 30, 0, DateTimeKind.Utc),
        };
        var json = parts.ToJson();
        if (!narrative) {
            return json;
        }
        var node = JsonNode.Parse(json)!.AsObject();
        node["rationale"] = NarrativeMarker;
        node["threats"] = NarrativeMarker;
        return node.ToJsonString(new JsonSerializerOptions());
    }

    private static void Exec(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private sealed class Writer(SqliteConnection connection, SqliteTransaction tx) {
        private long _nextNameId = 1;
        public int TaxonCount { get; private set; }
        public int AssessmentCount { get; private set; }

        public void Meta(string key, string value) => Run("INSERT INTO meta(key, value) VALUES (@a, @b)", key, value);

        public void Taxon(long id, string name, string kind, string kingdom, string phylum, string className, string order, string family,
            string genus, string? authority = null, string? commonEn = null, string? enwiki = null, string? qid = null, string? colId = null,
            long? parent = null, long? latest = null, string? subpopulation = null, string? infraRank = null, string? infraName = null,
            bool inRelease = true, long? currentTaxon = null, string? qidSource = null, string? p141 = null, string? itemDownloaded = null,
            bool p627Deprecated = false, string? otherItems = null) {
            TaxonCount++;
            Run("""
                INSERT INTO taxon(taxon_id, scientific_name, kind, kingdom, phylum, class_name, order_name, family, genus,
                    infra_rank, infra_name, subpopulation_name, authority, parent_taxon_id, common_name_en, enwiki_title,
                    wikidata_qid, col_id, latest_global_assessment_id, in_release, current_taxon_id,
                    wikidata_qid_source, wikidata_p141, wikidata_item_downloaded, wikidata_p627_deprecated, wikidata_other_items)
                VALUES (@a, @b, @c, @d, @e, @f, @g, @h, @i, @j, @k, @l, @m, @n, @o, @p, @q, @r, @s, @t, @u, @v, @w, @x, @y, @z)
                """,
                id, name, kind, kingdom, phylum, className, order, family, genus, infraRank, infraName, subpopulation, authority,
                parent, commonEn, enwiki, qid, colId, latest, inRelease ? 1 : 0, currentTaxon,
                qidSource ?? (qid is null ? null : "p627"), p141, itemDownloaded, p627Deprecated ? 1 : 0, otherItems);
            var words = name.Split(' ');
            if (words.Length > 1) {
                Run("UPDATE taxon SET species_epithet = @a WHERE taxon_id = @b", words[1], id);
            }
        }

        public void EpbcListing(long taxonId, long spratId, string listedName, string? status, string appliesTo, string? population) =>
            Run("""
                INSERT INTO epbc_listing(taxon_id, sprat_taxon_id, listed_name, status, applies_to, population)
                VALUES (@a, @b, @c, @d, @e, @f)
                """,
                taxonId, spratId, listedName, status, appliesTo, population);

        public void Assessment(long id, long taxonId, string scope, bool latest, string category, bool possiblyExtinct = false,
            string? criteria = null, string? criteriaVersion = null, int? year = null, string? date = null, string? trend = null,
            string? citation = null, long? replacedBy = null, string? wikidataItem = null, string? wikidataItemProperties = null,
            string? wikidataItemTitles = null, string? wikidataItemLabelEn = null, long? wikidataItemAssessment = null,
            bool? taxonomicNotes = null, bool apiNotFound = false) {
            AssessmentCount++;
            Run("""
                INSERT INTO assessment(assessment_id, taxon_id, scope, is_latest, category, possibly_extinct,
                    possibly_extinct_in_the_wild, criteria, criteria_version, year_published, assessment_date, population_trend, citation_json,
                    replaced_by_assessment_id, wikidata_item_qid, wikidata_item_properties, wikidata_item_titles, wikidata_item_label_en,
                    wikidata_item_assessment_id)
                VALUES (@a, @b, @c, @d, @e, @f, 0, @g, @h, @i, @j, @k, @l, @m, @n, @o, @p, @q, @r)
                """,
                id, taxonId, scope, latest ? 1 : 0, category, possiblyExtinct ? 1 : 0, criteria, criteriaVersion, year, date, trend, citation,
                replacedBy, wikidataItem, wikidataItemProperties, wikidataItemTitles, wikidataItemLabelEn,
                wikidataItem is null ? null : wikidataItemAssessment ?? id);
            if (apiNotFound) {
                Run("UPDATE assessment SET api_not_found = 1 WHERE assessment_id = @a", id);
            }
            if (taxonomicNotes is { } notes) {
                Run("UPDATE assessment SET has_taxonomic_notes = @a WHERE assessment_id = @b", notes ? 1 : 0, id);
            }
        }

        public void Group(int id, int? parent, int depth, string rank, string name, string source, string kingdom, int first, int last,
            int species = 0, string? common = null, string? enwiki = null, string? colId = null, string? linkQuery = null) =>
            Run("""
                INSERT INTO higher_taxon(node_id, parent_node_id, depth, rank, name, name_key, link_query, source, show_rank, kingdom, col_id,
                    common_name_en, common_name_source, enwiki_title, first_pos, last_pos, species_count, infra_count, subpopulation_count)
                VALUES (@a, @b, @c, @d, @e, @f, @g, @h, 1, @i, @j, @k, @l, @m, @n, @o, @p, 0, 0)
                """,
                id, parent, depth, rank, name, SiteNameKey.Fold(name), linkQuery, source, kingdom, colId, common,
                common is null ? null : "rules", enwiki, first, last, species);

        public void GroupCount(int id, string category, int species) =>
            Run("INSERT INTO higher_taxon_count(node_id, category, species_count, infra_count, subpopulation_count) VALUES (@a, @b, @c, 0, 0)",
                id, category, species);

        public void GroupName(int id, string name, string source = "col") =>
            Run("INSERT INTO higher_taxon_name(node_id, name, source, name_key) VALUES (@a, @b, @c, @d)", id, name, source, SiteNameKey.Fold(name));

        public void Place(long taxonId, int nodeId, int treePos, string? listArticle) =>
            Run("UPDATE taxon SET node_id = @a, tree_pos = @b, list_article_title = @c WHERE taxon_id = @d", nodeId, treePos, listArticle, taxonId);

        public void TaxonLink(long taxonId, long currentTaxonId, string kind) =>
            Run("INSERT INTO taxon_link(taxon_id, current_taxon_id, link_kind) VALUES (@a, @b, @c)", taxonId, currentTaxonId, kind);

        public void Name(long taxonId, string name, string type, string? language, string source, bool preferred = false,
            string? authority = null) {
            var nameId = _nextNameId++;
            Run("INSERT INTO name(name_id, taxon_id, name, name_type, language, source, is_preferred, authority) VALUES (@a, @b, @c, @d, @e, @f, @g, @h)",
                nameId, taxonId, name, type, language, source, preferred ? 1 : 0, authority);
            Run("INSERT OR IGNORE INTO name_key(key, taxon_id, name_id) VALUES (@a, @b, @c)", SiteNameKey.Fold(name), taxonId, nameId);
        }

        private void Run(string sql, params object?[] values) {
            using var command = connection.CreateCommand();
            command.Transaction = tx;
            command.CommandText = sql;
            for (var i = 0; i < values.Length; i++) {
                command.Parameters.AddWithValue("@" + (char)('a' + i), values[i] ?? DBNull.Value);
            }
            command.ExecuteNonQuery();
        }
    }
}
