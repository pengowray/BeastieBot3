using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;

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
    public const long SumatranTiger = 15966;
    public const long SumatranTigerLatest = 136285;

    public const long Lion = 15951;
    public const long LionLatest = 280792135;
    public const long WestAfricanLion = 68933833;
    public const long WestAfricanLionLatest = 68933837;

    public const long RegionalOnly = 135570;
    public const long RegionalOnlyEurope = 135570001;
    public const long RegionalOnlyMediterranean = 135570002;

    public const long Variety = 34010;

    public const long Koala = 16892;
    public const long KoalaLatest = 166496779;
    public const long KoalaSprat = 85104;

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
    /// deleted when the test run ends. schemaVersion and dropTable make the broken variants.
    public static string Create(string name, string? schemaVersion = null, string? dropTable = null) {
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
            Populate(writer, schemaVersion ?? SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture));
            tx.Commit();
            Exec(connection, "INSERT INTO name_fts(name_fts) VALUES('rebuild')");
            if (dropTable is not null) {
                Exec(connection, $"DROP TABLE {dropTable}");
            }
        }
        return path;
    }

    private static void Populate(Writer w, string schemaVersion) {
        // Polar bear: a species with a global history, a citation with a DOI from GBIF, an
        // unsplit author, common names in three languages and synonyms.
        w.Taxon(PolarBear, "Ursus maritimus", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "URSIDAE", "Ursus",
            authority: "Phipps, 1774", commonEn: "Polar bear", enwiki: "Polar bear", qid: "Q33609", colId: "4QHKG", latest: PolarBearLatest);
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
                doi: null, doiSource: DoiSource.None, text: null));
        w.Assessment(PolarBear1996, PolarBear, "Global", false, "LR/cd", criteriaVersion: "2.3", year: 1996, date: "1996-06-30");
        w.Assessment(PolarBear1988Nt, PolarBear, "Global", false, "nt", criteriaVersion: "Earlier Version", year: 1988);
        w.Name(PolarBear, "Ursus maritimus", "scientific", null, "iucn");
        w.Name(PolarBear, "Polar bear", "common", "en", "iucn", preferred: true);
        w.Name(PolarBear, "Polar Bear", "common", "en", "wikidata");
        w.Name(PolarBear, "White bear", "common", "en", "col");
        w.Name(PolarBear, "Ours polaire", "common", "fr", "iucn");
        w.Name(PolarBear, "Ours blanc", "common", "fr", "wikidata");
        w.Name(PolarBear, "Oso polar", "common", "es", "iucn");
        w.Name(PolarBear, "Thalarctos maritimus", "synonym", null, "iucn");
        w.Name(PolarBear, "Ursus marinus Pallas, 1776", "synonym", null, "col");

        // House sparrow: a BirdLife assessment with an organisation as author and a regional one.
        w.Taxon(HouseSparrow, "Passer domesticus", "species", "ANIMALIA", "CHORDATA", "AVES", "PASSERIFORMES", "PASSERIDAE", "Passer",
            authority: "(Linnaeus, 1758)", commonEn: "House sparrow", enwiki: "House sparrow", qid: "Q28922", latest: HouseSparrowLatest);
        w.Assessment(HouseSparrowLatest, HouseSparrow, "Global", true, "LC", criteriaVersion: "3.1", year: 2019, date: "2018-08-07", trend: "Decreasing",
            citation: Citation(HouseSparrow, HouseSparrowLatest, 2019, "Passer domesticus", [Organisation("BirdLife International")],
                doi: "10.2305/IUCN.UK.2019-3.RLTS.T103818789A155522130.en", doiSource: DoiSource.Citation, text: "BirdLife International. 2019. Passer domesticus."));
        w.Assessment(HouseSparrowEurope, HouseSparrow, "Europe", true, "LC", criteriaVersion: "3.1", year: 2021, date: "2021-03-01",
            citation: Citation(HouseSparrow, HouseSparrowEurope, 2021, "Passer domesticus", [Organisation("BirdLife International")],
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe"));
        w.Name(HouseSparrow, "Passer domesticus", "scientific", null, "iucn");
        w.Name(HouseSparrow, "House sparrow", "common", "en", "iucn", preferred: true);

        // Baiji: CR with possibly extinct, a DOI from Wikidata, and a pre-1994 "Ex" in its history.
        w.Taxon(Baiji, "Lipotes vexillifer", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CETARTIODACTYLA", "LIPOTIDAE", "Lipotes",
            authority: "Miller, 1918", commonEn: "Baiji", enwiki: "Baiji", qid: "Q190826", latest: BaijiLatest);
        w.Assessment(BaijiLatest, Baiji, "Global", true, "CR", possiblyExtinct: true, criteria: "A2cd; C2a(ii); D", criteriaVersion: "3.1",
            year: 2017, date: "2017-07-01", trend: "Unknown",
            citation: Citation(Baiji, BaijiLatest, 2017, "Lipotes vexillifer",
                [Person("Smith", "B.D."), Person("Wang", "D."), Person("Braulik", "G.T."), Person("Reeves", "R.")],
                doi: "10.2305/IUCN.UK.2017-3.RLTS.T12119A50358152.en", doiSource: DoiSource.Wikidata, text: null));
        w.Assessment(Baiji1986Ex, Baiji, "Global", false, "Ex", criteriaVersion: "Earlier Version", year: 1986);
        w.Name(Baiji, "Lipotes vexillifer", "scientific", null, "iucn");
        w.Name(Baiji, "Baiji", "common", "en", "iucn", preferred: true);

        // Tiger and the Sumatran tiger: a species with a subspecies.
        w.Taxon(Tiger, "Panthera tigris", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            authority: "(Linnaeus, 1758)", commonEn: "Tiger", enwiki: "Tiger", qid: "Q132186", latest: TigerLatest);
        w.Assessment(TigerLatest, Tiger, "Global", true, "EN", criteria: "A2abcd+4abcd", criteriaVersion: "3.1", year: 2022, date: "2021-12-01",
            trend: "Stable",
            citation: Citation(Tiger, TigerLatest, 2022, "Panthera tigris", [Person("Goodrich", "J."), Person("Wibisono", "H.")],
                doi: "10.2305/IUCN.UK.2022-1.RLTS.T15955A214862019.en", doiSource: DoiSource.Gbif, text: null));
        w.Name(Tiger, "Panthera tigris", "scientific", null, "iucn");
        w.Name(Tiger, "Tiger", "common", "en", "iucn", preferred: true);
        w.Name(Tiger, "Big cat", "common", "en", "wikidata");
        w.Name(Tiger, "Felis tigris", "synonym", null, "iucn");

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
            authority: "(Linnaeus, 1758)", commonEn: "Lion", enwiki: "Lion", qid: "Q140", latest: LionLatest);
        w.Assessment(LionLatest, Lion, "Global", true, "VU", criteria: "A2abcd", criteriaVersion: "3.1", year: 2025, date: "2024-09-01",
            trend: "Decreasing",
            citation: Citation(Lion, LionLatest, 2025, "Panthera leo", [Person("Nicholson", "S."), Person("Bauer", "H.")],
                doi: "10.2305/IUCN.UK.2025-2.RLTS.T15951A280792135.en", doiSource: DoiSource.Citation, text: null));
        w.Name(Lion, "Panthera leo", "scientific", null, "iucn");
        w.Name(Lion, "Lion", "common", "en", "iucn", preferred: true);
        w.Name(Lion, "Big cat", "common", "en", "wikidata");

        w.Taxon(WestAfricanLion, "Panthera leo West Africa subpopulation", "subpopulation", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera",
            subpopulation: "West Africa subpopulation", commonEn: "West African lion", parent: Lion, latest: WestAfricanLionLatest);
        w.Assessment(WestAfricanLionLatest, WestAfricanLion, "Global", true, "CR", criteria: "C2a(i)", criteriaVersion: "3.1", year: 2015,
            date: "2015-01-01", trend: "Decreasing");
        w.Name(WestAfricanLion, "Panthera leo West Africa subpopulation", "scientific", null, "iucn");
        w.Name(WestAfricanLion, "West African lion", "common", "en", "iucn", preferred: true);

        // A taxon with regional assessments only.
        w.Taxon(RegionalOnly, "Gobio kovatschevi", "species", "ANIMALIA", "CHORDATA", "ACTINOPTERYGII", "CYPRINIFORMES", "GOBIONIDAE", "Gobio",
            authority: "Chichkoff, 1937");
        w.Assessment(RegionalOnlyEurope, RegionalOnly, "Europe", true, "LC", criteriaVersion: "3.1", year: 2008, date: "2008-01-01",
            citation: Citation(RegionalOnly, RegionalOnlyEurope, 2008, "Gobio kovatschevi", [Person("Freyhof", "J.")],
                doi: null, doiSource: DoiSource.None, text: null, region: "Europe"));
        w.Assessment(RegionalOnlyMediterranean, RegionalOnly, "Mediterranean", true, "DD", criteriaVersion: "3.1", year: 2010, date: "2010-01-01");
        w.Name(RegionalOnly, "Gobio kovatschevi", "scientific", null, "iucn");

        // Koala: SPRAT profile and EPBC listing.
        w.Taxon(Koala, "Phascolarctos cinereus", "species", "ANIMALIA", "CHORDATA", "MAMMALIA", "DIPROTODONTIA", "PHASCOLARCTIDAE", "Phascolarctos",
            authority: "(Goldfuss, 1817)", commonEn: "Koala", enwiki: "Koala", qid: "Q36101", latest: KoalaLatest, spratId: KoalaSprat, epbc: "EN");
        w.Assessment(KoalaLatest, Koala, "Global", true, "VU", criteria: "A2bc", criteriaVersion: "3.1", year: 2016, date: "2014-07-08",
            trend: "Decreasing");
        w.Name(Koala, "Phascolarctos cinereus", "scientific", null, "iucn");
        w.Name(Koala, "Koala", "common", "en", "iucn", preferred: true);

        // A variety, for the kind label.
        w.Taxon(Variety, "Cupressus arizonica var. glabra", "variety", "PLANTAE", "TRACHEOPHYTA", "PINOPSIDA", "PINALES", "CUPRESSACEAE", "Cupressus",
            authority: "(Sudw.) Little", latest: 34010001, infraRank: "var.", infraName: "glabra");
        w.Assessment(34010001, Variety, "Global", true, "VU", criteria: "B2ab(iii)", criteriaVersion: "3.1", year: 2013, date: "2012-05-08");
        w.Name(Variety, "Cupressus arizonica var. glabra", "scientific", null, "iucn");

        // Enough taxa sharing a genus name to fill more than one page of search results.
        for (var i = 0; i < FillerCount; i++) {
            var id = FillerFirstId + i;
            var epithet = "filler" + i.ToString("D3", CultureInfo.InvariantCulture);
            w.Taxon(id, $"Fillerus {epithet}", "species", "ANIMALIA", "ARTHROPODA", "INSECTA", "COLEOPTERA", "FILLERIDAE", "Fillerus",
                latest: id * 10);
            w.Assessment(id * 10, id, "Global", true, "LC", criteriaVersion: "3.1", year: 2020);
            w.Name(id, $"Fillerus {epithet}", "scientific", null, "iucn");
        }

        w.Meta(SiteDbSchema.MetaKeys.SchemaVersion, schemaVersion);
        w.Meta(SiteDbSchema.MetaKeys.BuiltAtUtc, "2026-10-02T09:00:00Z");
        w.Meta(SiteDbSchema.MetaKeys.IucnRelease, "2026-1");
        w.Meta(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom, "2026-08-18");
        w.Meta(SiteDbSchema.MetaKeys.IucnApiDownloadedTo, "2026-09-01");
        w.Meta(SiteDbSchema.MetaKeys.GbifChecklistVersion, "2026-1");
        w.Meta(SiteDbSchema.MetaKeys.GbifChecklistPublished, "2026-07-28");
        w.Meta(SiteDbSchema.MetaKeys.ColRelease, "COL26.7 XR");
        w.Meta(SiteDbSchema.MetaKeys.SpratReport, "01102026-023504-report.csv");
        w.Meta(SiteDbSchema.MetaKeys.TaxonCount, w.TaxonCount.ToString(CultureInfo.InvariantCulture));
        w.Meta(SiteDbSchema.MetaKeys.AssessmentCount, w.AssessmentCount.ToString(CultureInfo.InvariantCulture));
    }

    public static int GlobalTaxonCount => 9 + FillerCount;

    private static CitationAuthor Person(string last, string initials) =>
        new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials);

    private static CitationAuthor Organisation(string name) => new(CitationAuthorKind.Organisation, name);

    private static CitationAuthor Verbatim(string name) => new(CitationAuthorKind.Verbatim, name);

    private static string Citation(long taxonId, long assessmentId, int year, string name, IReadOnlyList<CitationAuthor> authors,
        string? doi, DoiSource doiSource, string? text, bool etAl = false, string? region = null, bool narrative = false) {
        var parts = new IucnCitationParts {
            TaxonId = taxonId,
            AssessmentId = assessmentId,
            Year = year,
            ScientificName = name,
            RegionalScope = region,
            Authors = authors,
            AuthorsEtAl = etAl,
            Doi = doi,
            DoiSource = doiSource,
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
            long? spratId = null, string? epbc = null) {
            TaxonCount++;
            Run("""
                INSERT INTO taxon(taxon_id, scientific_name, kind, kingdom, phylum, class_name, order_name, family, genus,
                    infra_rank, infra_name, subpopulation_name, authority, parent_taxon_id, common_name_en, enwiki_title,
                    wikidata_qid, col_id, sprat_taxon_id, epbc_status, latest_global_assessment_id)
                VALUES (@a, @b, @c, @d, @e, @f, @g, @h, @i, @j, @k, @l, @m, @n, @o, @p, @q, @r, @s, @t, @u)
                """,
                id, name, kind, kingdom, phylum, className, order, family, genus, infraRank, infraName, subpopulation, authority,
                parent, commonEn, enwiki, qid, colId, spratId, epbc, latest);
        }

        public void Assessment(long id, long taxonId, string scope, bool latest, string category, bool possiblyExtinct = false,
            string? criteria = null, string? criteriaVersion = null, int? year = null, string? date = null, string? trend = null,
            string? citation = null) {
            AssessmentCount++;
            Run("""
                INSERT INTO assessment(assessment_id, taxon_id, scope, is_latest, category, possibly_extinct,
                    possibly_extinct_in_the_wild, criteria, criteria_version, year_published, assessment_date, population_trend, citation_json)
                VALUES (@a, @b, @c, @d, @e, @f, 0, @g, @h, @i, @j, @k, @l)
                """,
                id, taxonId, scope, latest ? 1 : 0, category, possiblyExtinct ? 1 : 0, criteria, criteriaVersion, year, date, trend, citation);
        }

        public void Name(long taxonId, string name, string type, string? language, string source, bool preferred = false) {
            var nameId = _nextNameId++;
            Run("INSERT INTO name(name_id, taxon_id, name, name_type, language, source, is_preferred) VALUES (@a, @b, @c, @d, @e, @f, @g)",
                nameId, taxonId, name, type, language, source, preferred ? 1 : 0);
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
