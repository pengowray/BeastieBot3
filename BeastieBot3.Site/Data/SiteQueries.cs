using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

namespace BeastieBot3.Site.Data;

/// Every query the site runs. All of them are parameterized and read-only.
public sealed partial class SiteQueries {
    private readonly SiteDatabase _db;

    public SiteQueries(SiteDatabase db) {
        _db = db;
    }

    private const string TaxonColumns = """
        t.taxon_id, t.scientific_name, t.kind, t.kingdom, t.phylum, t.class_name, t.order_name, t.family,
        t.genus, t.subpopulation_name, t.authority, t.parent_taxon_id, t.common_name_en, t.enwiki_title,
        t.wikidata_qid, t.col_id, t.latest_global_assessment_id, t.in_release, t.current_taxon_id,
        t.wikidata_qid_source, t.wikidata_p141, t.wikidata_item_downloaded, t.wikidata_p627_deprecated, t.wikidata_other_items,
        t.node_id, t.species_epithet
        """;

    // A taxon with the category of its latest global assessment; the column order SummaryAt reads.
    private const string SummaryColumns = """
        t.taxon_id, t.scientific_name, t.subpopulation_name, t.kind, t.common_name_en,
        a.category, a.possibly_extinct, a.possibly_extinct_in_the_wild, t.in_release
        """;

    // The number of columns in SummaryColumns, where the columns after them start.
    private const int SummaryColumnCount = 9;

    private const string SummaryJoin = "LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id";

    /// The status updater's reads over one connection, for one request. Dispose it.
    /// region: compare with the latest assessments in this IUCN region ("Europe") instead of the global ones.
    public SiteStatusLookup OpenStatusLookup(string? region = null) => new(_db.OpenConnection(), region);

    public TaxonRow? GetTaxon(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {TaxonColumns} FROM taxon t WHERE t.taxon_id = @id";
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        return reader.Read() ? TaxonAt(reader) : null;
    }

    private static TaxonRow TaxonAt(SqliteDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetString(2),
        Text(reader, 3), Text(reader, 4), Text(reader, 5), Text(reader, 6), Text(reader, 7), Text(reader, 8),
        Text(reader, 9),
        Text(reader, 10),
        Long(reader, 11),
        Text(reader, 12),
        Text(reader, 13),
        Text(reader, 14),
        Text(reader, 15),
        Long(reader, 16),
        reader.GetInt64(17) != 0,
        Long(reader, 18),
        Text(reader, 19),
        Text(reader, 20),
        Text(reader, 21),
        !reader.IsDBNull(22) && reader.GetInt64(22) != 0,
        Text(reader, 23),
        reader.IsDBNull(24) ? null : reader.GetInt32(24),
        Text(reader, 25));

    /// The taxa linked to this one in taxon_link: for a taxon in the release, the taxa not in the
    /// release (old ids) linked to it; for a taxon not in the release, the taxa in the release it is
    /// linked to; and the working-name and provisional-name links between taxa in the release, both
    /// ways. Taxa in the release first, then by id.
    public IReadOnlyList<TaxonLinkRow> GetLinkedTaxa(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        // Ordered and read by column name, so columns added to TaxonColumns cannot shift them.
        command.CommandText = $"""
            SELECT * FROM (
                SELECT {TaxonColumns}, l.link_kind AS link_kind, 1 AS is_from
                FROM taxon_link l JOIN taxon t ON t.taxon_id = l.current_taxon_id
                WHERE l.taxon_id = @id
                UNION ALL
                SELECT {TaxonColumns}, l.link_kind AS link_kind, 0 AS is_from
                FROM taxon_link l JOIN taxon t ON t.taxon_id = l.taxon_id
                WHERE l.current_taxon_id = @id)
            ORDER BY in_release DESC, taxon_id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var kind = reader.GetOrdinal("link_kind");
        var isFrom = reader.GetOrdinal("is_from");
        var rows = new List<TaxonLinkRow>();
        while (reader.Read()) {
            rows.Add(new TaxonLinkRow(TaxonAt(reader), reader.GetString(kind), reader.GetInt64(isFrom) == 1));
        }
        return rows;
    }

    /// has_taxonomic_notes of each of these assessments: true or false, or null when the build had
    /// no cached payload for it. Assessments not in the database are left out.
    public IReadOnlyDictionary<long, bool?> GetTaxonomicNotesFlags(IReadOnlyCollection<long> assessmentIds) {
        var flags = new Dictionary<long, bool?>();
        if (assessmentIds.Count == 0) {
            return flags;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var names = new List<string>();
        foreach (var id in assessmentIds.Distinct()) {
            var name = "@a" + names.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
            names.Add(name);
            command.Parameters.AddWithValue(name, id);
        }
        command.CommandText = $"SELECT assessment_id, has_taxonomic_notes FROM assessment WHERE assessment_id IN ({string.Join(", ", names)})";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            flags[reader.GetInt64(0)] = reader.IsDBNull(1) ? null : reader.GetInt64(1) != 0;
        }
        return flags;
    }

    /// The taxon's SPRAT profiles and EPBC Act listings: the profile of the whole taxon first, then
    /// the profiles of populations by name.
    public IReadOnlyList<EpbcListingRow> GetEpbcListings(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT sprat_taxon_id, listed_name, status, applies_to, population
            FROM epbc_listing
            WHERE taxon_id = @id
            ORDER BY CASE applies_to WHEN 'taxon' THEN 0 ELSE 1 END, population, sprat_taxon_id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<EpbcListingRow>();
        while (reader.Read()) {
            rows.Add(new EpbcListingRow(reader.GetInt64(0), reader.GetString(1), Text(reader, 2), reader.GetString(3), Text(reader, 4)));
        }
        return rows;
    }

    /// The taxon's IUCN Green Status assessment; null when it has none.
    public GreenStatusRow? GetGreenStatus(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT red_list_assessment_id, url, assessment_date, published_year, red_list_year,
                recovery_category, recovery_best, recovery_min, recovery_max, legacy_category, legacy_best, legacy_min, legacy_max,
                dependence_category, dependence_best, dependence_min, dependence_max, gain_category, gain_best, gain_min, gain_max,
                potential_category, potential_best, potential_min, potential_max, assessors, reviewers, contributors, facilitators, compilers,
                citation_json
            FROM green_status
            WHERE taxon_id = @id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        int? Int(int i) => reader.IsDBNull(i) ? null : reader.GetInt32(i);
        GreenStatusMetric Metric(int i) => new(Text(reader, i), Int(i + 1), Int(i + 2), Int(i + 3));
        return new GreenStatusRow(reader.IsDBNull(0) ? null : reader.GetInt64(0), reader.GetString(1), reader.GetString(2), Int(3), Int(4),
            Metric(5), Metric(9), Metric(13), Metric(17), Metric(21),
            Text(reader, 25), Text(reader, 26), Text(reader, 27), Text(reader, 28), Text(reader, 29), reader.GetString(30));
    }

    /// The taxon's statuses in lists other than the IUCN Red List, in the order of
    /// OtherStatusSystems.All, then of the lists of a system (other_status_list.sort_order); within a
    /// list, the listing of the whole taxon first, then those of populations by name.
    public IReadOnlyList<OtherStatusRow> GetOtherStatuses(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT s.system, s.status, s.status_code, s.listed_name, s.population, s.source, s.source_id, s.url, s.listed_on, s.report,
                   s.country, s.qualifier, s.listed_under,
                   l.list_key, l.system, l.country, l.region, l.name, l.title, l.sort_order, l.publisher, l.licence, l.licence_url,
                   l.citation, l.url, l.version, l.fetched
            FROM other_status s
            LEFT JOIN other_status_list l ON l.list_key = s.list_key
            WHERE s.taxon_id = @id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<OtherStatusRow>();
        while (reader.Read()) {
            var list = reader.IsDBNull(13) ? null : new OtherStatusListRow(reader.GetString(13), reader.GetString(14), Text(reader, 15),
                Text(reader, 16), reader.GetString(17), Text(reader, 18), reader.GetInt32(19), Text(reader, 20), Text(reader, 21),
                Text(reader, 22), Text(reader, 23), Text(reader, 24), Text(reader, 25), Text(reader, 26));
            rows.Add(new OtherStatusRow(reader.GetString(0), reader.GetString(1), Text(reader, 2), Text(reader, 3), Text(reader, 4),
                reader.GetString(5), reader.GetString(6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10),
                Text(reader, 11), list, Text(reader, 12)));
        }
        return rows
            .OrderBy(r => BeastieBot3.Shared.SiteData.OtherStatusSystems.Order(r.System))
            .ThenBy(r => r.List?.SortOrder ?? 0)
            .ThenBy(r => r.Population is null ? 0 : 1)
            .ThenBy(r => r.ListedOn, StringComparer.Ordinal)
            .ThenBy(r => r.Population, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.SourceId, StringComparer.Ordinal)
            .ToList();
    }

    /// The lists of a status system with several (other_status_list), by country and name; empty for a
    /// database without the table.
    public IReadOnlyList<OtherStatusListRow> GetOtherStatusLists(string system) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT list_key, system, country, region, name, title, sort_order, publisher, licence, licence_url, citation, url, version, fetched
            FROM other_status_list
            WHERE system = @system
            ORDER BY country, name
            """;
        command.Parameters.AddWithValue("@system", system);
        var lists = new List<OtherStatusListRow>();
        try {
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                lists.Add(new OtherStatusListRow(reader.GetString(0), reader.GetString(1), Text(reader, 2), Text(reader, 3), reader.GetString(4),
                    Text(reader, 5), reader.GetInt32(6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11),
                    Text(reader, 12), Text(reader, 13)));
            }
        } catch (Microsoft.Data.Sqlite.SqliteException) {
            return [];
        }
        return lists;
    }

    /// The reasons IUCN's Table 7 gives for the category changes of these taxa's assessments, by
    /// assessment id.
    public IReadOnlyDictionary<long, CategoryChangeRow> GetCategoryChanges(IReadOnlyCollection<long> taxonIds) {
        var changes = new Dictionary<long, CategoryChangeRow>();
        if (taxonIds.Count == 0) return changes;
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT c.assessment_id, c.reason, c.old_category, c.new_category, c.red_list_version, t.release, t.url
            FROM category_change c JOIN summary_table t ON t.summary_table_id = c.summary_table_id
            WHERE c.taxon_id IN (SELECT value FROM json_each(@ids))
            """;
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            changes[reader.GetInt64(0)] = new CategoryChangeRow(reader.GetInt64(0), reader.GetString(1), Text(reader, 2), Text(reader, 3),
                Text(reader, 4), reader.GetString(5), reader.GetString(6));
        }
        return changes;
    }

    /// The PDF of IUCN's summary table of this number and Red List version ("2008"); null when the
    /// database has none.
    public string? GetSummaryTableUrl(int table, string release) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT url FROM summary_table WHERE table_no = @t AND release = @r ORDER BY summary_table_id DESC LIMIT 1";
        command.Parameters.AddWithValue("@t", table);
        command.Parameters.AddWithValue("@r", release);
        return command.ExecuteScalar() as string;
    }

    /// The assessments of these taxa that IUCN's summary tables list as Possibly Extinct or Possibly
    /// Extinct in the Wild, by assessment id.
    public IReadOnlyDictionary<long, IReadOnlyList<PossiblyExtinctListingRow>> GetPossiblyExtinctListings(IReadOnlyCollection<long> taxonIds) {
        var listings = new Dictionary<long, List<PossiblyExtinctListingRow>>();
        if (taxonIds.Count == 0) return new Dictionary<long, IReadOnlyList<PossiblyExtinctListingRow>>();
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT assessment_id, tag, first_release, last_release, tables
            FROM possibly_extinct_listing
            WHERE taxon_id IN (SELECT value FROM json_each(@ids))
            ORDER BY assessment_id, tag
            """;
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            if (!listings.TryGetValue(id, out var rows)) listings[id] = rows = [];
            rows.Add(new PossiblyExtinctListingRow(id, reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4)));
        }
        return listings.ToDictionary(p => p.Key, p => (IReadOnlyList<PossiblyExtinctListingRow>)p.Value);
    }

    /// The EPBC Act listings of these taxa, by taxon id: the listing of the whole taxon first, then
    /// those of populations. Taxa with no listing are left out.
    public IReadOnlyDictionary<long, IReadOnlyList<EpbcListingRow>> GetEpbcListings(IReadOnlyCollection<long> taxonIds) {
        var listings = new Dictionary<long, List<EpbcListingRow>>();
        if (taxonIds.Count == 0) {
            return new Dictionary<long, IReadOnlyList<EpbcListingRow>>();
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT taxon_id, sprat_taxon_id, listed_name, status, applies_to, population
            FROM epbc_listing
            WHERE taxon_id IN (SELECT value FROM json_each(@ids)) AND status IS NOT NULL
            ORDER BY taxon_id, CASE applies_to WHEN 'taxon' THEN 0 ELSE 1 END, population, sprat_taxon_id
            """;
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            if (!listings.TryGetValue(id, out var rows)) {
                listings[id] = rows = [];
            }
            rows.Add(new EpbcListingRow(reader.GetInt64(1), reader.GetString(2), Text(reader, 3), reader.GetString(4), Text(reader, 5)));
        }
        return listings.ToDictionary(p => p.Key, p => (IReadOnlyList<EpbcListingRow>)p.Value);
    }

    /// A taxon's classification in another source (ladder_node: "col" or "wikidata"), top down, from
    /// the node with this id. Empty when the database has none.
    /// The taxon's ids in other databases (taxon_external_id), by Wikidata property.
    public IReadOnlyList<(string Property, string Value)> GetExternalIds(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT property, value FROM taxon_external_id WHERE taxon_id = @id ORDER BY property, value";
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var ids = new List<(string, string)>();
        while (reader.Read()) {
            ids.Add((reader.GetString(0), reader.GetString(1)));
        }
        return ids;
    }

    public EnwikiTaxoboxStatusRow? GetEnwikiTaxoboxStatus(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT status, status_system, ref_assessment_id, revision_id, downloaded FROM enwiki_taxobox_status WHERE taxon_id = @id";
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        return reader.Read()
            ? new EnwikiTaxoboxStatusRow(Text(reader, 0), Text(reader, 1), reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3), reader.GetString(4))
            : null;
    }

    public IReadOnlyList<(string Id, string? Rank, string Name)> GetLadder(string source, string id) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            WITH RECURSIVE up(id, depth) AS (
                SELECT @id, 0
                UNION ALL
                SELECT n.parent_id, up.depth + 1 FROM up JOIN ladder_node n ON n.source = @source AND n.id = up.id
                WHERE n.parent_id IS NOT NULL AND up.depth < 80
            )
            SELECT n.id, n.rank, n.name FROM up JOIN ladder_node n ON n.source = @source AND n.id = up.id ORDER BY up.depth DESC
            """;
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@id", id);
        using var reader = command.ExecuteReader();
        var steps = new List<(string, string?, string)>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (reader.Read()) {
            if (seen.Add(reader.GetString(0))) {
                steps.Add((reader.GetString(0), Text(reader, 1), reader.GetString(2)));
            }
        }
        return steps;
    }

    public TaxonSummary? GetSummary(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {SummaryColumns} FROM taxon t {SummaryJoin} WHERE t.taxon_id = @id";
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        return reader.Read() ? SummaryAt(reader, 0) : null;
    }

    /// Every assessment of the taxon, newest first.
    public IReadOnlyList<AssessmentRow> GetAssessments(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT assessment_id, taxon_id, scope, is_latest, category, possibly_extinct,
                   possibly_extinct_in_the_wild, criteria, criteria_version, year_published,
                   assessment_date, population_trend, citation_json, replaced_by_assessment_id,
                   wikidata_item_qid, wikidata_item_properties, wikidata_item_titles, wikidata_item_label_en, wikidata_item_assessment_id,
                   population_size, api_not_found
            FROM assessment
            WHERE taxon_id = @id
            ORDER BY year_published DESC, assessment_date DESC, assessment_id DESC
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<AssessmentRow>();
        while (reader.Read()) {
            rows.Add(new AssessmentRow(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.GetString(2),
                reader.GetInt64(3) != 0,
                reader.GetString(4),
                reader.GetInt64(5) != 0,
                reader.GetInt64(6) != 0,
                Text(reader, 7),
                Text(reader, 8),
                reader.IsDBNull(9) ? null : reader.GetInt32(9),
                Text(reader, 10),
                Text(reader, 11),
                Text(reader, 12),
                Long(reader, 13),
                Text(reader, 14),
                Text(reader, 15),
                Text(reader, 16),
                Text(reader, 17),
                Long(reader, 18),
                Text(reader, 19),
                reader.GetInt64(20) != 0));
        }
        return rows;
    }

    public IReadOnlyList<NameRow> GetNames(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT name_id, name, name_type, language, source, is_preferred, authority
            FROM name
            WHERE taxon_id = @id
            ORDER BY is_preferred DESC, name_id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<NameRow>();
        while (reader.Read()) {
            rows.Add(new NameRow(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2),
                Text(reader, 3),
                reader.GetString(4),
                reader.GetInt64(5) != 0,
                Text(reader, 6)));
        }
        return rows;
    }

    // A related taxon: SummaryColumns, then the latest global assessment's criteria, year and id.
    private const string RelatedColumns = SummaryColumns + ", a.criteria, a.year_published, a.assessment_id";

    private static RelatedTaxonRow RelatedAt(SqliteDataReader reader) {
        const int next = SummaryColumnCount;
        return new RelatedTaxonRow(SummaryAt(reader, 0), Text(reader, next),
            reader.IsDBNull(next + 1) ? null : reader.GetInt32(next + 1), Long(reader, next + 2));
    }

    private IReadOnlyList<RelatedTaxonRow> ReadRelated(string where, params (string Name, object Value)[] parameters) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT {RelatedColumns}
            FROM taxon t {SummaryJoin}
            WHERE {where}
            ORDER BY CASE t.kind WHEN 'species' THEN 0 WHEN 'subspecies' THEN 1 WHEN 'variety' THEN 2 WHEN 'subpopulation' THEN 3 ELSE 4 END,
                     t.in_release DESC, t.scientific_name
            """;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<RelatedTaxonRow>();
        while (reader.Read()) {
            rows.Add(RelatedAt(reader));
        }
        return rows;
    }

    /// The taxon with its latest global assessment's criteria and year; null when there is no such taxon.
    public RelatedTaxonRow? GetRelated(long taxonId) =>
        ReadRelated("t.taxon_id = @id", ("@id", taxonId)).FirstOrDefault();

    /// Taxa whose parent is this one: subspecies, then varieties, then subpopulations; within each,
    /// taxa in the release first, then by name.
    public IReadOnlyList<RelatedTaxonRow> GetChildren(long taxonId) =>
        ReadRelated("t.parent_taxon_id = @id", ("@id", taxonId));

    /// The other subspecies, varieties and subpopulations in the release with the same genus and
    /// species epithet as this one, for a taxon whose species IUCN has not assessed (so no taxon is
    /// their parent). Read from the taxon's group (its genus) in tree order, through taxon_tree.
    public IReadOnlyList<RelatedTaxonRow> GetUnassessedSpeciesSiblings(TaxonRow taxon) {
        if (taxon.NodeId is not { } nodeId || taxon.Genus is null || taxon.SpeciesEpithet is null) {
            return [];
        }
        return ReadRelated("""
            t.tree_pos BETWEEN (SELECT first_pos FROM higher_taxon WHERE node_id = @node)
                           AND (SELECT last_pos FROM higher_taxon WHERE node_id = @node)
            AND t.genus = @genus AND t.species_epithet = @epithet AND t.kind <> 'species' AND t.taxon_id <> @id
            """, ("@node", nodeId), ("@genus", taxon.Genus), ("@epithet", taxon.SpeciesEpithet), ("@id", taxon.TaxonId));
    }

    private static TaxonSummary SummaryAt(SqliteDataReader reader, int start) => new(
        reader.GetInt64(start),
        reader.GetString(start + 1),
        Text(reader, start + 2),
        reader.GetString(start + 3),
        Text(reader, start + 4),
        Text(reader, start + 5),
        !reader.IsDBNull(start + 6) && reader.GetInt64(start + 6) != 0,
        !reader.IsDBNull(start + 7) && reader.GetInt64(start + 7) != 0,
        reader.GetInt64(start + 8) != 0);

    private static long? Long(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetInt64(i);
}
