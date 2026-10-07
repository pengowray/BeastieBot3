using System.Text;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;
using Microsoft.Data.Sqlite;

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
    public SiteStatusLookup OpenStatusLookup() => new(_db.OpenConnection());

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
    /// linked to. Taxa in the release first, then by id.
    public IReadOnlyList<TaxonLinkRow> GetLinkedTaxa(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        // Ordered and read by column name, so columns added to TaxonColumns cannot shift them.
        command.CommandText = $"""
            SELECT * FROM (
                SELECT {TaxonColumns}, l.link_kind AS link_kind
                FROM taxon_link l JOIN taxon t ON t.taxon_id = l.current_taxon_id
                WHERE l.taxon_id = @id
                UNION ALL
                SELECT {TaxonColumns}, l.link_kind AS link_kind
                FROM taxon_link l JOIN taxon t ON t.taxon_id = l.taxon_id
                WHERE l.current_taxon_id = @id)
            ORDER BY in_release DESC, taxon_id
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var kind = reader.GetOrdinal("link_kind");
        var rows = new List<TaxonLinkRow>();
        while (reader.Read()) {
            rows.Add(new TaxonLinkRow(TaxonAt(reader), reader.GetString(kind)));
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

    /// The taxa an IdQuery names, taxon id first, then assessment id. A taxon id and an assessment id
    /// given together (from a DOI or "T22823A14871490") give the assessment only, when it exists.
    public IReadOnlyList<IdHit> FindByIds(IdQuery query) {
        using var connection = _db.OpenConnection();
        var hits = new List<IdHit>();
        var assessmentId = query.AssessmentId ?? query.Number;
        IdHit? assessmentHit = null;
        if (assessmentId is { } aid) {
            using var command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT {SummaryColumns}, s.scope, s.year_published, t.latest_global_assessment_id
                FROM assessment s
                JOIN taxon t ON t.taxon_id = s.taxon_id
                {SummaryJoin}
                WHERE s.assessment_id = @id
                """;
            command.Parameters.AddWithValue("@id", aid);
            using var reader = command.ExecuteReader();
            if (reader.Read()) {
                const int next = SummaryColumnCount;
                var latest = Long(reader, next + 2);
                assessmentHit = new IdHit(SummaryAt(reader, 0), aid, reader.GetString(next),
                    reader.IsDBNull(next + 1) ? null : reader.GetInt32(next + 1), latest == aid);
            }
        }
        var taxonId = query.TaxonId ?? query.Number;
        // The taxon is listed too when the assessment belongs to another taxon.
        var skipTaxon = query.TaxonId is not null && query.AssessmentId is not null && assessmentHit?.Taxon.TaxonId == query.TaxonId;
        if (taxonId is { } tid && !skipTaxon) {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT {SummaryColumns} FROM taxon t {SummaryJoin} WHERE t.taxon_id = @id";
            command.Parameters.AddWithValue("@id", tid);
            using var reader = command.ExecuteReader();
            if (reader.Read()) {
                hits.Add(new IdHit(SummaryAt(reader, 0), null, null, null, true));
            }
        }
        if (assessmentHit is not null) {
            hits.Add(assessmentHit);
        }
        return hits;
    }

    /// The name type ("scientific", "common", "synonym") by which the text names this taxon, best
    /// first; null when it does not name it at all.
    public string? NameTypeFor(long taxonId, string text) {
        var key = SiteNameKey.Fold(text);
        if (key.Length == 0) {
            return null;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT n.name_type
            FROM name_key k JOIN name n ON n.name_id = k.name_id
            WHERE k.key = @key AND k.taxon_id = @id
            ORDER BY CASE n.name_type WHEN 'scientific' THEN 0 WHEN 'common' THEN 1 ELSE 2 END
            LIMIT 1
            """;
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@id", taxonId);
        return command.ExecuteScalar() as string;
    }

    /// Taxa matching the text, one row per taxon, best first:
    /// 1. a name equal to the text after folding (SiteNameKey.Fold);
    /// 2. a name that starts with the text;
    /// 3. any other name whose words start with the words typed (name_fts).
    /// Within each group taxa in the release come before taxa that are not (no current assessment);
    /// then a scientific name beats a common name, which beats a synonym; species come before
    /// infraspecific taxa and subpopulations; then shorter names first.
    /// With exactOnly, only group 1 is searched. TotalTaxa is counted only when countAll is set and
    /// the limit was reached; otherwise it is the number of hits returned.
    /// When cancellationToken is cancelled (the visitor closed the page), the running query is
    /// interrupted and OperationCanceledException is thrown.
    public SearchResult Search(string text, int limit, bool exactOnly = false, bool countAll = true,
        CancellationToken cancellationToken = default) {
        var key = SiteNameKey.Fold(text);
        if (key.Length == 0) {
            return SearchResult.Empty;
        }
        cancellationToken.ThrowIfCancellationRequested();
        var match = exactOnly ? null : FtsQuery.Build(text);
        var hitsSql = BuildHitsSql(match is not null);

        using var connection = _db.OpenConnection();
        // Declared after the connection so it is disposed first: no interrupt can reach the
        // connection once it is back in the pool.
        using var interrupt = InterruptOnCancel(connection, cancellationToken);
        try {
            return RunSearch(connection, hitsSql, key, text, match, limit, countAll);
        } catch (SqliteException ex) when (ex.SqliteErrorCode == SqliteInterruptCode && cancellationToken.IsCancellationRequested) {
            throw new OperationCanceledException("The search was cancelled.", ex, cancellationToken);
        }
    }

    // SQLITE_INTERRUPT: the statement was stopped by sqlite3_interrupt.
    internal const int SqliteInterruptCode = 9;

    // Microsoft.Data.Sqlite does not cancel a running statement (SqliteCommand.Cancel does nothing),
    // so a cancelled token calls sqlite3_interrupt on the connection. Interrupting a connection
    // with no statement running has no effect.
    internal static CancellationTokenRegistration InterruptOnCancel(SqliteConnection connection, CancellationToken cancellationToken) =>
        cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => SQLitePCL.raw.sqlite3_interrupt(((SqliteConnection)state!).Handle), connection)
            : default;

    private static SearchResult RunSearch(SqliteConnection connection, string hitsSql, string key, string text, string? match,
        int limit, bool countAll) {
        var hits = new List<SearchHit>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                WITH hits AS ({hitsSql}),
                best AS (
                    SELECT h.taxon_id,
                           MIN(CASE WHEN h.name_id IN (SELECT name_id FROM name_key WHERE key = @key) THEN 0
                                    WHEN h.name LIKE @prefix ESCAPE '\' THEN 1
                                    ELSE 2 END * 100000
                               + CASE WHEN t.in_release = 1 THEN 0 ELSE 1 END * 50000
                               + CASE h.name_type WHEN 'scientific' THEN 0 WHEN 'common' THEN 1 ELSE 2 END * 10000
                               + CASE t.kind WHEN 'species' THEN 0 ELSE 1 END * 1000
                               + CASE WHEN LENGTH(h.name) > 999 THEN 999 ELSE LENGTH(h.name) END) AS score,
                           h.name, h.name_type, h.language,
                           MAX(h.name_type <> 'common' AND h.name_id IN (SELECT name_id FROM name_key WHERE key = @key)) AS exact_not_common
                    FROM hits h JOIN taxon t ON t.taxon_id = h.taxon_id
                    GROUP BY h.taxon_id
                )
                SELECT {SummaryColumns}, b.name, b.name_type, b.language, b.score, b.exact_not_common, t.enwiki_title
                FROM best b
                JOIN taxon t ON t.taxon_id = b.taxon_id
                {SummaryJoin}
                ORDER BY b.score, t.scientific_name
                LIMIT @limit
                """;
            AddSearchParameters(command, key, text, match);
            command.Parameters.AddWithValue("@limit", limit);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                const int next = SummaryColumnCount;
                var summary = SummaryAt(reader, 0);
                var exact = reader.GetInt64(next + 3) < 100000;
                hits.Add(new SearchHit(
                    summary,
                    reader.GetString(next),
                    reader.GetString(next + 1),
                    Text(reader, next + 2),
                    exact,
                    exact && (reader.GetInt64(next + 4) == 1
                        || IsKey(summary.CommonNameEn, key)
                        || IsKey(Text(reader, next + 5), key))));
            }
        }

        long total = hits.Count;
        if (countAll && hits.Count >= limit) {
            using var count = connection.CreateCommand();
            count.CommandText = $"WITH hits AS ({hitsSql}) SELECT COUNT(DISTINCT taxon_id) FROM hits";
            AddSearchParameters(count, key, text, match);
            total = Convert.ToInt64(count.ExecuteScalar());
        }
        return new SearchResult(hits, total);
    }

    private static bool IsKey(string? name, string key) => name is not null && SiteNameKey.Fold(name) == key;

    // The names that match: FTS hits (when there is a MATCH expression) plus exact folded-key hits.
    private static string BuildHitsSql(bool withFts) {
        const string exact = """
            SELECT n.taxon_id, n.name_id, n.name, n.name_type, n.language
            FROM name_key k JOIN name n ON n.name_id = k.name_id
            WHERE k.key = @key
            """;
        if (!withFts) {
            return exact;
        }
        return """
            SELECT n.taxon_id, n.name_id, n.name, n.name_type, n.language
            FROM name_fts JOIN name n ON n.name_id = name_fts.rowid
            WHERE name_fts MATCH @match
            UNION
            """ + "\n" + exact;
    }

    private static void AddSearchParameters(SqliteCommand command, string key, string text, string? match) {
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@prefix", LikePrefix(text));
        if (match is not null) {
            command.Parameters.AddWithValue("@match", match);
        }
    }

    // "Panthera t" -> "Panthera t%", with LIKE's own wildcards escaped. SQLite's LIKE ignores case
    // for ASCII letters, which is enough to rank "starts with" above "contains a word starting with".
    private static string LikePrefix(string text) {
        var collapsed = string.Join(' ', text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        var sb = new StringBuilder(collapsed.Length + 1);
        foreach (var c in collapsed) {
            if (c is '\\' or '%' or '_') {
                sb.Append('\\');
            }
            sb.Append(c);
        }
        return sb.Append('%').ToString();
    }

    // ------------------------------------------------------------ groups (higher_taxon)

    private const string GroupColumns = """
        h.node_id, h.parent_node_id, h.depth, h.rank, h.name, h.source, h.show_rank, h.kingdom, h.col_id,
        h.common_name_en, h.common_name_source, h.enwiki_title, h.first_pos, h.last_pos,
        h.species_count, h.infra_count, h.subpopulation_count, h.link_query
        """;

    private static GroupRow GroupAt(SqliteDataReader reader) => new(
        reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetInt32(1), reader.GetInt32(2),
        reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetInt64(6) != 0, reader.GetString(7),
        Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11),
        reader.GetInt32(12), reader.GetInt32(13), reader.GetInt32(14), reader.GetInt32(15), reader.GetInt32(16), Text(reader, 17));

    private IReadOnlyList<GroupRow> ReadGroups(string sql, params (string Name, object Value)[] parameters) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<GroupRow>();
        while (reader.Read()) {
            rows.Add(GroupAt(reader));
        }
        return rows;
    }

    /// The groups with this rank and name (folded with SiteNameKey.Fold), in tree order. More than
    /// one when two kingdoms, or two IUCN classes, use the name.
    public IReadOnlyList<GroupRow> FindGroups(string rank, string name) =>
        ReadGroups($"SELECT {GroupColumns} FROM higher_taxon h WHERE h.name_key = @key AND h.rank = @rank ORDER BY h.node_id",
            ("@key", SiteNameKey.Fold(name)), ("@rank", rank.Trim().ToLowerInvariant()));

    /// Groups whose name (folded) is the text, any rank, for search. Biggest first.
    /// Also the groups that have the text as the title of their English Wikipedia article or of a
    /// redirect to it (higher_taxon_name, source 'wikipedia'), after those with the name itself.
    public IReadOnlyList<GroupHit> FindGroupsByName(string text, int limit) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        // MIN(m.how) makes SQLite take m.matched from the row with the lowest `how`: the group's own
        // name before a Wikipedia title.
        command.CommandText = $"""
            SELECT {GroupColumns}, m.matched, MIN(m.how) AS how
            FROM (
                SELECT node_id, NULL AS matched, 0 AS how FROM higher_taxon WHERE name_key = @key
                UNION ALL
                SELECT node_id, name, 1 FROM higher_taxon_name WHERE name_key = @key AND source = 'wikipedia'
            ) m
            JOIN higher_taxon h ON h.node_id = m.node_id
            GROUP BY h.node_id
            ORDER BY how, h.species_count DESC, h.node_id
            LIMIT @limit
            """;
        command.Parameters.AddWithValue("@key", SiteNameKey.Fold(text));
        command.Parameters.AddWithValue("@limit", limit);
        using var reader = command.ExecuteReader();
        var hits = new List<GroupHit>();
        while (reader.Read()) {
            hits.Add(new GroupHit(GroupAt(reader), Text(reader, GroupColumnCount)));
        }
        return hits;
    }

    // The number of columns in GroupColumns, where the columns after them start.
    private const int GroupColumnCount = 18;

    public GroupRow? GetGroup(int nodeId) =>
        ReadGroups($"SELECT {GroupColumns} FROM higher_taxon h WHERE h.node_id = @id", ("@id", nodeId)).FirstOrDefault();

    /// The group and every group above it, kingdom first.
    public IReadOnlyList<GroupRow> GetGroupPath(int nodeId) =>
        ReadGroups($"""
            WITH RECURSIVE up(id) AS (
                SELECT @id
                UNION ALL
                SELECT p.parent_node_id FROM higher_taxon p JOIN up ON p.node_id = up.id WHERE p.parent_node_id IS NOT NULL)
            SELECT {GroupColumns} FROM higher_taxon h WHERE h.node_id IN (SELECT id FROM up) ORDER BY h.depth
            """, ("@id", nodeId));

    /// The groups directly under a group, in tree order.
    public IReadOnlyList<GroupRow> GetChildGroups(int nodeId) =>
        ReadGroups($"SELECT {GroupColumns} FROM higher_taxon h WHERE h.parent_node_id = @id ORDER BY h.node_id", ("@id", nodeId));

    /// Every group inside a group (not the group itself), in tree order.
    public IReadOnlyList<GroupRow> GetGroupsWithin(GroupRow group) =>
        ReadGroups($"""
            SELECT {GroupColumns} FROM higher_taxon h
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            ORDER BY h.node_id
            """, ("@first", group.FirstPos), ("@last", group.LastPos), ("@id", group.NodeId));

    /// The ranks of the groups inside a group.
    public IReadOnlyList<GroupRank> GetRanksWithin(GroupRow group) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT h.rank, MIN(h.depth), MIN(h.source = 'col') FROM higher_taxon h
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            GROUP BY h.rank
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        command.Parameters.AddWithValue("@id", group.NodeId);
        using var reader = command.ExecuteReader();
        var ranks = new List<GroupRank>();
        while (reader.Read()) {
            ranks.Add(new GroupRank(reader.GetString(0), reader.GetInt32(1), reader.GetInt64(2) != 0));
        }
        return ranks;
    }

    public IReadOnlyList<GroupCategoryCount> GetGroupCounts(int nodeId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT category, species_count, infra_count, subpopulation_count FROM higher_taxon_count WHERE node_id = @id";
        command.Parameters.AddWithValue("@id", nodeId);
        using var reader = command.ExecuteReader();
        var rows = new List<GroupCategoryCount>();
        while (reader.Read()) {
            rows.Add(new GroupCategoryCount(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3)));
        }
        return rows;
    }

    /// The counts of several groups at once, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<GroupCategoryCount>> GetGroupCounts(IReadOnlyCollection<int> nodeIds) {
        var result = new Dictionary<int, IReadOnlyList<GroupCategoryCount>>();
        if (nodeIds.Count == 0) {
            return result;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var names = new List<string>();
        var i = 0;
        foreach (var id in nodeIds) {
            var name = "@n" + i++;
            names.Add(name);
            command.Parameters.AddWithValue(name, id);
        }
        command.CommandText = $"""
            SELECT node_id, category, species_count, infra_count, subpopulation_count FROM higher_taxon_count
            WHERE node_id IN ({string.Join(", ", names)})
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<GroupCategoryCount>();
            }
            ((List<GroupCategoryCount>)list).Add(new GroupCategoryCount(reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4)));
        }
        return result;
    }

    /// The counts of every group inside a group, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<GroupCategoryCount>> GetGroupCountsWithin(GroupRow group) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT c.node_id, c.category, c.species_count, c.infra_count, c.subpopulation_count
            FROM higher_taxon h JOIN higher_taxon_count c ON c.node_id = h.node_id
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        command.Parameters.AddWithValue("@id", group.NodeId);
        var result = new Dictionary<int, IReadOnlyList<GroupCategoryCount>>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<GroupCategoryCount>();
            }
            ((List<GroupCategoryCount>)list).Add(new GroupCategoryCount(reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4)));
        }
        return result;
    }

    /// The Catalogue of Life's English names of several groups, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<string>> GetGroupColNames(IReadOnlyCollection<int> nodeIds) {
        var result = new Dictionary<int, IReadOnlyList<string>>();
        if (nodeIds.Count == 0) {
            return result;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var names = new List<string>();
        var i = 0;
        foreach (var id in nodeIds) {
            var name = "@n" + i++;
            names.Add(name);
            command.Parameters.AddWithValue(name, id);
        }
        command.CommandText = $"""
            SELECT node_id, name FROM higher_taxon_name WHERE node_id IN ({string.Join(", ", names)}) AND source = 'col'
            ORDER BY node_id, name COLLATE NOCASE
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<string>();
            }
            ((List<string>)list).Add(reader.GetString(1));
        }
        return result;
    }

    /// The Catalogue of Life's English names of a group, as CoL writes them.
    public IReadOnlyList<string> GetGroupColNames(int nodeId) => GetGroupNames(nodeId, "col");

    /// The group's names from English Wikipedia: the title of its article and the titles of the
    /// redirects to it. Each is a page title on English Wikipedia.
    public IReadOnlyList<string> GetGroupWikipediaNames(int nodeId) => GetGroupNames(nodeId, "wikipedia");

    private IReadOnlyList<string> GetGroupNames(int nodeId, string source) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM higher_taxon_name WHERE node_id = @id AND source = @source ORDER BY name COLLATE NOCASE";
        command.Parameters.AddWithValue("@id", nodeId);
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read()) {
            names.Add(reader.GetString(0));
        }
        return names;
    }

    /// The taxa of a group that have a latest global assessment, in tree order, of the given kinds. A
    /// subpopulation with no English name of its own has its species' name.
    public IReadOnlyList<ListTaxonRow> GetListTaxa(GroupRow group, IReadOnlyCollection<string> kinds) =>
        [.. ListTaxa(group, kinds, area: null).Select(t => t.Row)];

    /// The group's taxa of these kinds whose latest global assessment codes the area, with the record.
    public IReadOnlyList<(ListTaxonRow Row, AreaRecord Record)> GetListTaxaInArea(GroupRow group, IReadOnlyCollection<string> kinds, string area) =>
        [.. ListTaxa(group, kinds, area).Select(t => (t.Row, t.Record!))];

    /// The area's records for these taxa; taxa with none are left out.
    public IReadOnlyDictionary<long, AreaRecord> GetAreaRecords(string area, IReadOnlyCollection<long> taxonIds) {
        var records = new Dictionary<long, AreaRecord>();
        if (taxonIds.Count == 0) {
            return records;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT taxon_id, origin, presence, endemic FROM taxon_area
            WHERE area = @area AND taxon_id IN (SELECT value FROM json_each(@ids))
            """;
        command.Parameters.AddWithValue("@area", area);
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            records[reader.GetInt64(0)] = new AreaRecord((AreaOrigin)reader.GetInt32(1), (AreaPresence)reader.GetInt32(2), reader.GetInt64(3) != 0);
        }
        return records;
    }

    private sealed record AreaCache(SiteSnapshot Snapshot, AreaNames Names);
    private volatile AreaCache? _areaCache;

    /// The area names, read once per database file.
    public AreaNames AreaNames() {
        var snapshot = _db.Snapshot;
        if (_areaCache is { } known && ReferenceEquals(known.Snapshot, snapshot)) {
            return known.Names;
        }
        var names = new AreaNames(GetAreas());
        if (snapshot is not null) {
            _areaCache = new AreaCache(snapshot, names);
        }
        return names;
    }

    /// The countries and areas the latest global assessments code (the area table), for choosing one.
    public IReadOnlyList<AreaName> GetAreas() {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT code, name, country FROM area ORDER BY name";
        using var reader = command.ExecuteReader();
        var areas = new List<AreaName>();
        while (reader.Read()) {
            areas.Add(new AreaName(reader.GetString(0), reader.GetString(1), Text(reader, 2)));
        }
        return areas;
    }

    private List<(ListTaxonRow Row, AreaRecord? Record)> ListTaxa(GroupRow group, IReadOnlyCollection<string> kinds, string? area) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var kindNames = new List<string>();
        var i = 0;
        foreach (var kind in kinds) {
            var name = "@k" + i++;
            kindNames.Add(name);
            command.Parameters.AddWithValue(name, kind);
        }
        if (kindNames.Count == 0) {
            return [];
        }
        command.CommandText = $"""
            SELECT t.taxon_id, t.scientific_name, t.kind, t.kingdom, t.genus, t.species_epithet, t.infra_rank, t.infra_name,
                   t.subpopulation_name, COALESCE(t.common_name_en, p.common_name_en), t.list_article_title, t.list_parent_article_title,
                   t.parent_taxon_id, t.node_id, t.tree_pos, a.assessment_id, a.category, a.possibly_extinct, a.possibly_extinct_in_the_wild,
                   a.year_published, t.authority{(area is null ? "" : ", x.origin, x.presence, x.endemic")}
            FROM taxon t LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id
            LEFT JOIN taxon p ON p.taxon_id = t.parent_taxon_id AND t.kind = 'subpopulation'
            {(area is null ? "" : "JOIN taxon_area x ON x.taxon_id = t.taxon_id AND x.area = @area")}
            WHERE t.tree_pos BETWEEN @first AND @last AND t.kind IN ({string.Join(", ", kindNames)})
            ORDER BY t.tree_pos
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        if (area is not null) {
            command.Parameters.AddWithValue("@area", area);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<(ListTaxonRow, AreaRecord?)>();
        while (reader.Read()) {
            rows.Add((new ListTaxonRow(
                reader.GetInt64(0), reader.GetString(1), reader.GetString(2), Text(reader, 3), Text(reader, 4), Text(reader, 5),
                Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11),
                Long(reader, 12), reader.GetInt32(13), reader.GetInt32(14), Long(reader, 15), Text(reader, 16),
                !reader.IsDBNull(17) && reader.GetInt64(17) != 0, !reader.IsDBNull(18) && reader.GetInt64(18) != 0,
                reader.IsDBNull(19) ? null : reader.GetInt32(19), Text(reader, 20)),
                area is null ? null : new AreaRecord((AreaOrigin)reader.GetInt32(21), (AreaPresence)reader.GetInt32(22), reader.GetInt64(23) != 0)));
        }
        return rows;
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

    private static string? Text(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetString(i);

    private static long? Long(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetInt64(i);
}
