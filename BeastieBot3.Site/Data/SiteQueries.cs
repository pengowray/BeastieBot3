using System.Text;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Site.Data;

/// Every query the site runs. All of them are parameterized and read-only.
public sealed class SiteQueries {
    private readonly SiteDatabase _db;

    public SiteQueries(SiteDatabase db) {
        _db = db;
    }

    private const string TaxonColumns = """
        t.taxon_id, t.scientific_name, t.kind, t.kingdom, t.phylum, t.class_name, t.order_name, t.family,
        t.genus, t.subpopulation_name, t.authority, t.parent_taxon_id, t.common_name_en, t.enwiki_title,
        t.wikidata_qid, t.col_id, t.sprat_taxon_id, t.epbc_status, t.latest_global_assessment_id
        """;

    // A taxon with the category of its latest global assessment; the column order SummaryAt reads.
    private const string SummaryColumns = """
        t.taxon_id, t.scientific_name, t.subpopulation_name, t.kind, t.common_name_en,
        a.category, a.possibly_extinct, a.possibly_extinct_in_the_wild
        """;

    private const string SummaryJoin = "LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id";

    public TaxonRow? GetTaxon(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {TaxonColumns} FROM taxon t WHERE t.taxon_id = @id";
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return new TaxonRow(
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
            Text(reader, 17),
            Long(reader, 18));
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
                   assessment_date, population_trend, citation_json
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
                Text(reader, 12)));
        }
        return rows;
    }

    public IReadOnlyList<NameRow> GetNames(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT name_id, name, name_type, language, source, is_preferred
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
                reader.GetInt64(5) != 0));
        }
        return rows;
    }

    /// Taxa whose parent is this one: subspecies, then varieties, then subpopulations, each by name.
    public IReadOnlyList<TaxonSummary> GetChildren(long taxonId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT {SummaryColumns}
            FROM taxon t {SummaryJoin}
            WHERE t.parent_taxon_id = @id
            ORDER BY CASE t.kind WHEN 'subspecies' THEN 0 WHEN 'variety' THEN 1 WHEN 'subpopulation' THEN 2 ELSE 3 END,
                     t.scientific_name
            """;
        command.Parameters.AddWithValue("@id", taxonId);
        using var reader = command.ExecuteReader();
        var rows = new List<TaxonSummary>();
        while (reader.Read()) {
            rows.Add(SummaryAt(reader, 0));
        }
        return rows;
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
    /// Within each group a scientific name beats a common name, which beats a synonym; species come
    /// before infraspecific taxa and subpopulations; then shorter names first.
    /// With exactOnly, only group 1 is searched. TotalTaxa is counted only when countAll is set and
    /// the limit was reached; otherwise it is the number of hits returned.
    public SearchResult Search(string text, int limit, bool exactOnly = false, bool countAll = true) {
        var key = SiteNameKey.Fold(text);
        if (key.Length == 0) {
            return SearchResult.Empty;
        }
        var match = exactOnly ? null : FtsQuery.Build(text);
        var hitsSql = BuildHitsSql(match is not null);

        using var connection = _db.OpenConnection();
        var hits = new List<SearchHit>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                WITH hits AS ({hitsSql}),
                best AS (
                    SELECT h.taxon_id,
                           MIN(CASE WHEN h.name_id IN (SELECT name_id FROM name_key WHERE key = @key) THEN 0
                                    WHEN h.name LIKE @prefix ESCAPE '\' THEN 1
                                    ELSE 2 END * 100000
                               + CASE h.name_type WHEN 'scientific' THEN 0 WHEN 'common' THEN 1 ELSE 2 END * 10000
                               + CASE t.kind WHEN 'species' THEN 0 ELSE 1 END * 1000
                               + CASE WHEN LENGTH(h.name) > 999 THEN 999 ELSE LENGTH(h.name) END) AS score,
                           h.name, h.name_type, h.language
                    FROM hits h JOIN taxon t ON t.taxon_id = h.taxon_id
                    GROUP BY h.taxon_id
                )
                SELECT {SummaryColumns}, b.name, b.name_type, b.language, b.score
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
                hits.Add(new SearchHit(
                    SummaryAt(reader, 0),
                    reader.GetString(8),
                    reader.GetString(9),
                    Text(reader, 10),
                    reader.GetInt64(11) < 100000));
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

    private static TaxonSummary SummaryAt(SqliteDataReader reader, int start) => new(
        reader.GetInt64(start),
        reader.GetString(start + 1),
        Text(reader, start + 2),
        reader.GetString(start + 3),
        Text(reader, start + 4),
        Text(reader, start + 5),
        !reader.IsDBNull(start + 6) && reader.GetInt64(start + 6) != 0,
        !reader.IsDBNull(start + 7) && reader.GetInt64(start + 7) != 0);

    private static string? Text(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetString(i);

    private static long? Long(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : reader.GetInt64(i);
}
