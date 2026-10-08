using System.Text;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

// Search: taxa found by their ids, by a name, or by full-text search over the names.

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    /// The taxa an IdQuery names, taxon id first, then assessment id. A taxon id and an assessment id
    /// given together (from a DOI or "T22823A14871490") give the assessment only, when it exists.
    public IReadOnlyList<IdHit> FindByIds(IdQuery query) {
        using var connection = _db.OpenConnection();
        var hits = new List<IdHit>();
        if (query.WikidataItem is { } item) {
            FindByWikidataItem(connection, "Q" + item.ToString(System.Globalization.CultureInfo.InvariantCulture), hits);
        }
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

    // The taxa whose Wikidata item is qid, then the assessments whose Wikidata item is qid.
    private static void FindByWikidataItem(SqliteConnection connection, string qid, List<IdHit> hits) {
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"SELECT {SummaryColumns} FROM taxon t {SummaryJoin} WHERE t.wikidata_qid = @qid ORDER BY t.taxon_id";
            command.Parameters.AddWithValue("@qid", qid);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                hits.Add(new IdHit(SummaryAt(reader, 0), null, null, null, true, qid));
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                SELECT {SummaryColumns}, s.assessment_id, s.scope, s.year_published, t.latest_global_assessment_id
                FROM assessment s
                JOIN taxon t ON t.taxon_id = s.taxon_id
                {SummaryJoin}
                WHERE s.wikidata_item_qid = @qid
                ORDER BY s.assessment_id
                """;
            command.Parameters.AddWithValue("@qid", qid);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                const int next = SummaryColumnCount;
                var aid = reader.GetInt64(next);
                var latest = Long(reader, next + 3);
                hits.Add(new IdHit(SummaryAt(reader, 0), aid, reader.GetString(next + 1),
                    reader.IsDBNull(next + 2) ? null : reader.GetInt32(next + 2), latest == aid, qid));
            }
        }
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
    /// then a scientific name beats an English common name, which beats a common name in another
    /// language, which beats a synonym, and in group 1 the taxon's English name
    /// (taxon.common_name_en) beats its other English names; species come before infraspecific taxa
    /// and subpopulations; then shorter names first.
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
        // Folds taxon.common_name_en for the ranking; called only for names equal to the text.
        connection.CreateFunction("site_fold", (string? name) => name is null ? null : SiteNameKey.Fold(name), isDeterministic: true);
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
                               + CASE h.name_type
                                     WHEN 'scientific' THEN 0
                                     WHEN 'common' THEN
                                         CASE WHEN COALESCE(h.language, '') NOT IN ('en', 'eng') AND COALESCE(h.language, '') NOT LIKE 'en-%' THEN 3
                                              WHEN h.name_id NOT IN (SELECT name_id FROM name_key WHERE key = @key) THEN 2
                                              WHEN site_fold(t.common_name_en) = @key THEN 1
                                              ELSE 2 END
                                     ELSE 4 END * 10000
                               + CASE t.kind WHEN 'species' THEN 0 ELSE 1 END * 1000
                               + CASE WHEN LENGTH(h.name) > 999 THEN 999 ELSE LENGTH(h.name) END) AS score,
                           h.name, h.name_type, h.language,
                           -- SUM, not MAX: with MIN the only min() or max() aggregate, SQLite takes the bare
                           -- columns (h.name, h.name_type, h.language) from the row with the lowest score.
                           SUM(h.name_type <> 'common' AND h.name_id IN (SELECT name_id FROM name_key WHERE key = @key)) > 0 AS exact_not_common,
                           SUM(h.name_id IN (SELECT name_id FROM name_key WHERE key = @key)
                               AND (h.name_type <> 'common' OR COALESCE(h.language, '') IN ('en', 'eng') OR COALESCE(h.language, '') LIKE 'en-%')) > 0
                               AS exact_not_other_language
                    FROM hits h JOIN taxon t ON t.taxon_id = h.taxon_id
                    GROUP BY h.taxon_id
                )
                SELECT {SummaryColumns}, b.name, b.name_type, b.language, b.score, b.exact_not_common, t.enwiki_title, b.exact_not_other_language
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
                        || IsKey(Text(reader, next + 5), key)),
                    exact && reader.GetInt64(next + 6) == 0));
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
}
