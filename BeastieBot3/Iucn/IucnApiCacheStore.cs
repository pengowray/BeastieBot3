using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;

// SQLite store for IUCN API v4 responses (Datastore:IUCN_api_cache_sqlite).
// Schema: taxa (root_sis_id PK, json), taxa_lookup, assessments (assessment_id PK, sis_id, json),
// taxa_assessment_backlog, failed_requests, plus the HTTP request log via ApiImportMetadataStore.
// Separate from CSV-imported database; provides synonyms, population trends, HTML
// narratives not in CSV exports. Consumed by IucnSynonymService, CommonNameAggregateCommand.
// Created incrementally by IucnApiCacheTaxa/AssessmentsCommands.

namespace BeastieBot3.Iucn;

internal sealed class IucnApiCacheStore : HttpCacheSqliteStore {
    // Retry delay for a permanent failure (a 404): far enough out that GetFailedEntityIds and the
    // ShouldDownload checks never re-queue it. --force still re-requests.
    public static readonly TimeSpan PermanentRetryDelay = TimeSpan.FromDays(3650);

    private IucnApiCacheStore(SqliteConnection connection) : base(connection) {
    }

    public static IucnApiCacheStore Open(string databasePath) {
        var connection = OpenConnection(databasePath);
        var store = new IucnApiCacheStore(connection);
        store.EnsureImportSchema();
        store.EnsureSchema();
        return store;
    }

    /// <summary>Test/advanced seam: build the store over a caller-owned (e.g. <c>:memory:</c>) connection.</summary>
    internal static IucnApiCacheStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new IucnApiCacheStore(connection);
        store.EnsureImportSchema();
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = @"
    CREATE TABLE IF NOT EXISTS taxa (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_sis_id INTEGER NOT NULL UNIQUE,
    import_id INTEGER NOT NULL REFERENCES http_request_log(id) ON DELETE RESTRICT,
    downloaded_at TEXT NOT NULL,
    json TEXT NOT NULL,
    has_latest_flag_in_assessments INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS taxa_lookup (
    sis_id INTEGER PRIMARY KEY,
    taxa_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
    root_sis_id INTEGER NOT NULL,
    scope TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_taxa_lookup_taxa_id ON taxa_lookup(taxa_id);
CREATE INDEX IF NOT EXISTS idx_taxa_downloaded_at ON taxa(downloaded_at);
CREATE TABLE IF NOT EXISTS taxa_assessment_backlog (
    assessment_id INTEGER PRIMARY KEY,
    taxa_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
    root_sis_id INTEGER NOT NULL,
    sis_id INTEGER NOT NULL,
    latest INTEGER NOT NULL,
    year_published INTEGER,
    queued_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_assessment_backlog_latest ON taxa_assessment_backlog(latest DESC, year_published DESC);
CREATE INDEX IF NOT EXISTS idx_assessment_backlog_taxa_latest ON taxa_assessment_backlog(taxa_id, latest);
CREATE TABLE IF NOT EXISTS assessments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assessment_id INTEGER NOT NULL UNIQUE,
    sis_id INTEGER NOT NULL,
    import_id INTEGER NOT NULL REFERENCES http_request_log(id) ON DELETE RESTRICT,
    downloaded_at TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS failed_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_status INTEGER,
    last_attempt_at TEXT,
    next_attempt_after TEXT,
    UNIQUE(endpoint, entity_id)
);
";
        command.ExecuteNonQuery();

        MigrateHasLatestFlag();
    }

    private void MigrateHasLatestFlag() {
        using var checkNew = _connection.CreateCommand();
        checkNew.CommandText = "SELECT COUNT(*) FROM pragma_table_info('taxa') WHERE name='has_latest_flag_in_assessments'";
        var newExists = (long)(checkNew.ExecuteScalar() ?? 0L) > 0;

        if (!newExists) {
            // Check whether the old column name (has_current_assessment) exists.
            using var checkOld = _connection.CreateCommand();
            checkOld.CommandText = "SELECT COUNT(*) FROM pragma_table_info('taxa') WHERE name='has_current_assessment'";
            var oldExists = (long)(checkOld.ExecuteScalar() ?? 0L) > 0;

            if (oldExists) {
                using var rename = _connection.CreateCommand();
                rename.CommandText = "ALTER TABLE taxa RENAME COLUMN has_current_assessment TO has_latest_flag_in_assessments";
                rename.ExecuteNonQuery();
            }
            else {
                using var alter = _connection.CreateCommand();
                alter.CommandText = "ALTER TABLE taxa ADD COLUMN has_latest_flag_in_assessments INTEGER NOT NULL DEFAULT 1";
                alter.ExecuteNonQuery();

                using var backfill = _connection.CreateCommand();
                backfill.CommandText = @"UPDATE taxa SET has_latest_flag_in_assessments = (
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM taxa_assessment_backlog b WHERE b.taxa_id = taxa.id AND b.latest = 1
    ) THEN 1 ELSE 0 END
)";
                backfill.ExecuteNonQuery();
            }
        }

        // Clean up old index name, ensure new index exists.
        using var dropOld = _connection.CreateCommand();
        dropOld.CommandText = "DROP INDEX IF EXISTS idx_taxa_has_current_assessment";
        dropOld.ExecuteNonQuery();

        using var idx = _connection.CreateCommand();
        idx.CommandText = "CREATE INDEX IF NOT EXISTS idx_taxa_has_latest_flag ON taxa(has_latest_flag_in_assessments)";
        idx.ExecuteNonQuery();
    }

    public DateTime? GetTaxaDownloadedAt(long sisId) {
        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT t.downloaded_at FROM taxa t
JOIN taxa_lookup l ON l.taxa_id = t.id
WHERE l.sis_id = @sisId LIMIT 1";
        command.Parameters.AddWithValue("@sisId", sisId);
        var result = command.ExecuteScalar() as string;
        return DateTime.TryParse(result, out var parsed) ? parsed : null;
    }

    /// <summary>
    /// When was the taxon with this <c>root_sis_id</c> fetched as its OWN record? Unlike
    /// <see cref="GetTaxaDownloadedAt"/> (which resolves via <c>taxa_lookup</c> and so reports an
    /// infrarank sis_id as "downloaded" via its parent species), this checks the taxon's own row —
    /// the correct "have we fetched /taxa/sis/{id} for this taxon itself" test for the infrarank phase.
    /// </summary>
    public DateTime? GetTaxaDownloadedAtByRoot(long rootSisId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT downloaded_at FROM taxa WHERE root_sis_id=@root LIMIT 1";
        command.Parameters.AddWithValue("@root", rootSisId);
        var result = command.ExecuteScalar() as string;
        return DateTime.TryParse(result, out var parsed) ? parsed : null;
    }

    public DateTime? GetAssessmentDownloadedAt(long assessmentId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT downloaded_at FROM assessments WHERE assessment_id=@id LIMIT 1";
        command.Parameters.AddWithValue("@id", assessmentId);
        var result = command.ExecuteScalar() as string;
        return DateTime.TryParse(result, out var parsed) ? parsed : null;
    }

    public long UpsertTaxa(long rootSisId, long importId, string json, DateTime downloadedAt) {
        using var tx = _connection.BeginTransaction();

        long taxaId;
        using (var command = _connection.CreateCommand()) {
            command.Transaction = tx;
            command.CommandText = "SELECT id FROM taxa WHERE root_sis_id=@root LIMIT 1";
            command.Parameters.AddWithValue("@root", rootSisId);
            var existing = command.ExecuteScalar();
            if (existing is long id) {
                taxaId = id;
                command.CommandText = "UPDATE taxa SET import_id=@import, downloaded_at=@downloaded, json=@json WHERE id=@id";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@import", importId);
                command.Parameters.AddWithValue("@downloaded", downloadedAt.ToString("O"));
                command.Parameters.AddWithValue("@json", json);
                command.Parameters.AddWithValue("@id", taxaId);
                command.ExecuteNonQuery();
            }
            else {
                command.CommandText = "INSERT INTO taxa(root_sis_id, import_id, downloaded_at, json) VALUES (@root, @import, @downloaded, @json); SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@root", rootSisId);
                command.Parameters.AddWithValue("@import", importId);
                command.Parameters.AddWithValue("@downloaded", downloadedAt.ToString("O"));
                command.Parameters.AddWithValue("@json", json);
                taxaId = (long)(command.ExecuteScalar() ?? 0L);
            }
        }

        tx.Commit();
        return taxaId;
    }

    public void ReplaceTaxaLookups(long taxaId, IEnumerable<TaxaLookupRow> mappings) {
        using var tx = _connection.BeginTransaction();

        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM taxa_lookup WHERE taxa_id=@taxaId";
            delete.Parameters.AddWithValue("@taxaId", taxaId);
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR REPLACE INTO taxa_lookup(sis_id, taxa_id, root_sis_id, scope) VALUES (@sis,@taxa,@root,@scope)";
            var sisParam = insert.Parameters.Add("@sis", SqliteType.Integer);
            var taxaParam = insert.Parameters.Add("@taxa", SqliteType.Integer);
            var rootParam = insert.Parameters.Add("@root", SqliteType.Integer);
            var scopeParam = insert.Parameters.Add("@scope", SqliteType.Text);

            foreach (var mapping in mappings) {
                sisParam.Value = mapping.SisId;
                taxaParam.Value = taxaId;
                rootParam.Value = mapping.RootSisId;
                scopeParam.Value = mapping.Scope;
                insert.ExecuteNonQuery();
            }
        }

        tx.Commit();
    }

    public long UpsertAssessment(long assessmentId, long sisId, long importId, string json, DateTime downloadedAt) {
        using var tx = _connection.BeginTransaction();

        long recordId;
        using (var command = _connection.CreateCommand()) {
            command.Transaction = tx;
            command.CommandText = "SELECT id FROM assessments WHERE assessment_id=@assessment LIMIT 1";
            command.Parameters.AddWithValue("@assessment", assessmentId);
            var existing = command.ExecuteScalar();
            if (existing is long id) {
                recordId = id;
                command.CommandText = "UPDATE assessments SET sis_id=@sis, import_id=@import, downloaded_at=@downloaded, json=@json WHERE id=@id";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@sis", sisId);
                command.Parameters.AddWithValue("@import", importId);
                command.Parameters.AddWithValue("@downloaded", downloadedAt.ToString("O"));
                command.Parameters.AddWithValue("@json", json);
                command.Parameters.AddWithValue("@id", recordId);
                command.ExecuteNonQuery();
            }
            else {
                command.CommandText = "INSERT INTO assessments(assessment_id, sis_id, import_id, downloaded_at, json) VALUES (@assessment, @sis, @import, @downloaded, @json); SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@assessment", assessmentId);
                command.Parameters.AddWithValue("@sis", sisId);
                command.Parameters.AddWithValue("@import", importId);
                command.Parameters.AddWithValue("@downloaded", downloadedAt.ToString("O"));
                command.Parameters.AddWithValue("@json", json);
                recordId = (long)(command.ExecuteScalar() ?? 0L);
            }
        }

        tx.Commit();
        return recordId;
    }

    public IReadOnlyList<AssessmentQueueRow> GetAssessmentBacklogOrdered() {
        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT b.assessment_id,
       b.sis_id,
       b.root_sis_id,
       b.latest,
       b.year_published,
       a.downloaded_at
FROM taxa_assessment_backlog b
LEFT JOIN assessments a ON a.assessment_id = b.assessment_id
ORDER BY b.latest DESC, IFNULL(b.year_published, 0) DESC, b.assessment_id DESC";

        var list = new List<AssessmentQueueRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var assessmentId = reader.GetInt64(0);
            var sisId = reader.GetInt64(1);
            var rootSisId = reader.GetInt64(2);
            var latest = reader.GetInt64(3) != 0;
            int? year = reader.IsDBNull(4) ? null : reader.GetInt32(4);
            var downloaded = reader.IsDBNull(5) ? null : reader.GetString(5);
            DateTime? downloadedAt = null;
            if (!string.IsNullOrEmpty(downloaded) && DateTime.TryParse(downloaded, out var parsed)) {
                downloadedAt = parsed;
            }

            list.Add(new AssessmentQueueRow(assessmentId, sisId, rootSisId, latest, year, downloadedAt));
        }

        return list;
    }

    public void ReplaceAssessmentBacklog(long taxaId, long rootSisId, IReadOnlyList<IucnAssessmentHeader> assessments) {
        using var tx = _connection.BeginTransaction();

        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM taxa_assessment_backlog WHERE taxa_id=@taxaId";
            delete.Parameters.AddWithValue("@taxaId", taxaId);
            delete.ExecuteNonQuery();
        }

        var hasCurrent = false;
        if (assessments.Count > 0) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR REPLACE INTO taxa_assessment_backlog(assessment_id, taxa_id, root_sis_id, sis_id, latest, year_published, queued_at) VALUES (@assessment,@taxa,@root,@sis,@latest,@year,@queued)";
            var assessmentParam = insert.Parameters.Add("@assessment", SqliteType.Integer);
            var taxaParam = insert.Parameters.Add("@taxa", SqliteType.Integer);
            var rootParam = insert.Parameters.Add("@root", SqliteType.Integer);
            var sisParam = insert.Parameters.Add("@sis", SqliteType.Integer);
            var latestParam = insert.Parameters.Add("@latest", SqliteType.Integer);
            var yearParam = insert.Parameters.Add("@year", SqliteType.Integer);
            var queuedParam = insert.Parameters.Add("@queued", SqliteType.Text);

            foreach (var assessment in assessments) {
                assessmentParam.Value = assessment.AssessmentId;
                taxaParam.Value = taxaId;
                rootParam.Value = rootSisId;
                sisParam.Value = assessment.SisId;
                latestParam.Value = assessment.Latest ? 1 : 0;
                if (assessment.YearPublished.HasValue) {
                    yearParam.Value = assessment.YearPublished.Value;
                }
                else {
                    yearParam.Value = DBNull.Value;
                }

                queuedParam.Value = DateTime.UtcNow.ToString("O");
                insert.ExecuteNonQuery();

                if (assessment.Latest) {
                    hasCurrent = true;
                }
            }
        }

        // Update the denormalized flag on the taxa row.
        using (var update = _connection.CreateCommand()) {
            update.Transaction = tx;
            update.CommandText = "UPDATE taxa SET has_latest_flag_in_assessments=@flag WHERE id=@taxaId";
            update.Parameters.AddWithValue("@flag", hasCurrent ? 1 : 0);
            update.Parameters.AddWithValue("@taxaId", taxaId);
            update.ExecuteNonQuery();
        }

        tx.Commit();
    }

    /// <summary>
    /// Infraspecific (subspecies/variety) SIS ids discovered from cached species' <c>taxon.infrarank_taxa</c>
    /// (scope = "infrarank" in <c>taxa_lookup</c>), paired with their parent species' root SIS id. These have
    /// a taxon record listed but their own assessments are NOT in the parent's payload — each needs its own
    /// <c>/taxa/sis/{id}</c> fetch (see discover-by-family's infrarank phase) to queue its assessments.
    /// </summary>
    public IReadOnlyList<(long SisId, long RootSisId)> GetInfrarankSisIds() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT sis_id, root_sis_id FROM taxa_lookup WHERE scope='infrarank'";
        var list = new List<(long, long)>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add((reader.GetInt64(0), reader.GetInt64(1)));
        }
        return list;
    }

    /// <summary>
    /// True if this entity previously failed with a permanent status (404/410) — i.e. it has no
    /// standalone record and should not be re-requested (removed taxon / unassessed infraspecific
    /// taxon). Callers gate downloads on this so a tombstoned id isn't re-probed every run.
    /// </summary>
    public bool HasPermanentFailure(string endpoint, long entityId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM failed_requests WHERE endpoint=@endpoint AND entity_id=@entity AND last_status IN (404, 410) LIMIT 1";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@entity", entityId.ToString());
        return command.ExecuteScalar() is not null;
    }

    public IReadOnlyList<long> GetFailedEntityIds(string endpoint) {
        using var command = _connection.CreateCommand();
        // Skip permanent failures (404/410 — no standalone record): retrying never helps, and they'd
        // otherwise be re-requested every run. HasPermanentFailure gates the main discovery path; this
        // gates the failed-retry path (--failed-only / the failed-first queue).
        command.CommandText = "SELECT entity_id FROM failed_requests WHERE endpoint=@endpoint AND (last_status IS NULL OR last_status NOT IN (404, 410)) AND (next_attempt_after IS NULL OR next_attempt_after <= @now)";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        var list = new List<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (long.TryParse(reader.GetString(0), out var id)) {
                list.Add(id);
            }
        }
        return list;
    }

    /// <summary>
    /// Entity ids that should NOT be retried this run: a failure whose <c>next_attempt_after</c> is
    /// still in the future (backed off) or that is permanent (404/410). The download queue excludes
    /// these so a persistently-broken entity is skipped instantly instead of re-attempted every run.
    /// </summary>
    public IReadOnlyList<long> GetSuppressedEntityIds(string endpoint) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT entity_id FROM failed_requests WHERE endpoint=@endpoint AND ((next_attempt_after IS NOT NULL AND next_attempt_after > @now) OR last_status IN (404, 410))";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        var list = new List<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (long.TryParse(reader.GetString(0), out var id)) {
                list.Add(id);
            }
        }
        return list;
    }

    public void RecordFailedRequest(string endpoint, long entityId, string error, int? statusCode, TimeSpan? retryDelay = null) {
        // Escalate the retry back-off with each prior failure so a persistently-broken entity
        // (e.g. an assessment IUCN's API consistently 500s on) stops being retried at the front of
        // every run. An explicit retryDelay (e.g. the permanent 404 tombstone) overrides this.
        var priorAttempts = 0;
        using (var read = _connection.CreateCommand()) {
            read.CommandText = "SELECT attempt_count FROM failed_requests WHERE endpoint=@endpoint AND entity_id=@entity";
            read.Parameters.AddWithValue("@endpoint", endpoint);
            read.Parameters.AddWithValue("@entity", entityId.ToString());
            priorAttempts = Convert.ToInt32(read.ExecuteScalar() ?? 0L);
        }
        var attempts = priorAttempts + 1;
        var delay = retryDelay ?? EscalatingRetryDelay(attempts);

        using var command = _connection.CreateCommand();
        command.CommandText = @"INSERT INTO failed_requests(endpoint, entity_id, attempt_count, last_error, last_status, last_attempt_at, next_attempt_after)
VALUES(@endpoint,@entity,@attempt,@error,@status,@attempted,@next)
ON CONFLICT(endpoint, entity_id) DO UPDATE SET
    attempt_count = @attempt,
    last_error = excluded.last_error,
    last_status = excluded.last_status,
    last_attempt_at = excluded.last_attempt_at,
    next_attempt_after = excluded.next_attempt_after";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@entity", entityId.ToString());
        command.Parameters.AddWithValue("@attempt", attempts);
        command.Parameters.AddWithValue("@error", error);
        command.Parameters.AddWithValue("@status", statusCode.HasValue ? statusCode : DBNull.Value);
        var attemptedAt = DateTime.UtcNow;
        command.Parameters.AddWithValue("@attempted", attemptedAt.ToString("O"));
        command.Parameters.AddWithValue("@next", attemptedAt.Add(delay).ToString("O"));
        command.ExecuteNonQuery();
    }

    // Exponential back-off capped at ~3 days: 5min, 10, 20, 40, 80, … so an entity that keeps
    // failing is retried ever less often instead of at the start of every run.
    private static TimeSpan EscalatingRetryDelay(int attempts) {
        var minutes = 5.0 * Math.Pow(2, Math.Min(Math.Max(attempts, 1) - 1, 10));
        return TimeSpan.FromMinutes(Math.Min(minutes, TimeSpan.FromDays(3).TotalMinutes));
    }

    public void ClearFailedRequest(string endpoint, long entityId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM failed_requests WHERE endpoint=@endpoint AND entity_id=@entity";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@entity", entityId.ToString());
        command.ExecuteNonQuery();
    }
}

internal sealed record TaxaLookupRow(long SisId, long RootSisId, string Scope);

internal sealed record AssessmentQueueRow(long AssessmentId, long SisId, long RootSisId, bool Latest, int? YearPublished, DateTime? DownloadedAt);
