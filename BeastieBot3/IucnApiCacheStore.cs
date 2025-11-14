using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class IucnApiCacheStore : IDisposable {
    private readonly SqliteConnection _connection;

    private IucnApiCacheStore(SqliteConnection connection) {
        _connection = connection;
    }

    public static IucnApiCacheStore Open(string databasePath) {
        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA journal_mode = WAL;";
            pragma.ExecuteNonQuery();
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        var store = new IucnApiCacheStore(connection);
        store.EnsureSchema();
        return store;
    }

    public void Dispose() => _connection.Dispose();

    private void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = @"
CREATE TABLE IF NOT EXISTS import_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    duration_ms INTEGER,
    http_status INTEGER,
    payload_bytes INTEGER,
    error TEXT
);
CREATE TABLE IF NOT EXISTS taxa (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_sis_id INTEGER NOT NULL UNIQUE,
    import_id INTEGER NOT NULL REFERENCES import_metadata(id) ON DELETE RESTRICT,
    downloaded_at TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS taxa_lookup (
    sis_id INTEGER PRIMARY KEY,
    taxa_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
    root_sis_id INTEGER NOT NULL,
    scope TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_taxa_lookup_taxa_id ON taxa_lookup(taxa_id);
CREATE INDEX IF NOT EXISTS idx_taxa_downloaded_at ON taxa(downloaded_at);
CREATE TABLE IF NOT EXISTS assessments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assessment_id INTEGER NOT NULL UNIQUE,
    sis_id INTEGER NOT NULL,
    import_id INTEGER NOT NULL REFERENCES import_metadata(id) ON DELETE RESTRICT,
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
    }

    public long BeginImport(string url) {
        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT INTO import_metadata(url, started_at) VALUES (@url, @started_at); SELECT last_insert_rowid();";
        command.Parameters.AddWithValue("@url", url);
        command.Parameters.AddWithValue("@started_at", DateTime.UtcNow.ToString("O"));
        return (long)(command.ExecuteScalar() ?? 0L);
    }

    public void CompleteImportSuccess(long importId, int httpStatus, long payloadBytes, TimeSpan duration) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE import_metadata SET ended_at=@ended, duration_ms=@duration, http_status=@status, payload_bytes=@bytes WHERE id=@id";
        command.Parameters.AddWithValue("@ended", DateTime.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("@duration", (long)duration.TotalMilliseconds);
        command.Parameters.AddWithValue("@status", httpStatus);
        command.Parameters.AddWithValue("@bytes", payloadBytes);
        command.Parameters.AddWithValue("@id", importId);
        command.ExecuteNonQuery();
    }

    public void CompleteImportFailure(long importId, string errorMessage, int? statusCode, TimeSpan duration) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE import_metadata SET duration_ms=@duration, http_status=@status, error=@error WHERE id=@id";
        command.Parameters.AddWithValue("@duration", (long)duration.TotalMilliseconds);
        command.Parameters.AddWithValue("@status", statusCode.HasValue ? statusCode : DBNull.Value);
        command.Parameters.AddWithValue("@error", errorMessage);
        command.Parameters.AddWithValue("@id", importId);
        command.ExecuteNonQuery();
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

    public IReadOnlyList<long> GetFailedEntityIds(string endpoint) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT entity_id FROM failed_requests WHERE endpoint=@endpoint AND (next_attempt_after IS NULL OR next_attempt_after <= @now)";
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
        using var command = _connection.CreateCommand();
        command.CommandText = @"INSERT INTO failed_requests(endpoint, entity_id, attempt_count, last_error, last_status, last_attempt_at, next_attempt_after)
VALUES(@endpoint,@entity,@attempt,@error,@status,@attempted,@next)
ON CONFLICT(endpoint, entity_id) DO UPDATE SET
    attempt_count = failed_requests.attempt_count + 1,
    last_error = excluded.last_error,
    last_status = excluded.last_status,
    last_attempt_at = excluded.last_attempt_at,
    next_attempt_after = excluded.next_attempt_after";
        command.Parameters.AddWithValue("@endpoint", endpoint);
        command.Parameters.AddWithValue("@entity", entityId.ToString());
        command.Parameters.AddWithValue("@attempt", 1);
        command.Parameters.AddWithValue("@error", error);
        command.Parameters.AddWithValue("@status", statusCode.HasValue ? statusCode : DBNull.Value);
        var attemptedAt = DateTime.UtcNow;
        command.Parameters.AddWithValue("@attempted", attemptedAt.ToString("O"));
        var next = retryDelay.HasValue ? attemptedAt.Add(retryDelay.Value) : attemptedAt.AddMinutes(5);
        command.Parameters.AddWithValue("@next", next.ToString("O"));
        command.ExecuteNonQuery();
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
