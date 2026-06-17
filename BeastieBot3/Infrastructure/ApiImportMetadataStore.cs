using System;
using Microsoft.Data.Sqlite;

// Shared component embedded in each cache database (IucnApiCacheStore, WikidataCacheStore,
// WikipediaCacheStore). Creates an http_request_log table that logs each HTTP request with
// timing, status codes, and payload sizes. Used for debugging, progress tracking, and
// identifying failed requests that need retry. (Named http_request_log to avoid colliding
// with the unrelated CSV/dataset import_metadata table in the IUCN CSV/projection databases.)

namespace BeastieBot3.Infrastructure;

/// <summary>
/// Shared helper for tracking HTTP import executions inside cache databases.
/// </summary>
internal sealed class ApiImportMetadataStore {
    private readonly SqliteConnection _connection;

    public ApiImportMetadataStore(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public void EnsureSchema() {
        MigrateLegacyTableName();

        using var command = _connection.CreateCommand();
        command.CommandText = @"
CREATE TABLE IF NOT EXISTS http_request_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    duration_ms INTEGER,
    http_status INTEGER,
    payload_bytes INTEGER,
    error TEXT
);";
        command.ExecuteNonQuery();
    }

    // This HTTP-request log used to be called import_metadata. Rename existing cache DBs in
    // place: SQLite's ALTER TABLE ... RENAME also rewrites the REFERENCES in the child tables
    // (taxa/assessments/wiki_pages/...), so the FK targets stay consistent. Runs only in cache
    // DBs (where this store is embedded) -- the CSV/projection import_metadata lives in other files.
    private void MigrateLegacyTableName() {
        long hasOld = 0, hasNew = 0;
        using (var probe = _connection.CreateCommand()) {
            probe.CommandText = @"SELECT
    (SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='import_metadata'),
    (SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='http_request_log');";
            using var reader = probe.ExecuteReader();
            if (reader.Read()) {
                hasOld = reader.GetInt64(0);
                hasNew = reader.GetInt64(1);
            }
        }

        if (hasOld > 0 && hasNew == 0) {
            using var rename = _connection.CreateCommand();
            rename.CommandText = "ALTER TABLE import_metadata RENAME TO http_request_log;";
            rename.ExecuteNonQuery();
        }
    }

    public long BeginImport(string url) {
        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT INTO http_request_log(url, started_at) VALUES (@url, @started_at); SELECT last_insert_rowid();";
        command.Parameters.AddWithValue("@url", url);
        command.Parameters.AddWithValue("@started_at", DateTime.UtcNow.ToString("O"));
        return (long)(command.ExecuteScalar() ?? 0L);
    }

    public void CompleteImportSuccess(long importId, int httpStatus, long payloadBytes, TimeSpan duration) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE http_request_log SET ended_at=@ended, duration_ms=@duration, http_status=@status, payload_bytes=@bytes WHERE id=@id";
        command.Parameters.AddWithValue("@ended", DateTime.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("@duration", (long)duration.TotalMilliseconds);
        command.Parameters.AddWithValue("@status", httpStatus);
        command.Parameters.AddWithValue("@bytes", payloadBytes);
        command.Parameters.AddWithValue("@id", importId);
        command.ExecuteNonQuery();
    }

    public void CompleteImportFailure(long importId, string errorMessage, int? statusCode, TimeSpan duration) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE http_request_log SET duration_ms=@duration, http_status=@status, error=@error WHERE id=@id";
        command.Parameters.AddWithValue("@duration", (long)duration.TotalMilliseconds);
        command.Parameters.AddWithValue("@status", statusCode.HasValue ? statusCode : DBNull.Value);
        command.Parameters.AddWithValue("@error", errorMessage);
        command.Parameters.AddWithValue("@id", importId);
        command.ExecuteNonQuery();
    }
}
