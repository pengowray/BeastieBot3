using System;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

/// <summary>
/// Shared helper for tracking HTTP import executions inside cache databases.
/// </summary>
internal sealed class ApiImportMetadataStore {
    private readonly SqliteConnection _connection;

    public ApiImportMetadataStore(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public void EnsureSchema() {
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
);";
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
}
