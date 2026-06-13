using System;
using System.IO;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Infrastructure;

/// <summary>
/// Base for the project's SQLite stores. Owns the connection, the directory-create +
/// open + WAL/foreign-keys pragma sequence, and <see cref="IDisposable"/>. Subclasses keep
/// their private constructor + static <c>Open()</c> factory but call <see cref="OpenConnection"/>
/// instead of inlining the boilerplate, chain <c>: base(connection)</c>, and implement
/// <see cref="EnsureSchema"/>.
/// </summary>
internal abstract class SqliteStore : IDisposable {
    protected readonly SqliteConnection _connection;

    protected SqliteStore(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    /// Creates the parent directory (if any), opens a <c>ReadWriteCreate</c> connection, and
    /// sets <c>PRAGMA journal_mode=WAL</c> (plus <c>PRAGMA foreign_keys=ON</c> unless
    /// <paramref name="foreignKeys"/> is false). This is the byte-for-byte sequence every store
    /// previously inlined in its own <c>Open()</c>.
    /// </summary>
    internal static SqliteConnection OpenConnection(string databasePath, bool foreignKeys = true) {
        if (string.IsNullOrWhiteSpace(databasePath)) {
            throw new ArgumentException("Database path must be provided", nameof(databasePath));
        }

        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directory)) {
            Directory.CreateDirectory(directory);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };
        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        using var pragma = connection.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode = WAL;";
        pragma.ExecuteNonQuery();
        if (foreignKeys) {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }
        return connection;
    }

    /// <summary>Creates the store's tables/indexes/views. Called once from the factory after open.</summary>
    protected abstract void EnsureSchema();

    public virtual void Dispose() => _connection.Dispose();
}
