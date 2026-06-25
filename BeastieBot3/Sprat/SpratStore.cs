using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;

// SQLite store for the Australian SPRAT (Species Profile and Threats Database) dataset, imported
// from the EPBC report CSV. Follows the project store pattern (private ctor + static Open factory +
// OpenFromConnection test seam). EnsureSchema only creates the import_metadata bookkeeping table —
// the wide sprat_species data table is built from the CSV header at import time by SpratImporter
// (via DelimitedTableImporter), exactly like the IUCN/CoL importers. Used by SpratImportCommand to
// write and by SpratListQueryService to read.

namespace BeastieBot3.Sprat;

internal sealed class SpratStore : SqliteStore {
    private SpratStore(SqliteConnection connection) : base(connection) {
    }

    /// <summary>The store's open connection — handed to <see cref="SpratImporter"/> for the data load.</summary>
    internal SqliteConnection Connection => _connection;

    public static SpratStore Open(string databasePath) {
        var connection = OpenConnection(databasePath);
        var store = new SpratStore(connection);
        store.EnsureSchema();
        return store;
    }

    /// <summary>Test/advanced seam: build a store over a caller-owned, already-open connection
    /// (e.g. a shared <c>:memory:</c> connection) so it can be exercised without a file.</summary>
    internal static SpratStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new SpratStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var cmd = _connection.CreateCommand();
        // import_metadata is the FK target DelimitedTableImporter.EnsureTable references. The column
        // name `redlist_version` is reused from the IUCN schema (CoL does the same); for SPRAT it
        // holds the report release label (the CSV file stem), not an IUCN Red List version.
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS import_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    redlist_version TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_import_metadata_filename ON import_metadata(filename);";
        cmd.ExecuteNonQuery();
    }
}
