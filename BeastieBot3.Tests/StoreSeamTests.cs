using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Proves the R5 injectable-connection seam: a store can be opened over a caller-owned in-memory
// SQLite connection (no file), runs its schema, and shares that connection. This is the hook the
// rest of the DB-backed test story hangs off.
public class StoreSeamTests {
    [Fact]
    public void OpenFromConnection_CreatesSchemaOnInMemoryConnection() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();

        // Opening the store runs EnsureSchema against the very connection we pass in.
        var store = CommonNameStore.OpenFromConnection(conn);
        Assert.NotNull(store);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='taxa';";
        Assert.Equal("taxa", cmd.ExecuteScalar() as string);

        cmd.CommandText = "PRAGMA foreign_keys;";
        Assert.Equal(1L, (long)cmd.ExecuteScalar()!);
    }
}
