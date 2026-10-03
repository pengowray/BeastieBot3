using System;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Col;

namespace BeastieBot3.Tests;

// The SQLite bundled with Microsoft.Data.Sqlite 10 is built without double-quoted string literals
// (SQLITE_DQS=0). A double-quoted name that is not a column is now an error, where older builds
// silently read it as the string 'name' and matched nothing. String values in SQL must use single
// quotes or parameters, and a double-quoted column name must be one the table has.
public class SqliteQuotingTests {
    [Fact]
    public void DoubleQuotedValueInAQuery_IsAnError() {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (a TEXT);");
        var ex = Assert.Throws<SqliteException>(() => Exec(conn, "SELECT a FROM t WHERE a = \"foo\";"));
        Assert.Contains("no such column", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DoubleQuotedMissingColumnInAnIndex_IsAnError() {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (a TEXT);");
        Assert.Throws<SqliteException>(() => Exec(conn, "CREATE INDEX idx_t_b ON t(\"b\");"));
    }

    [Fact]
    public void SingleQuotedValue_StillWorks() {
        using var conn = Open();
        Exec(conn, "CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('foo');");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t WHERE a = 'foo';";
        Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    // ColTaxonRepository's near-match lookups double-quote genericName / specificEpithet. A ColDP
    // without those columns still works: the select clause names them as aliases (the fallback
    // column, or NULL), and SQLite resolves a WHERE name to a result alias before giving up.
    [Fact]
    public void ColNearMatchLookups_WorkWithoutGenericNameOrSpecificEpithetColumns() {
        using var conn = Open();
        Exec(conn, """
            CREATE TABLE nameusage (ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, rank TEXT, kingdom TEXT, genus TEXT);
            INSERT INTO nameusage VALUES ('P_LEO', NULL, 'accepted', 'Panthera leo', 'species', 'Animalia', 'Panthera');
            """);
        var repo = new ColTaxonRepository(conn);

        var byGenus = repo.FindByGenericName("Panthera", CancellationToken.None);
        Assert.Equal("Panthera leo", Assert.Single(byGenus).ScientificName);

        Assert.Empty(repo.FindBySpecificEpithet("leo", CancellationToken.None));
        Assert.Empty(repo.FindByComponents("Panthera", "leo", null, CancellationToken.None));
        Assert.Empty(repo.FindByComponents("Panthera", "leo", "persica", CancellationToken.None));
    }

    private static SqliteConnection Open() {
        var conn = new SqliteConnection("Data Source=:memory:;Pooling=False");
        conn.Open();
        return conn;
    }

    private static void Exec(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
