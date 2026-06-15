using System;
using System.Collections.Generic;
using System.Threading;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the shared CSV/TSV table-load engine (R3) over :memory:, in both the IUCN config
// (mapped names, INTEGER typing, INSERT OR IGNORE, import_id index, legacy-table guard) and the
// CoL config (all-TEXT, plain INSERT, no import_id index).
public class DelimitedTableImporterTests {
    private static SqliteConnection Mem() {
        var c = new SqliteConnection("Data Source=:memory:");
        c.Open();
        return c;
    }

    private static (Func<bool> Read, Func<int, string?> Get) Rows(IReadOnlyList<string?[]> rows) {
        var idx = -1;
        return (() => ++idx < rows.Count, i => rows[idx][i]);
    }

    private static string? Type(SqliteConnection c, string table, string column) {
        using var cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT type FROM pragma_table_info('{table}') WHERE name = '{column}';";
        return cmd.ExecuteScalar() as string;
    }

    private static long Count(SqliteConnection c, string sql) {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    // Microsoft.Data.Sqlite enables foreign_keys by default, so the import_id FK target must exist.
    private static void SeedImportMetadata(SqliteConnection c, params long[] ids) {
        using (var create = c.CreateCommand()) {
            create.CommandText = "CREATE TABLE IF NOT EXISTS import_metadata (id INTEGER PRIMARY KEY);";
            create.ExecuteNonQuery();
        }
        foreach (var id in ids) {
            using var ins = c.CreateCommand();
            ins.CommandText = "INSERT OR IGNORE INTO import_metadata(id) VALUES (@id);";
            ins.Parameters.AddWithValue("@id", id);
            ins.ExecuteNonQuery();
        }
    }

    [Fact]
    public void EnsureTable_IucnConfig_TypedColumnsImportIdAndIndex() {
        using var c = Mem();
        var cols = new[] {
            new DelimitedColumn("assessmentId", "INTEGER NOT NULL"),
            new DelimitedColumn("taxonId", "INTEGER NOT NULL"),
            new DelimitedColumn("scientificName", "TEXT"),
        };

        var set = DelimitedTableImporter.EnsureTable(c, "assessments", cols,
            indexImportIdColumn: true, requireImportIdOnExisting: true);

        Assert.Contains("import_id", set);
        Assert.Contains("assessmentId", set);
        Assert.Equal("INTEGER", Type(c, "assessments", "assessmentId"));
        Assert.Equal("TEXT", Type(c, "assessments", "scientificName"));
        Assert.Equal(1, Count(c, "SELECT \"notnull\" FROM pragma_table_info('assessments') WHERE name='assessmentId';"));
        Assert.Equal(1, Count(c,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_assessments_import_id';"));
    }

    [Fact]
    public void EnsureTable_ColConfig_NoImportIdIndex() {
        using var c = Mem();
        var cols = new[] { new DelimitedColumn("name", "TEXT"), new DelimitedColumn("language", "TEXT") };

        DelimitedTableImporter.EnsureTable(c, "vernacularname", cols,
            indexImportIdColumn: false, requireImportIdOnExisting: false);

        Assert.Equal(0, Count(c,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_vernacularname_import_id';"));
        Assert.Equal("TEXT", Type(c, "vernacularname", "name"));
    }

    [Fact]
    public void EnsureTable_AddsMissingColumnsOnSecondPass() {
        using var c = Mem();
        DelimitedTableImporter.EnsureTable(c, "t",
            new[] { new DelimitedColumn("a", "TEXT") }, false, false);

        var set = DelimitedTableImporter.EnsureTable(c, "t",
            new[] { new DelimitedColumn("a", "TEXT"), new DelimitedColumn("b", "TEXT") }, false, false);

        Assert.Contains("b", set);
        Assert.Equal("TEXT", Type(c, "t", "b"));
    }

    [Fact]
    public void EnsureTable_RequireImportId_ThrowsOnLegacyTable() {
        using var c = Mem();
        using (var cmd = c.CreateCommand()) {
            cmd.CommandText = "CREATE TABLE legacy (foo TEXT);";
            cmd.ExecuteNonQuery();
        }

        Assert.Throws<InvalidOperationException>(() => DelimitedTableImporter.EnsureTable(
            c, "legacy", new[] { new DelimitedColumn("foo", "TEXT") },
            indexImportIdColumn: false, requireImportIdOnExisting: true));
    }

    [Fact]
    public void BulkInsert_InsertsRows_EmptyBecomesNull() {
        using var c = Mem();
        SeedImportMetadata(c, 7);
        var cols = new[] { new DelimitedColumn("a", "TEXT"), new DelimitedColumn("b", "TEXT") };
        DelimitedTableImporter.EnsureTable(c, "t", cols, false, false);

        var (read, get) = Rows(new List<string?[]> { new[] { "x", "y" }, new[] { "z", "" } });
        var (inserted, duplicates) = DelimitedTableImporter.BulkInsert(
            c, null, "t", cols, importId: 7, read, get,
            insertOrIgnore: false, prepare: false, CancellationToken.None);

        Assert.Equal(2, inserted);
        Assert.Equal(0, duplicates);
        Assert.Equal(2, Count(c, "SELECT COUNT(*) FROM t WHERE import_id = 7;"));
        Assert.Equal(1, Count(c, "SELECT COUNT(*) FROM t WHERE b IS NULL;")); // the "" cell
    }

    [Fact]
    public void BulkInsert_OrIgnore_SkipsDuplicatesAgainstUniqueIndex() {
        using var c = Mem();
        SeedImportMetadata(c, 1);
        var cols = new[] { new DelimitedColumn("k", "INTEGER NOT NULL"), new DelimitedColumn("v", "TEXT") };
        DelimitedTableImporter.EnsureTable(c, "t", cols, false, false);
        using (var cmd = c.CreateCommand()) {
            cmd.CommandText = "CREATE UNIQUE INDEX uq ON t(k);";
            cmd.ExecuteNonQuery();
        }

        var (read, get) = Rows(new List<string?[]> {
            new[] { "1", "a" }, new[] { "2", "b" }, new[] { "1", "c" }, // third collides on k=1
        });
        var (inserted, duplicates) = DelimitedTableImporter.BulkInsert(
            c, null, "t", cols, importId: 1, read, get,
            insertOrIgnore: true, prepare: true, CancellationToken.None);

        Assert.Equal(2, inserted);
        Assert.Equal(1, duplicates);
        Assert.Equal("a", new SqliteCommand("SELECT v FROM t WHERE k = 1;", c).ExecuteScalar() as string); // first wins
    }

    [Fact]
    public void QuoteIdentifier_EscapesEmbeddedQuotes() {
        Assert.Equal("\"plain\"", DelimitedTableImporter.QuoteIdentifier("plain"));
        Assert.Equal("\"a\"\"b\"", DelimitedTableImporter.QuoteIdentifier("a\"b"));
    }
}
