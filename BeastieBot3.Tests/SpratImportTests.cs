using System.IO;
using System.Text;
using System.Threading;
using BeastieBot3.Infrastructure;
using BeastieBot3.Sprat;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins the SPRAT importer: the OpenFromConnection :memory: seam, and a round-trip of a tiny
// two-row-header CSV through SpratImporter — proving the category-grouping row is discarded, known
// headers map to the canonical SpratColumns names, the duplicate "Listed Name" columns de-dup, and
// rows land with their values (empty -> NULL).
public class SpratImportTests {
    private static long Count(SqliteConnection c, string sql) {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    private static string? Scalar(SqliteConnection c, string sql) {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar() as string;
    }

    private static bool HasColumn(SqliteConnection c, string table, string column) =>
        Count(c, $"SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = '{column}';") == 1;

    private static IAnsiConsole SilentConsole() =>
        AnsiConsole.Create(new AnsiConsoleSettings {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
            Out = new AnsiConsoleOutput(new StringWriter()),
        });

    [Fact]
    public void OpenFromConnection_CreatesImportMetadataWithForeignKeys() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();

        var store = SpratStore.OpenFromConnection(conn);
        Assert.NotNull(store);

        Assert.Equal("import_metadata",
            Scalar(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name='import_metadata';"));

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA foreign_keys;";
        Assert.Equal(1L, (long)cmd.ExecuteScalar()!);
    }

    [Fact]
    public void Run_TwoRowHeader_MapsCanonicalColumnsAndImportsRows() {
        // Row 1 = category grouping (discarded). Row 2 = real column names. Two duplicate "Listed
        // Name" columns exercise de-duplication; an unknown "Marine Status" column is auto-sanitised.
        var csv = string.Join("\n", new[] {
            "\"Identity\",\"Identity\",\"EPBC\",\"EPBC\",\"State\",\"State\",\"Taxonomy\",\"Taxonomy\",\"Other\"",
            "\"Taxon ID\",\"Scientific Name\",\"EPBC Threat Status\",\"Listed Name\",\"NSW TSC Act and FM Act\",\"Listed Name\",\"Kingdom\",\"Class\",\"Marine Status\"",
            "\"66785\",\"Potorous gilbertii\",\"Critically Endangered\",\"Potorous gilbertii\",\"\",\"\",\"Animalia\",\"Mammalia\",\"Listed\"",
            "\"3924\",\"Acacia gunnii\",\"\",\"\",\"Endangered\",\"Acacia gunnii\",\"Plantae\",\"Magnoliopsida\",\"\"",
        });

        var path = Path.Combine(Path.GetTempPath(), $"sprat_test_{System.Guid.NewGuid():N}.csv");
        File.WriteAllText(path, csv, new UTF8Encoding(false));
        try {
            using var conn = new SqliteConnection("Data Source=:memory:");
            conn.Open();
            using var store = SpratStore.OpenFromConnection(conn);

            new SpratImporter(SilentConsole(), store.Connection, path, "test-release").Run(CancellationToken.None);

            // Two data rows imported (the category-grouping row was discarded).
            Assert.Equal(2, Count(conn, $"SELECT COUNT(*) FROM {SpratColumns.Table};"));

            // Known headers mapped to canonical names; the duplicate "Listed Name" de-duped.
            Assert.True(HasColumn(conn, SpratColumns.Table, SpratColumns.SpratTaxonId));
            Assert.True(HasColumn(conn, SpratColumns.Table, SpratColumns.EpbcStatus));
            Assert.True(HasColumn(conn, SpratColumns.Table, SpratColumns.NswStatus));
            Assert.True(HasColumn(conn, SpratColumns.Table, SpratColumns.ClassName));
            Assert.True(HasColumn(conn, SpratColumns.Table, "Listed_Name"));
            Assert.True(HasColumn(conn, SpratColumns.Table, "Listed_Name_2"));

            // Values land on the right canonical columns; empty cells become NULL.
            Assert.Equal("Critically Endangered",
                Scalar(conn, $"SELECT {SpratColumns.EpbcStatus} FROM {SpratColumns.Table} WHERE {SpratColumns.ScientificName} = 'Potorous gilbertii';"));
            Assert.Equal("Endangered",
                Scalar(conn, $"SELECT {SpratColumns.NswStatus} FROM {SpratColumns.Table} WHERE {SpratColumns.ScientificName} = 'Acacia gunnii';"));
            Assert.Equal(1,
                Count(conn, $"SELECT COUNT(*) FROM {SpratColumns.Table} WHERE {SpratColumns.EpbcStatus} IS NULL;"));

            // A completed import is recorded.
            Assert.Equal(1, Count(conn, "SELECT COUNT(*) FROM import_metadata WHERE ended_at IS NOT NULL;"));
        } finally {
            File.Delete(path);
        }
    }
}
