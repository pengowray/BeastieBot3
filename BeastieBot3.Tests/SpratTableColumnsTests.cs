using BeastieBot3.Sprat;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// SpratTableColumns: the one rule SpratListQueryService and `site build-db` use to select a SPRAT
// column the report may lack.
public sealed class SpratTableColumnsTests {
    [Fact]
    public void Select_GivesTheQuotedColumn_OrNullWhenTheTableLacksIt() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var create = connection.CreateCommand()) {
            create.CommandText = "CREATE TABLE sprat_species (sprat_taxon_id TEXT, IUCN_Red_List_Listed_Names TEXT)";
            create.ExecuteNonQuery();
        }

        var columns = SpratTableColumns.Read(connection)!;

        Assert.True(columns.Has(SpratColumns.IucnListedName));
        Assert.True(columns.Has("IUCN_RED_LIST_LISTED_NAMES"));
        Assert.Equal("\"IUCN_Red_List_Listed_Names\"", columns.Select(SpratColumns.IucnListedName));
        Assert.Equal("NULL", columns.Select(SpratColumns.EpbcListedName));

        // The selection runs: the missing column reads as NULL.
        using var select = connection.CreateCommand();
        select.CommandText = $"SELECT {columns.Select(SpratColumns.EpbcListedName)} FROM {SpratTableColumns.Quote(SpratColumns.Table)}";
        Assert.Null(select.ExecuteScalar());
    }

    [Fact]
    public void Read_IsNullWithoutTheTable() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        Assert.Null(SpratTableColumns.Read(connection));
        Assert.Equal("NULL", SpratTableColumns.None.Select(SpratColumns.ScientificName));
    }
}
