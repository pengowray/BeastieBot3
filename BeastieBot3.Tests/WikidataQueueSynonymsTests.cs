using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

public sealed class WikidataQueueSynonymsTests {
    private static void AddEntity(SqliteConnection connection, long id, string? json) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at, has_p141, has_p627, json_downloaded, json)
            VALUES (@id, @entity, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 0, 1, @downloaded, @json)
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@entity", "Q" + id);
        command.Parameters.AddWithValue("@downloaded", json is null ? 0 : 1);
        command.Parameters.AddWithValue("@json", (object?)json ?? DBNull.Value);
        command.ExecuteNonQuery();
    }

    private static string Synonyms(params long[] ids) =>
        "{\"entities\":{\"Q1\":{\"claims\":{\"P1420\":[" + string.Join(",", ids.Select(i =>
            $"{{\"rank\":\"normal\",\"mainsnak\":{{\"datavalue\":{{\"value\":{{\"numeric-id\":{i}}}}}}}}}")) + "]}}}}";

    [Fact]
    public void QueuesSynonymItemsNotAlreadyInTheCache() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = WikidataCacheStore.OpenFromConnection(connection);
        AddEntity(connection, 1, Synonyms(10, 11));
        AddEntity(connection, 2, Synonyms(11, 3));
        AddEntity(connection, 3, "{}");
        AddEntity(connection, 4, null);

        Assert.Equal(new SynonymQueueResult(2, 3, 1, 2), store.QueueTaxonSynonymItems(CancellationToken.None));
        Assert.Equal(new long[] { 10, 11 }, store.GetPendingEntities(10, null).Select(e => e.NumericId).Where(i => i >= 10).Order());
        // Run again: nothing new.
        Assert.Equal(0, store.QueueTaxonSynonymItems(CancellationToken.None).Queued);
    }
}
