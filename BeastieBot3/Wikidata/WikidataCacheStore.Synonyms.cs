using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Data.Sqlite;

// Queues the items that cached items name as taxon synonym (P1420), so `wikidata cache-entities`
// downloads them and their taxon names become synonyms on the species site.

namespace BeastieBot3.Wikidata;

internal sealed record SynonymQueueResult(int ItemsWithSynonyms, int SynonymItems, int AlreadyQueued, int Queued);

internal sealed partial class WikidataCacheStore {
    /// Adds every item that a downloaded item names as taxon synonym (P1420) to the queue, unless
    /// it is already there. Reads every downloaded item whose JSON mentions P1420.
    public SynonymQueueResult QueueTaxonSynonymItems(CancellationToken cancellationToken) {
        var targets = new HashSet<long>();
        var withSynonyms = 0;
        using (var scan = _connection.CreateCommand()) {
            scan.CommandText = "SELECT json FROM wikidata_entities WHERE json_downloaded = 1 AND json LIKE '%\"P1420\"%'";
            scan.CommandTimeout = 0;
            using var reader = scan.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (reader.IsDBNull(0)) {
                    continue;
                }
                var items = WikidataTaxonSynonyms.ItemsIn(reader.GetString(0));
                if (items.Count > 0) {
                    withSynonyms++;
                    targets.UnionWith(items);
                }
            }
        }

        var now = DateTime.UtcNow.ToString("O");
        var queued = 0;
        using var tx = _connection.BeginTransaction();
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT OR IGNORE INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at, has_p141, has_p627)
            VALUES (@id, @entity, @now, @now, 0, 0)
            """;
        var id = insert.Parameters.Add("@id", SqliteType.Integer);
        var entity = insert.Parameters.Add("@entity", SqliteType.Text);
        insert.Parameters.AddWithValue("@now", now);
        foreach (var target in targets) {
            id.Value = target;
            entity.Value = "Q" + target;
            queued += insert.ExecuteNonQuery();
        }
        tx.Commit();
        return new SynonymQueueResult(withSynonyms, targets.Count, targets.Count - queued, queued);
    }
}
