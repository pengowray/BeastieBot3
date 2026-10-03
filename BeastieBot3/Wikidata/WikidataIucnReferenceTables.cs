using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

// Two small tables of the Wikidata cache that `wikidata iucn-assessment-items` fills from SPARQL
// and `site build-db` reads, for the IUCN conservation status commands on the public site:
//
//   wikidata_iucn_red_list_editions      every edition of the IUCN Red List (P629 = Q32059), such as
//                                        Q136547248 "The IUCN Red List of Threatened Species 2025.2".
//                                        A P141 reference whose stated in (P248) is one of them, the
//                                        Red List itself or IUCN cites IUCN, even without an IUCN
//                                        taxon ID (P627) in it (about 35 statements in October 2026).
//   wikidata_deprecated_iucn_taxon_ids   every item with an IUCN taxon ID (P627) statement at
//                                        deprecated rank, and that id, when the item does not also
//                                        state the same id at another rank (11 items on 3 October
//                                        2026). The cache's wikidata_p627_values does not record rank,
//                                        and reading the rank from every item's JSON takes over a
//                                        minute, so the query service is asked instead.
//
// Each run replaces a table's rows only when its query succeeded. A cache from before October 2026
// has neither table; the readers return null for a missing table so the caller can say so.

namespace BeastieBot3.Wikidata;

internal static class WikidataIucnReferenceTables {
    public const string EditionsTable = "wikidata_iucn_red_list_editions";
    public const string DeprecatedTaxonIdsTable = "wikidata_deprecated_iucn_taxon_ids";

    public const string Ddl =
        """
CREATE TABLE IF NOT EXISTS wikidata_iucn_red_list_editions (
    qid TEXT PRIMARY KEY,
    fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wikidata_deprecated_iucn_taxon_ids (
    qid_numeric INTEGER NOT NULL,
    iucn_taxon_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (qid_numeric, iucn_taxon_id)
);
""";

    /// IUCN taxon ID (P627) statements at deprecated rank, on items that do not also state the same
    /// id at another rank. Main graph; 11 rows in about 2 seconds on 3 October 2026.
    public static string DeprecatedTaxonIdsQuery() =>
        """
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT ?item ?id WHERE {
  ?item p:P627 ?statement .
  ?statement ps:P627 ?id ;
             wikibase:rank wikibase:DeprecatedRank .
  FILTER NOT EXISTS {
    ?item p:P627 ?other .
    ?other ps:P627 ?id ;
           wikibase:rank ?rank .
    FILTER(?rank != wikibase:DeprecatedRank)
  }
}
""";

    /// (item, IUCN taxon id) pairs from the query's answer.
    public static IReadOnlyList<(long Item, string TaxonId)> ParseDeprecatedTaxonIds(string json) {
        var pairs = new List<(long, string)>();
        using var document = System.Text.Json.JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("results", out var results) || !results.TryGetProperty("bindings", out var bindings)) {
            return pairs;
        }
        foreach (var binding in bindings.EnumerateArray()) {
            if (!binding.TryGetProperty("item", out var item) || !binding.TryGetProperty("id", out var id)) {
                continue;
            }
            var qid = WikidataAssessmentItemQueries.ToItemId(item.GetProperty("value").GetString());
            var value = id.GetProperty("value").GetString()?.Trim();
            if (qid is null || string.IsNullOrEmpty(value)) {
                continue;
            }
            pairs.Add((long.Parse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture), value));
        }
        return pairs.Distinct().ToList();
    }

    public static bool Exists(SqliteConnection connection, string table) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name";
        command.Parameters.AddWithValue("@name", table);
        return command.ExecuteScalar() is not null;
    }

    /// The edition items ("Q136547248"); null when the cache has no such table.
    public static IReadOnlySet<string>? ReadEditions(SqliteConnection connection) {
        if (!Exists(connection, EditionsTable)) {
            return null;
        }
        var editions = new HashSet<string>(StringComparer.Ordinal);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT qid FROM {EditionsTable}";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            editions.Add(reader.GetString(0));
        }
        return editions;
    }

    /// (item numeric id, IUCN taxon id) pairs stated only at deprecated rank; null when the cache
    /// has no such table.
    public static IReadOnlySet<(long Item, string TaxonId)>? ReadDeprecatedTaxonIds(SqliteConnection connection) {
        if (!Exists(connection, DeprecatedTaxonIdsTable)) {
            return null;
        }
        var pairs = new HashSet<(long, string)>();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT qid_numeric, iucn_taxon_id FROM {DeprecatedTaxonIdsTable}";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            pairs.Add((reader.GetInt64(0), reader.GetString(1)));
        }
        return pairs;
    }
}

internal sealed partial class WikidataCacheStore {
    private void EnsureIucnReferenceSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = WikidataIucnReferenceTables.Ddl;
        command.ExecuteNonQuery();
    }

    /// Replaces the stored editions of the IUCN Red List.
    public void ReplaceRedListEditions(IEnumerable<string> qids, DateTime fetchedAtUtc) {
        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = $"DELETE FROM {WikidataIucnReferenceTables.EditionsTable}";
            delete.ExecuteNonQuery();
        }
        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = $"INSERT OR IGNORE INTO {WikidataIucnReferenceTables.EditionsTable} (qid, fetched_at) VALUES (@qid, @at)";
            var qid = insert.Parameters.Add("@qid", SqliteType.Text);
            insert.Parameters.AddWithValue("@at", FormatUtc(fetchedAtUtc));
            foreach (var value in qids) {
                qid.Value = value;
                insert.ExecuteNonQuery();
            }
        }
        tx.Commit();
    }

    /// Replaces the stored IUCN taxon IDs at deprecated rank.
    public void ReplaceDeprecatedIucnTaxonIds(IEnumerable<(long Item, string TaxonId)> pairs, DateTime fetchedAtUtc) {
        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = $"DELETE FROM {WikidataIucnReferenceTables.DeprecatedTaxonIdsTable}";
            delete.ExecuteNonQuery();
        }
        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = $"INSERT OR IGNORE INTO {WikidataIucnReferenceTables.DeprecatedTaxonIdsTable} (qid_numeric, iucn_taxon_id, fetched_at) VALUES (@item, @id, @at)";
            var item = insert.Parameters.Add("@item", SqliteType.Integer);
            var id = insert.Parameters.Add("@id", SqliteType.Text);
            insert.Parameters.AddWithValue("@at", FormatUtc(fetchedAtUtc));
            foreach (var pair in pairs) {
                item.Value = pair.Item;
                id.Value = pair.TaxonId;
                insert.ExecuteNonQuery();
            }
        }
        tx.Commit();
    }

    private static string FormatUtc(DateTime value) =>
        DateTime.SpecifyKind(value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value, DateTimeKind.Utc)
            .ToString("O", CultureInfo.InvariantCulture);
}
