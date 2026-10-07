using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The taxon's ids in other databases (ExternalDatabases: GBIF, iNaturalist, NCBI ...), read from
// the external identifiers on its Wikidata item in the Wikidata cache, for the species page's links
// (taxon_external_id). Deprecated statements are left out.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteExternalId(long TaxonId, string Property, string Value);

internal static class SiteExternalIds {
    public static List<SiteExternalId> Read(string wikidataCache, IEnumerable<(long TaxonId, string Qid)> taxa, CancellationToken ct) {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = wikidataCache, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        var properties = string.Join(", ", ExternalDatabases.All.Select((d, i) => $"@p{i}"));
        command.CommandText = $"""
            SELECT p.key, json_extract(c.value, '$.mainsnak.datavalue.value'), json_extract(c.value, '$.rank')
            FROM wikidata_entities e,
                 json_each(e.json, '$.entities.' || e.entity_id || '.claims') p,
                 json_each(p.value) c
            WHERE e.entity_numeric_id = @id AND e.json_downloaded = 1 AND p.key IN ({properties})
            """;
        var id = command.Parameters.Add("@id", SqliteType.Integer);
        for (var i = 0; i < ExternalDatabases.All.Count; i++) {
            command.Parameters.AddWithValue($"@p{i}", ExternalDatabases.All[i].Property);
        }
        var rows = new List<SiteExternalId>();
        foreach (var (taxonId, qid) in taxa) {
            ct.ThrowIfCancellationRequested();
            if (!long.TryParse(qid.TrimStart('Q'), out var number)) {
                continue;
            }
            id.Value = number;
            using var reader = command.ExecuteReader();
            var seen = new HashSet<(string, string)>();
            while (reader.Read()) {
                if (reader.IsDBNull(1) || reader.GetValue(1) is not string value || value.Trim().Length == 0
                    || (!reader.IsDBNull(2) && reader.GetString(2) == "deprecated")) {
                    continue;
                }
                if (seen.Add((reader.GetString(0), value.Trim()))) {
                    rows.Add(new SiteExternalId(taxonId, reader.GetString(0), value.Trim()));
                }
            }
        }
        return rows;
    }
}
