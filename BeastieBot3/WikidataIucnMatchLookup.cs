using System.Data;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class WikidataIucnMatchLookup {
    private readonly SqliteConnection? _connection;

    public WikidataIucnMatchLookup(SqliteConnection? connection) {
        _connection = connection;
    }

    public WikidataIucnMatchCandidate? GetCandidate(string? iucnTaxonId) {
        if (_connection is null || string.IsNullOrWhiteSpace(iucnTaxonId)) {
            return null;
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT m.entity_numeric_id,
       m.entity_id,
       m.matched_name,
       m.match_method,
       m.is_synonym,
       e.json
FROM wikidata_pending_iucn_matches m
JOIN wikidata_entities e ON e.entity_numeric_id = m.entity_numeric_id
WHERE m.iucn_taxon_id = @id
LIMIT 1
""";
        command.Parameters.AddWithValue("@id", iucnTaxonId.Trim());
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return null;
        }

        var matchedName = reader.IsDBNull(2) ? null : reader.GetString(2);
        var matchMethod = reader.IsDBNull(3) ? "wikidata" : reader.GetString(3);
        var isSynonym = !reader.IsDBNull(4) && reader.GetInt64(4) != 0;
        string? title = null;
        if (!reader.IsDBNull(5)) {
            var json = reader.GetString(5);
            if (WikidataSitelinkExtractor.TryGetEnwikiTitle(json, out var enwiki) && !string.IsNullOrWhiteSpace(enwiki)) {
                title = enwiki;
            }
        }

        if (string.IsNullOrWhiteSpace(title)) {
            title = matchedName;
        }

        return string.IsNullOrWhiteSpace(title)
            ? null
            : new WikidataIucnMatchCandidate(title.Trim(), matchMethod ?? "wikidata", matchedName, isSynonym);
    }
}

internal sealed record WikidataIucnMatchCandidate(
    string Title,
    string MatchMethod,
    string? MatchedName,
    bool IsSynonym
);
