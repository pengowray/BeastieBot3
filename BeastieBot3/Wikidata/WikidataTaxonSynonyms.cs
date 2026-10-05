using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

// Taxon synonym (P1420) statements of Wikidata items: each names another item, whose taxon name
// (P225) is the synonym. `wikidata queue-synonyms` queues those items for download, and
// `site build-db` reads the names of the ones downloaded.

namespace BeastieBot3.Wikidata;

internal static class WikidataTaxonSynonyms {
    public const string Property = "P1420";

    /// The items an entity JSON (wbgetentities form, or the entity object itself) names as taxon
    /// synonym, leaving out statements at deprecated rank. Empty for JSON that cannot be read.
    public static IReadOnlyList<long> ItemsIn(string json) {
        var items = new List<long>();
        try {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var entity = root.TryGetProperty("entities", out var entities) && entities.ValueKind == JsonValueKind.Object
                ? entities.EnumerateObject().Select(p => p.Value).FirstOrDefault()
                : root;
            if (entity.ValueKind != JsonValueKind.Object || !entity.TryGetProperty("claims", out var claims)
                || !claims.TryGetProperty(Property, out var statements) || statements.ValueKind != JsonValueKind.Array) {
                return items;
            }
            foreach (var statement in statements.EnumerateArray()) {
                if (statement.TryGetProperty("rank", out var rank) && rank.GetString() == "deprecated") {
                    continue;
                }
                if (statement.TryGetProperty("mainsnak", out var snak) && snak.TryGetProperty("datavalue", out var value)
                    && value.TryGetProperty("value", out var inner) && inner.ValueKind == JsonValueKind.Object
                    && inner.TryGetProperty("numeric-id", out var id) && id.TryGetInt64(out var numeric)) {
                    items.Add(numeric);
                }
            }
        } catch (JsonException) {
        }
        return items;
    }
}
