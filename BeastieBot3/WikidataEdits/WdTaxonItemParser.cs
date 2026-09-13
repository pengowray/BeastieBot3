using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

// Turns one cached Wikidata entity payload into a WdTaxonItem. Pure: no database, no network.
//
// The cache (wikidata_entities.json) stores the whole wbgetentities response for a single id:
//   {"entities":{"Q24024":{"id":"Q24024","lastrevid":...,"labels":{...},"claims":{...},...}},"success":1}
// Every downloaded row checked on 2026-09-13 (187,911) had exactly one entity, keyed by the row's
// entity_id, with no redirects or "missing" entities.
//
// P627 and P141 statements keep a detached copy of their JSON (Raw) because the edit planner
// builds wbeditentity payloads from them: id, rank, mainsnak, qualifiers, qualifiers-order and
// references (hash, snaks, snaks-order) all survive, in cached key order. Statement ids are kept
// verbatim; some are lower-case ("q24024$..."), and that is the id Wikidata matches on.

namespace BeastieBot3.WikidataEdits;

internal static class WdTaxonItemParser {
    private const string NoValue = "novalue";
    private const string SomeValue = "somevalue";

    /// Returns null when the payload holds no usable item: no entity, a "missing" entity, a
    /// non-item id, or no lastrevid. Malformed JSON throws JsonException.
    public static WdTaxonItem? Parse(string cachedJson, DateTime? downloadedAtUtc) {
        if (string.IsNullOrWhiteSpace(cachedJson)) {
            return null;
        }

        using var document = JsonDocument.Parse(cachedJson);
        if (!TryGetEntity(document.RootElement, out var entity)) {
            return null;
        }

        var qid = GetString(entity, "id");
        if (qid is null || qid.Length < 2 || (qid[0] != 'Q' && qid[0] != 'q')) {
            return null;
        }

        if (!entity.TryGetProperty("lastrevid", out var revElement)
            || revElement.ValueKind != JsonValueKind.Number
            || !revElement.TryGetInt64(out var lastRevId)) {
            return null;
        }

        var claims = entity.TryGetProperty("claims", out var claimsElement) && claimsElement.ValueKind == JsonValueKind.Object
            ? claimsElement
            : default;

        return new WdTaxonItem {
            Qid = qid,
            LastRevId = lastRevId,
            DownloadedAtUtc = downloadedAtUtc,
            LabelEn = GetLabel(entity, "en"),
            InstanceOf = CurrentValues(claims, "P31", itemValues: true),
            TaxonNames = CurrentValues(claims, "P225", itemValues: false),
            TaxonRanks = CurrentValues(claims, "P105", itemValues: true),
            ParentTaxa = CurrentValues(claims, "P171", itemValues: true),
            IucnTaxonIds = Statements(claims, "P627"),
            ConservationStatuses = Statements(claims, "P141"),
        };
    }

    private static bool TryGetEntity(JsonElement root, out JsonElement entity) {
        entity = default;
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("entities", out var entities)
            || entities.ValueKind != JsonValueKind.Object) {
            return false;
        }

        foreach (var property in entities.EnumerateObject()) {
            if (property.Value.ValueKind != JsonValueKind.Object || property.Value.TryGetProperty("missing", out _)) {
                continue;
            }

            entity = property.Value;
            return true;
        }

        return false;
    }

    private static string? GetLabel(JsonElement entity, string language) {
        if (!entity.TryGetProperty("labels", out var labels) || labels.ValueKind != JsonValueKind.Object) {
            return null;
        }

        return labels.TryGetProperty(language, out var label) && label.ValueKind == JsonValueKind.Object
            ? GetString(label, "value")
            : null;
    }

    // Values of non-deprecated statements with a real value (not somevalue/novalue), in cached
    // order, duplicates dropped. P31 follows the same rule as P225/P105/P171: a deprecated
    // statement is one Wikidata marks as wrong.
    private static IReadOnlyList<string> CurrentValues(JsonElement claims, string property, bool itemValues) {
        if (!TryGetStatementArray(claims, property, out var statements)) {
            return Array.Empty<string>();
        }

        var values = new List<string>();
        foreach (var statement in statements.EnumerateArray()) {
            if (statement.ValueKind != JsonValueKind.Object
                || string.Equals(GetString(statement, "rank"), "deprecated", StringComparison.Ordinal)
                || !statement.TryGetProperty("mainsnak", out var mainsnak)
                || !TryGetDataValue(mainsnak, out var value, out var type)) {
                continue;
            }

            var text = itemValues
                ? (type == "wikibase-entityid" ? EntityId(value) : null)
                : (type == "string" ? value.GetString() : null);
            if (!string.IsNullOrWhiteSpace(text) && !values.Contains(text)) {
                values.Add(text);
            }
        }

        return values;
    }

    private static IReadOnlyList<WdStatement> Statements(JsonElement claims, string property) {
        if (!TryGetStatementArray(claims, property, out var statements)) {
            return Array.Empty<WdStatement>();
        }

        var result = new List<WdStatement>();
        foreach (var statement in statements.EnumerateArray()) {
            var parsed = ParseStatement(statement, property);
            if (parsed is not null) {
                result.Add(parsed);
            }
        }

        return result;
    }

    private static WdStatement? ParseStatement(JsonElement statement, string claimProperty) {
        if (statement.ValueKind != JsonValueKind.Object) {
            return null;
        }

        var id = GetString(statement, "id");
        if (string.IsNullOrEmpty(id)) {
            return null;
        }

        var property = claimProperty;
        string? valueId = null;
        string? valueString = null;
        if (statement.TryGetProperty("mainsnak", out var mainsnak) && mainsnak.ValueKind == JsonValueKind.Object) {
            property = GetString(mainsnak, "property") ?? claimProperty;
            if (TryGetDataValue(mainsnak, out var value, out var type)) {
                if (type == "wikibase-entityid") {
                    valueId = EntityId(value);
                }
                else {
                    valueString = RenderValue(value, type);
                }
            }
        }

        var references = new List<WdReference>();
        if (statement.TryGetProperty("references", out var referenceArray) && referenceArray.ValueKind == JsonValueKind.Array) {
            foreach (var reference in referenceArray.EnumerateArray()) {
                if (reference.ValueKind != JsonValueKind.Object) {
                    continue;
                }

                references.Add(new WdReference(
                    GetString(reference, "hash"),
                    SnakGroups(reference, "snaks", "snaks-order"),
                    Detach(reference)));
            }
        }

        return new WdStatement(
            id,
            property,
            GetString(statement, "rank") ?? "normal",
            valueId,
            valueString,
            SnakGroups(statement, "qualifiers", "qualifiers-order"),
            references,
            Detach(statement));
    }

    // Qualifiers or reference snaks as property -> rendered values, in the order the "-order"
    // array gives (then any property it leaves out, in cached order). somevalue/novalue snaks
    // render as "somevalue"/"novalue" so the property's presence is still visible.
    private static IReadOnlyDictionary<string, IReadOnlyList<string>> SnakGroups(JsonElement container, string groupsKey, string orderKey) {
        if (!container.TryGetProperty(groupsKey, out var groups) || groups.ValueKind != JsonValueKind.Object) {
            return EmptyGroups;
        }

        var result = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        if (container.TryGetProperty(orderKey, out var order) && order.ValueKind == JsonValueKind.Array) {
            foreach (var entry in order.EnumerateArray()) {
                var property = entry.ValueKind == JsonValueKind.String ? entry.GetString() : null;
                if (property is not null && !result.ContainsKey(property) && groups.TryGetProperty(property, out var snaks)) {
                    result[property] = RenderSnaks(snaks);
                }
            }
        }

        foreach (var group in groups.EnumerateObject()) {
            if (!result.ContainsKey(group.Name)) {
                result[group.Name] = RenderSnaks(group.Value);
            }
        }

        return result;
    }

    private static readonly IReadOnlyDictionary<string, IReadOnlyList<string>> EmptyGroups =
        new Dictionary<string, IReadOnlyList<string>>(0);

    private static IReadOnlyList<string> RenderSnaks(JsonElement snaks) {
        if (snaks.ValueKind != JsonValueKind.Array) {
            return Array.Empty<string>();
        }

        var values = new List<string>();
        foreach (var snak in snaks.EnumerateArray()) {
            if (snak.ValueKind != JsonValueKind.Object) {
                continue;
            }

            var snakType = GetString(snak, "snaktype");
            if (snakType == NoValue || snakType == SomeValue) {
                values.Add(snakType);
                continue;
            }

            if (TryGetDataValue(snak, out var value, out var type)) {
                var text = type == "wikibase-entityid" ? EntityId(value) : RenderValue(value, type);
                if (text is not null) {
                    values.Add(text);
                }
            }
        }

        return values;
    }

    /// A datavalue as a plain string: strings, external ids and URLs as is; a time as its Wikibase
    /// time string ("+2025-11-12T00:00:00Z"); monolingual text as its text; a quantity as its
    /// amount, followed by the unit's entity id when it has one; a coordinate as "lat,lon".
    internal static string? RenderValue(JsonElement value, string? type) {
        switch (type) {
            case "string":
                return value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
            case "wikibase-entityid":
                return EntityId(value);
            case "time":
                return GetString(value, "time");
            case "monolingualtext":
                return GetString(value, "text");
            case "quantity": {
                var amount = GetString(value, "amount");
                var unit = GetString(value, "unit");
                if (amount is null) {
                    return null;
                }

                if (string.IsNullOrEmpty(unit) || unit == "1") {
                    return amount;
                }

                var slash = unit.LastIndexOf('/');
                return $"{amount} {(slash >= 0 ? unit[(slash + 1)..] : unit)}";
            }
            case "globecoordinate":
                if (value.ValueKind == JsonValueKind.Object
                    && value.TryGetProperty("latitude", out var lat) && lat.ValueKind == JsonValueKind.Number
                    && value.TryGetProperty("longitude", out var lon) && lon.ValueKind == JsonValueKind.Number) {
                    return string.Create(CultureInfo.InvariantCulture, $"{lat.GetDouble()},{lon.GetDouble()}");
                }

                return value.GetRawText();
            default:
                return value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
        }
    }

    private static string? EntityId(JsonElement value) {
        if (value.ValueKind != JsonValueKind.Object) {
            return null;
        }

        var id = GetString(value, "id");
        if (!string.IsNullOrEmpty(id)) {
            return id;
        }

        // Older serialisations carry only entity-type + numeric-id.
        if (value.TryGetProperty("numeric-id", out var numeric) && numeric.TryGetInt64(out var n)) {
            var prefix = GetString(value, "entity-type") switch {
                "property" => "P",
                "lexeme" => "L",
                _ => "Q",
            };
            return string.Create(CultureInfo.InvariantCulture, $"{prefix}{n}");
        }

        return null;
    }

    private static bool TryGetStatementArray(JsonElement claims, string property, out JsonElement statements) {
        statements = default;
        return claims.ValueKind == JsonValueKind.Object
            && claims.TryGetProperty(property, out statements)
            && statements.ValueKind == JsonValueKind.Array;
    }

    private static bool TryGetDataValue(JsonElement snak, out JsonElement value, out string? type) {
        value = default;
        type = null;
        if (snak.ValueKind != JsonValueKind.Object) {
            return false;
        }

        var snakType = GetString(snak, "snaktype");
        if ((snakType is not null && snakType != "value")
            || !snak.TryGetProperty("datavalue", out var dataValue)
            || dataValue.ValueKind != JsonValueKind.Object
            || !dataValue.TryGetProperty("value", out value)) {
            return false;
        }

        type = GetString(dataValue, "type");
        return true;
    }

    private static string? GetString(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object
        && element.TryGetProperty(property, out var value)
        && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    // Clone() copies the element out of the pooled document, so the node stays valid after the
    // document is disposed; JsonObject.Create keeps the cached key order.
    private static JsonObject Detach(JsonElement element) => JsonObject.Create(element.Clone())!;
}
