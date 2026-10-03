using System.Globalization;
using System.Text.Json;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.SiteBuild;

// Which IUCN conservation status (P141) statements of a Wikidata item cite IUCN in a part of the
// reference that the cache's index (wikidata_p141_references) does not record: a reference URL
// (P854) on iucnredlist.org or a subdomain of it, or a stated in (P248) after the first one. The
// index has only the first stated in of each reference and its IUCN taxon IDs (P627), so
// `site build-db` reads the cached JSON of the few items with a statement whose references the index
// shows citing something else or nothing (89 statements on 89 items in October 2026; 35 of them have
// an IUCN reference URL, such as a pre-publication PDF on nc.iucnredlist.org).

internal enum P141JsonCitation {
    /// A reference URL (P854) on iucnredlist.org or a subdomain.
    ReferenceUrl,
    /// A stated in (P248) item that cites IUCN; the index records only a reference's first one.
    StatedIn,
}

internal static class P141JsonReferences {
    /// The statements among statementIds (compared exactly; the cache keeps ids such as
    /// "q571449$...") that have a reference citing IUCN, and how the first such reference cites it.
    /// json: the item as wbgetentities returns it ({"entities": {"Q1": {...}}}). isIucnSource: whether
    /// a stated in item cites IUCN (the Red List, IUCN, an edition, an assessment's item). A
    /// statement the JSON does not have is left out; JSON that cannot be read gives none.
    public static IReadOnlyDictionary<string, P141JsonCitation> StatementsCitingIucn(string? json, IReadOnlySet<string> statementIds,
        Func<long, bool> isIucnSource) {
        var found = new Dictionary<string, P141JsonCitation>(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(json) || statementIds.Count == 0) {
            return found;
        }
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            return found;
        }
        using (document) {
            if (!document.RootElement.TryGetProperty("entities", out var entities) || entities.ValueKind != JsonValueKind.Object) {
                return found;
            }
            foreach (var entity in entities.EnumerateObject()) {
                if (!entity.Value.TryGetProperty("claims", out var claims) || claims.ValueKind != JsonValueKind.Object
                    || !claims.TryGetProperty("P141", out var statements) || statements.ValueKind != JsonValueKind.Array) {
                    continue;
                }
                foreach (var statement in statements.EnumerateArray()) {
                    if (!statement.TryGetProperty("id", out var idElement) || idElement.GetString() is not { } id
                        || !statementIds.Contains(id) || found.ContainsKey(id)) {
                        continue;
                    }
                    if (Citation(statement, isIucnSource) is { } citation) {
                        found[id] = citation;
                    }
                }
            }
        }
        return found;
    }

    private static P141JsonCitation? Citation(JsonElement statement, Func<long, bool> isIucnSource) {
        if (!statement.TryGetProperty("references", out var references) || references.ValueKind != JsonValueKind.Array) {
            return null;
        }
        foreach (var reference in references.EnumerateArray()) {
            if (!reference.TryGetProperty("snaks", out var snaks) || snaks.ValueKind != JsonValueKind.Object) {
                continue;
            }
            foreach (var value in Values(snaks, "P248")) {
                if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty("id", out var item)
                    && item.GetString() is { Length: > 1 } qid && (qid[0] == 'Q' || qid[0] == 'q')
                    && long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var numeric)
                    && isIucnSource(numeric)) {
                    return P141JsonCitation.StatedIn;
                }
            }
            foreach (var value in Values(snaks, "P854")) {
                if (value.ValueKind == JsonValueKind.String && WikidataStatusStatement.IsIucnRedListUrl(value.GetString())) {
                    return P141JsonCitation.ReferenceUrl;
                }
            }
        }
        return null;
    }

    // The datavalue values of a property's snaks in a reference; "somevalue" and "novalue" snaks have none.
    private static IEnumerable<JsonElement> Values(JsonElement snaks, string property) {
        if (!snaks.TryGetProperty(property, out var list) || list.ValueKind != JsonValueKind.Array) {
            yield break;
        }
        foreach (var snak in list.EnumerateArray()) {
            if (snak.TryGetProperty("datavalue", out var datavalue) && datavalue.ValueKind == JsonValueKind.Object
                && datavalue.TryGetProperty("value", out var value)) {
                yield return value;
            }
        }
    }
}
