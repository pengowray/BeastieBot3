using System;
using System.Collections.Generic;
using System.Text.Json;

namespace BeastieBot3;

internal static class WikidataEntityParser {
    public static WikidataEntityRecord Parse(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (!root.TryGetProperty("entities", out var entities)) {
            throw new InvalidOperationException("Wikidata payload did not contain an 'entities' object.");
        }

        JsonProperty entityProperty = default;
        foreach (var property in entities.EnumerateObject()) {
            entityProperty = property;
            break;
        }

        if (entityProperty.Equals(default(JsonProperty))) {
            throw new InvalidOperationException("Wikidata payload did not include any entity data.");
        }

        var entity = entityProperty.Value;
        var entityId = entityProperty.Name;
        var numericId = ExtractNumericId(entityId);

        var labelEn = TryGetLanguageValue(entity, "labels", "en");
        var descriptionEn = TryGetLanguageValue(entity, "descriptions", "en");

        var claims = entity.TryGetProperty("claims", out var claimsElement) ? claimsElement : default;
        var p627Claims = ParseStringClaims(claims, "P627");
        var scientificNames = ParseMonolingualClaims(claims, "P225");
        var rankQid = ParseSingleEntityId(claims, "P105");
        var parentTaxa = ParseEntityIds(claims, "P171");
        var p141Statements = ParseP141Statements(claims);
        var commonNames = ParseMonolingualClaims(claims, "P1843");
        var p627FromReferences = CollectP627FromReferences(p141Statements);

        return new WikidataEntityRecord(
            numericId,
            entityId,
            labelEn,
            descriptionEn,
            p141Statements.Count > 0,
            p627Claims.Count > 0,
            p627Claims,
            p627FromReferences,
            p141Statements,
            scientificNames,
            commonNames,
            rankQid,
            parentTaxa
        );
    }

    private static IReadOnlyList<string> ParseStringClaims(JsonElement claims, string propertyId) {
        var values = new List<string>();
        if (claims.ValueKind != JsonValueKind.Object) {
            return values;
        }

        if (!claims.TryGetProperty(propertyId, out var entries)) {
            return values;
        }

        foreach (var claim in entries.EnumerateArray()) {
            if (!TryGetDataValue(claim, out var dataValue)) {
                continue;
            }

            if (dataValue.ValueKind == JsonValueKind.String) {
                var text = dataValue.GetString();
                if (!string.IsNullOrWhiteSpace(text)) {
                    values.Add(text.Trim());
                }
            }
            else if (dataValue.ValueKind == JsonValueKind.Object && dataValue.TryGetProperty("text", out var textElement)) {
                var text = textElement.GetString();
                if (!string.IsNullOrWhiteSpace(text)) {
                    values.Add(text.Trim());
                }
            }
        }

        return values;
    }

    private static IReadOnlyList<WikidataMonolingualText> ParseMonolingualClaims(JsonElement claims, string propertyId) {
        var values = new List<WikidataMonolingualText>();
        if (claims.ValueKind != JsonValueKind.Object) {
            return values;
        }

        if (!claims.TryGetProperty(propertyId, out var entries)) {
            return values;
        }

        foreach (var claim in entries.EnumerateArray()) {
            if (!TryGetDataValue(claim, out var dataValue)) {
                continue;
            }

            string? text = null;
            string language = "und";

            if (dataValue.ValueKind == JsonValueKind.Object) {
                text = dataValue.TryGetProperty("text", out var textElement) ? textElement.GetString() : null;
                var lang = dataValue.TryGetProperty("language", out var langElement) ? langElement.GetString() : null;
                if (!string.IsNullOrWhiteSpace(lang)) {
                    language = lang.Trim();
                }
            }
            else if (dataValue.ValueKind == JsonValueKind.String) {
                text = dataValue.GetString();
            }

            if (!string.IsNullOrWhiteSpace(text)) {
                values.Add(new WikidataMonolingualText(language, text!.Trim()));
            }
        }

        return values;
    }

    private static long? ParseSingleEntityId(JsonElement claims, string propertyId) {
        var ids = ParseEntityIds(claims, propertyId);
        return ids.Count > 0 ? ids[0] : null;
    }

    private static IReadOnlyList<long> ParseEntityIds(JsonElement claims, string propertyId) {
        var values = new List<long>();
        if (claims.ValueKind != JsonValueKind.Object) {
            return values;
        }

        if (!claims.TryGetProperty(propertyId, out var entries)) {
            return values;
        }

        foreach (var claim in entries.EnumerateArray()) {
            if (!TryGetDataValue(claim, out var dataValue)) {
                continue;
            }

            var entityId = ExtractEntityId(dataValue);
            if (entityId is null) {
                continue;
            }

            if (TryParseNumericId(entityId, out var numeric)) {
                values.Add(numeric);
            }
        }

        return values;
    }

    private static IReadOnlyList<WikidataP141Statement> ParseP141Statements(JsonElement claims) {
        var result = new List<WikidataP141Statement>();
        if (claims.ValueKind != JsonValueKind.Object) {
            return result;
        }

        if (!claims.TryGetProperty("P141", out var entries)) {
            return result;
        }

        foreach (var claim in entries.EnumerateArray()) {
            if (!claim.TryGetProperty("id", out var idElement)) {
                continue;
            }

            var statementId = idElement.GetString() ?? string.Empty;
            if (!TryGetDataValue(claim, out var dataValue)) {
                continue;
            }

            var statusId = ExtractEntityId(dataValue);
            if (statusId is null || !TryParseNumericId(statusId, out var statusNumeric)) {
                continue;
            }

            var rank = claim.TryGetProperty("rank", out var rankElement) ? rankElement.GetString() ?? "normal" : "normal";
            var references = ParseReferences(claim);
            result.Add(new WikidataP141Statement(statementId, statusNumeric, statusId, rank, references));
        }

        return result;
    }

    private static IReadOnlyList<WikidataP141Reference> ParseReferences(JsonElement claim) {
        var result = new List<WikidataP141Reference>();
        if (!claim.TryGetProperty("references", out var references)) {
            return result;
        }

        var index = 0;
        foreach (var reference in references.EnumerateArray()) {
            var hash = reference.TryGetProperty("hash", out var hashElement)
                ? hashElement.GetString()
                : $"missing-hash-{index++}";

            if (!reference.TryGetProperty("snaks", out var snaks)) {
                result.Add(new WikidataP141Reference(hash ?? $"missing-hash-{index}", null, Array.Empty<string>()));
                continue;
            }

            long? sourceQid = null;
            if (snaks.TryGetProperty("P248", out var sourceSnaks)) {
                foreach (var snak in sourceSnaks.EnumerateArray()) {
                    if (!TryGetDataValue(snak, out var dataValue)) {
                        continue;
                    }

                    var entityId = ExtractEntityId(dataValue);
                    if (entityId is not null && TryParseNumericId(entityId, out var numeric)) {
                        sourceQid = numeric;
                        break;
                    }
                }
            }

            var iucnIds = new List<string>();
            if (snaks.TryGetProperty("P627", out var p627Snaks)) {
                foreach (var snak in p627Snaks.EnumerateArray()) {
                    if (!TryGetDataValue(snak, out var value)) {
                        continue;
                    }

                    if (value.ValueKind == JsonValueKind.String) {
                        var text = value.GetString();
                        if (!string.IsNullOrWhiteSpace(text)) {
                            iucnIds.Add(text.Trim());
                        }
                    }
                }
            }

            result.Add(new WikidataP141Reference(hash ?? $"missing-hash-{index}", sourceQid, iucnIds));
        }

        return result;
    }

    private static IReadOnlyList<string> CollectP627FromReferences(IReadOnlyList<WikidataP141Statement> statements) {
        var values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var statement in statements) {
            foreach (var reference in statement.References) {
                foreach (var value in reference.IucnTaxonIds) {
                    if (!string.IsNullOrWhiteSpace(value)) {
                        values.Add(value.Trim());
                    }
                }
            }
        }

        return new List<string>(values);
    }

    private static bool TryGetDataValue(JsonElement element, out JsonElement value) {
        value = default;
        if (TryGetDirectDataValue(element, out value)) {
            return true;
        }

        if (!element.TryGetProperty("mainsnak", out var snak)) {
            return false;
        }

        return TryGetDirectDataValue(snak, out value);
    }

    private static bool TryGetDirectDataValue(JsonElement element, out JsonElement value) {
        value = default;
        if (!element.TryGetProperty("datavalue", out var dataValue) || dataValue.ValueKind != JsonValueKind.Object) {
            return false;
        }

        if (!dataValue.TryGetProperty("value", out value)) {
            return false;
        }

        return true;
    }

    private static long ExtractNumericId(string entityId) {
        if (!TryParseNumericId(entityId, out var numeric)) {
            throw new InvalidOperationException($"Unable to parse numeric id from '{entityId}'.");
        }

        return numeric;
    }

    private static bool TryParseNumericId(string entityId, out long numeric) {
        numeric = 0;
        if (string.IsNullOrWhiteSpace(entityId) || entityId.Length < 2) {
            return false;
        }

        var span = entityId.AsSpan();
        var start = span[0] == 'Q' || span[0] == 'P' ? 1 : 0;
        return long.TryParse(span[start..], out numeric);
    }

    private static string? ExtractEntityId(JsonElement dataValue) {
        if (dataValue.ValueKind != JsonValueKind.Object) {
            return null;
        }

        if (dataValue.TryGetProperty("id", out var idElement)) {
            return idElement.GetString();
        }

        if (dataValue.TryGetProperty("value", out var nested) && nested.ValueKind == JsonValueKind.Object && nested.TryGetProperty("id", out var nestedId)) {
            return nestedId.GetString();
        }

        return null;
    }

    private static string? TryGetLanguageValue(JsonElement entity, string propertyName, string language) {
        if (!entity.TryGetProperty(propertyName, out var container)) {
            return null;
        }

        if (!container.TryGetProperty(language, out var entry)) {
            return null;
        }

        return entry.TryGetProperty("value", out var value) ? value.GetString() : null;
    }
}

internal sealed record WikidataEntityRecord(
    long NumericId,
    string EntityId,
    string? LabelEn,
    string? DescriptionEn,
    bool HasP141,
    bool HasP627,
    IReadOnlyList<string> P627Claims,
    IReadOnlyList<string> P627References,
    IReadOnlyList<WikidataP141Statement> P141Statements,
    IReadOnlyList<WikidataMonolingualText> ScientificNames,
    IReadOnlyList<WikidataMonolingualText> CommonNames,
    long? RankQid,
    IReadOnlyList<long> ParentTaxaQids
);

internal sealed record WikidataP141Statement(
    string StatementId,
    long StatusNumericId,
    string StatusEntityId,
    string Rank,
    IReadOnlyList<WikidataP141Reference> References
);

internal sealed record WikidataP141Reference(
    string ReferenceHash,
    long? SourceQid,
    IReadOnlyList<string> IucnTaxonIds
);

internal sealed record WikidataMonolingualText(string Language, string Value);
