using System;
using System.Collections.Generic;
using System.Text.Json;

// Parses /api/v4/taxa/family/ and /api/v4/taxa/family/{name} JSON responses.
// The family list endpoint returns family names; the family taxa endpoint returns
// paginated assessment summaries from which we extract SIS IDs for cache discovery.
// Written defensively since the OpenAPI spec does not define response schemas.

namespace BeastieBot3.Iucn;

internal static class IucnFamilyResponseParser {
    /// <summary>
    /// Parses the response from /api/v4/taxa/family/ into a list of family names.
    /// Handles both a flat string array and an array of objects with a name/family_name property.
    /// </summary>
    public static IReadOnlyList<string> ParseFamilyList(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var families = new List<string>();

        // The response may be a top-level array or an object with an array property
        var arrayElement = root.ValueKind == JsonValueKind.Array
            ? root
            : TryGetArrayProperty(root);

        if (arrayElement.ValueKind != JsonValueKind.Array) {
            return families;
        }

        foreach (var item in arrayElement.EnumerateArray()) {
            var name = item.ValueKind switch {
                JsonValueKind.String => item.GetString(),
                JsonValueKind.Object => TryGetStringProperty(item, "name")
                    ?? TryGetStringProperty(item, "family_name"),
                _ => null
            };

            if (!string.IsNullOrWhiteSpace(name)) {
                families.Add(name);
            }
        }

        return families;
    }

    /// <summary>
    /// Parses one page from /api/v4/taxa/family/{name}?page=N, extracting distinct SIS IDs.
    /// Returns the SIS IDs found on this page and whether more pages likely exist.
    /// </summary>
    public static FamilyTaxaPage ParseFamilyTaxaPage(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var sisIds = new HashSet<long>();

        // Look for assessments array — the response should contain assessment records
        var assessments = JsonValueKind.Undefined;

        if (root.TryGetProperty("assessments", out var assessmentsElement) && assessmentsElement.ValueKind == JsonValueKind.Array) {
            assessments = JsonValueKind.Array;
            ExtractSisIds(assessmentsElement, sisIds);
        }
        else if (root.ValueKind == JsonValueKind.Array) {
            assessments = JsonValueKind.Array;
            ExtractSisIds(root, sisIds);
        }

        // Determine whether more pages exist: look for per_page (100 max typically)
        // If the page returned exactly per_page records, there might be more
        var perPage = TryGetInt(root, "per_page") ?? 100;
        var recordCount = assessments == JsonValueKind.Array
            ? CountArrayItems(root)
            : 0;
        var hasMorePages = recordCount >= perPage;

        return new FamilyTaxaPage(new List<long>(sisIds), hasMorePages);
    }

    private static void ExtractSisIds(JsonElement arrayElement, HashSet<long> sisIds) {
        foreach (var item in arrayElement.EnumerateArray()) {
            if (item.ValueKind != JsonValueKind.Object) {
                continue;
            }

            // Try sis_id first (root taxon level), then sis_taxon_id (assessment level)
            if (TryGetLong(item, "sis_id") is { } sisId) {
                sisIds.Add(sisId);
            }
            else if (TryGetLong(item, "sis_taxon_id") is { } taxonId) {
                sisIds.Add(taxonId);
            }

            // Also check nested taxon object
            if (item.TryGetProperty("taxon", out var taxonElement) && taxonElement.ValueKind == JsonValueKind.Object) {
                if (TryGetLong(taxonElement, "sis_id") is { } nestedSisId) {
                    sisIds.Add(nestedSisId);
                }
            }
        }
    }

    private static int CountArrayItems(JsonElement root) {
        if (root.TryGetProperty("assessments", out var arr) && arr.ValueKind == JsonValueKind.Array) {
            return arr.GetArrayLength();
        }

        return root.ValueKind == JsonValueKind.Array ? root.GetArrayLength() : 0;
    }

    private static JsonElement TryGetArrayProperty(JsonElement root) {
        // Common patterns: { "families": [...] }, { "data": [...] }, { "results": [...] }
        foreach (var name in new[] { "families", "data", "results" }) {
            if (root.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.Array) {
                return prop;
            }
        }

        // Fall back to first array property found
        if (root.ValueKind == JsonValueKind.Object) {
            foreach (var property in root.EnumerateObject()) {
                if (property.Value.ValueKind == JsonValueKind.Array) {
                    return property.Value;
                }
            }
        }

        return default;
    }

    private static string? TryGetStringProperty(JsonElement element, string propertyName) {
        return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static long? TryGetLong(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) {
            return null;
        }

        return prop.ValueKind switch {
            JsonValueKind.Number => prop.GetInt64(),
            JsonValueKind.String when long.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static int? TryGetInt(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) {
            return null;
        }

        return prop.ValueKind switch {
            JsonValueKind.Number => prop.GetInt32(),
            JsonValueKind.String when int.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => null
        };
    }
}

internal sealed record FamilyTaxaPage(IReadOnlyList<long> SisIds, bool HasMorePages);
