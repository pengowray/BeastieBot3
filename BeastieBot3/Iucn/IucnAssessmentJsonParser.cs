using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

// Parses one cached /api/v4/assessment/{id} JSON blob into the fields the
// CSV-shaped projection needs (IucnApiProjectionStore). Field names verified
// against real cached payloads:
//   possibly_extinct / possibly_extinct_in_the_wild : bool
//   red_list_category : { code, description: { en } }
//   scopes  : [ { description: { en }, code } ]      e.g. en="Global"
//   systems : [ { description: { en }, code } ]      e.g. en="Terrestrial"
//   year_published : string|number
//   latest : bool|string
//   taxon  : { scientific_name, authority, kingdom_name..species_name,
//              subpopulation_name, infrarank, infra_name }
//
// Booleans are projected to the literal TEXT "true"/"false" the CSV columns use,
// scopes are joined CSV-style ("Global, Europe & Mediterranean") so the consumer
// predicate `scopes LIKE '%Global%'` matches, and systems are pipe-joined to match
// the CSV `systems` column (consumer does `systems LIKE '%X%'`).

namespace BeastieBot3.Iucn;

internal static class IucnAssessmentJsonParser {
    public static ProjectedAssessment? Parse(string json) {
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            return null;
        }

        using (document) {
            var root = document.RootElement;
            var assessmentId = TryGetLong(root, "assessment_id");
            if (assessmentId is null) {
                return null;
            }

            var taxon = root.TryGetProperty("taxon", out var taxonEl) && taxonEl.ValueKind == JsonValueKind.Object
                ? taxonEl
                : (JsonElement?)null;

            var sisTaxonId = TryGetLong(root, "sis_taxon_id")
                ?? (taxon is { } t1 ? TryGetLong(t1, "sis_id") : null);

            var (categoryCode, categoryEn) = ReadCategory(root);

            return new ProjectedAssessment(
                AssessmentId: assessmentId.Value,
                TaxonId: sisTaxonId,
                Latest: ReadBool(root, "latest"),
                YearPublished: TryGetString(root, "year_published"),
                RedlistCategoryCode: categoryCode,
                RedlistCategoryEn: categoryEn,
                PossiblyExtinct: BoolText(root, "possibly_extinct"),
                PossiblyExtinctInTheWild: BoolText(root, "possibly_extinct_in_the_wild"),
                Scopes: JoinScopes(root, "scopes"),
                Systems: JoinDescriptions(root, "systems", "|"),
                ScientificName: taxon is { } t2 ? TryGetString(t2, "scientific_name") ?? TryGetString(t2, "taxon_name") : null,
                Authority: taxon is { } t3 ? TryGetString(t3, "authority") : null,
                KingdomName: taxon is { } t4 ? TryGetString(t4, "kingdom_name") : null,
                PhylumName: taxon is { } t5 ? TryGetString(t5, "phylum_name") : null,
                ClassName: taxon is { } t6 ? TryGetString(t6, "class_name") : null,
                OrderName: taxon is { } t7 ? TryGetString(t7, "order_name") : null,
                FamilyName: taxon is { } t8 ? TryGetString(t8, "family_name") : null,
                GenusName: taxon is { } t9 ? TryGetString(t9, "genus_name") : null,
                SpeciesName: taxon is { } t10 ? TryGetString(t10, "species_name") : null,
                SubpopulationName: taxon is { } t11 ? TryGetString(t11, "subpopulation_name") : null,
                InfraType: taxon is { } t12 ? TryGetString(t12, "infrarank") ?? TryGetString(t12, "infra_type") : null,
                InfraName: taxon is { } t13 ? TryGetString(t13, "infra_name") : null,
                InfraAuthority: taxon is { } t14 ? TryGetString(t14, "infra_authority") : null);
        }
    }

    private static (string? code, string? en) ReadCategory(JsonElement root) {
        if (!root.TryGetProperty("red_list_category", out var cat) || cat.ValueKind != JsonValueKind.Object) {
            // Some payloads expose a flat code instead of the nested object.
            return (TryGetString(root, "red_list_category_code"), null);
        }
        var code = TryGetString(cat, "code");
        string? en = null;
        if (cat.TryGetProperty("description", out var desc) && desc.ValueKind == JsonValueKind.Object) {
            en = TryGetString(desc, "en");
        }
        return (code, en);
    }

    // Joins scope English descriptions CSV-style: "Global", "A & B", "A, B & C".
    private static string? JoinScopes(JsonElement root, string property) {
        var names = ReadDescriptions(root, property);
        if (names.Count == 0) return null;
        if (names.Count == 1) return names[0];
        return string.Join(", ", names.Take(names.Count - 1)) + " & " + names[^1];
    }

    private static string? JoinDescriptions(JsonElement root, string property, string separator) {
        var names = ReadDescriptions(root, property);
        return names.Count == 0 ? null : string.Join(separator, names);
    }

    private static List<string> ReadDescriptions(JsonElement root, string property) {
        var names = new List<string>();
        if (!root.TryGetProperty(property, out var arr) || arr.ValueKind != JsonValueKind.Array) {
            return names;
        }
        foreach (var item in arr.EnumerateArray()) {
            if (item.ValueKind != JsonValueKind.Object) continue;
            if (item.TryGetProperty("description", out var desc) && desc.ValueKind == JsonValueKind.Object) {
                var en = TryGetString(desc, "en");
                if (!string.IsNullOrWhiteSpace(en)) names.Add(en!.Trim());
            }
        }
        return names;
    }

    // Project a JSON boolean (or "true"/"false" string) to the literal CSV TEXT.
    private static string BoolText(JsonElement element, string propertyName) =>
        ReadBool(element, propertyName) ? "true" : "false";

    private static bool ReadBool(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) return false;
        return prop.ValueKind switch {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => false,
        };
    }

    private static long? TryGetLong(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) return null;
        return prop.ValueKind switch {
            JsonValueKind.Number when prop.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => null,
        };
    }

    private static string? TryGetString(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) return null;
        return prop.ValueKind switch {
            JsonValueKind.String => prop.GetString(),
            JsonValueKind.Number => prop.GetRawText(),
            JsonValueKind.Null => null,
            _ => null,
        };
    }
}

internal sealed record ProjectedAssessment(
    long AssessmentId,
    long? TaxonId,
    bool Latest,
    string? YearPublished,
    string? RedlistCategoryCode,
    string? RedlistCategoryEn,
    string PossiblyExtinct,
    string PossiblyExtinctInTheWild,
    string? Scopes,
    string? Systems,
    string? ScientificName,
    string? Authority,
    string? KingdomName,
    string? PhylumName,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string? GenusName,
    string? SpeciesName,
    string? SubpopulationName,
    string? InfraType,
    string? InfraName,
    string? InfraAuthority);
