using System.Text.Json;

// Lightweight extractor for taxonomy hierarchy fields from cached
// /api/v4/taxa/sis/{id} JSON. The IUCN API taxa response includes
// kingdom_name, phylum_name, class_name, order_name, family_name
// at the taxon level. Used by reports that need phylogenetic grouping
// independently of the CSV-imported database.

namespace BeastieBot3.Iucn;

internal static class IucnTaxaTaxonomyExtractor {
    /// <summary>
    /// Extracts taxonomy and identification fields from cached taxa JSON.
    /// Returns null only if the JSON is completely unparseable.
    /// </summary>
    public static TaxaTaxonomyInfo? Extract(string json) {
        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            var sisId = TryGetLong(root, "sis_id") ?? 0;

            // Try taxon sub-object first, then root level
            var taxon = root.TryGetProperty("taxon", out var taxonElement) && taxonElement.ValueKind == JsonValueKind.Object
                ? taxonElement
                : root;

            var scientificName = TryGetString(taxon, "scientific_name")
                ?? TryGetString(taxon, "taxon_name");
            var kingdomName = TryGetString(taxon, "kingdom_name");
            var phylumName = TryGetString(taxon, "phylum_name");
            var className = TryGetString(taxon, "class_name");
            var orderName = TryGetString(taxon, "order_name");
            var familyName = TryGetString(taxon, "family_name");
            var genusName = TryGetString(taxon, "genus_name");
            var speciesName = TryGetString(taxon, "species_name");

            // Also try root level for fields that may appear there instead
            kingdomName ??= TryGetString(root, "kingdom_name");
            phylumName ??= TryGetString(root, "phylum_name");
            className ??= TryGetString(root, "class_name");
            orderName ??= TryGetString(root, "order_name");
            familyName ??= TryGetString(root, "family_name");

            // Common name: look for main_common_name or first English common name
            var commonName = TryGetString(taxon, "main_common_name")
                ?? TryGetString(root, "main_common_name");

            if (string.IsNullOrWhiteSpace(commonName)) {
                commonName = ExtractFirstEnglishCommonName(taxon)
                    ?? ExtractFirstEnglishCommonName(root);
            }

            return new TaxaTaxonomyInfo(
                SisId: sisId,
                ScientificName: scientificName,
                KingdomName: kingdomName,
                PhylumName: phylumName,
                ClassName: className,
                OrderName: orderName,
                FamilyName: familyName,
                GenusName: genusName,
                SpeciesName: speciesName,
                CommonName: commonName);
        }
        catch (JsonException) {
            return null;
        }
    }

    private static string? ExtractFirstEnglishCommonName(JsonElement element) {
        if (!element.TryGetProperty("common_names", out var commonNamesElement) || commonNamesElement.ValueKind != JsonValueKind.Array) {
            return null;
        }

        foreach (var item in commonNamesElement.EnumerateArray()) {
            if (item.ValueKind != JsonValueKind.Object) {
                continue;
            }

            var language = TryGetString(item, "language");
            if (!string.Equals(language, "eng", System.StringComparison.OrdinalIgnoreCase)
                && !string.Equals(language, "English", System.StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var name = TryGetString(item, "name");
            if (!string.IsNullOrWhiteSpace(name)) {
                return name;
            }
        }

        return null;
    }

    private static string? TryGetString(JsonElement element, string propertyName) {
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
}

internal sealed record TaxaTaxonomyInfo(
    long SisId,
    string? ScientificName,
    string? KingdomName,
    string? PhylumName,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string? GenusName,
    string? SpeciesName,
    string? CommonName);
