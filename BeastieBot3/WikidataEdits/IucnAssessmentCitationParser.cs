using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using BeastieBot3.Iucn.Citations;

// Reads one cached /api/v4/assessment/{id} payload into an IucnGlobalAssessment: the fields a
// Wikidata P141 statement and its reference need, including the citation and the people credited.
// IucnAssessmentJsonParser covers the CSV-shaped projection and ignores all of that.
//
// Field names checked against the 2026-1 cache (Aug 2026): url, citation, year_published (a string),
// assessment_date ("2019-09-02T01:00:00.000+01:00"), criteria, red_list_category {code, version},
// possibly_extinct, possibly_extinct_in_the_wild, scopes [{code}], errata [{reason}],
// credits [{credit_type_name, full, value[]}], taxon {sis_id, scientific_name, infrarank}.
//
// Credits. IUCN's type names are "assessor", "evaluator", "contributor", "facilitators" and
// "institutions" (plural, as sent). CreditNameSplitter splits each credit's `full` string into
// names, using the count of distinct value[] entries to confirm a doubtful split. When a payload
// repeats a credits block, the repeat adds only the names the earlier blocks of that type don't
// already hold.

namespace BeastieBot3.WikidataEdits;

internal static class IucnAssessmentCitationParser {
    private const string GlobalScopeCode = "1";

    public static IucnGlobalAssessment? Parse(string json, DateTime downloadedAtUtc) {
        if (string.IsNullOrWhiteSpace(json)) return null;
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            return null;
        }
        using (document) {
            return Parse(document.RootElement, downloadedAtUtc);
        }
    }

    /// Null when the assessment has no Global scope, or lacks an id, taxon id, name or category.
    internal static IucnGlobalAssessment? Parse(JsonElement root, DateTime downloadedAtUtc) {
        if (root.ValueKind != JsonValueKind.Object) return null;
        if (!HasGlobalScope(root)) return null;

        var assessmentId = GetLong(root, "assessment_id");
        var taxon = root.TryGetProperty("taxon", out var taxonEl) && taxonEl.ValueKind == JsonValueKind.Object
            ? taxonEl
            : (JsonElement?)null;
        var taxonId = GetLong(root, "sis_taxon_id") ?? (taxon is { } t1 ? GetLong(t1, "sis_id") : null);
        var scientificName = (taxon is { } t2 ? GetString(t2, "scientific_name") : null)
            ?? GetString(root, "taxon_scientific_name");
        var (categoryCode, criteriaVersion) = ReadCategory(root);
        if (assessmentId is null || taxonId is null
            || string.IsNullOrWhiteSpace(scientificName) || string.IsNullOrWhiteSpace(categoryCode)) {
            return null;
        }

        var rawCitation = GetString(root, "citation");
        var citation = string.IsNullOrWhiteSpace(rawCitation) ? null : IucnCitationText.StripAccessedOn(rawCitation);
        var url = GetString(root, "url");

        return new IucnGlobalAssessment {
            TaxonId = taxonId.Value,
            AssessmentId = assessmentId.Value,
            ScientificName = scientificName.Trim(),
            IsInfrarank = taxon is { } t3 && GetBool(t3, "infrarank"),
            CategoryCode = categoryCode.Trim(),
            PossiblyExtinct = GetBool(root, "possibly_extinct"),
            PossiblyExtinctInTheWild = GetBool(root, "possibly_extinct_in_the_wild"),
            Criteria = NullIfBlank(GetString(root, "criteria")),
            CriteriaVersion = NullIfBlank(criteriaVersion),
            YearPublished = int.TryParse(GetString(root, "year_published"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var year) ? year : null,
            AssessmentDate = ParseAssessmentDate(GetString(root, "assessment_date")),
            Url = string.IsNullOrWhiteSpace(url)
                ? $"https://www.iucnredlist.org/species/{taxonId.Value}/{assessmentId.Value}"
                : url.Trim(),
            Citation = string.IsNullOrWhiteSpace(citation) ? null : citation,
            Doi = IucnCitationText.ExtractDoi(rawCitation),
            Credits = ReadCredits(root),
            DownloadedAtUtc = downloadedAtUtc,
            IsAmended = root.TryGetProperty("errata", out var errata)
                && errata.ValueKind == JsonValueKind.Array && errata.GetArrayLength() > 0,
        };
    }

    /// True when scopes[] has an entry with code "1" (Global). Works on a full assessment and on
    /// the assessment headers embedded in a /taxa payload, which carry the same scopes array.
    internal static bool HasGlobalScope(JsonElement assessment) {
        if (!assessment.TryGetProperty("scopes", out var scopes) || scopes.ValueKind != JsonValueKind.Array) return false;
        foreach (var scope in scopes.EnumerateArray()) {
            if (scope.ValueKind == JsonValueKind.Object && GetString(scope, "code") == GlobalScopeCode) return true;
        }
        return false;
    }

    /// True when scopes[] is missing or empty: the handful of assessments IUCN publishes with no scope.
    internal static bool HasBlankScope(JsonElement assessment) =>
        !assessment.TryGetProperty("scopes", out var scopes)
        || scopes.ValueKind != JsonValueKind.Array
        || scopes.GetArrayLength() == 0;

    // ------------------------------------------------------------ citation

    private static DateOnly? ParseAssessmentDate(string? text) {
        if (string.IsNullOrWhiteSpace(text)) return null;
        // The offset is IUCN's server clock (+00:00 or +01:00 around midnight); the date as written is
        // the date IUCN means, so take it before any conversion to UTC can move it.
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)) {
            return DateOnly.FromDateTime(parsed.DateTime);
        }
        return DateOnly.TryParse(text, CultureInfo.InvariantCulture, out var date) ? date : null;
    }

    private static (string? Code, string? Version) ReadCategory(JsonElement root) {
        if (root.TryGetProperty("red_list_category", out var category) && category.ValueKind == JsonValueKind.Object) {
            return (GetString(category, "code"), GetString(category, "version"));
        }
        return (GetString(root, "red_list_category_code"), null);
    }

    // ------------------------------------------------------------ credits

    /// Assessors first, then the other types in the order they first appear; Order counts from 1
    /// within each type.
    internal static IReadOnlyList<IucnCredit> ReadCredits(JsonElement root) {
        if (!root.TryGetProperty("credits", out var credits) || credits.ValueKind != JsonValueKind.Array) {
            return Array.Empty<IucnCredit>();
        }

        var byType = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var typeOrder = new List<string>();
        foreach (var credit in credits.EnumerateArray()) {
            if (credit.ValueKind != JsonValueKind.Object) continue;
            var type = GetString(credit, "credit_type_name")?.Trim().ToLowerInvariant();
            var full = GetString(credit, "full");
            if (string.IsNullOrEmpty(type) || string.IsNullOrWhiteSpace(full)) continue;

            if (!byType.TryGetValue(type, out var names)) {
                names = new List<string>();
                byType[type] = names;
                typeOrder.Add(type);
            }
            CreditNameSplitter.AddNamesNotYetHeld(names, CreditNameSplitter.Split(full, CreditNameSplitter.DistinctValueCount(credit)));
        }

        var result = new List<IucnCredit>();
        foreach (var type in typeOrder.OrderBy(t => t == "assessor" ? 0 : 1)) {
            var order = 0;
            foreach (var name in byType[type]) {
                result.Add(new IucnCredit(type, name, ++order));
            }
        }
        return result;
    }

    // ------------------------------------------------------------ JSON helpers

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static bool GetBool(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return false;
        return value.ValueKind switch {
            JsonValueKind.True => true,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            _ => false,
        };
    }

    private static long? GetLong(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.Number when value.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n,
            _ => null,
        };
    }

    private static string? GetString(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }
}
