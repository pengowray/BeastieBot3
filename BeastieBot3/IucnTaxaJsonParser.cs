using System;
using System.Collections.Generic;
using System.Text.Json;

namespace BeastieBot3;

internal static class IucnTaxaJsonParser {
    public static ParsedTaxaDocument Parse(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var rootSisId = root.GetProperty("sis_id").GetInt64();

        var mappings = new List<TaxaLookupRow> {
            new(rootSisId, rootSisId, "species")
        };

        if (root.TryGetProperty("taxon", out var taxonElement)) {
            AppendScopeArray(taxonElement, "species_taxa", "species", rootSisId, mappings);
            AppendScopeArray(taxonElement, "subpopulation_taxa", "subpopulation", rootSisId, mappings);
            AppendScopeArray(taxonElement, "infrarank_taxa", "infrarank", rootSisId, mappings);
        }

        var assessments = new List<IucnAssessmentHeader>();
        if (root.TryGetProperty("assessments", out var assessmentsElement) && assessmentsElement.ValueKind == JsonValueKind.Array) {
            foreach (var item in assessmentsElement.EnumerateArray()) {
                if (!item.TryGetProperty("assessment_id", out var assessmentIdElement) || assessmentIdElement.ValueKind != JsonValueKind.Number) {
                    continue;
                }

                var assessmentId = assessmentIdElement.GetInt64();
                var sisId = item.TryGetProperty("sis_taxon_id", out var sisElement) && sisElement.ValueKind == JsonValueKind.Number
                    ? sisElement.GetInt64()
                    : rootSisId;
                var latest = false;
                if (item.TryGetProperty("latest", out var latestElement)) {
                    latest = latestElement.ValueKind switch {
                        JsonValueKind.True => true,
                        JsonValueKind.False => false,
                        JsonValueKind.String when bool.TryParse(latestElement.GetString(), out var parsedBool) => parsedBool,
                        _ => latest
                    };
                }
                int? yearPublished = null;
                if (item.TryGetProperty("year_published", out var yearElement)) {
                    switch (yearElement.ValueKind) {
                        case JsonValueKind.String when int.TryParse(yearElement.GetString(), out var parsedYear):
                            yearPublished = parsedYear;
                            break;
                        case JsonValueKind.Number:
                            yearPublished = yearElement.GetInt32();
                            break;
                    }
                }

                assessments.Add(new IucnAssessmentHeader(assessmentId, sisId, latest, yearPublished));
            }
        }

        return new ParsedTaxaDocument(rootSisId, mappings, assessments);
    }

    private static void AppendScopeArray(JsonElement taxonElement, string propertyName, string scopeName, long rootSisId, ICollection<TaxaLookupRow> output) {
        if (!taxonElement.TryGetProperty(propertyName, out var scopeElement) || scopeElement.ValueKind != JsonValueKind.Array) {
            return;
        }

        foreach (var item in scopeElement.EnumerateArray()) {
            if (!item.TryGetProperty("sis_id", out var sisElement) || sisElement.ValueKind != JsonValueKind.Number) {
                continue;
            }

            output.Add(new TaxaLookupRow(sisElement.GetInt64(), rootSisId, scopeName));
        }
    }
}

internal sealed record ParsedTaxaDocument(long RootSisId, IReadOnlyList<TaxaLookupRow> Mappings, IReadOnlyList<IucnAssessmentHeader> Assessments);

internal sealed record IucnAssessmentHeader(long AssessmentId, long SisId, bool Latest, int? YearPublished);
