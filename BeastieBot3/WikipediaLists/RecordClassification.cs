using System;
using System.Linq;
using BeastieBot3.Iucn;

namespace BeastieBot3.WikipediaLists;

// Pure classification/scope predicates over an IUCN species record. Extracted from
// WikipediaListGenerator so the count logic lives in one small, testable place and can be
// shared by the generator and its prose/table builders. Import with
// `using static BeastieBot3.WikipediaLists.RecordClassification;` to keep call sites terse.
internal static class RecordClassification {
    public static bool IsSubspecies(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(record.InfraName) && (infraType.Contains("subsp") || infraType.Contains("ssp"));
    }

    public static bool IsVariety(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(record.InfraName) && infraType.Contains("var");
    }

    public static bool IsInfraspecific(IucnSpeciesRecord record) {
        return !string.IsNullOrWhiteSpace(record.InfraName) && !string.IsNullOrWhiteSpace(record.InfraType);
    }

    public static bool IsRegionalAssessment(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
            return true;
        }

        var scopes = record.Scopes;
        if (string.IsNullOrWhiteSpace(scopes)) {
            return false;
        }

        var parts = scopes.Split(new[] { ',', ';', '&' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var hasGlobalScope = parts.Any(part => part.Contains("global", StringComparison.OrdinalIgnoreCase));
        if (hasGlobalScope) {
            return false;
        }

        return parts.Length > 0;
    }

    public static string? GetRegionalScopeLabel(IucnSpeciesRecord record) {
        if (!IsRegionalAssessment(record)) {
            return null;
        }

        if (string.IsNullOrWhiteSpace(record.Scopes)) {
            return null;
        }

        var parts = record.Scopes
            .Split(new[] { ',', ';', '&' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(part => !part.Contains("global", StringComparison.OrdinalIgnoreCase))
            .ToList();

        return parts.Count == 0 ? null : string.Join(", ", parts);
    }

    public static string GetParentSpeciesKey(IucnSpeciesRecord record) {
        return $"{record.GenusName?.ToLowerInvariant()}|{record.SpeciesName?.ToLowerInvariant()}";
    }
}
