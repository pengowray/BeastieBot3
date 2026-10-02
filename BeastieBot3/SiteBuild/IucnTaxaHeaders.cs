using System.Globalization;
using System.Text.Json;

// The assessment headers in a cached /api/v4/taxa payload: one per assessment of the taxon, with
// its id, `latest` flag, year published and scopes. "Latest" is taken from here, not from the
// assessment payload's own flag: a payload downloaded before a newer assessment was published still
// says latest=true (taxon 193274's 2019 payload, downloaded in November 2025).
//
// Predecessors. An errata version replaces an assessment and keeps its year published, so the
// assessments it may have replaced are the taxon's other assessments that are no longer latest,
// were published the same year and share a scope. In 2026-1, 4,644 of the 4,768 latest global
// errata versions have exactly one such assessment; 60 have two or more and all are returned.

namespace BeastieBot3.SiteBuild;

internal sealed record IucnAssessmentHeader(
    long AssessmentId,
    long? TaxonId,
    bool Latest,
    string? YearPublished,
    IReadOnlyList<string> ScopeCodes);

internal static class IucnTaxaHeaders {
    private const string GlobalScopeCode = "1";

    /// The headers in taxa payload order; entries without an assessment id are skipped.
    public static IReadOnlyList<IucnAssessmentHeader> Read(JsonElement taxaRoot) {
        if (taxaRoot.ValueKind != JsonValueKind.Object
            || !taxaRoot.TryGetProperty("assessments", out var assessments)
            || assessments.ValueKind != JsonValueKind.Array) {
            return Array.Empty<IucnAssessmentHeader>();
        }
        var headers = new List<IucnAssessmentHeader>();
        foreach (var header in assessments.EnumerateArray()) {
            if (header.ValueKind != JsonValueKind.Object) continue;
            if (ReadLong(header, "assessment_id") is not { } id) continue;
            headers.Add(new IucnAssessmentHeader(
                id,
                ReadLong(header, "sis_taxon_id"),
                IsTrue(header, "latest"),
                ReadString(header, "year_published")?.Trim(),
                ReadScopeCodes(header)));
        }
        return headers;
    }

    public static bool IsGlobal(IucnAssessmentHeader header) => header.ScopeCodes.Contains(GlobalScopeCode);

    /// The assessments an errata version with this id may have replaced (see the file comment).
    /// Empty when the id is not among the headers.
    public static IReadOnlyList<long> PredecessorIds(IReadOnlyList<IucnAssessmentHeader> headers, long assessmentId) {
        var target = headers.FirstOrDefault(h => h.AssessmentId == assessmentId);
        if (target is null || string.IsNullOrEmpty(target.YearPublished)) return Array.Empty<long>();
        return headers
            .Where(h => h.AssessmentId != assessmentId
                && !h.Latest
                && string.Equals(h.YearPublished, target.YearPublished, StringComparison.Ordinal)
                && (h.TaxonId is null || target.TaxonId is null || h.TaxonId == target.TaxonId)
                && SharesScope(h, target))
            .Select(h => h.AssessmentId)
            .ToList();
    }

    private static bool SharesScope(IucnAssessmentHeader a, IucnAssessmentHeader b) =>
        (a.ScopeCodes.Count == 0 && b.ScopeCodes.Count == 0)
        || a.ScopeCodes.Intersect(b.ScopeCodes, StringComparer.Ordinal).Any();

    private static IReadOnlyList<string> ReadScopeCodes(JsonElement header) {
        if (!header.TryGetProperty("scopes", out var scopes) || scopes.ValueKind != JsonValueKind.Array) {
            return Array.Empty<string>();
        }
        var codes = new List<string>();
        foreach (var scope in scopes.EnumerateArray()) {
            if (scope.ValueKind == JsonValueKind.Object && ReadString(scope, "code") is { Length: > 0 } code) codes.Add(code.Trim());
        }
        return codes;
    }

    private static bool IsTrue(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value)
        && (value.ValueKind == JsonValueKind.True
            || (value.ValueKind == JsonValueKind.String && bool.TryParse(value.GetString(), out var parsed) && parsed));

    private static long? ReadLong(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.Number when value.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n,
            _ => null,
        };
    }

    private static string? ReadString(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }
}
