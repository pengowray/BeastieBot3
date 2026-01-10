using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BeastieBot3;

internal static class IucnDataCleanupAnalyzer {
    public static DataCleanupAnalysisResult Analyze(IEnumerable<IucnTaxonomyRow> rows, int maxSamples) {
        if (rows is null) {
            throw new ArgumentNullException(nameof(rows));
        }

        if (maxSamples <= 0) {
            maxSamples = 5;
        }

        var result = new DataCleanupAnalysisResult(maxSamples);

        foreach (var row in rows) {
            result.TotalRows++;

            AnalyzeScientificNameWhitespace(row, row.ScientificNameAssessments, "scientificName", DataCleanupIssueKind.ScientificNameWhitespace, result);
            AnalyzeScientificNameWhitespace(row, row.ScientificNameTaxonomy, "scientificName:1", DataCleanupIssueKind.TaxonomyScientificNameWhitespace, result);
            AnalyzeFieldWhitespace(row, row.InfraName, "infraName", DataCleanupIssueKind.InfraNameWhitespace, result);
            AnalyzeFieldWhitespace(row, row.SubpopulationName, "subpopulationName", DataCleanupIssueKind.SubpopulationWhitespace, result);
            AnalyzeFieldWhitespace(row, row.Authority, "authority", DataCleanupIssueKind.AuthorityWhitespace, result);
            AnalyzeFieldWhitespace(row, row.InfraAuthority, "infraAuthority", DataCleanupIssueKind.InfraAuthorityWhitespace, result);

            AnalyzeFieldDisagreement(row, result);
            AnalyzeInfraMarker(row, result);
        }

        return result;
    }

    private static void AnalyzeScientificNameWhitespace(IucnTaxonomyRow row, string? value, string fieldName, DataCleanupIssueKind kind, DataCleanupAnalysisResult result) {
        var suggestion = GetWhitespaceSuggestion(value);
        if (suggestion is null) {
            return;
        }

        var field = new DataCleanupFieldSuggestion(fieldName, value, suggestion.Value.NormalizedValue);
        result.AddIssue(kind, row, suggestion.Value.Detail, new[] { field });
    }

    private static void AnalyzeFieldWhitespace(IucnTaxonomyRow row, string? value, string fieldName, DataCleanupIssueKind kind, DataCleanupAnalysisResult result) {
        var suggestion = GetWhitespaceSuggestion(value);
        if (suggestion is null) {
            return;
        }

        var field = new DataCleanupFieldSuggestion(fieldName, value, suggestion.Value.NormalizedValue);
        result.AddIssue(kind, row, suggestion.Value.Detail, new[] { field });
    }

    private static void AnalyzeFieldDisagreement(IucnTaxonomyRow row, DataCleanupAnalysisResult result) {
        if (row.ScientificNameAssessments is null || row.ScientificNameTaxonomy is null) {
            return;
        }

        if (string.Equals(row.ScientificNameAssessments, row.ScientificNameTaxonomy, StringComparison.Ordinal)) {
            return;
        }

        var assessSuggestion = GetWhitespaceSuggestion(row.ScientificNameAssessments);
        var taxonomySuggestion = GetWhitespaceSuggestion(row.ScientificNameTaxonomy);

        var normalizedAssess = assessSuggestion?.NormalizedValue ?? NormalizeWhitespace(row.ScientificNameAssessments);
        var normalizedTaxonomy = taxonomySuggestion?.NormalizedValue ?? NormalizeWhitespace(row.ScientificNameTaxonomy);

        var fields = new List<DataCleanupFieldSuggestion>();

        if (assessSuggestion is not null && !string.Equals(row.ScientificNameAssessments, assessSuggestion.Value.NormalizedValue, StringComparison.Ordinal)) {
            fields.Add(new DataCleanupFieldSuggestion("scientificName", row.ScientificNameAssessments, assessSuggestion.Value.NormalizedValue));
        }

        if (taxonomySuggestion is not null && !string.Equals(row.ScientificNameTaxonomy, taxonomySuggestion.Value.NormalizedValue, StringComparison.Ordinal)) {
            fields.Add(new DataCleanupFieldSuggestion("scientificName:1", row.ScientificNameTaxonomy, taxonomySuggestion.Value.NormalizedValue));
        }

        string detail;
        if (string.Equals(normalizedAssess, normalizedTaxonomy, StringComparison.Ordinal)) {
            detail = "Values differ only by whitespace; normalize both fields to the shared value.";
            if (fields.Count == 0) {
                fields.Add(new DataCleanupFieldSuggestion("scientificName", row.ScientificNameAssessments, normalizedAssess));
                fields.Add(new DataCleanupFieldSuggestion("scientificName:1", row.ScientificNameTaxonomy, normalizedAssess));
            }
        } else {
            detail = "Values differ beyond whitespace normalization; manual review recommended.";
            if (fields.Count == 0) {
                fields.Add(new DataCleanupFieldSuggestion("scientificName", row.ScientificNameAssessments, normalizedAssess));
                fields.Add(new DataCleanupFieldSuggestion("scientificName:1", row.ScientificNameTaxonomy, normalizedTaxonomy));
            }
        }

        result.AddIssue(DataCleanupIssueKind.ScientificNameDisagreement, row, detail, fields);
    }

    private static void AnalyzeInfraMarker(IucnTaxonomyRow row, DataCleanupAnalysisResult result) {
        if (string.IsNullOrWhiteSpace(row.InfraName)) {
            return;
        }

        var trimmed = row.InfraName.Trim();
        var tokens = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length < 2) {
            return;
        }

        var firstToken = tokens[0];
        if (!IsMarkerToken(firstToken)) {
            return;
        }

        var suggestedValue = string.Join(" ", tokens.Skip(1));
        if (string.Equals(trimmed, suggestedValue, StringComparison.Ordinal)) {
            return;
        }

        var detail = $"Remove infrarank marker '{firstToken}' from infraName; markers belong in the scientific name fields.";
        var field = new DataCleanupFieldSuggestion("infraName", row.InfraName, suggestedValue);
        result.AddIssue(DataCleanupIssueKind.InfraNameMarkerPrefix, row, detail, new[] { field });
    }

    private static bool IsMarkerToken(string value) {
        if (string.IsNullOrEmpty(value)) {
            return false;
        }

        var normalized = value.TrimEnd('.');
        return normalized.Length > 0 && normalized switch {
            var marker when marker.Equals("ssp", StringComparison.OrdinalIgnoreCase) => true,
            var marker when marker.Equals("subsp", StringComparison.OrdinalIgnoreCase) => true,
            var marker when marker.Equals("var", StringComparison.OrdinalIgnoreCase) => true,
            var marker when marker.Equals("variety", StringComparison.OrdinalIgnoreCase) => true,
            _ => false
        };
    }

    private static StringCleanupSuggestion? GetWhitespaceSuggestion(string? value) {
        if (value is null) {
            return null;
        }

        var normalized = NormalizeWhitespace(value);
        if (string.Equals(value, normalized, StringComparison.Ordinal)) {
            return null;
        }

        var reasons = new List<string>();

        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal)) {
            reasons.Add("trim leading/trailing spaces");
        }

        if (ContainsSpecialWhitespace(value)) {
            reasons.Add("replace non-breaking or tab characters with spaces");
        }

        if (HasRepeatedWhitespace(value)) {
            reasons.Add("collapse repeated whitespace");
        }

        if (reasons.Count == 0) {
            reasons.Add("normalize whitespace");
        }

        var detail = string.Join("; ", reasons);
        return new StringCleanupSuggestion(normalized, detail);
    }

    private static bool ContainsSpecialWhitespace(string value) {
        foreach (var ch in value) {
            if (ch is '\u00A0' or '\u2007' or '\u202F' or '\u2009' or '\t' or '\r' or '\n') {
                return true;
            }
        }
        return false;
    }

    private static bool HasRepeatedWhitespace(string value) {
        var previousWasWhitespace = false;
        foreach (var ch in value) {
            var isWhitespace = char.IsWhiteSpace(ch);
            if (isWhitespace) {
                if (previousWasWhitespace) {
                    return true;
                }
                previousWasWhitespace = true;
            } else {
                previousWasWhitespace = false;
            }
        }
        return false;
    }

    private static string NormalizeWhitespace(string value) {
        if (value.Length == 0) {
            return value;
        }

        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return string.Empty;
        }

        var builder = new StringBuilder(trimmed.Length);
        var previousWasSpace = false;

        foreach (var ch in trimmed) {
            var normalized = ch switch {
                '\u00A0' => ' ',
                '\u2007' => ' ',
                '\u202F' => ' ',
                '\u2009' => ' ',
                '\t' => ' ',
                '\r' => ' ',
                '\n' => ' ',
                _ => ch
            };

            if (char.IsWhiteSpace(normalized)) {
                if (previousWasSpace) {
                    continue;
                }
                builder.Append(' ');
                previousWasSpace = true;
            } else {
                builder.Append(normalized);
                previousWasSpace = false;
            }
        }

        return builder.ToString();
    }

    private readonly record struct StringCleanupSuggestion(string NormalizedValue, string Detail);
}

internal sealed class DataCleanupAnalysisResult {
    private readonly int _maxSamples;
    private readonly Dictionary<DataCleanupIssueKind, List<DataCleanupIssueSample>> _samples;

    public DataCleanupAnalysisResult(int maxSamples) {
        _maxSamples = maxSamples;
        IssueCounts = Enum.GetValues<DataCleanupIssueKind>()
            .ToDictionary(kind => kind, _ => 0L, EqualityComparer<DataCleanupIssueKind>.Default);
        _samples = Enum.GetValues<DataCleanupIssueKind>()
            .ToDictionary(kind => kind, _ => new List<DataCleanupIssueSample>(), EqualityComparer<DataCleanupIssueKind>.Default);
    }

    public long TotalRows { get; set; }

    public Dictionary<DataCleanupIssueKind, long> IssueCounts { get; }

    public IReadOnlyList<DataCleanupIssueSample> GetSamples(DataCleanupIssueKind kind) => _samples[kind];

    public void AddIssue(DataCleanupIssueKind kind, IucnTaxonomyRow row, string detail, IReadOnlyList<DataCleanupFieldSuggestion> fields) {
        if (fields is null) {
            throw new ArgumentNullException(nameof(fields));
        }

        IssueCounts[kind] = IssueCounts[kind] + 1;

        var bucket = _samples[kind];
        if (bucket.Count < _maxSamples) {
            bucket.Add(new DataCleanupIssueSample(row.AssessmentId, row.TaxonId, detail, fields));
        }
    }
}

internal enum DataCleanupIssueKind {
    ScientificNameWhitespace,
    TaxonomyScientificNameWhitespace,
    ScientificNameDisagreement,
    InfraNameWhitespace,
    InfraNameMarkerPrefix,
    SubpopulationWhitespace,
    AuthorityWhitespace,
    InfraAuthorityWhitespace
}

internal sealed record DataCleanupIssueSample(
    long AssessmentId,
    long TaxonId,
    string Detail,
    IReadOnlyList<DataCleanupFieldSuggestion> Fields
);

internal sealed record DataCleanupFieldSuggestion(
    string FieldName,
    string? CurrentValue,
    string? SuggestedValue
);