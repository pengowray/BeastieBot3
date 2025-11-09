using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BeastieBot3;

internal static class IucnScientificNameVerifier {
    public static ScientificNameVerificationResult Analyze(IEnumerable<IucnTaxonomyRow> rows, int maxSamples) {
        if (rows is null) {
            throw new ArgumentNullException(nameof(rows));
        }

        if (maxSamples <= 0) {
            maxSamples = 5;
        }

        var result = new ScientificNameVerificationResult(maxSamples);

        foreach (var row in rows) {
            result.TotalRows++;

            var composition = ScientificNameComposer.Compose(row);
            result.RegisterClassification(composition.Classification);
            result.RegisterInfraType(row.InfraType);

            var normalizedAssess = NormalizeName(row.ScientificNameAssessments);
            var normalizedTaxonomy = NormalizeName(row.ScientificNameTaxonomy);
            var normalizedReconstructed = NormalizeName(composition.FullName);
            var primaryName = normalizedAssess ?? normalizedTaxonomy;

            if (row.ScientificNameAssessments is null) {
                result.NullScientificNameCount++;
            }

            if (row.ScientificNameTaxonomy is null) {
                result.NullTaxonomyScientificNameCount++;
            }

            if (normalizedAssess is not null && normalizedTaxonomy is not null && !string.Equals(normalizedAssess, normalizedTaxonomy, StringComparison.Ordinal)) {
                result.FieldMismatchCount++;
                result.AddSample(ScientificNameMismatchKind.FieldDisagreement, BuildSample(row, composition, normalizedAssess, normalizedTaxonomy, normalizedReconstructed, "scientificName vs scientificName:1"));
            }

            if (primaryName is not null && normalizedReconstructed is not null && !string.Equals(primaryName, normalizedReconstructed, StringComparison.Ordinal)) {
                result.ReconstructionMismatchCount++;
                var detail = normalizedAssess is null ? "using taxonomy fallback" : "using assessments field";
                result.AddSample(ScientificNameMismatchKind.ReconstructionFailure, BuildSample(row, composition, primaryName, normalizedTaxonomy, normalizedReconstructed, detail));
            }

            if (primaryName is null) {
                continue;
            }

            if (!StartsWithGenus(primaryName, row.GenusName)) {
                result.GenusMismatchCount++;
                result.AddSample(ScientificNameMismatchKind.GenusMismatch, BuildSample(row, composition, primaryName, normalizedTaxonomy, normalizedReconstructed, "genus mismatch"));
            }

            if (!ContainsComponent(primaryName, row.SpeciesName)) {
                result.SpeciesMismatchCount++;
                result.AddSample(ScientificNameMismatchKind.SpeciesMismatch, BuildSample(row, composition, primaryName, normalizedTaxonomy, normalizedReconstructed, "species missing"));
            }

            if (!string.IsNullOrWhiteSpace(row.InfraName) && !ContainsComponent(primaryName, row.InfraName)) {
                result.InfraNameMismatchCount++;
                result.AddSample(ScientificNameMismatchKind.InfraNameMismatch, BuildSample(row, composition, primaryName, normalizedTaxonomy, normalizedReconstructed, "infra name missing"));
            }

            if (!string.IsNullOrWhiteSpace(row.SubpopulationName) && !EndsWithComponent(primaryName, row.SubpopulationName)) {
                result.SubpopulationMismatchCount++;
                result.AddSample(ScientificNameMismatchKind.SubpopulationMismatch, BuildSample(row, composition, primaryName, normalizedTaxonomy, normalizedReconstructed, "subpopulation mismatch"));
            }
        }

        return result;
    }

    private static ScientificNameMismatchSample BuildSample(IucnTaxonomyRow row, ScientificNameComposition composition, string? normalizedAssess, string? normalizedTaxonomy, string? normalizedReconstructed, string detail) {
        return new ScientificNameMismatchSample(
            row.AssessmentId,
            row.InternalTaxonId,
            composition.Classification.ToString(),
            row.ScientificNameAssessments,
            row.ScientificNameTaxonomy,
            composition.FullName,
            row.InfraType,
            row.SubpopulationName,
            row.KingdomName,
            detail,
            normalizedAssess,
            normalizedTaxonomy,
            normalizedReconstructed
        );
    }

    private static string? NormalizeName(string? value) {
        if (value is null) {
            return null;
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
                '\r' => ' ',
                '\n' => ' ',
                '\t' => ' ',
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

    private static bool StartsWithGenus(string text, string genus) {
        if (string.IsNullOrWhiteSpace(genus)) {
            return true;
        }

        var trimmedGenus = genus.Trim();
        if (trimmedGenus.Length == 0) {
            return true;
        }

        if (text.StartsWith(trimmedGenus, StringComparison.Ordinal)) {
            if (text.Length == trimmedGenus.Length) {
                return true;
            }

            var nextChar = text[trimmedGenus.Length];
            return IsBoundary(nextChar);
        }

        return false;
    }

    private static bool ContainsComponent(string text, string component) {
        if (string.IsNullOrWhiteSpace(component)) {
            return true;
        }

        var normalizedComponent = NormalizeName(component);
        if (normalizedComponent is null || normalizedComponent.Length == 0) {
            return true;
        }

        var index = text.IndexOf(normalizedComponent, StringComparison.Ordinal);
        while (index >= 0) {
            var beforeOk = index == 0 || IsBoundary(text[index - 1]);
            var afterIndex = index + normalizedComponent.Length;
            var afterOk = afterIndex >= text.Length || IsBoundary(text[afterIndex]);
            if (beforeOk && afterOk) {
                return true;
            }
            index = text.IndexOf(normalizedComponent, index + 1, StringComparison.Ordinal);
        }

        return false;
    }

    private static bool EndsWithComponent(string text, string component) {
        if (string.IsNullOrWhiteSpace(component)) {
            return true;
        }

        var normalizedComponent = NormalizeName(component);
        if (normalizedComponent is null || normalizedComponent.Length == 0) {
            return true;
        }

        if (!text.EndsWith(normalizedComponent, StringComparison.Ordinal)) {
            return false;
        }

        if (text.Length == normalizedComponent.Length) {
            return true;
        }

        var preceding = text[text.Length - normalizedComponent.Length - 1];
        return IsBoundary(preceding);
    }

    private static bool IsBoundary(char ch) {
        if (char.IsLetterOrDigit(ch)) {
            return false;
        }
        return ch != '_';
    }
}

internal sealed class ScientificNameVerificationResult {
    private readonly int _maxSamples;
    private readonly Dictionary<ScientificNameMismatchKind, List<ScientificNameMismatchSample>> _samples;

    public ScientificNameVerificationResult(int maxSamples) {
        _maxSamples = maxSamples;
        _samples = Enum.GetValues<ScientificNameMismatchKind>()
            .ToDictionary(kind => kind, _ => new List<ScientificNameMismatchSample>(), EqualityComparer<ScientificNameMismatchKind>.Default);
        InfraTypeCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
    }

    public long TotalRows { get; set; }
    public long NullScientificNameCount { get; set; }
    public long NullTaxonomyScientificNameCount { get; set; }
    public long FieldMismatchCount { get; set; }
    public long ReconstructionMismatchCount { get; set; }
    public long GenusMismatchCount { get; set; }
    public long SpeciesMismatchCount { get; set; }
    public long InfraNameMismatchCount { get; set; }
    public long SubpopulationMismatchCount { get; set; }
    public long SpeciesOrHigherCount { get; private set; }
    public long InfraspecificCount { get; private set; }
    public long SubpopulationCount { get; private set; }
    public Dictionary<string, long> InfraTypeCounts { get; }

    public IReadOnlyList<ScientificNameMismatchSample> GetSamples(ScientificNameMismatchKind kind) => _samples[kind];

    public void AddSample(ScientificNameMismatchKind kind, ScientificNameMismatchSample sample) {
        var bucket = _samples[kind];
        if (bucket.Count < _maxSamples) {
            bucket.Add(sample);
        }
    }

    public void RegisterClassification(NameClassification classification) {
        switch (classification) {
            case NameClassification.SpeciesOrHigher:
                SpeciesOrHigherCount++;
                break;
            case NameClassification.Infraspecific:
                InfraspecificCount++;
                break;
            case NameClassification.Subpopulation:
                SubpopulationCount++;
                break;
            default:
                break;
        }
    }

    public void RegisterInfraType(string? infraType) {
        if (string.IsNullOrWhiteSpace(infraType)) {
            return;
        }

        var key = infraType.Trim();
        if (key.Length == 0) {
            return;
        }

        if (InfraTypeCounts.TryGetValue(key, out var existing)) {
            InfraTypeCounts[key] = existing + 1;
        } else {
            InfraTypeCounts[key] = 1;
        }
    }
}

internal enum ScientificNameMismatchKind {
    FieldDisagreement,
    ReconstructionFailure,
    GenusMismatch,
    SpeciesMismatch,
    InfraNameMismatch,
    SubpopulationMismatch
}

internal sealed record ScientificNameMismatchSample(
    string AssessmentId,
    string InternalTaxonId,
    string Classification,
    string? ScientificNameAssessments,
    string? ScientificNameTaxonomy,
    string? ReconstructedName,
    string? InfraType,
    string? SubpopulationName,
    string KingdomName,
    string Detail,
    string? NormalizedAssess,
    string? NormalizedTaxonomy,
    string? NormalizedReconstructed
);

internal static class ScientificNameComposer {
    public static ScientificNameComposition Compose(IucnTaxonomyRow row) {
        if (row is null) {
            throw new ArgumentNullException(nameof(row));
        }

        var parts = new List<string>();
        var genus = row.GenusName?.Trim();
        if (!string.IsNullOrEmpty(genus)) {
            parts.Add(genus);
        }

        var species = row.SpeciesName?.Trim();
        if (!string.IsNullOrEmpty(species)) {
            parts.Add(species);
        }

        var hasInfra = !string.IsNullOrWhiteSpace(row.InfraName);
        var infraName = row.InfraName?.Trim();
        var hasSubpopulation = !string.IsNullOrWhiteSpace(row.SubpopulationName);
        string? marker = null;
        var existingMarker = InferExistingMarker(row);

        if (hasInfra && !string.IsNullOrEmpty(infraName)) {
            marker = existingMarker;
            if (marker is null && !hasSubpopulation) {
                marker = GetInfraMarker(row);
            }
            if (!string.IsNullOrEmpty(marker)) {
                parts.Add(marker);
            }
            parts.Add(infraName!);
        }

        var baseName = string.Join(" ", parts);
        var classification = hasInfra ? NameClassification.Infraspecific : NameClassification.SpeciesOrHigher;

        var subpopulation = hasSubpopulation ? row.SubpopulationName!.Trim() : null;
        var fullName = baseName;
        if (!string.IsNullOrEmpty(subpopulation)) {
            classification = NameClassification.Subpopulation;
            fullName = baseName.Length == 0 ? subpopulation! : baseName + " " + subpopulation;
        }

        return new ScientificNameComposition(baseName, fullName, marker, classification, hasInfra);
    }

    private static string? GetInfraMarker(IucnTaxonomyRow row) {
        if (string.IsNullOrWhiteSpace(row.InfraType)) {
            return null;
        }

        var trimmed = row.InfraType.Trim();
        if (trimmed.Length == 0) {
            return null;
        }

        if (trimmed.Equals("variety", StringComparison.OrdinalIgnoreCase)) {
            return "var.";
        }

        if (trimmed.Equals("subspecies (plantae)", StringComparison.OrdinalIgnoreCase)) {
            return "subsp.";
        }

        if (trimmed.Equals("subspecies", StringComparison.OrdinalIgnoreCase)) {
            return string.Equals(row.KingdomName, "PLANTAE", StringComparison.OrdinalIgnoreCase)
                ? "subsp."
                : "ssp.";
        }

        return trimmed;
    }

    private static string? InferExistingMarker(IucnTaxonomyRow row) {
        if (string.IsNullOrWhiteSpace(row.InfraName)) {
            return null;
        }

        var baseName = row.ScientificNameAssessments ?? row.ScientificNameTaxonomy;
        if (string.IsNullOrWhiteSpace(baseName)) {
            return null;
        }

        var tokens = baseName.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0) {
            return null;
        }

        var infraName = row.InfraName.Trim();
        var comparer = StringComparer.OrdinalIgnoreCase;

        for (var i = 0; i < tokens.Length; i++) {
            if (!comparer.Equals(tokens[i], infraName)) {
                continue;
            }

            if (i == 0) {
                continue;
            }

            var candidate = tokens[i - 1];
            if (IsKnownMarker(candidate)) {
                return candidate;
            }
        }

        return null;
    }

    private static bool IsKnownMarker(string candidate) {
        if (string.IsNullOrEmpty(candidate)) {
            return false;
        }

        var normalized = candidate.TrimEnd('.');
        return normalized.Length > 0 && normalized switch {
            var value when value.Equals("ssp", StringComparison.OrdinalIgnoreCase) => true,
            var value when value.Equals("subsp", StringComparison.OrdinalIgnoreCase) => true,
            var value when value.Equals("var", StringComparison.OrdinalIgnoreCase) => true,
            var value when value.Equals("variety", StringComparison.OrdinalIgnoreCase) => true,
            _ => false
        };
    }
}

internal sealed record ScientificNameComposition(
    string BaseName,
    string FullName,
    string? InfraMarker,
    NameClassification Classification,
    bool HasInfra
);

internal enum NameClassification {
    SpeciesOrHigher,
    Infraspecific,
    Subpopulation
}