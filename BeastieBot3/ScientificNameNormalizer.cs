using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace BeastieBot3;

/// <summary>
/// Normalizes scientific names and generates synonym variants for matching.
/// Handles subgenus variations, rank markers (var., subsp., f., etc.), and various formatting differences.
/// </summary>
internal static class ScientificNameNormalizer {
    // Regex patterns for normalization
    private static readonly Regex SubgenusPattern = new(@"^(\S+)\s*\((\S+)\)\s+(.+)$", RegexOptions.Compiled);
    private static readonly Regex RankMarkerPattern = new(@"\b(var\.|subsp\.|ssp\.|spp\.|f\.|fo\.|subf\.|nothosubsp\.|nothovar\.|cv\.|cultivar|variety|subspecies|forma?)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex MultipleSpacesPattern = new(@"\s{2,}", RegexOptions.Compiled);
    private static readonly Regex AuthorityPattern = new(@"\s*\([^)]*\d{4}[^)]*\)\s*$|\s+[A-Z][a-z]*\.?,?\s+\d{4}\s*$", RegexOptions.Compiled);

    /// <summary>
    /// Normalizes a scientific name for consistent comparison.
    /// - Converts to lowercase
    /// - Removes rank markers (var., subsp., etc.)
    /// - Collapses multiple spaces
    /// - Removes trailing authority if present
    /// </summary>
    public static string? Normalize(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }

        var result = name.Trim();

        // Remove subgenus parentheses if present: "Genus (Subgenus) species" -> "genus species"
        var subgenusMatch = SubgenusPattern.Match(result);
        if (subgenusMatch.Success) {
            result = $"{subgenusMatch.Groups[1].Value} {subgenusMatch.Groups[3].Value}";
        }

        // Remove rank markers
        result = RankMarkerPattern.Replace(result, " ");

        // Remove authority patterns (e.g., "(Linnaeus, 1758)" or "Smith, 1900")
        result = AuthorityPattern.Replace(result, "");

        // Collapse multiple spaces
        result = MultipleSpacesPattern.Replace(result, " ").Trim();

        // Lowercase for consistent matching
        return result.ToLowerInvariant();
    }

    /// <summary>
    /// Generates all synonym variants for a scientific name, including subgenus variations.
    /// </summary>
    public static IReadOnlyList<ScientificNameVariant> GenerateVariants(
        string? genus,
        string? subgenus,
        string? specificEpithet,
        string? infraspecificEpithet,
        string? rankLabel) {
        var variants = new List<ScientificNameVariant>();

        if (string.IsNullOrWhiteSpace(genus)) {
            return variants;
        }

        genus = genus.Trim();
        subgenus = subgenus?.Trim();
        specificEpithet = specificEpithet?.Trim();
        infraspecificEpithet = infraspecificEpithet?.Trim();
        rankLabel = NormalizeRankLabel(rankLabel?.Trim());

        // 1. Basic binomial: "Genus species"
        if (!string.IsNullOrWhiteSpace(specificEpithet)) {
            var binomial = $"{genus} {specificEpithet}";
            variants.Add(new ScientificNameVariant(binomial, Normalize(binomial)!, "canonical"));

            // 2. With subgenus: "Genus (Subgenus) species"
            if (!string.IsNullOrWhiteSpace(subgenus)) {
                var withSubgenus = $"{genus} ({subgenus}) {specificEpithet}";
                variants.Add(new ScientificNameVariant(withSubgenus, Normalize(withSubgenus)!, "subgenus_variant"));

                // 3. Subgenus as genus: "Subgenus species" (in case it was elevated)
                var subgenusAsGenus = $"{subgenus} {specificEpithet}";
                variants.Add(new ScientificNameVariant(subgenusAsGenus, Normalize(subgenusAsGenus)!, "subgenus_variant"));
            }

            // 4. Trinomials with infraspecific epithet
            if (!string.IsNullOrWhiteSpace(infraspecificEpithet)) {
                // Plain trinomial: "Genus species infraname"
                var trinomial = $"{genus} {specificEpithet} {infraspecificEpithet}";
                variants.Add(new ScientificNameVariant(trinomial, Normalize(trinomial)!, "canonical"));

                // With rank label: "Genus species var. infraname"
                if (!string.IsNullOrWhiteSpace(rankLabel)) {
                    var withRank = $"{genus} {specificEpithet} {rankLabel} {infraspecificEpithet}";
                    variants.Add(new ScientificNameVariant(withRank, Normalize(withRank)!, "rank_variant"));

                    // Alternative rank labels
                    foreach (var altRank in GetAlternativeRankLabels(rankLabel)) {
                        var withAltRank = $"{genus} {specificEpithet} {altRank} {infraspecificEpithet}";
                        variants.Add(new ScientificNameVariant(withAltRank, Normalize(withAltRank)!, "rank_variant"));
                    }
                }

                // With subgenus and infraspecific
                if (!string.IsNullOrWhiteSpace(subgenus)) {
                    var trinomialWithSubgenus = $"{genus} ({subgenus}) {specificEpithet} {infraspecificEpithet}";
                    variants.Add(new ScientificNameVariant(trinomialWithSubgenus, Normalize(trinomialWithSubgenus)!, "subgenus_variant"));
                }
            }
        } else {
            // Genus-only
            variants.Add(new ScientificNameVariant(genus, Normalize(genus)!, "canonical"));

            if (!string.IsNullOrWhiteSpace(subgenus)) {
                var genusWithSubgenus = $"{genus} ({subgenus})";
                variants.Add(new ScientificNameVariant(genusWithSubgenus, Normalize(genusWithSubgenus)!, "subgenus_variant"));
            }
        }

        return variants;
    }

    /// <summary>
    /// Generates synonym variants from a full scientific name string.
    /// Attempts to parse genus, subgenus, species, and infraspecific parts.
    /// </summary>
    public static IReadOnlyList<ScientificNameVariant> GenerateVariantsFromFullName(string? fullName) {
        if (string.IsNullOrWhiteSpace(fullName)) {
            return Array.Empty<ScientificNameVariant>();
        }

        var (genus, subgenus, species, infra, rankLabel) = ParseScientificName(fullName);
        return GenerateVariants(genus, subgenus, species, infra, rankLabel);
    }

    /// <summary>
    /// Parses a scientific name into its component parts.
    /// </summary>
    public static (string? Genus, string? Subgenus, string? Species, string? Infra, string? RankLabel) ParseScientificName(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return (null, null, null, null, null);
        }

        var trimmed = name.Trim();
        string? genus = null;
        string? subgenus = null;
        string? species = null;
        string? infra = null;
        string? rankLabel = null;

        // Check for subgenus pattern: "Genus (Subgenus) species ..."
        var subgenusMatch = SubgenusPattern.Match(trimmed);
        if (subgenusMatch.Success) {
            genus = subgenusMatch.Groups[1].Value;
            subgenus = subgenusMatch.Groups[2].Value;
            trimmed = subgenusMatch.Groups[3].Value;
        }

        // Split remaining parts
        var parts = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length == 0) {
            return (genus, subgenus, null, null, null);
        }

        var startIndex = 0;
        if (genus == null) {
            genus = parts[0];
            startIndex = 1;
        }

        if (parts.Length > startIndex) {
            species = parts[startIndex];
            startIndex++;
        }

        // Look for rank marker and infraspecific epithet
        for (var i = startIndex; i < parts.Length; i++) {
            var part = parts[i];
            if (RankMarkerPattern.IsMatch(part)) {
                rankLabel = NormalizeRankLabel(part);
                if (i + 1 < parts.Length) {
                    infra = parts[i + 1];
                }
                break;
            } else if (infra == null && !part.Contains('.') && part.Length > 1 && char.IsLower(part[0])) {
                // Assume it's the infraspecific epithet if lowercase and no rank marker found yet
                infra = part;
            }
        }

        return (genus, subgenus, species, infra, rankLabel);
    }

    /// <summary>
    /// Normalizes rank labels to a standard form.
    /// </summary>
    private static string? NormalizeRankLabel(string? label) {
        if (string.IsNullOrWhiteSpace(label)) {
            return null;
        }

        var lower = label.ToLowerInvariant().TrimEnd('.');
        return lower switch {
            "var" or "variety" => "var.",
            "subsp" or "ssp" or "subspecies" => "subsp.",
            "f" or "fo" or "forma" or "form" => "f.",
            "subf" or "subforma" => "subf.",
            "cv" or "cultivar" => "cv.",
            "nothosubsp" => "nothosubsp.",
            "nothovar" => "nothovar.",
            _ => label.EndsWith('.') ? label : $"{label}."
        };
    }

    /// <summary>
    /// Gets alternative rank labels for synonym generation.
    /// </summary>
    private static IEnumerable<string> GetAlternativeRankLabels(string rankLabel) {
        var lower = rankLabel.ToLowerInvariant().TrimEnd('.');
        return lower switch {
            "var" => new[] { "variety" },
            "subsp" => new[] { "ssp.", "subspecies" },
            "ssp" => new[] { "subsp.", "subspecies" },
            "f" => new[] { "fo.", "forma" },
            _ => Array.Empty<string>()
        };
    }

    /// <summary>
    /// Determines the taxonomic rank from a scientific name or rank string.
    /// </summary>
    public static string DetermineRank(string? explicitRank, string? genus, string? species, string? infra) {
        if (!string.IsNullOrWhiteSpace(explicitRank)) {
            var lower = explicitRank.ToLowerInvariant();
            if (lower.Contains("kingdom")) return "kingdom";
            if (lower.Contains("phylum")) return "phylum";
            if (lower.Contains("class")) return "class";
            if (lower.Contains("order")) return "order";
            if (lower.Contains("family")) return "family";
            if (lower.Contains("genus")) return "genus";
            if (lower.Contains("subsp") || lower.Contains("ssp")) return "subspecies";
            if (lower.Contains("var")) return "variety";
            if (lower.Contains("form") || lower == "f" || lower == "f.") return "form";
            if (lower.Contains("species")) return "species";
        }

        // Infer from parts
        if (!string.IsNullOrWhiteSpace(infra)) {
            return "subspecies"; // Could be variety/form, but subspecies is most common
        }
        if (!string.IsNullOrWhiteSpace(species)) {
            return "species";
        }
        if (!string.IsNullOrWhiteSpace(genus)) {
            return "genus";
        }
        return "unknown";
    }
}

/// <summary>
/// Represents a variant of a scientific name for synonym matching.
/// </summary>
public record ScientificNameVariant(
    string OriginalForm,
    string NormalizedForm,
    string VariantType // "canonical", "subgenus_variant", "rank_variant"
);
