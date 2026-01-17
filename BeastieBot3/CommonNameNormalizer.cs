using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3;

/// <summary>
/// Normalizes common names for comparison and display.
/// - For matching: strips all non-alphanumeric characters, lowercase
/// - For display: applies capitalization rules from caps.txt
/// </summary>
internal static class CommonNameNormalizer {
    // Pattern to match Wikipedia disambiguation suffixes like "(fish)" or "(mammal)"
    private static readonly Regex DisambiguationSuffixPattern = new(@"\s*\([^)]+\)\s*$", RegexOptions.Compiled);

    // Pattern to match all non-alphanumeric characters (for normalized matching)
    private static readonly Regex NonAlphanumericPattern = new(@"[^a-z0-9]", RegexOptions.Compiled);

    // Pattern for splitting on word boundaries (keeping hyphens with words)
    private static readonly Regex WordSplitPattern = new(@"[\s\-']+", RegexOptions.Compiled);

    /// <summary>
    /// Normalizes a common name for comparison purposes.
    /// Removes all non-alphanumeric characters and converts to lowercase.
    /// Example: "Buff-breasted Paradise-Kingfisher" -> "buffbreastedparadisekingfisher"
    /// </summary>
    public static string? NormalizeForMatching(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }

        // First, remove any Wikipedia disambiguation suffix like "(fish)"
        var cleaned = DisambiguationSuffixPattern.Replace(name.Trim(), "");

        // Convert to lowercase
        var lower = cleaned.ToLowerInvariant();

        // Remove all non-alphanumeric characters
        var normalized = NonAlphanumericPattern.Replace(lower, "");

        return string.IsNullOrEmpty(normalized) ? null : normalized;
    }

    /// <summary>
    /// Removes Wikipedia disambiguation suffix from a name.
    /// Example: "Red fox (mammal)" -> "Red fox"
    /// </summary>
    public static string RemoveDisambiguationSuffix(string name) {
        return DisambiguationSuffixPattern.Replace(name.Trim(), "").Trim();
    }

    /// <summary>
    /// Applies capitalization rules to a common name for display.
    /// First word is always title case, subsequent words follow caps rules.
    /// </summary>
    public static string ApplyCapitalization(string name, Func<string, string?> capsLookup) {
        if (string.IsNullOrWhiteSpace(name)) {
            return name;
        }

        // Remove disambiguation suffix first
        var cleaned = RemoveDisambiguationSuffix(name);

        // Split into words, preserving separators
        var result = new StringBuilder();
        var isFirstWord = true;
        var lastEnd = 0;

        foreach (Match match in Regex.Matches(cleaned, @"[\w']+")) {
            // Add any separator characters before this word
            if (match.Index > lastEnd) {
                result.Append(cleaned.Substring(lastEnd, match.Index - lastEnd));
            }

            var word = match.Value;
            string capitalizedWord;

            if (isFirstWord) {
                // First word is always title case
                capitalizedWord = ToTitleCase(word);
                isFirstWord = false;
            } else {
                // Look up correct capitalization
                var correctForm = capsLookup(word.ToLowerInvariant());
                if (correctForm != null) {
                    capitalizedWord = correctForm;
                } else {
                    // Default to lowercase for unknown words (common name convention)
                    capitalizedWord = word.ToLowerInvariant();
                }
            }

            result.Append(capitalizedWord);
            lastEnd = match.Index + match.Length;
        }

        // Add any trailing characters
        if (lastEnd < cleaned.Length) {
            result.Append(cleaned.Substring(lastEnd));
        }

        return result.ToString();
    }

    /// <summary>
    /// Extracts individual words from a common name for caps checking.
    /// Returns words in their original form (preserving case as found).
    /// </summary>
    public static IReadOnlyList<string> ExtractWords(string name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return Array.Empty<string>();
        }

        var cleaned = RemoveDisambiguationSuffix(name);
        var words = new List<string>();

        foreach (Match match in Regex.Matches(cleaned, @"[\w']+")) {
            var word = match.Value;
            // Skip single characters and numbers
            if (word.Length > 1 && !int.TryParse(word, out _)) {
                words.Add(word);
            }
        }

        return words;
    }

    /// <summary>
    /// Checks if a common name might be a scientific name (should be rejected).
    /// </summary>
    public static bool LooksLikeScientificName(string name, string? genus, string? specificEpithet) {
        if (string.IsNullOrWhiteSpace(name)) {
            return false;
        }

        var normalized = name.ToLowerInvariant().Trim();

        // Check if it matches the genus
        if (!string.IsNullOrWhiteSpace(genus) && normalized.Equals(genus.ToLowerInvariant(), StringComparison.Ordinal)) {
            return true;
        }

        // Check if it matches the specific epithet
        if (!string.IsNullOrWhiteSpace(specificEpithet) && normalized.Equals(specificEpithet.ToLowerInvariant(), StringComparison.Ordinal)) {
            return true;
        }

        // Check if it looks like a binomial (two italic-style words)
        var parts = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 2) {
            // If both parts are single lowercase words without spaces, might be scientific
            if (parts[0].All(char.IsLetter) && parts[1].All(char.IsLetter)) {
                // Check if it matches "Genus species" pattern
                if (!string.IsNullOrWhiteSpace(genus) && !string.IsNullOrWhiteSpace(specificEpithet)) {
                    var expectedBinomial = $"{genus.ToLowerInvariant()} {specificEpithet.ToLowerInvariant()}";
                    if (normalized.Equals(expectedBinomial, StringComparison.Ordinal)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Converts a word to title case (first letter uppercase, rest lowercase).
    /// </summary>
    private static string ToTitleCase(string word) {
        if (string.IsNullOrEmpty(word)) {
            return word;
        }
        if (word.Length == 1) {
            return word.ToUpperInvariant();
        }
        return char.ToUpperInvariant(word[0]) + word.Substring(1).ToLowerInvariant();
    }

    /// <summary>
    /// Finds words in a common name that are not in the caps rules dictionary.
    /// Returns only non-first words (since first word is always title case).
    /// </summary>
    public static IReadOnlyList<string> FindMissingCapsWords(string name, Func<string, bool> capsExists) {
        var words = ExtractWords(name);
        var missing = new List<string>();

        // Skip first word (always title case regardless of dictionary)
        for (var i = 1; i < words.Count; i++) {
            var word = words[i];
            var lower = word.ToLowerInvariant();

            // Skip if it's in the dictionary
            if (capsExists(lower)) {
                continue;
            }

            // Skip obviously lowercase words (articles, prepositions, etc. that are commonly lowercase)
            if (IsCommonLowercaseWord(lower)) {
                continue;
            }

            // This word might need a caps rule
            missing.Add(word);
        }

        return missing;
    }

    /// <summary>
    /// Common lowercase words that don't need explicit caps rules.
    /// </summary>
    private static bool IsCommonLowercaseWord(string word) {
        // Common articles, prepositions, and conjunctions that are always lowercase
        return word switch {
            "a" or "an" or "the" => true,
            "and" or "or" or "but" or "nor" => true,
            "of" or "in" or "on" or "at" or "to" or "for" or "with" or "by" or "from" => true,
            "as" or "is" or "it" => true,
            _ => false
        };
    }

    /// <summary>
    /// Determines if a word should be capitalized based on common patterns.
    /// Returns true if the word is likely a proper noun (geographic, personal name, etc.).
    /// </summary>
    public static bool ShouldLikelyBeCapitalized(string word) {
        if (string.IsNullOrWhiteSpace(word) || word.Length < 2) {
            return false;
        }

        // Already has uppercase in original form (except first letter)
        if (word.Length > 1 && word.Substring(1).Any(char.IsUpper)) {
            return true;
        }

        // Common patterns for proper nouns in species names
        var lower = word.ToLowerInvariant();

        // Geographic suffixes
        if (lower.EndsWith("ean") || lower.EndsWith("ian") || lower.EndsWith("ese") || lower.EndsWith("ish")) {
            return true;
        }

        // Possessive forms often indicate personal names
        if (lower.EndsWith("'s") || lower.EndsWith("s'")) {
            return true;
        }

        return false;
    }
}
