using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

// Two-mode normalizer for common names: ForMatching() strips all non-alphanumeric
// and lowercases for database comparisons; ForDisplay() applies caps.txt rules
// for proper rendering. Strips Wikipedia disambiguation suffixes like "(fish)".
// Used by CommonNameStore for indexing and StoreBackedCommonNameProvider for output.

namespace BeastieBot3.CommonNames;

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

    // Collapses runs of whitespace (incl. the stray double-spaces seen in source vernaculars).
    private static readonly Regex WhitespaceRunPattern = new(@"\s+", RegexOptions.Compiled);

    /// <summary>
    /// Normalizes display typography in a name: straightens curly quotes/apostrophes to ASCII and
    /// collapses internal whitespace runs to a single space. Applied before capitalization so the
    /// common-names report and the Wikipedia list generator emit consistent, MoS-friendly text.
    /// </summary>
    public static string NormalizeDisplayTypography(string name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return name;
        }

        var sb = new StringBuilder(name.Length);
        foreach (var ch in name) {
            switch (ch) {
                case '‘': // left single quotation mark
                case '’': // right single quotation mark (curly apostrophe)
                case 'ʼ': // modifier letter apostrophe
                    sb.Append('\'');
                    break;
                case '“': // left double quotation mark
                case '”': // right double quotation mark
                    sb.Append('"');
                    break;
                default:
                    sb.Append(ch);
                    break;
            }
        }

        return WhitespaceRunPattern.Replace(sb.ToString().Trim(), " ");
    }

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

    // Longest multi-word caps phrase we will try to match (e.g. "lesser bird of paradise").
    private const int MaxPhraseWords = 5;

    /// <summary>
    /// Applies capitalization rules to a common name for display, driven by the caps.txt rule set.
    /// The first word is title-cased; subsequent words are lowercased unless a rule keeps them
    /// capitalized. Rules may be single words ("guinea" → "Guinea") or multi-word phrases
    /// ("guinea pig" → "guinea pig"); a phrase rule wins over its constituent single-word rules, so
    /// "Santa Catarina's guinea pig" lowercases "guinea pig" even though "guinea" alone is "Guinea".
    /// Words with an internal capital ("McGregor") or a possessive proper noun ("Nordmann's") keep
    /// their capitalization even without an explicit rule. Also straightens apostrophes/quotes and
    /// collapses stray double-spaces.
    /// </summary>
    public static string ApplyCapitalization(string name, IReadOnlyDictionary<string, string> capsRules) {
        if (string.IsNullOrWhiteSpace(name)) {
            return name;
        }

        // Remove disambiguation suffix first, then straighten apostrophes/quotes and collapse
        // stray double-spaces so the rendered name is typographically clean.
        var cleaned = NormalizeDisplayTypography(RemoveDisambiguationSuffix(name));

        // Word tokens, with positions preserved so the original separators are re-emitted verbatim.
        var words = Regex.Matches(cleaned, @"[\w']+");
        var result = new StringBuilder();
        var lastEnd = 0;

        for (var i = 0; i < words.Count; i++) {
            var match = words[i];

            // Re-emit any separator characters before this word.
            if (match.Index > lastEnd) {
                result.Append(cleaned.Substring(lastEnd, match.Index - lastEnd));
            }

            // Prefer the longest multi-word phrase rule that starts at this word.
            var phraseWords = TryMatchPhraseRule(cleaned, words, i, capsRules, out var phraseForm);
            if (phraseWords > 0) {
                result.Append(phraseForm);
                var last = words[i + phraseWords - 1];
                lastEnd = last.Index + last.Length;
                i += phraseWords - 1;
                continue;
            }

            var word = match.Value;
            string capitalizedWord;

            if (i == 0) {
                // First word: a single-word rule may override, else preserve its own caps signal,
                // else title-case it. The trailing EnsureFirstLetterUpper guarantees the leading cap.
                capitalizedWord = LookupSingleWord(capsRules, word)
                    ?? (PreservesOwnCapitalization(word) ? word : ToTitleCase(word));
            } else if (LookupSingleWord(capsRules, word) is { } correctForm) {
                capitalizedWord = correctForm;
            } else if (PreservesOwnCapitalization(word)) {
                // Keep words that carry their own capitalization signal even without an explicit
                // caps.txt rule yet: internal capitals ("McGregor", "DNA") or a possessive proper
                // noun ("Nordmann's"). This avoids over-lowercasing names caps.txt hasn't caught up to.
                capitalizedWord = word;
            } else {
                // Default to lowercase for unknown words (common name convention).
                capitalizedWord = word.ToLowerInvariant();
            }

            result.Append(capitalizedWord);
            lastEnd = match.Index + match.Length;
        }

        // Add any trailing characters
        if (lastEnd < cleaned.Length) {
            result.Append(cleaned.Substring(lastEnd));
        }

        // Common names are sentence-case: guarantee the first visible letter is capitalized.
        return EnsureFirstLetterUpper(result.ToString());
    }

    private static string? LookupSingleWord(IReadOnlyDictionary<string, string> capsRules, string word) {
        return capsRules.TryGetValue(word.ToLowerInvariant(), out var form) ? form : null;
    }

    /// <summary>
    /// Greedily matches the longest multi-word caps phrase (2..MaxPhraseWords words) starting at
    /// <paramref name="start"/>, requiring the words be separated by single spaces. Returns the word
    /// count consumed (0 if no phrase rule matched) and the correctly-cased phrase via
    /// <paramref name="phraseForm"/>.
    /// </summary>
    private static int TryMatchPhraseRule(string cleaned, MatchCollection words, int start,
        IReadOnlyDictionary<string, string> capsRules, out string phraseForm) {
        phraseForm = string.Empty;
        var maxLen = Math.Min(MaxPhraseWords, words.Count - start);

        for (var len = maxLen; len >= 2; len--) {
            // Require single-space separators between every word in the candidate phrase.
            var contiguous = true;
            for (var k = start; k < start + len - 1; k++) {
                var gapStart = words[k].Index + words[k].Length;
                var gapEnd = words[k + 1].Index;
                if (gapEnd - gapStart != 1 || cleaned[gapStart] != ' ') {
                    contiguous = false;
                    break;
                }
            }
            if (!contiguous) {
                continue;
            }

            var key = new StringBuilder();
            for (var k = start; k < start + len; k++) {
                if (k > start) {
                    key.Append(' ');
                }
                key.Append(words[k].Value.ToLowerInvariant());
            }

            if (capsRules.TryGetValue(key.ToString(), out var form)) {
                phraseForm = form;
                return len;
            }
        }

        return 0;
    }

    private static string EnsureFirstLetterUpper(string value) {
        if (value.Length > 0 && char.IsLower(value[0])) {
            return char.ToUpperInvariant(value[0]) + value[1..];
        }
        return value;
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
    /// True when a word should keep its existing capitalization rather than be lowercased:
    /// it has an internal capital (e.g. "McGregor", "DNA") or is a possessive that is almost
    /// always a proper noun (e.g. "Nordmann's", "Hales'"). Used as a fallback when caps.txt has
    /// no explicit rule, so genuine proper nouns are not flattened to lowercase.
    /// </summary>
    private static bool PreservesOwnCapitalization(string word) {
        if (string.IsNullOrEmpty(word) || word.Length < 2) {
            return false;
        }

        if (word.Substring(1).Any(char.IsUpper)) {
            return true;
        }

        return (word.EndsWith("'s", StringComparison.OrdinalIgnoreCase) || word.EndsWith("s'", StringComparison.Ordinal))
            && char.IsUpper(word[0]);
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
