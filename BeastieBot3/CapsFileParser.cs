using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace BeastieBot3;

/// <summary>
/// Parses caps.txt files containing capitalization rules for common names.
/// Format: "Word // example1, example2, example3"
/// Lines starting with "//" are comment-only lines and are skipped.
/// </summary>
internal static class CapsFileParser {
    // Pattern to split on the comment marker
    private static readonly Regex CommentPattern = new(@"\s*//\s*", RegexOptions.Compiled);

    /// <summary>
    /// Parses a caps.txt file and returns capitalization rules.
    /// </summary>
    public static IReadOnlyList<CapsRule> ParseFile(string filePath) {
        if (!File.Exists(filePath)) {
            throw new FileNotFoundException($"Caps file not found: {filePath}");
        }

        var rules = new List<CapsRule>();
        var lineNumber = 0;

        foreach (var line in File.ReadLines(filePath)) {
            lineNumber++;

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line)) {
                continue;
            }

            // Skip pure comment lines (starting with //)
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//")) {
                continue;
            }

            // Split on "//" to separate word from examples
            var parts = CommentPattern.Split(line, 2);
            var word = parts[0].Trim();

            if (string.IsNullOrWhiteSpace(word)) {
                continue;
            }

            string? examples = parts.Length > 1 ? parts[1].Trim() : null;
            if (string.IsNullOrWhiteSpace(examples)) {
                examples = null;
            }

            rules.Add(new CapsRule(
                LowercaseWord: word.ToLowerInvariant(),
                CorrectForm: word,
                Examples: examples,
                SourceLine: lineNumber
            ));
        }

        return rules;
    }

    /// <summary>
    /// Loads caps rules into a dictionary for quick lookup.
    /// </summary>
    public static Dictionary<string, string> LoadAsDictionary(string filePath) {
        var rules = ParseFile(filePath);
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rule in rules) {
            // Use TryAdd to keep first occurrence if there are duplicates
            dict.TryAdd(rule.LowercaseWord, rule.CorrectForm);
        }

        return dict;
    }

    /// <summary>
    /// Gets the default caps.txt path relative to the application.
    /// </summary>
    public static string GetDefaultCapsFilePath() {
        var baseDir = AppContext.BaseDirectory;

        // Try relative to base directory
        var rulesPath = Path.Combine(baseDir, "rules", "caps.txt");
        if (File.Exists(rulesPath)) {
            return rulesPath;
        }

        // Try sibling of base directory (for development)
        var parentDir = Path.GetDirectoryName(baseDir);
        if (parentDir != null) {
            rulesPath = Path.Combine(parentDir, "rules", "caps.txt");
            if (File.Exists(rulesPath)) {
                return rulesPath;
            }
        }

        // Try current working directory
        rulesPath = Path.Combine(Directory.GetCurrentDirectory(), "rules", "caps.txt");
        if (File.Exists(rulesPath)) {
            return rulesPath;
        }

        // Fallback to project structure for development
        rulesPath = Path.Combine(Directory.GetCurrentDirectory(), "BeastieBot3", "rules", "caps.txt");
        if (File.Exists(rulesPath)) {
            return rulesPath;
        }

        throw new FileNotFoundException("Could not locate caps.txt file. Tried rules/caps.txt in various locations.");
    }
}

/// <summary>
/// Represents a capitalization rule from caps.txt.
/// </summary>
public record CapsRule(
    string LowercaseWord,
    string CorrectForm,
    string? Examples,
    int SourceLine
);
