using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Structural quality metrics for a single generated Wikipedia list.
/// Collected during generation and written to the JSON metrics file.
/// </summary>
internal sealed class ListStructureMetrics {
    [JsonPropertyName("list_id")]
    public string ListId { get; init; } = string.Empty;

    [JsonPropertyName("file")]
    public string FileName { get; init; } = string.Empty;

    [JsonPropertyName("total_taxa")]
    public int TotalTaxa { get; set; }

    [JsonPropertyName("heading_count")]
    public int HeadingCount { get; set; }

    [JsonPropertyName("single_item_headings")]
    public int SingleItemHeadings { get; set; }

    [JsonPropertyName("empty_headings")]
    public int EmptyHeadings { get; set; }

    [JsonPropertyName("other_unknown_headings")]
    public int OtherUnknownHeadings { get; set; }

    [JsonPropertyName("other_unknown_items")]
    public int OtherUnknownItems { get; set; }

    [JsonPropertyName("max_leaf_size")]
    public int MaxLeafSize { get; set; }

    [JsonPropertyName("max_heading_depth")]
    public int MaxHeadingDepth { get; set; }

    [JsonPropertyName("auto_split_attempts")]
    public int AutoSplitAttempts { get; set; }

    [JsonPropertyName("auto_split_accepted")]
    public int AutoSplitAccepted { get; set; }

    [JsonPropertyName("decisions")]
    public List<AutoSplitDecisionRecord> Decisions { get; init; } = new();

    [JsonPropertyName("problems")]
    public List<string> Problems { get; init; } = new();
}

/// <summary>
/// JSON-serializable version of an auto-split decision.
/// </summary>
internal sealed class AutoSplitDecisionRecord {
    [JsonPropertyName("parent")]
    public string ParentPath { get; init; } = string.Empty;

    [JsonPropertyName("items")]
    public int ItemCount { get; init; }

    [JsonPropertyName("rank")]
    public string CandidateRank { get; init; } = string.Empty;

    [JsonPropertyName("outcome")]
    public string Outcome { get; init; } = string.Empty;

    [JsonPropertyName("groups")]
    public int GroupCount { get; init; }

    [JsonPropertyName("meaningful")]
    public int MeaningfulGroups { get; init; }

    [JsonPropertyName("other_fraction")]
    public double OtherFraction { get; init; }

    [JsonPropertyName("largest")]
    public int LargestGroup { get; init; }

    public static AutoSplitDecisionRecord From(AutoSplitDecision d) => new() {
        ParentPath = d.ParentPath,
        ItemCount = d.ItemCount,
        CandidateRank = d.CandidateRank,
        Outcome = d.Outcome,
        GroupCount = d.GroupCount,
        MeaningfulGroups = d.MeaningfulGroups,
        OtherFraction = d.OtherFraction,
        LargestGroup = d.LargestGroup
    };
}

/// <summary>
/// Container for the full metrics report, serialized to JSON.
/// </summary>
internal sealed class GenerationMetricsReport {
    [JsonPropertyName("generated_at")]
    public string GeneratedAt { get; init; } = string.Empty;

    [JsonPropertyName("dataset_version")]
    public string DatasetVersion { get; init; } = string.Empty;

    [JsonPropertyName("lists")]
    public List<ListStructureMetrics> Lists { get; init; } = new();
}

/// <summary>
/// Computes structural quality metrics by parsing generated wikitext.
/// This avoids threading metrics through all the complex code paths.
/// </summary>
internal static class WikitextMetricsCollector {
    /// <summary>
    /// Parse generated wikitext to collect structural metrics.
    /// </summary>
    public static void CollectFromWikitext(string wikitext, ListStructureMetrics metrics) {
        var lines = wikitext.Split('\n');
        var headingStack = new List<HeadingContext>(); // Stack of (level, isOtherUnknown, itemCount)

        foreach (var rawLine in lines) {
            var line = rawLine.TrimEnd('\r');

            if (IsHeadingLine(line, out var level, out var headingText)) {
                // Flush previous heading at same or higher level
                FlushHeadings(headingStack, level, metrics);

                // Skip non-taxonomic footer headings from metrics
                if (IsFooterHeading(headingText)) {
                    continue;
                }

                var isOtherUnknown = IsOtherOrUnknownLabel(headingText);
                headingStack.Add(new HeadingContext(level, isOtherUnknown, 0));

                if (level > metrics.MaxHeadingDepth) {
                    metrics.MaxHeadingDepth = level;
                }

                if (isOtherUnknown) {
                    metrics.OtherUnknownHeadings++;
                }
            } else if (line.StartsWith("* ") || line.StartsWith("*[") || line.StartsWith("*'")) {
                // Species entry line
                if (headingStack.Count > 0) {
                    headingStack[headingStack.Count - 1] = headingStack[headingStack.Count - 1].WithIncrementedItems();
                }
            }
        }

        // Flush remaining headings
        FlushHeadings(headingStack, 0, metrics);
    }

    /// <summary>
    /// Pops headings from the stack that are at or deeper than <paramref name="newLevel"/>,
    /// recording metrics for each. Only leaf headings (no child sub-headings) are checked
    /// for empty/single-item status — parent headings naturally have 0 direct items.
    /// Also marks the new stack top as having children, so it won't be flagged as empty.
    /// </summary>
    private static void FlushHeadings(List<HeadingContext> stack, int newLevel, ListStructureMetrics metrics) {
        // Pop headings that are at the same level or deeper than the new heading
        while (stack.Count > 0 && stack[stack.Count - 1].Level >= newLevel) {
            var ctx = stack[stack.Count - 1];
            stack.RemoveAt(stack.Count - 1);

            // Only leaf headings (no child headings) are checked for empty/single-item
            if (!ctx.HasChildHeadings) {
                if (ctx.ItemCount == 0) {
                    metrics.EmptyHeadings++;
                } else if (ctx.ItemCount == 1) {
                    metrics.SingleItemHeadings++;
                }
            }

            if (ctx.ItemCount > metrics.MaxLeafSize) {
                metrics.MaxLeafSize = ctx.ItemCount;
            }

            if (ctx.IsOtherUnknown) {
                metrics.OtherUnknownItems += ctx.ItemCount;
            }

            // Mark the new top-of-stack (parent) as having child headings
            if (stack.Count > 0) {
                stack[stack.Count - 1] = stack[stack.Count - 1].WithChildHeading();
            }
        }
    }

    /// <summary>
    /// Parses a wikitext heading line (e.g., "=== Carnivora ===") to extract its level (2-6)
    /// and inner text. Returns false for non-heading lines or invalid heading levels.
    /// </summary>
    private static bool IsHeadingLine(string line, out int level, out string headingText) {
        level = 0;
        headingText = string.Empty;

        if (!line.StartsWith("=")) {
            return false;
        }

        // Count leading '=' characters
        int start = 0;
        while (start < line.Length && line[start] == '=') {
            start++;
        }

        level = start;
        if (level < 2 || level > 6) {
            level = 0;
            return false;
        }

        // Find trailing '=' and extract heading text
        int end = line.Length - 1;
        while (end > start && line[end] == '=') {
            end--;
        }

        headingText = line.Substring(start, end - start + 1).Trim();
        return true;
    }

    /// <summary>
    /// Detect structural problems and add them to the metrics.
    /// </summary>
    public static void DetectProblems(ListStructureMetrics metrics) {
        // FRAGMENTED: more than 1 heading per 4 species
        if (metrics.TotalTaxa > 0 && metrics.HeadingCount > 0) {
            double ratio = (double)metrics.HeadingCount / metrics.TotalTaxa;
            if (ratio > 0.25) {
                metrics.Problems.Add($"FRAGMENTED: heading-to-species ratio {ratio:F3} (>{0.25})");
            }
        }

        // OVER-SPLIT: many single-item headings
        if (metrics.SingleItemHeadings > 10) {
            metrics.Problems.Add($"OVER-SPLIT: {metrics.SingleItemHeadings} single-item headings");
        }

        // EMPTY: headings with no content
        if (metrics.EmptyHeadings > 0) {
            metrics.Problems.Add($"EMPTY: {metrics.EmptyHeadings} empty headings");
        }

        // DOMINATED: Other/Unknown items outnumber named items
        if (metrics.TotalTaxa > 0 && metrics.OtherUnknownItems > metrics.TotalTaxa / 2) {
            metrics.Problems.Add($"DOMINATED: {metrics.OtherUnknownItems}/{metrics.TotalTaxa} items in Other/Unknown sections");
        }

        // DEPTH: heading depth at maximum
        if (metrics.MaxHeadingDepth >= 4) {
            metrics.Problems.Add($"DEPTH: max heading depth {metrics.MaxHeadingDepth} (nesting may hit level 6 cap)");
        }
    }

    /// <summary>
    /// Returns true for standard Wikipedia footer headings (References, See also, etc.)
    /// that should be excluded from structural quality metrics.
    /// </summary>
    private static bool IsFooterHeading(string value) {
        var trimmed = value.Trim();
        return trimmed.Equals("References", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("See also", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("External links", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Notes", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Further reading", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsOtherOrUnknownLabel(string value) {
        var trimmed = value.Trim();
        return trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Tracks state for a heading during wikitext parsing. <c>HasChildHeadings</c> distinguishes
    /// parent headings (which naturally have 0 direct items) from leaf headings (which are
    /// flagged as empty/single-item when appropriate).
    /// </summary>
    private sealed record HeadingContext(int Level, bool IsOtherUnknown, int ItemCount, bool HasChildHeadings = false) {
        public HeadingContext WithIncrementedItems() => this with { ItemCount = ItemCount + 1 };
        public HeadingContext WithChildHeading() => this with { HasChildHeadings = true };
    }
}
