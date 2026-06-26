using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// Loads one-time, human/LLM-written commentary from rules/audit/commentary.yml. Each entry is
// pinned to a report and a release (or "any"), so notes written about one release are shown only
// for that release. This keeps interpretive prose (which is written once for this document)
// separate from the release-agnostic notes generated in code from the data itself.
//
// commentary.yml is a top-level list of entries:
//   - report: failed-assessments      # report id
//     release: 2025-2                  # this release only, or "any" to carry forward
//     scope: report                    # "report" (whole page) or "finding" (one row)
//     key: "12345:EmptyScope"          # required when scope is finding; matches AuditFinding.Key
//     title: "Optional heading"
//     markdown: |
//       Free text in the small markdown subset.

namespace BeastieBot3.Audit.Commentary;

internal sealed class CommentaryEntry {
    public string Report { get; init; } = "";
    public string Release { get; init; } = "any";
    public string Scope { get; init; } = "report";
    public string? Key { get; init; }
    public string? Title { get; init; }
    public string Markdown { get; init; } = "";
}

internal sealed class AuditCommentary {
    private readonly List<CommentaryEntry> _entries;
    public string? SourcePath { get; }

    private AuditCommentary(List<CommentaryEntry> entries, string? sourcePath) {
        _entries = entries;
        SourcePath = sourcePath;
    }

    public static AuditCommentary Empty => new(new List<CommentaryEntry>(), null);

    // Loads from {rulesDir}/audit/commentary.yml. Missing file is fine: returns no commentary.
    public static AuditCommentary Load(string rulesDir) {
        var path = Path.Combine(rulesDir, "audit", "commentary.yml");
        if (!File.Exists(path)) {
            return new AuditCommentary(new List<CommentaryEntry>(), null);
        }

        var yaml = File.ReadAllText(path);
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        var entries = deserializer.Deserialize<List<CommentaryEntry>>(yaml) ?? new List<CommentaryEntry>();
        return new AuditCommentary(entries, path);
    }

    private bool ReleaseMatches(CommentaryEntry e, string release) =>
        string.Equals(e.Release, "any", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(e.Release, release, StringComparison.OrdinalIgnoreCase);

    // Report-level commentary entries for a report, in file order.
    public IReadOnlyList<CommentaryEntry> ForReport(string reportId, string release) =>
        _entries.Where(e =>
                string.Equals(e.Scope, "report", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(e.Report, reportId, StringComparison.OrdinalIgnoreCase) &&
                ReleaseMatches(e, release))
            .ToList();

    // Finding-level commentary text for a specific finding key, if any.
    public string? ForFinding(string reportId, string? findingKey, string release) {
        if (string.IsNullOrEmpty(findingKey)) {
            return null;
        }
        var entry = _entries.FirstOrDefault(e =>
            string.Equals(e.Scope, "finding", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(e.Report, reportId, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(e.Key, findingKey, StringComparison.Ordinal) &&
            ReleaseMatches(e, release));
        return entry?.Markdown;
    }
}
