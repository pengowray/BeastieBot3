using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using YamlDotNet.Serialization;

// Headline row counts per report per release, kept in rules/audit/release-counts.yml, so the index
// can say whether each list grew, shrank, or stayed flat since the previous release. The build reads
// every release recorded there and prints the current release's block for pasting back once the
// release is final; it never writes the file itself, so a --limit test run cannot corrupt history.
//
// release-counts.yml:
//   2025-2:
//     empty-scope: 23
//     no-latest: 3898

namespace BeastieBot3.Audit.Commentary;

internal sealed class AuditReleaseCounts {
    private readonly Dictionary<string, Dictionary<string, int>> _byRelease;
    public string? SourcePath { get; }

    private AuditReleaseCounts(Dictionary<string, Dictionary<string, int>> byRelease, string? sourcePath) {
        _byRelease = byRelease;
        SourcePath = sourcePath;
    }

    public static AuditReleaseCounts Empty => new(new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal), null);

    public static string PathFor(string rulesDir) => Path.Combine(rulesDir, "audit", "release-counts.yml");

    public static AuditReleaseCounts Load(string rulesDir) {
        var path = PathFor(rulesDir);
        if (!File.Exists(path)) {
            return Empty;
        }
        var deserializer = new DeserializerBuilder().IgnoreUnmatchedProperties().Build();
        var raw = deserializer.Deserialize<Dictionary<string, Dictionary<string, int>>>(File.ReadAllText(path))
                  ?? new Dictionary<string, Dictionary<string, int>>();
        var byRelease = raw.ToDictionary(kv => kv.Key.Trim(), kv => kv.Value ?? new Dictionary<string, int>(), StringComparer.Ordinal);
        return new AuditReleaseCounts(byRelease, path);
    }

    public IReadOnlyCollection<string> Releases => _byRelease.Keys;

    // The most recent recorded release that is older than the given one. Release labels are
    // "YYYY-N", so ordinal string order is chronological order.
    public string? PreviousRelease(string release) => _byRelease.Keys
        .Where(r => string.CompareOrdinal(r, release) < 0)
        .OrderByDescending(r => r, StringComparer.Ordinal)
        .FirstOrDefault();

    public int? Count(string release, string reportId) =>
        _byRelease.TryGetValue(release, out var counts) && counts.TryGetValue(reportId, out var n) ? n : null;

    // The yml block for a release, ready to paste into release-counts.yml.
    public static string FormatBlock(string release, IEnumerable<(string ReportId, int Count)> counts) {
        var lines = new List<string> { $"{release}:" };
        lines.AddRange(counts.Select(c => $"  {c.ReportId}: {c.Count}"));
        return string.Join(Environment.NewLine, lines);
    }
}
