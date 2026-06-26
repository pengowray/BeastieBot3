using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;

// Column-level text-hygiene profile of the IUCN taxonomy table: for each text column, the share of
// values with surrounding whitespace, repeated spaces, non-breaking or control characters, non-ASCII
// content, or non-NFC normalisation. A focused profiler (counts are exact over the table) rather than
// a per-taxon list.

namespace BeastieBot3.Audit.Producers;

internal sealed class FieldHygieneProducer : IAuditReportProducer {
    public string Id => "field-hygiene";
    private const string Table = "taxonomy";

    // Whitespace and zero-width characters that are not a plain ASCII space: non-breaking space
    // (U+00A0), figure space (U+2007), narrow no-break space (U+202F), thin space (U+2009),
    // zero-width space (U+200B), zero-width joiner (U+200D), BOM (U+FEFF), tab, CR, LF. Written as
    // escape sequences so the code points cannot be silently collapsed to an ASCII space.
    private static bool IsSpecialWhitespace(char c) =>
        c is ' ' or ' ' or ' ' or ' ' or '​' or '‍' or '﻿' or '\t' or '\r' or '\n';

    private sealed class ColumnStat {
        public string Name = "";
        public long Total;
        public long Null;
        public long Empty;
        public long LeadingTrailing;
        public long Repeated;
        public long Special;
        public long NonAscii;
        public long NotNfc;
        public string? ExampleTrim;
        public string? ExampleSpecial;
        public string? ExampleNotNfc;
        public string? ExampleNonAscii;
    }

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, Table)) {
            return null;
        }

        var columns = TextColumns(conn);
        if (columns.Count == 0) {
            return null;
        }
        var stats = Scan(conn, ctx, columns);

        var findings = new List<AuditFinding>();
        foreach (var s in stats) {
            if (s.Total == 0 && s.Null > 0) {
                findings.Add(Make(s, "all-null-column", s.Null, null, null, "Every value in this column is null in this release."));
                continue;
            }
            if (s.LeadingTrailing > 0) {
                findings.Add(Make(s, "leading-or-trailing-whitespace", s.LeadingTrailing, s.ExampleTrim, s.ExampleTrim?.Trim(), "Values carry surrounding whitespace that the suggested value trims."));
            }
            if (s.Repeated > 0) {
                findings.Add(Make(s, "repeated-spaces", s.Repeated, null, null, "Values contain repeated spaces."));
            }
            if (s.Special > 0) {
                findings.Add(Make(s, "non-breaking-or-control-whitespace", s.Special, s.ExampleSpecial, null, "Values contain non-breaking, zero-width, or control whitespace characters."));
            }
            if (s.NotNfc > 0) {
                findings.Add(Make(s, "not-nfc-normalised", s.NotNfc, s.ExampleNotNfc, s.ExampleNotNfc?.Normalize(NormalizationForm.FormC), "Some values are not in NFC normalisation form."));
            }
            if (s.NonAscii > 0) {
                findings.Add(Make(s, "non-ascii-content", s.NonAscii, s.ExampleNonAscii, null, "Values contain non-ASCII characters (often expected for names and authorities)."));
            }
        }

        var ordered = findings
            .OrderByDescending(f => long.TryParse(f.Get("affectedRows")?.Replace(",", ""), out var n) ? n : 0)
            .ThenBy(f => f.Field, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Text hygiene by taxonomy field",
            Tier = AuditReportTier.Methodology,
            Breakage = BreakageClass.Advisory,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export, taxonomy table)",
            Summary =
                "Each row describes one text characteristic of one taxonomy column: the share of values with surrounding whitespace, repeated spaces, non-breaking or control characters, non-ASCII content, or non-NFC normalisation. " +
                "Counts are exact over the whole table. Non-ASCII content is often expected for names and authorities and is listed for completeness.",
            Columns = new List<AuditColumn> {
                AuditColumns.Field(),
                AuditColumns.IssueType("Characteristic"),
                AuditColumns.Custom("affectedRows", "Rows affected", AuditColumnType.Number),
                AuditColumns.Custom("affectedPct", "% of values", AuditColumnType.Number),
                AuditColumns.CurrentValue("Example", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Suggested", AuditColumnType.Code),
                AuditColumns.Detail(),
            },
            Findings = ordered,
            HeadlineCount = ordered.Count,
        };
    }

    private static AuditFinding Make(ColumnStat s, string issue, long affected, string? example, string? suggested, string detail) {
        var denom = s.Total + s.Null;
        var pct = denom > 0 ? (double)affected / denom * 100 : 0;
        var f = new AuditFinding {
            ReportId = "field-hygiene",
            DataSource = "iucn-csv",
            Field = s.Name,
            IssueType = issue,
            CurrentValue = example,
            SuggestedValue = suggested,
            Detail = $"{detail} {affected:N0} of {denom:N0} values ({pct.ToString("0.###", CultureInfo.InvariantCulture)}%).",
        };
        f.Extra["affectedRows"] = affected.ToString("N0");
        f.Extra["affectedPct"] = pct.ToString("0.###", CultureInfo.InvariantCulture);
        return f;
    }

    private static List<string> TextColumns(SqliteConnection conn) {
        var columns = new List<string>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info(\"{Table}\")";
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var name = reader.GetString(1);
            var type = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var upper = type.ToUpperInvariant();
            var isText = type.Length == 0 || upper.Contains("CHAR") || upper.Contains("CLOB") || upper.Contains("TEXT");
            if (isText) {
                columns.Add(name);
            }
        }
        return columns;
    }

    private static List<ColumnStat> Scan(SqliteConnection conn, AuditContext ctx, List<string> columns) {
        var stats = columns.Select(c => new ColumnStat { Name = c }).ToList();
        var quoted = string.Join(", ", columns.Select(c => "\"" + c.Replace("\"", "\"\"") + "\""));
        var sql = $"SELECT {quoted} FROM \"{Table}\"";
        using var cmd = conn.CreateCommand();
        cmd.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
        cmd.CommandTimeout = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            for (var i = 0; i < stats.Count; i++) {
                var s = stats[i];
                if (reader.IsDBNull(i)) {
                    s.Null++;
                    continue;
                }
                var value = reader.GetString(i);
                s.Total++;
                if (value.Length == 0) {
                    s.Empty++;
                    continue;
                }
                if (!string.Equals(value, value.Trim(), StringComparison.Ordinal)) {
                    s.LeadingTrailing++;
                    s.ExampleTrim ??= value;
                }
                if (HasRepeatedSpace(value)) {
                    s.Repeated++;
                }
                if (value.Any(IsSpecialWhitespace)) {
                    s.Special++;
                    s.ExampleSpecial ??= value;
                }
                if (value.Any(c => c > 127)) {
                    s.NonAscii++;
                    s.ExampleNonAscii ??= value;
                }
                if (!value.IsNormalized(NormalizationForm.FormC)) {
                    s.NotNfc++;
                    s.ExampleNotNfc ??= value;
                }
            }
        }
        return stats;
    }

    private static bool HasRepeatedSpace(string value) {
        for (var i = 1; i < value.Length; i++) {
            if (value[i] == ' ' && value[i - 1] == ' ') {
                return true;
            }
        }
        return false;
    }
}
