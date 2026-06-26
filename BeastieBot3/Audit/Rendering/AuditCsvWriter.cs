using System.Collections.Generic;
using System.Text;
using BeastieBot3.Audit.Model;

// Writes a report's full finding set to CSV using the same AuditColumn definitions the HTML
// table uses, so the download and the on-screen list always carry the same columns in the same
// order. Values are the raw column values (no HTML, no whitespace markers).

namespace BeastieBot3.Audit.Rendering;

internal static class AuditCsvWriter {
    public static string Write(AuditReport report) => Write(report.Columns, report.CsvRows);

    public static string Write(IReadOnlyList<AuditColumn> columns, IEnumerable<AuditFinding> findings) {
        var sb = new StringBuilder();
        for (var i = 0; i < columns.Count; i++) {
            if (i > 0) {
                sb.Append(',');
            }
            sb.Append(Escape(columns[i].Key));
        }
        sb.Append('\n');

        foreach (var f in findings) {
            for (var i = 0; i < columns.Count; i++) {
                if (i > 0) {
                    sb.Append(',');
                }
                sb.Append(Escape(columns[i].Value(f) ?? "", columns[i].IsNumeric));
            }
            sb.Append('\n');
        }
        return sb.ToString();
    }

    private static string Escape(string value) => Escape(value, false);

    private static string Escape(string value, bool numeric) {
        // Neutralise spreadsheet formula injection: a cell starting with =, +, -, @, tab, or CR is
        // evaluated as a formula on import. Prefix such a value with an apostrophe so it stays text.
        // Skip the '-' guard for numeric columns so genuine negative numbers are preserved.
        if (value.Length > 0) {
            var c = value[0];
            var dangerous = c is '=' or '+' or '@' or '\t' or '\r' || (c == '-' && !numeric);
            if (dangerous) {
                value = "'" + value;
            }
        }
        var needsQuote = value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r');
        if (!needsQuote) {
            return value;
        }
        return "\"" + value.Replace("\"", "\"\"") + "\"";
    }
}
