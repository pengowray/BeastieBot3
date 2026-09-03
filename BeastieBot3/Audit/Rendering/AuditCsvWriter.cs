using System.Collections.Generic;
using System.Linq;
using System.Text;
using BeastieBot3.Audit.Model;

// Writes a report's full finding set to CSV using the same AuditColumn definitions the HTML
// table uses, so the download and the on-screen list always carry the same columns in the same
// order. Values are the raw column values (no HTML, no whitespace markers). A leading "id" column
// carries the finding's stable key ("{report}:{key}") so a row can be cited, tracked across
// releases, and matched to commentary without depending on its position.

namespace BeastieBot3.Audit.Rendering;

internal static class AuditCsvWriter {
    public const string IdColumn = "id";

    public static string Write(AuditReport report) => Write(report.Columns, report.CsvRows);

    public static string StableId(AuditFinding f) =>
        string.IsNullOrEmpty(f.Key) ? "" : $"{f.ReportId}:{f.Key}";

    public static string Write(IReadOnlyList<AuditColumn> allColumns, IEnumerable<AuditFinding> findings) {
        // HTML-only columns (e.g. a modal-viewer button) have no meaningful flat-CSV value.
        var columns = allColumns.Where(c => !c.HtmlOnly).ToList();
        var sb = new StringBuilder();
        sb.Append(IdColumn);
        foreach (var column in columns) {
            sb.Append(',');
            sb.Append(Escape(column.Key));
        }
        sb.Append('\n');

        foreach (var f in findings) {
            sb.Append(Escape(StableId(f)));
            foreach (var column in columns) {
                sb.Append(',');
                sb.Append(Escape(column.Value(f) ?? "", column.IsNumeric));
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
