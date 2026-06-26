using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using BeastieBot3.Audit.Model;

// Renders a finding list (the columns defined on an AuditReport) as one sortable, filterable
// HTML table. Every listing in the audit goes through here, so the look, the click-to-sort
// behaviour, and the cell formatting are identical across reports. The matching CSV is written
// from the same column definitions by AuditCsvWriter.

namespace BeastieBot3.Audit.Rendering;

internal static class HtmlListRenderer {
    private const int LongTextPreview = 140;

    // A table with no surrounding controls (used for the short embedded preview).
    public static string Table(AuditReport report, IEnumerable<AuditFinding> findings) =>
        TableHtml(report.Columns, findings);

    // A full table preceded by a filter box and a row counter. tableId is unique per page.
    public static string FilterableTable(AuditReport report, IEnumerable<AuditFinding> findings, string tableId) {
        var list = findings as IReadOnlyList<AuditFinding> ?? findings.ToList();
        var sb = new StringBuilder();
        sb.Append("<div class=\"table-controls\">\n");
        sb.Append($"<input type=\"search\" class=\"table-filter\" data-table=\"{tableId}\" placeholder=\"Filter these rows…\" aria-label=\"Filter rows\">\n");
        sb.Append($"<span class=\"row-count\" data-table=\"{tableId}\">{list.Count:N0} rows</span>\n");
        sb.Append("</div>\n");
        sb.Append(TableHtml(report.Columns, list, tableId));
        return sb.ToString();
    }

    private static string TableHtml(IReadOnlyList<AuditColumn> columns, IEnumerable<AuditFinding> findings, string? tableId = null) {
        var sb = new StringBuilder();
        sb.Append("<div class=\"table-wrap\">\n");
        sb.Append(tableId is null ? "<table class=\"audit-table\">\n" : $"<table class=\"audit-table sortable\" id=\"{tableId}\">\n");
        var sortable = tableId is not null;
        sb.Append("<thead><tr>");
        for (var i = 0; i < columns.Count; i++) {
            var col = columns[i];
            var cls = col.IsNumeric ? " class=\"num\"" : "";
            var help = string.IsNullOrEmpty(col.Help) ? "" : $" title=\"{HtmlText.Escape(col.Help)}\"";
            // Sortable headers are keyboard-operable: focusable, button role, and an aria-label.
            var a11y = sortable ? $" tabindex=\"0\" role=\"button\" aria-label=\"Sort by {HtmlText.Escape(col.Header)}\"" : "";
            sb.Append($"<th{cls} data-col=\"{i}\" data-numeric=\"{(col.IsNumeric ? "true" : "false")}\"{help}{a11y}>{HtmlText.Escape(col.Header)}</th>");
        }
        sb.Append("</tr></thead>\n<tbody>\n");
        foreach (var f in findings) {
            sb.Append("<tr>");
            foreach (var col in columns) {
                sb.Append(Cell(col, f));
            }
            sb.Append("</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n</div>\n");
        return sb.ToString();
    }

    private static string Cell(AuditColumn col, AuditFinding f) {
        var raw = col.Value(f) ?? "";
        var sortKey = col.SortKey?.Invoke(f) ?? SortValue(col, raw);
        var sortAttr = $" data-sort=\"{HtmlText.Escape(sortKey)}\"";
        var numClass = col.IsNumeric ? " class=\"num\"" : "";

        switch (col.Type) {
            case AuditColumnType.Status: {
                if (string.IsNullOrWhiteSpace(raw)) {
                    return $"<td{sortAttr}></td>";
                }
                var v = IucnStatusVisuals.For(raw);
                var badge = $"<span class=\"status-badge\" style=\"background:{v.Background};color:{v.Text}\" title=\"{HtmlText.Escape(v.Label)}\">{HtmlText.Escape(v.Code)}</span>";
                return $"<td{sortAttr}>{badge}</td>";
            }
            case AuditColumnType.Taxon: {
                var href = col.Href?.Invoke(f) ?? f.RedlistUrl;
                var name = $"<em>{HtmlText.Escape(raw)}</em>";
                var inner = HtmlText.IsSafeHref(href)
                    ? $"<a href=\"{HtmlText.Escape(href!)}\" rel=\"noopener\" target=\"_blank\">{name}</a>"
                    : name;
                return $"<td{sortAttr}>{inner}</td>";
            }
            case AuditColumnType.Url: {
                var href = col.Href?.Invoke(f) ?? raw;
                if (!HtmlText.IsSafeHref(href)) {
                    return $"<td{sortAttr}>{HtmlText.Escape(raw)}</td>";
                }
                var label = raw.StartsWith("http", System.StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(raw) ? "view" : raw;
                return $"<td{sortAttr}><a href=\"{HtmlText.Escape(href!)}\" rel=\"noopener\" target=\"_blank\">{HtmlText.Escape(label)}</a></td>";
            }
            case AuditColumnType.Code:
                return $"<td{sortAttr}><code>{HtmlText.Escape(raw)}</code></td>";
            case AuditColumnType.Number:
                return $"<td{numClass}{sortAttr}>{HtmlText.Escape(raw)}</td>";
            case AuditColumnType.Whitespace:
                return $"<td class=\"ws-cell\"{sortAttr}>{HtmlText.Visualise(raw)}</td>";
            case AuditColumnType.LongText: {
                if (raw.Length <= LongTextPreview) {
                    return $"<td class=\"longtext\"{sortAttr}>{HtmlText.Escape(raw)}</td>";
                }
                var preview = HtmlText.Escape(HtmlText.Truncate(raw, LongTextPreview));
                var full = HtmlText.Escape(raw);
                return $"<td class=\"longtext\"{sortAttr}><span title=\"{full}\">{preview}</span></td>";
            }
            default:
                return $"<td{sortAttr}>{HtmlText.Escape(raw)}</td>";
        }
    }

    private static string SortValue(AuditColumn col, string raw) {
        if (col.IsNumeric) {
            var cleaned = raw.Replace(",", "").Replace("%", "").Trim();
            return double.TryParse(cleaned, NumberStyles.Any, CultureInfo.InvariantCulture, out var n)
                ? n.ToString("F6", CultureInfo.InvariantCulture)
                : "0";
        }
        return raw.ToLowerInvariant();
    }
}
