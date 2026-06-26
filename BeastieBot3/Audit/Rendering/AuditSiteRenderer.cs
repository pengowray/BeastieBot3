using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;

// Turns an AuditDocument into a self-contained static bundle: index, one detail page per report
// (with a short embedded preview that links out to the full list and the CSV), one full-list page
// per report (split into a simple per-group tree when very large), a methodology page, the CSV
// downloads, and the shared assets. Every listing is rendered by HtmlListRenderer, so the look and
// the sort/filter behaviour are identical across the whole site.

namespace BeastieBot3.Audit.Rendering;

internal static class AuditSiteRenderer {
    private const int PreviewRows = 15;
    private const int TreeSplitThreshold = 2500;

    private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
    private static readonly Encoding Utf8Bom = new UTF8Encoding(true);

    public static void Write(AuditDocument doc, string outputDir, Action<string>? log = null) {
        Directory.CreateDirectory(outputDir);
        Directory.CreateDirectory(Path.Combine(outputDir, "assets"));
        Directory.CreateDirectory(Path.Combine(outputDir, "csv"));

        File.WriteAllText(Path.Combine(outputDir, "assets", "audit.css"), AuditAssets.Css, Utf8NoBom);
        File.WriteAllText(Path.Combine(outputDir, "assets", "audit.js"), AuditAssets.Js, Utf8NoBom);

        foreach (var report in doc.Reports) {
            if (report.CsvRows.Count > 0) {
                File.WriteAllText(Path.Combine(outputDir, "csv", $"{report.Id}.csv"), AuditCsvWriter.Write(report), Utf8Bom);
            }
            WriteReportPage(doc, report, outputDir);
            WriteFullListPages(doc, report, outputDir);
            log?.Invoke($"  {report.Id}: {report.Count:N0}");
        }

        File.WriteAllText(Path.Combine(outputDir, "index.html"), BuildIndex(doc), Utf8NoBom);
        File.WriteAllText(Path.Combine(outputDir, "methodology.html"), BuildMethodology(doc), Utf8NoBom);
    }

    // -- index -----------------------------------------------------------------------------

    private static string BuildIndex(AuditDocument doc) {
        var sb = new StringBuilder();

        sb.Append("<div class=\"disclaimer\">\n");
        sb.Append("<strong>This is an unofficial, independent compilation.</strong> ");
        sb.Append("It is not produced, reviewed, or endorsed by the IUCN or the IUCN Red List. ");
        sb.Append("It gathers observations noticed while preparing Red List data for use on Wikipedia and Wikidata, ");
        sb.Append("and it is shared in the hope that some are useful for a future release.\n");
        sb.Append("</div>\n");

        sb.Append("<section>\n");
        sb.Append("<p class=\"lede\">This page collects observations about the data in IUCN Red List version ");
        sb.Append($"{HtmlText.Escape(doc.Release)}. Each section describes one kind of observation, shows a short preview, ");
        sb.Append("and links to a full sortable list and a CSV download. The intent is to help with data review for the next release. ");
        sb.Append("Every observation may be incomplete or mistaken.</p>\n");

        sb.Append("<dl class=\"meta-grid\">\n");
        sb.Append($"<dt>Release reviewed</dt><dd>IUCN Red List version {HtmlText.Escape(doc.Release)}</dd>\n");
        sb.Append($"<dt>Generated</dt><dd>{HtmlText.Escape(doc.GeneratedAt)}</dd>\n");
        foreach (var src in doc.DataSources) {
            sb.Append($"<dt>{HtmlText.Escape(src.Name)}</dt><dd>{HtmlText.Escape(src.Detail)}</dd>\n");
        }
        sb.Append("</dl>\n");
        sb.Append("<p><a href=\"methodology.html\">How this was put together, and what it does and does not cover →</a></p>\n");
        sb.Append("</section>\n");

        AppendReportTable(sb, doc, AuditReportTier.IucnCore,
            "Observations in the Red List data",
            "These concern records in the Red List itself.");
        AppendReportTable(sb, doc, AuditReportTier.Methodology,
            "Methodology and coverage",
            "Count reconciliation and field-level summaries that give context to the lists above.");

        return AuditPageLayout.Page(doc, "", null, sb.ToString());
    }

    private static void AppendReportTable(StringBuilder sb, AuditDocument doc, AuditReportTier tier, string heading, string blurb) {
        var reports = doc.Reports.Where(r => r.Tier == tier).ToList();
        if (reports.Count == 0) {
            return;
        }
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(heading)}</h2>\n");
        sb.Append($"<p>{HtmlText.Escape(blurb)}</p>\n");
        sb.Append("<table class=\"index\">\n<thead><tr><th>Observation</th><th>Kind</th><th class=\"count\">Rows</th><th>Open</th></tr></thead>\n<tbody>\n");
        foreach (var r in reports) {
            sb.Append("<tr>\n");
            sb.Append($"<td><div class=\"report-title\"><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a></div>");
            sb.Append($"<div class=\"report-desc\">{HtmlText.Escape(FirstSentence(r.Summary))}</div></td>\n");
            sb.Append($"<td>{AuditPageLayout.BreakageBadge(r.Breakage)}</td>\n");
            sb.Append($"<td class=\"count\">{r.Count:N0}</td>\n");
            sb.Append("<td class=\"links\">");
            sb.Append($"<a href=\"{r.Id}.html\">details</a>");
            if (r.Findings.Count > 0) {
                sb.Append($" · <a href=\"{r.Id}-list.html\">full list</a> · <a href=\"csv/{r.Id}.csv\">csv</a>");
            }
            sb.Append("</td>\n</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n</section>\n");
    }

    // -- report detail ---------------------------------------------------------------------

    private static void WriteReportPage(AuditDocument doc, AuditReport report, string outputDir) {
        var sb = new StringBuilder();
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(report.Title)} {AuditPageLayout.BreakageBadge(report.Breakage)}</h2>\n");
        sb.Append($"<p class=\"report-desc\"><small>Source: {HtmlText.Escape(report.DataSourceLabel)}");
        if (report.Findings.Count > 0) {
            sb.Append($" · {report.Findings.Count:N0} rows");
        }
        sb.Append("</small></p>\n");
        sb.Append($"<div class=\"description\">{HtmlText.Markdown(report.Summary)}</div>\n");

        AppendCommentary(sb, doc, report);

        foreach (var table in report.SummaryTables) {
            AppendSummaryTable(sb, table);
        }

        if (report.Findings.Count > 0) {
            sb.Append("<h3>Preview</h3>\n");
            var preview = report.Findings.Take(PreviewRows).ToList();
            sb.Append(HtmlListRenderer.Table(report, preview));
            sb.Append("<p class=\"preview-foot\">");
            if (report.Findings.Count > preview.Count) {
                sb.Append($"Showing the first {preview.Count:N0} of {report.Findings.Count:N0} rows. ");
            }
            sb.Append($"<a href=\"{report.Id}-list.html\">View the full list →</a>");
            sb.Append($" &nbsp; <a href=\"csv/{report.Id}.csv\">Download CSV ({report.CsvRows.Count:N0} rows)</a>");
            sb.Append("</p>\n");
        } else if (report.SummaryTables.Count == 0) {
            sb.Append("<p>No observations of this kind in the current release.</p>\n");
        }

        sb.Append("</section>\n");

        var crumbs = AuditPageLayout.Crumbs(("Home", "index.html"), (report.Title, null));
        var html = AuditPageLayout.Page(doc, report.Title, crumbs, sb.ToString());
        File.WriteAllText(Path.Combine(outputDir, $"{report.Id}.html"), html, Utf8NoBom);
    }

    private static void AppendCommentary(StringBuilder sb, AuditDocument doc, AuditReport report) {
        var entries = doc.CommentarySource?.ForReport(report.Id, doc.Release) ?? (IReadOnlyList<CommentaryEntry>)Array.Empty<CommentaryEntry>();
        foreach (var entry in entries) {
            sb.Append("<div class=\"commentary\">\n");
            if (!string.IsNullOrWhiteSpace(entry.Title)) {
                sb.Append($"<h3>{HtmlText.Escape(entry.Title)}</h3>\n");
            }
            sb.Append(HtmlText.Markdown(entry.Markdown));
            sb.Append("</div>\n");
        }
    }

    private static void AppendSummaryTable(StringBuilder sb, AuditSummaryTable table) {
        sb.Append($"<h3>{HtmlText.Escape(table.Title)}</h3>\n");
        if (!string.IsNullOrWhiteSpace(table.Note)) {
            sb.Append($"<p>{HtmlText.Markdown(table.Note!)}</p>\n");
        }
        var numeric = new HashSet<int>(table.NumericColumns);
        sb.Append("<table class=\"summary\">\n<thead><tr>");
        for (var i = 0; i < table.Headers.Count; i++) {
            sb.Append(numeric.Contains(i) ? "<th class=\"num\">" : "<th>");
            sb.Append(HtmlText.Escape(table.Headers[i]));
            sb.Append("</th>");
        }
        sb.Append("</tr></thead>\n<tbody>\n");
        foreach (var row in table.Rows) {
            sb.Append("<tr>");
            for (var i = 0; i < row.Count; i++) {
                sb.Append(numeric.Contains(i) ? "<td class=\"num\">" : "<td>");
                sb.Append(HtmlText.Escape(row[i]));
                sb.Append("</td>");
            }
            sb.Append("</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n");
    }

    // -- full list (with optional recursive per-group tree) --------------------------------

    private static void WriteFullListPages(AuditDocument doc, AuditReport report, string outputDir) {
        if (report.Findings.Count == 0) {
            return;
        }
        var rootCrumbs = new List<(string, string?)> {
            ("Home", "index.html"),
            (report.Title, $"{report.Id}.html"),
        };
        WriteListNode(doc, report, outputDir, report.Findings, levelIndex: 0,
            fileName: $"{report.Id}-list.html", nodeLabel: "Full list", parentCrumbs: rootCrumbs, tableId: report.Id);
    }

    // Renders one node of the full-list tree. A node is either a leaf (a single filterable table)
    // or, when it is over the size threshold and another grouping level remains, an index page that
    // links to one child node per group value. Children recurse to the next level.
    private static void WriteListNode(
        AuditDocument doc, AuditReport report, string outputDir,
        IReadOnlyList<AuditFinding> findings, int levelIndex,
        string fileName, string nodeLabel, List<(string Label, string? Href)> parentCrumbs, string tableId) {

        var crumbs = new List<(string, string?)>(parentCrumbs) { (nodeLabel, null) };
        var heading = nodeLabel == "Full list" ? $"{report.Title}: full list" : $"{report.Title}: {nodeLabel}";

        var body = new StringBuilder();
        body.Append("<section>\n");
        body.Append($"<h2>{HtmlText.Escape(heading)}</h2>\n");
        body.Append($"<p><a href=\"{report.Id}.html\">Back to the description</a> &nbsp; ");
        body.Append($"<a href=\"csv/{report.Id}.csv\">Download CSV ({report.CsvRows.Count:N0} rows)</a></p>\n");

        var canSplit = levelIndex < report.GroupLevels.Count && findings.Count > TreeSplitThreshold;
        if (!canSplit) {
            body.Append(HtmlListRenderer.FilterableTable(report, findings, $"tbl-{tableId}"));
            body.Append("</section>\n");
            WritePage(doc, outputDir, fileName, heading, crumbs, body.ToString());
            return;
        }

        var level = report.GroupLevels[levelIndex];
        var groups = findings
            .GroupBy(f => GroupKey(level, f))
            .OrderByDescending(g => g.Count())
            .ToList();

        body.Append($"<p>This list is large, so it is grouped by {HtmlText.Escape(level.Label)}. ");
        body.Append("Each group below is a sortable, filterable table; the CSV above covers every row.</p>\n");
        body.Append($"<table class=\"index\">\n<thead><tr><th>{HtmlText.Escape(Capitalise(level.Label))}</th><th class=\"count\">Rows</th></tr></thead>\n<tbody>\n");
        var stem = fileName.EndsWith(".html", StringComparison.Ordinal) ? fileName[..^5] : fileName;
        foreach (var g in groups) {
            var childFile = $"{stem}-{Slug(g.Key)}.html";
            body.Append($"<tr><td><a href=\"{childFile}\">{HtmlText.Escape(g.Key)}</a></td>");
            body.Append($"<td class=\"count\">{g.Count():N0}</td></tr>\n");
        }
        body.Append("</tbody>\n</table>\n</section>\n");
        WritePage(doc, outputDir, fileName, heading, crumbs, body.ToString());

        var childCrumbs = new List<(string, string?)>(parentCrumbs) { (nodeLabel, fileName) };
        foreach (var g in groups) {
            var childFile = $"{stem}-{Slug(g.Key)}.html";
            WriteListNode(doc, report, outputDir, g.ToList(), levelIndex + 1, childFile, g.Key, childCrumbs, $"{tableId}-{Slug(g.Key)}");
        }
    }

    private static string GroupKey(AuditGroupLevel level, AuditFinding f) {
        var g = level.Selector(f);
        return string.IsNullOrWhiteSpace(g) ? "(unspecified)" : g!;
    }

    private static void WritePage(AuditDocument doc, string outputDir, string fileName, string pageTitle, List<(string, string?)> crumbs, string body) {
        var html = AuditPageLayout.Page(doc, pageTitle, AuditPageLayout.Crumbs(crumbs.ToArray()), body);
        File.WriteAllText(Path.Combine(outputDir, fileName), html, Utf8NoBom);
    }

    // -- methodology -----------------------------------------------------------------------

    private static string BuildMethodology(AuditDocument doc) {
        var sb = new StringBuilder();
        sb.Append("<section>\n<h2>How this was put together</h2>\n");
        sb.Append("<p>The observations here come from a local copy of the public IUCN Red List ");
        sb.Append($"version {HtmlText.Escape(doc.Release)} export, compared in some sections against the ");
        sb.Append("Catalogue of Life as a taxonomic reference. They were gathered while preparing Red List ");
        sb.Append("data for use on Wikipedia and Wikidata, where small differences in names, formatting, and ");
        sb.Append("coverage surface naturally.</p>\n");

        sb.Append("<h3>How to read the lists</h3>\n");
        sb.Append("<ul>\n");
        sb.Append("<li>Each report has a short preview on its page and a full sortable, filterable list behind the <em>full list</em> link.</li>\n");
        sb.Append("<li>Rows are ordered with the entries most likely to help first: full species before subspecies and varieties, and current assessments before historical ones where that information is available.</li>\n");
        sb.Append("<li>Where a row maps to a Red List page, the scientific name links to it.</li>\n");
        sb.Append("<li>Status badge colours are only a reading aid and are not the official IUCN category colours.</li>\n");
        sb.Append("</ul>\n");

        sb.Append("<h3>Scope of a \"species\"</h3>\n");
        sb.Append("<p>Where a report counts species, it means global, species-rank assessments: rows with no infraspecific ");
        sb.Append("rank (subspecies or variety) and no subpopulation or regional scope. Subspecies, varieties, ");
        sb.Append("subpopulations, and regional assessments are listed separately where relevant.</p>\n");

        sb.Append("<h3>What this does not cover</h3>\n");
        sb.Append("<ul>\n");
        sb.Append("<li>Grouping by assessing body (a specialist group or Red List Authority) is not available, because that field is not present in the public export used here.</li>\n");
        sb.Append("<li>Counts are taken from one local import of the public release and may differ slightly from figures published elsewhere.</li>\n");
        sb.Append("<li>Every observation is automated and may be incomplete or mistaken.</li>\n");
        sb.Append("</ul>\n");
        sb.Append("</section>\n");

        var methodologyReports = doc.Reports.Where(r => r.Tier == AuditReportTier.Methodology).ToList();
        if (methodologyReports.Count > 0) {
            sb.Append("<section>\n<h2>Methodology reports</h2>\n<ul>\n");
            foreach (var r in methodologyReports) {
                sb.Append($"<li><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a>: {HtmlText.Escape(FirstSentence(r.Summary))}</li>\n");
            }
            sb.Append("</ul>\n</section>\n");
        }

        var crumbs = AuditPageLayout.Crumbs(("Home", "index.html"), ("Methodology", null));
        return AuditPageLayout.Page(doc, "Methodology", crumbs, sb.ToString());
    }

    // -- helpers ---------------------------------------------------------------------------

    private static string FirstSentence(string text) {
        var trimmed = text.Trim();
        var idx = trimmed.IndexOf(". ", StringComparison.Ordinal);
        return idx > 0 ? trimmed[..(idx + 1)] : trimmed;
    }

    private static string Capitalise(string s) =>
        string.IsNullOrEmpty(s) ? s : char.ToUpperInvariant(s[0]) + s[1..];

    private static string Slug(string value) {
        var sb = new StringBuilder(value.Length);
        var lastDash = false;
        foreach (var c in value.ToLowerInvariant()) {
            if (char.IsLetterOrDigit(c)) {
                sb.Append(c);
                lastDash = false;
            } else if (!lastDash) {
                sb.Append('-');
                lastDash = true;
            }
        }
        var slug = sb.ToString().Trim('-');
        return slug.Length == 0 ? "none" : slug;
    }
}
