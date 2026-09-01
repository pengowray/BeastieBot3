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
        sb.Append($"{HtmlText.Escape(doc.Release)}. The tables below group the observations; each links to a description with a short preview, ");
        sb.Append("a full sortable list, and a CSV download. The intent is to help with data review for the next release. ");
        sb.Append("Every observation may be incomplete or mistaken.</p>\n");

        sb.Append("<dl class=\"meta-grid\">\n");
        sb.Append($"<dt>Release reviewed</dt><dd>IUCN Red List version {HtmlText.Escape(doc.Release)}</dd>\n");
        sb.Append($"<dt>Generated</dt><dd>{HtmlText.Escape(doc.GeneratedAt)}</dd>\n");
        foreach (var src in doc.DataSources) {
            sb.Append($"<dt>{HtmlText.Escape(src.Name)}</dt><dd>{HtmlText.Escape(src.Detail)}</dd>\n");
        }
        sb.Append("</dl>\n");
        sb.Append("<p><a href=\"methodology.html\">How this was put together, how to read the lists, and its caveats →</a></p>\n");
        sb.Append($"<p class=\"legend\">{HtmlText.Escape(TypeLegend)}</p>\n");
        sb.Append("</section>\n");

        AppendIndexSections(sb, doc);

        return AuditPageLayout.Page(doc, "", null, sb.ToString());
    }

    // The index in three blocks, in this order. Boundaries follow what the observation is about, not
    // what the Type chip says, because that is what tells a reader whether a block is theirs. The
    // crosscheck block repeats the order of the "you are here" table on each of its nine pages, so
    // the reader learns one order and meets it twice. That order is FamilyRank, by how likely a row is
    // to need a change, not by how close it is to a clean match: leading with the page whose own title
    // says "minor" buried the ones worth opening.
    private static readonly (string Id, string Heading, string Blurb)[] IndexSections = {
        ("records", "Missing and outdated records",
            "Assessments and taxa that are absent, unreachable in the API, or without a current assessment."),
        ("text", "Text cleanup",
            "Stray whitespace, markup, and values that disagree with each other in name, synonym, and narrative fields."),
        ("col", "Catalogue of Life crosscheck",
            "Differences found when checking every assessed name against the Catalogue of Life; each is on exactly one of these pages."),
    };

    // The chip is also the status light: any report can drop to "Nothing found" in a release, so it
    // stays on every row even in blocks where today's values happen to be uniform.
    private const string TypeLegend =
        "Missing data: a record is absent or incomplete. " +
        "Text cleanup: stray characters or markup in a field. " +
        "For review: a difference worth a look, not an error. " +
        "Nothing found: the check found nothing in this release.";

    private static void AppendIndexSections(StringBuilder sb, AuditDocument doc) {
        for (var i = 0; i < IndexSections.Length; i++) {
            var (id, heading, blurb) = IndexSections[i];
            var last = i == IndexSections.Length - 1;
            var reports = doc.Reports.Where(r => r.SectionId == id).ToList();
            // A report naming no block, or an unknown one, is listed in the last block rather than
            // disappearing from the index.
            if (last) {
                reports.AddRange(doc.Reports.Where(r =>
                    r.SectionId is null || !IndexSections.Any(sec => sec.Id == r.SectionId)));
            }
            if (string.Equals(id, "col", StringComparison.Ordinal)) {
                reports = reports.OrderBy(r => r.FamilyRank).ToList();
            }
            AppendReportTable(sb, reports, heading, blurb);
        }
    }

    private static void AppendReportTable(StringBuilder sb, IReadOnlyList<AuditReport> reports, string heading, string blurb) {
        if (reports.Count == 0) {
            return;
        }
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(heading)}</h2>\n");
        sb.Append($"<p>{HtmlText.Escape(blurb)}</p>\n");
        sb.Append("<table class=\"index\">\n<thead><tr><th>Observation</th><th class=\"kind\">Type</th><th class=\"count\">Rows</th><th>Open</th></tr></thead>\n<tbody>\n");
        foreach (var r in reports) {
            sb.Append("<tr>\n");
            sb.Append($"<td><div class=\"report-title\"><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a></div>");
            sb.Append($"<div class=\"report-desc\">{HtmlText.Escape(IndexBlurb(r))}</div></td>\n");
            sb.Append($"<td class=\"kind\">{AuditPageLayout.BreakageBadge(r.Breakage)}</td>\n");
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
        sb.Append($"<h2>{HtmlText.Escape(report.Title)} {AuditPageLayout.BreakageBadge(report.Breakage, inHeading: true)}</h2>\n");
        sb.Append($"<p class=\"report-desc\"><small>Source: {HtmlText.Escape(report.DataSourceLabel)}");
        if (report.Findings.Count > 0) {
            sb.Append($" · {report.Findings.Count:N0} rows");
        }
        sb.Append("</small></p>\n");
        sb.Append($"<div class=\"description\">{HtmlText.Markdown(report.Summary)}</div>\n");

        AppendFamilyTable(sb, doc, report);
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

    // A "you are here" table for a group of reports that partition one comparison. Its counts come
    // from the document, so they always match the pages they link to. Placed after the report's own
    // description, where it reads as the expansion of the sibling pages the description names.
    private static readonly Dictionary<string, (string Heading, string Intro)> FamilyHeadings = new() {
        ["col"] = ("The Catalogue of Life crosscheck",
            "Every assessed name is checked against the Catalogue of Life; each difference found is listed on exactly one of the pages below."),
    };

    private static void AppendFamilyTable(StringBuilder sb, AuditDocument doc, AuditReport report) {
        if (report.FamilyId is null || !FamilyHeadings.TryGetValue(report.FamilyId, out var heading)) {
            return;
        }
        var family = doc.Reports.Where(r => r.FamilyId == report.FamilyId).OrderBy(r => r.FamilyRank).ToList();
        if (family.Count < 2) {
            return;
        }
        sb.Append($"<h3>{HtmlText.Escape(heading.Heading)}</h3>\n");
        sb.Append($"<p>{HtmlText.Escape(heading.Intro)}</p>\n");
        sb.Append("<table class=\"summary family\">\n<thead><tr><th>Page</th><th class=\"num\">Names</th><th>What it lists</th></tr></thead>\n<tbody>\n");
        foreach (var r in family) {
            var here = r.Id == report.Id;
            sb.Append(here ? "<tr class=\"here\">" : "<tr>");
            sb.Append(here
                ? $"<td>{HtmlText.Escape(r.Title)} <span class=\"here-tag\">this page</span></td>"
                : $"<td><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a></td>");
            sb.Append($"<td class=\"num\">{r.Count:N0}</td>");
            sb.Append($"<td>{HtmlText.Escape(r.FamilyScope ?? string.Empty)}</td>");
            sb.Append("</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n");
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

    // A class breakdown runs to 15 rows and pushes the findings preview off the screen, so a long
    // aggregate table is clamped to its first few rows by audit.js (fade plus a toggle). Every row is
    // written into the HTML, so the table is complete with JS off.
    private const int SummaryCollapseOver = 8;
    private const int SummaryCollapseKeep = 6;

    private static void AppendSummaryTable(StringBuilder sb, AuditSummaryTable table) {
        sb.Append($"<h3>{HtmlText.Escape(table.Title)}</h3>\n");
        if (!string.IsNullOrWhiteSpace(table.Note)) {
            sb.Append($"<p>{HtmlText.Markdown(table.Note!)}</p>\n");
        }
        var numeric = new HashSet<int>(table.NumericColumns);
        var collapse = table.Rows.Count > SummaryCollapseOver ? $" data-collapse=\"{SummaryCollapseKeep}\"" : "";
        sb.Append($"<table class=\"summary\"{collapse}>\n<thead><tr>");
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

    // -- full list -------------------------------------------------------------------------

    // The full list is always one page that shows every row; it is never cut into per-group
    // pages. Long lists rely on the filter box and click-to-sort instead, and the page opts into
    // the wide layout so the table can use the full page width.
    private static void WriteFullListPages(AuditDocument doc, AuditReport report, string outputDir) {
        if (report.Findings.Count == 0) {
            return;
        }
        var heading = $"{report.Title}: full list";

        var body = new StringBuilder();
        body.Append("<section>\n");
        body.Append($"<h2>{HtmlText.Escape(heading)}</h2>\n");
        body.Append($"<p><a href=\"{report.Id}.html\">Back to the description</a> &nbsp; ");
        body.Append($"<a href=\"csv/{report.Id}.csv\">Download CSV ({report.CsvRows.Count:N0} rows)</a></p>\n");
        body.Append(HtmlListRenderer.FilterableTable(report, report.Findings, $"tbl-{report.Id}"));
        body.Append("</section>\n");

        var crumbs = AuditPageLayout.Crumbs(
            ("Home", "index.html"),
            (report.Title, $"{report.Id}.html"),
            ("Full list", null));
        var html = AuditPageLayout.Page(doc, heading, crumbs, body.ToString(), wide: true);
        File.WriteAllText(Path.Combine(outputDir, $"{report.Id}-list.html"), html, Utf8NoBom);
    }

    // -- methodology -----------------------------------------------------------------------

    private static string BuildMethodology(AuditDocument doc) {
        var sb = new StringBuilder();
        sb.Append("<section>\n<h2>How this was put together</h2>\n");
        sb.Append("<p>The observations here come from two public IUCN sources: the CSV export of IUCN Red List ");
        sb.Append($"version {HtmlText.Escape(doc.Release)} downloaded from iucnredlist.org, and the public IUCN API (api.iucnredlist.org). ");
        sb.Append("Each report names which of the two it reads under <em>Source</em>. Some reports compare the Red List against the ");
        sb.Append("Catalogue of Life as a taxonomic reference; the Catalogue of Life release used is named on those pages. ");
        sb.Append("The observations were gathered while preparing Red List data for use on Wikipedia and Wikidata, ");
        sb.Append("where small differences in names, formatting, and coverage surface naturally.</p>\n");

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

        sb.Append("<h3>Caveats</h3>\n");
        sb.Append("<ul>\n");
        sb.Append("<li>Counts are computed from the public CSV export and API data and may differ slightly from figures published elsewhere.</li>\n");
        sb.Append("<li>Every observation is automated and may be incomplete or mistaken.</li>\n");
        sb.Append("<li>Only names, classification, formatting, and coverage are examined. The scientific content of assessments (categories, criteria, narratives, ranges, maps, citations) is out of scope.</li>\n");
        sb.Append("</ul>\n");
        sb.Append("</section>\n");

        var crumbs = AuditPageLayout.Crumbs(("Home", "index.html"), ("Methodology", null));
        return AuditPageLayout.Page(doc, "Methodology", crumbs, sb.ToString());
    }

    // -- helpers ---------------------------------------------------------------------------

    // The one-line description under a report title on the index and methodology pages.
    // Prefers the purpose-written Blurb; otherwise falls back to the Summary's first
    // paragraph (stopping before any markdown heading), so headings and later sections
    // never leak into the listing.
    private static string IndexBlurb(AuditReport report) {
        if (!string.IsNullOrWhiteSpace(report.Blurb)) {
            return report.Blurb.Trim();
        }
        var lines = report.Summary.Trim().Replace("\r\n", "\n").Split('\n');
        var paragraph = new StringBuilder();
        foreach (var line in lines) {
            var t = line.Trim();
            if (t.Length == 0 || t.StartsWith('#')) {
                break;
            }
            if (paragraph.Length > 0) {
                paragraph.Append(' ');
            }
            paragraph.Append(t);
        }
        return paragraph.ToString();
    }
}
