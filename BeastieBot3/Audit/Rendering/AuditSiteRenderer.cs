using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;

// Turns an AuditDocument into a self-contained static bundle: index, one detail page per report
// (with a short embedded preview that links out to the full list and the CSV), one full-list page
// per report, an entry page for each report family (the Catalogue of Life crosscheck), the CSV
// downloads, and the shared assets. Every listing is rendered by HtmlListRenderer, so the look and
// the sort/filter behaviour are identical across the whole site. A full run also writes
// release-counts.yml. Files an earlier run wrote and this run did not are removed, so the folder
// holds one run's output; every run lists the files it wrote in .audit-files.txt for the next run
// to compare against.

namespace BeastieBot3.Audit.Rendering;

// What Write did with the files an earlier run left in the folder that this run did not write.
// Paths are relative to the output folder, with '/' separators.
internal sealed record AuditSiteWriteResult(
    IReadOnlyList<string> Removed,
    IReadOnlyList<string> Kept,        // not removed because Write was called with prune: false
    IReadOnlyList<string> NotRemoved); // File.Delete failed; each is logged with the reason

internal static class AuditSiteRenderer {
    private const int PreviewRows = 15;

    private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
    private static readonly Encoding Utf8Bom = new UTF8Encoding(true);

    public const string ReleaseCountsFileName = "release-counts.yml";

    // The list of files a run wrote, one relative path per line. The next run removes the files on
    // it that it does not write itself, and nothing else, so any other file in an --output folder
    // (a CNAME, notes, the owner's own CSVs) is never touched.
    public const string ManifestFileName = ".audit-files.txt";

    // prune: false keeps every file from an earlier run. The command passes it when a producer
    // failed, because that producer's pages were not rewritten and would otherwise all be removed.
    public static AuditSiteWriteResult Write(AuditDocument doc, string outputDir, Action<string>? log = null, bool prune = true) {
        Directory.CreateDirectory(outputDir);
        Directory.CreateDirectory(Path.Combine(outputDir, "assets"));
        Directory.CreateDirectory(Path.Combine(outputDir, "csv"));

        var written = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        void Save(string relativePath, string content, Encoding encoding) {
            var path = Path.GetFullPath(Path.Combine(outputDir, relativePath));
            File.WriteAllText(path, content, encoding);
            written.Add(path);
        }

        Save(Path.Combine("assets", "audit.css"), AuditAssets.Css, Utf8NoBom);
        Save(Path.Combine("assets", "audit.js"), AuditAssets.Js, Utf8NoBom);
        Save(Path.Combine("assets", "theme.js"), AuditAssets.ThemeJs, Utf8NoBom);

        foreach (var report in doc.Reports) {
            if (report.CsvRows.Count > 0) {
                Save(Path.Combine("csv", $"{report.Id}.csv"), AuditCsvWriter.Write(report), Utf8Bom);
            }
            Save($"{report.Id}.html", BuildReportPage(doc, report), Utf8NoBom);
            if (report.Findings.Count > 0) {
                Save($"{report.Id}-list.html", BuildFullListPage(doc, report), Utf8NoBom);
            }
            log?.Invoke($"  {report.Id}: {report.Count:N0}");
        }

        Save("index.html", BuildIndex(doc), Utf8NoBom);
        foreach (var family in FamilyHeadings) {
            if (doc.Reports.Any(r => r.FamilyId == family.Key)) {
                Save($"{family.Key}-crosscheck.html", BuildFamilyPage(doc, family.Key), Utf8NoBom);
            }
        }

        // The counts block for rules/audit/release-counts.yml, saved for pasting in once the release
        // is final. Never from a --limit run, whose counts are partial.
        if (!doc.IsLimited) {
            Save(ReleaseCountsFileName, ReleaseCountsBlock(doc) + Environment.NewLine, Utf8NoBom);
        }

        var result = RemoveStaleFiles(outputDir, written, prune, log);

        // Written last and not listed in itself. Files this run kept, or failed to remove, stay on
        // the list, so the next run still knows they came from the generator.
        var root = Path.GetFullPath(outputDir);
        var listed = written.Select(path => Relative(root, path))
            .Concat(result.Kept)
            .Concat(result.NotRemoved)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(path => path, StringComparer.Ordinal);
        var manifest = new StringBuilder();
        manifest.Append("# Files written by redlist audit-site. The next run removes any file on this list that it does not write again.\n");
        foreach (var path in listed) {
            manifest.Append(path).Append('\n');
        }
        File.WriteAllText(Path.Combine(root, ManifestFileName), manifest.ToString(), Utf8NoBom);

        return result;
    }

    public static string ReleaseCountsBlock(AuditDocument doc) =>
        AuditReleaseCounts.FormatBlock(doc.Release, doc.Reports.Select(r => (r.Id, r.Count)));

    // Files left by an earlier run that this run did not write: a report that now has no rows (its
    // full list and CSV), a report whose producer was skipped (every page of it), a page layout an
    // older version wrote, or release-counts.yml when this run is a --limit run. The index does not
    // link them, but they stay in the folder and in any copy of it, looking current.
    private static AuditSiteWriteResult RemoveStaleFiles(string outputDir, HashSet<string> written, bool prune, Action<string>? log) {
        var root = Path.GetFullPath(outputDir);
        var stale = PreviousRunFiles(root)
            .Where(full => !written.Contains(full) && File.Exists(full))
            .Select(full => Relative(root, full))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        var removed = new List<string>();
        var kept = new List<string>();
        var notRemoved = new List<string>();
        foreach (var relative in stale) {
            if (!prune) {
                kept.Add(relative);
                continue;
            }
            try {
                File.Delete(Path.Combine(root, relative));
                removed.Add(relative);
                log?.Invoke($"  removed {relative} (from an earlier run)");
            } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) {
                notRemoved.Add(relative);
                log?.Invoke($"  could not remove {relative} (from an earlier run): {ex.Message}");
            }
        }
        return new AuditSiteWriteResult(removed, kept, notRemoved);
    }

    // The files the previous run wrote, as full paths inside the folder. From its manifest when
    // there is one. A folder written before the manifest existed has none, and then the generator's
    // own files are recognised by their content: top-level pages that link assets/audit.css, and
    // CSVs in csv/ whose first row id starts with the file's own report id.
    private static IEnumerable<string> PreviousRunFiles(string root) {
        var manifest = ReadManifest(root);
        if (manifest is not null) {
            return manifest;
        }
        return SafeFiles(root, "*.html").Where(IsGeneratedPage)
            .Concat(SafeFiles(Path.Combine(root, "csv"), "*.csv").Where(IsGeneratedCsv))
            .Select(Path.GetFullPath);
    }

    // Null when the folder has no manifest or it cannot be read. Entries that would resolve outside
    // the folder, or to the manifest itself, are ignored.
    private static List<string>? ReadManifest(string root) {
        var path = Path.Combine(root, ManifestFileName);
        string[] lines;
        try {
            if (!File.Exists(path)) {
                return null;
            }
            lines = File.ReadAllLines(path);
        } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) {
            return null;
        }
        var files = new List<string>();
        foreach (var raw in lines) {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#') || Path.IsPathRooted(line)) {
                continue;
            }
            var full = Path.GetFullPath(Path.Combine(root, line));
            var relative = Path.GetRelativePath(root, full);
            if (relative == "." || relative.StartsWith("..", StringComparison.Ordinal) || Path.IsPathRooted(relative)
                || string.Equals(relative, ManifestFileName, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }
            files.Add(full);
        }
        return files;
    }

    private static string Relative(string root, string fullPath) =>
        Path.GetRelativePath(root, fullPath).Replace('\\', '/');

    private static IEnumerable<string> SafeFiles(string dir, string pattern) {
        try {
            return Directory.Exists(dir) ? Directory.GetFiles(dir, pattern, SearchOption.TopDirectoryOnly) : Array.Empty<string>();
        } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) {
            return Array.Empty<string>();
        }
    }

    // The stylesheet link sits in the head, within the first few hundred bytes; full-list pages
    // run to megabytes, so only the start of each file is read.
    private static bool IsGeneratedPage(string path) =>
        ReadStart(path, 4096)?.Contains(AuditPageLayout.StylesheetLink, StringComparison.Ordinal) == true;

    // An "id" header column is common in hand-made CSVs, so the first data row must also start with
    // the stable id AuditCsvWriter writes, "<report id>:<key>", where the report id is the file name.
    // The generator writes a CSV only for a report with rows, so every CSV it wrote has a data row.
    private static bool IsGeneratedCsv(string path) {
        var start = ReadStart(path, 4096);
        if (start is null) {
            return false;
        }
        start = start.TrimStart('\uFEFF');
        var headerEnd = start.IndexOf('\n');
        if (headerEnd < 0) {
            return false;
        }
        var header = start[..headerEnd].TrimEnd('\r');
        var idColumn = AuditCsvWriter.IdColumn;
        if (header != idColumn && !header.StartsWith(idColumn + ",", StringComparison.Ordinal)) {
            return false;
        }
        var firstCell = start[(headerEnd + 1)..];
        if (firstCell.StartsWith('"')) {
            firstCell = firstCell[1..];
        }
        return firstCell.StartsWith(Path.GetFileNameWithoutExtension(path) + ":", StringComparison.Ordinal);
    }

    private static string? ReadStart(string path, int chars) {
        try {
            using var reader = new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: false);
            var buffer = new char[chars];
            var read = reader.ReadBlock(buffer, 0, chars);
            return new string(buffer, 0, read);
        } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) {
            return null;
        }
    }

    // -- index -----------------------------------------------------------------------------

    private static string BuildIndex(AuditDocument doc) {
        var sb = new StringBuilder();

        sb.Append("<section>\n");
        sb.Append("<p class=\"lede\">Observations about the data in IUCN Red List version ");
        sb.Append($"{HtmlText.Escape(doc.Release)}, gathered while preparing Red List data for Wikipedia and Wikidata and shared for the next release's data review. ");
        sb.Append("Each observation links to a description with a short preview, a full sortable list, and a CSV download.</p>\n");

        sb.Append("<dl class=\"meta-grid\">\n");
        sb.Append($"<dt>Release reviewed</dt><dd>IUCN Red List version {HtmlText.Escape(doc.Release)}</dd>\n");
        sb.Append($"<dt>Generated</dt><dd>{HtmlText.Escape(doc.GeneratedAt)}</dd>\n");
        foreach (var src in doc.DataSources) {
            sb.Append($"<dt>{HtmlText.Escape(src.Name)}</dt><dd>{HtmlText.Escape(src.Detail)}</dd>\n");
        }
        sb.Append("</dl>\n");
        sb.Append("</section>\n");

        AppendTriage(sb, doc);
        AppendIndexSections(sb, doc);

        return AuditPageLayout.Page(doc, "", null, sb.ToString());
    }

    // The short list at the top: the reports worth doing before the next release, ranked by the
    // producers (TriageRank) with live counts. The
    // release-specific commentary for report "index" prints above it when present.
    private static void AppendTriage(StringBuilder sb, AuditDocument doc) {
        var triage = doc.Reports.Where(r => r.TriageRank > 0 && r.Count > 0).OrderBy(r => r.TriageRank).Take(5).ToList();
        if (triage.Count == 0) {
            return;
        }
        sb.Append("<section>\n<h2>Start here</h2>\n");
        sb.Append("<p>The observations most worth acting on before the next release.</p>\n");
        AppendCommentary(sb, doc, "index");
        sb.Append("<ol class=\"triage\">\n");
        foreach (var r in triage) {
            sb.Append("<li>");
            sb.Append($"<a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a> ");
            sb.Append($"<span class=\"triage-count\">({r.Count:N0} rows");
            sb.Append(")</span> ");
            sb.Append(AuditPageLayout.ActionBadge(r.Action));
            if (!string.IsNullOrWhiteSpace(r.TriageReason)) {
                sb.Append($"<div class=\"report-desc\">{HtmlText.Escape(r.TriageReason)}</div>");
            }
            sb.Append("</li>\n");
        }
        sb.Append("</ol>\n</section>\n");
    }

    // The index in three blocks, in this order. Boundaries follow what the observation is about, not
    // what the Action chip says, because that is what tells a reader whether a block is theirs. The
    // crosscheck block lists only its two highest-yield pages here and sends the reader to the
    // crosscheck's own entry page for the rest: nine pages of two catalogues disagreeing are not IUCN
    // errors, and listed beside whitespace findings they read as if the site thought they were.
    private static readonly (string Id, string Heading, string Blurb)[] IndexSections = {
        // One item per page in the block, in the order the rows appear under it, so a reader can
        // match each phrase to a row without decoding it.
        ("records", "Missing and outdated records",
            "Assessments and taxa missing something: a geographic scope, an API response, an assessed parent species, a current assessment, or a described name."),
        ("text", "Text cleanup",
            "Stray whitespace, markup, and values that disagree with each other in name, synonym, and narrative fields."),
        ("col", "Catalogue of Life crosscheck",
            "Every assessed name is checked against the Catalogue of Life. The two pages most likely to need a change are listed here."),
    };

    private static readonly string[] ColIndexHighlights = { "col-close-match", "col-classification" };

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
            string? footer = null;
            if (string.Equals(id, "col", StringComparison.Ordinal)) {
                var all = reports.Count;
                var rows = reports.Sum(r => r.Count);
                reports = reports.Where(r => ColIndexHighlights.Contains(r.Id) || r.FamilyId != "col").OrderBy(r => r.FamilyRank).ToList();
                footer = $"<p><a href=\"col-crosscheck.html\">All {all} crosscheck pages ({rows:N0} rows), for anyone reconciling the two catalogues &rarr;</a></p>\n";
            }
            AppendReportTable(sb, doc, reports, heading, blurb, footer);
        }
    }

    private static void AppendReportTable(StringBuilder sb, AuditDocument doc, IReadOnlyList<AuditReport> reports, string heading, string blurb, string? footerHtml = null) {
        if (reports.Count == 0) {
            return;
        }
        var since = doc.SinceRelease is null ? "" : $"<th class=\"since\">Since {HtmlText.Escape(doc.SinceRelease)}</th>";
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(heading)}</h2>\n");
        sb.Append($"<p>{HtmlText.Escape(blurb)}</p>\n");
        sb.Append($"<table class=\"index\">\n<thead><tr><th>Observation</th><th class=\"kind\">Action</th><th class=\"count\">Rows</th>{since}<th>Open</th></tr></thead>\n<tbody>\n");
        foreach (var r in reports) {
            sb.Append("<tr>\n");
            sb.Append($"<td><div class=\"report-title\"><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a></div>");
            sb.Append($"<div class=\"report-desc\">{HtmlText.Escape(IndexBlurb(r))}</div></td>\n");
            sb.Append($"<td class=\"kind\">{AuditPageLayout.ActionBadge(r.Action)}</td>\n");
            sb.Append($"<td class=\"count\">{r.Count:N0}</td>\n");
            if (doc.SinceRelease is not null) {
                var change = SinceText(doc, r);
                sb.Append($"<td class=\"since {change.Css}\">{HtmlText.Escape(change.Text)}</td>\n");
            }
            sb.Append("<td class=\"links\">");
            sb.Append($"<a href=\"{r.Id}.html\">details</a>");
            if (r.Findings.Count > 0) {
                sb.Append($" &middot; <a href=\"{r.Id}-list.html\">full list</a> &middot; <a href=\"csv/{r.Id}.csv\">csv</a>");
            }
            sb.Append("</td>\n</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n");
        if (footerHtml is not null) {
            sb.Append(footerHtml);
        }
        sb.Append("</section>\n");
    }

    // The change since the previous release, as a short phrase. Blank when that release recorded no
    // count for the report, which is not the same as zero.
    private static (string Text, string Css) SinceText(AuditDocument doc, AuditReport report) {
        if (doc.SinceRelease is null || doc.ReleaseCounts?.Count(doc.SinceRelease, report.Id) is not { } previous) {
            return ("", "");
        }
        if (report.Count == previous) {
            return ("unchanged", "");
        }
        if (report.Count == 0) {
            return ($"fixed (was {previous:N0})", "down");
        }
        return report.Count > previous
            ? ($"up from {previous:N0}", "up")
            : ($"down from {previous:N0}", "down");
    }

    // -- report detail ---------------------------------------------------------------------

    private static string BuildReportPage(AuditDocument doc, AuditReport report) {
        var sb = new StringBuilder();
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(report.Title)} {AuditPageLayout.ActionBadge(report.Action, inHeading: true)}</h2>\n");
        sb.Append($"<p class=\"report-desc\"><small>Source: {HtmlText.Escape(report.DataSourceLabel)}");
        if (report.Findings.Count > 0) {
            sb.Append($" · {report.Findings.Count:N0} rows");
        }
        sb.Append("</small></p>\n");
        sb.Append($"<div class=\"description\">{HtmlText.Markdown(report.Summary)}</div>\n");

        AppendFamilyTable(sb, doc, report);
        AppendCommentary(sb, doc, report.Id);

        foreach (var table in report.SummaryTables) {
            AppendSummaryTable(sb, table);
        }
        AppendGroupCounts(sb, report);

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
            sb.Append(CsvNote(report));
        } else if (report.SummaryTables.Count == 0) {
            sb.Append(doc.RowLimit is { } limit && !report.IgnoresRowLimit
                ? $"<p>No observations of this kind in the rows checked. This run used <code>--limit {limit.ToString(CultureInfo.InvariantCulture)}</code>, so a full run may find some.</p>\n"
                : "<p>No observations of this kind in the current release.</p>\n");
        }

        sb.Append("</section>\n");

        var crumbs = AuditPageLayout.Crumbs(("Home", "index.html"), (report.Title, null));
        return AuditPageLayout.Page(doc, report.Title, crumbs, sb.ToString());
    }

    // A "you are here" table for a group of reports that partition one comparison. Its counts come
    // from the document, so they always match the pages they link to. Placed after the report's own
    // description, where it reads as the expansion of the sibling pages the description names. The
    // same table, with a longer introduction, is the family's entry page.
    private static readonly Dictionary<string, (string Heading, string Intro)> FamilyHeadings = new() {
        ["col"] = ("The Catalogue of Life crosscheck",
            "Every assessed name is checked against the Catalogue of Life; each difference found is listed on exactly one of the pages below. Pages are ordered by how likely a row is to need a change."),
    };

    private static void AppendFamilyTable(StringBuilder sb, AuditDocument doc, AuditReport report) {
        if (report.FamilyId is null || !FamilyHeadings.TryGetValue(report.FamilyId, out var heading)) {
            return;
        }
        var family = doc.Reports.Where(r => r.FamilyId == report.FamilyId).OrderBy(r => r.FamilyRank).ToList();
        if (family.Count < 2) {
            return;
        }
        sb.Append($"<h3><a href=\"{report.FamilyId}-crosscheck.html\">{HtmlText.Escape(heading.Heading)}</a></h3>\n");
        sb.Append($"<p>{HtmlText.Escape(heading.Intro)}</p>\n");
        AppendFamilyRows(sb, doc, family, report.Id);
    }

    private static void AppendFamilyRows(StringBuilder sb, AuditDocument doc, IReadOnlyList<AuditReport> family, string? hereId) {
        var since = doc.SinceRelease is null ? "" : $"<th class=\"since\">Since {HtmlText.Escape(doc.SinceRelease)}</th>";
        sb.Append($"<table class=\"summary family\">\n<thead><tr><th>Page</th><th class=\"kind\">Action</th><th class=\"num\">Names</th>{since}<th>What it lists</th></tr></thead>\n<tbody>\n");
        var appendixStarted = false;
        foreach (var r in family.OrderBy(r => r.IsAppendix ? 1 : 0).ThenBy(r => r.FamilyRank)) {
            if (r.IsAppendix && !appendixStarted) {
                appendixStarted = true;
                var span = doc.SinceRelease is null ? 4 : 5;
                sb.Append($"<tr class=\"appendix-head\"><th colspan=\"{span}\">Appendix</th></tr>\n");
            }
            var here = r.Id == hereId;
            sb.Append(here ? "<tr class=\"here\">" : "<tr>");
            sb.Append(here
                ? $"<td>{HtmlText.Escape(r.Title)} <span class=\"here-tag\">this page</span></td>"
                : $"<td><a href=\"{r.Id}.html\">{HtmlText.Escape(r.Title)}</a></td>");
            sb.Append($"<td class=\"kind\">{AuditPageLayout.ActionBadge(r.Action)}</td>");
            sb.Append($"<td class=\"num\">{r.Count:N0}</td>");
            if (doc.SinceRelease is not null) {
                var change = SinceText(doc, r);
                sb.Append($"<td class=\"since {change.Css}\">{HtmlText.Escape(change.Text)}</td>");
            }
            sb.Append($"<td>{HtmlText.Escape(r.FamilyScope ?? string.Empty)}</td>");
            sb.Append("</tr>\n");
        }
        sb.Append("</tbody>\n</table>\n");
    }

    // The entry page for a report family: for the crosscheck, the page aimed at someone reconciling
    // the two catalogues rather than reviewing IUCN data. It says what the comparison is and is not,
    // then lists every page with the appendix set apart.
    private static string BuildFamilyPage(AuditDocument doc, string familyId) {
        var heading = FamilyHeadings[familyId];
        var family = doc.Reports.Where(r => r.FamilyId == familyId).OrderBy(r => r.FamilyRank).ToList();
        var sb = new StringBuilder();
        sb.Append("<section>\n");
        sb.Append($"<h2>{HtmlText.Escape(heading.Heading)}</h2>\n");
        var source = family.FirstOrDefault()?.DataSourceLabel;
        if (source is not null) {
            sb.Append($"<p class=\"report-desc\"><small>Source: {HtmlText.Escape(source)} &middot; {family.Sum(r => r.Count):N0} rows across {family.Count} pages</small></p>\n");
        }
        sb.Append("<div class=\"description\">\n");
        sb.Append("<p>These pages compare the scientific names and classification in the IUCN Red List with the Catalogue of Life (CoL). ");
        sb.Append("The two are independent catalogues with different sources and update cycles, and the Red List does not follow CoL, so a difference is not an error in either. ");
        sb.Append("The lists are for anyone reconciling the two: matching Red List assessments to CoL records, or checking where the catalogues have diverged.</p>\n");
        sb.Append("<p>Every assessed IUCN name (species, subspecies, and varieties, excluding subpopulations) is looked up in CoL by exact name, then by near spelling, then through IUCN's own synonym list, and finally through names recorded on Wikidata and English Wikipedia. ");
        sb.Append("Higher-rank names (genus to class) are compared separately. Each difference appears on exactly one page.</p>\n");
        sb.Append("</div>\n");
        AppendCommentary(sb, doc, $"{familyId}-crosscheck");
        sb.Append("<h3>Pages</h3>\n");
        AppendFamilyRows(sb, doc, family, hereId: null);
        sb.Append("</section>\n");
        var crumbs = AuditPageLayout.Crumbs(("Home", "index.html"), (heading.Heading, null));
        return AuditPageLayout.Page(doc, heading.Heading, crumbs, sb.ToString());
    }

    private static void AppendCommentary(StringBuilder sb, AuditDocument doc, string reportId) {
        var entries = doc.CommentarySource?.ForReport(reportId, doc.Release) ?? (IReadOnlyList<CommentaryEntry>)Array.Empty<CommentaryEntry>();
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

    // The taxonomic spread of a report's rows, as one line rather than a table: the reader wants
    // "mostly plants" at a glance, and a two-column table of the same thing pushed the actual
    // findings below the fold.
    private static void AppendGroupCounts(StringBuilder sb, AuditReport report) {
        if (!report.ShowGroupCounts || report.Findings.Count == 0) {
            return;
        }
        var line = TaxonGroups.CountLine(report.Findings);
        if (string.IsNullOrEmpty(line)) {
            return;
        }
        sb.Append($"<p class=\"group-counts\"><strong>Groups:</strong> {HtmlText.Escape(line)}");
        if (!string.IsNullOrWhiteSpace(report.GroupCountsNote)) {
            sb.Append($" <small>{HtmlText.Escape(report.GroupCountsNote!)}</small>");
        }
        sb.Append("</p>\n");
    }

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
    // the wide layout so the table can use the full page width. Written only for a report with rows.
    private static string BuildFullListPage(AuditDocument doc, AuditReport report) {
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
        return AuditPageLayout.Page(doc, heading, crumbs, body.ToString(), wide: true);
    }

    // -- helpers ---------------------------------------------------------------------------

    // Under the CSV link on a report page: whether the file can be applied as it stands, and that
    // every row carries a stable id.
    private static string CsvNote(AuditReport report) {
        var sb = new StringBuilder("<p class=\"patch-note\">");
        if (report.CsvIsPatch) {
            sb.Append("The CSV can be applied as-is: every row gives the taxon id, the field, the current value, and the replacement value. ");
        }
        sb.Append($"Each row has a stable <code>{AuditCsvWriter.IdColumn}</code>, kept across releases, for citing a row or tracking it in the next release.");
        sb.Append("</p>\n");
        return sb.ToString();
    }

    // The one-line description under a report title on the index.
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
