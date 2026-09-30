using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3.Audit;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Rendering;

namespace BeastieBot3.Tests;

// Pins the pure logic behind the redlist audit-site: rank/status mapping, status badge colours,
// HTML escaping and the whitespace visualiser, the shared table/CSV renderers, and the
// release-pinned commentary loader.
public class AuditRenderingTests {
    // ---- AuditMapping ----

    [Theory]
    [InlineData(null, null, "species", true)]
    [InlineData("", "", "species", true)]
    [InlineData("subspecies", null, "subspecies", false)]
    [InlineData("var.", null, "variety", false)]
    [InlineData(null, "Mediterranean", "subpopulation", false)]
    public void Rank_DerivesFromInfraAndSubpopulation(string? infra, string? subpop, string expectedRank, bool expectedFull) {
        var (rank, isFull) = AuditMapping.Rank(infra, subpop);
        Assert.Equal(expectedRank, rank);
        Assert.Equal(expectedFull, isFull);
    }

    [Fact]
    public void StatusSortKey_OrdersMostThreatenedFirst() {
        Assert.True(string.CompareOrdinal(AuditMapping.StatusSortKey("EX"), AuditMapping.StatusSortKey("CR")) < 0);
        Assert.True(string.CompareOrdinal(AuditMapping.StatusSortKey("CR"), AuditMapping.StatusSortKey("LC")) < 0);
        Assert.True(string.CompareOrdinal(AuditMapping.StatusSortKey("LC"), AuditMapping.StatusSortKey("DD")) < 0);
        Assert.Equal("99", AuditMapping.StatusSortKey(null));
    }

    [Fact]
    public void CodeFromCategory_FoldsPossiblyExtinct() {
        Assert.Equal("CR(PE)", AuditMapping.CodeFromCategory("Critically Endangered", "true", "false"));
        Assert.Equal("CR", AuditMapping.CodeFromCategory("Critically Endangered", "false", "false"));
        Assert.Equal("EX", AuditMapping.CodeFromCategory("Extinct"));
        Assert.Null(AuditMapping.CodeFromCategory(null));
    }

    // ---- IucnStatusVisuals ----

    [Fact]
    public void StatusVisual_AssignsThreatColours() {
        Assert.Equal("#cc3333", IucnStatusVisuals.For("CR").Background);
        Assert.Equal("#cc3333", IucnStatusVisuals.For("Critically Endangered").Background);
        Assert.Equal("#000000", IucnStatusVisuals.For("EX").Background);
        Assert.Equal("#006666", IucnStatusVisuals.For("LC").Background);
        Assert.Equal("#cccccc", IucnStatusVisuals.For(null).Background);
    }

    // ---- HtmlText ----

    [Fact]
    public void Escape_EscapesMarkupAndQuotes() {
        Assert.Equal("a&lt;b&gt;&amp;&quot;&#39;", HtmlText.Escape("a<b>&\"'"));
    }

    [Fact]
    public void Visualise_ShowsInvisibleCharacters() {
        var html = HtmlText.Visualise("a b"); // ASCII space
        Assert.Contains("·", html);
        var nbsp = HtmlText.Visualise("a b");
        Assert.Contains("non-breaking space", nbsp);
        Assert.Contains("(empty)", HtmlText.Visualise(""));
    }

    [Fact]
    public void Markdown_RendersSubsetAndBlocksRawHtml() {
        var html = HtmlText.Markdown("A **bold** and *em* and `code`.\n\n- one\n- two");
        Assert.Contains("<strong>bold</strong>", html);
        Assert.Contains("<em>em</em>", html);
        Assert.Contains("<code>code</code>", html);
        Assert.Contains("<li>one</li>", html);
        // Raw HTML in the source is escaped, not passed through.
        var injected = HtmlText.Markdown("<script>alert(1)</script>");
        Assert.DoesNotContain("<script>", injected);
        Assert.Contains("&lt;script&gt;", injected);
    }

    [Fact]
    public void Markdown_OnlyAllowsSafeLinkSchemes() {
        var safe = HtmlText.Markdown("see [here](https://www.iucnredlist.org)");
        Assert.Contains("<a href=\"https://www.iucnredlist.org\"", safe);
        var blocked = HtmlText.Markdown("[x](javascript:alert(1))");
        Assert.DoesNotContain("href", blocked);
        Assert.Contains("x", blocked);
    }

    // ---- HtmlListRenderer + AuditCsvWriter ----

    private static AuditReport SampleReport() {
        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName(),
            AuditColumns.Status(),
            AuditColumns.TaxonId(),
        };
        var f = new AuditFinding {
            ReportId = "demo",
            TaxonId = 12345,
            AssessmentId = 67890,
            RedlistUrl = "https://www.iucnredlist.org/species/12345/67890",
            ScientificName = "Panthera leo",
            StatusCode = "VU",
            StatusCategory = "Vulnerable",
        };
        return new AuditReport {
            Id = "demo", Title = "Demo", Summary = "x", DataSourceLabel = "src",
            Columns = columns, Findings = new[] { f },
        };
    }

    [Fact]
    public void HtmlTable_RendersBadgeLinkAndNumericSort() {
        var report = SampleReport();
        var html = HtmlListRenderer.Table(report, report.Findings);
        Assert.Contains("<em>Panthera leo</em>", html);
        Assert.Contains("href=\"https://www.iucnredlist.org/species/12345/67890\"", html);
        Assert.Contains("status-badge", html);
        Assert.Contains(">VU<", html);
        Assert.Contains("data-sort=\"12345", html); // numeric taxonId sort key
    }

    [Fact]
    public void FilterableTable_AddsControlsAndSortableClass() {
        var report = SampleReport();
        var html = HtmlListRenderer.FilterableTable(report, report.Findings, "tbl-demo");
        Assert.Contains("class=\"audit-table sortable\"", html);
        Assert.Contains("table-filter", html);
        Assert.Contains("1 rows", html);
        Assert.Contains("data-numeric=\"true\"", html); // taxonId header
    }

    [Fact]
    public void Csv_WritesHeaderKeysAndEscapes() {
        var columns = new List<AuditColumn> {
            new() { Key = "name", Header = "Name", Value = f => f.ScientificName },
            new() { Key = "detail", Header = "Detail", Value = f => f.Detail },
        };
        var findings = new[] {
            new AuditFinding { ReportId = "demo", Key = "1:x", ScientificName = "Aus, bus", Detail = "has \"quote\"" },
            new AuditFinding { ScientificName = "Cus dus" },
        };
        var csv = AuditCsvWriter.Write(columns, findings);
        var lines = csv.Replace("\r", "").Split('\n');
        Assert.Equal("id,name,detail", lines[0]);
        Assert.Equal("demo:1:x,\"Aus, bus\",\"has \"\"quote\"\"\"", lines[1]);
        // No key: the id cell is blank rather than invented.
        Assert.Equal(",Cus dus,", lines[2]);
    }

    // ---- AuditReleaseCounts ----

    [Fact]
    public void ReleaseCounts_FindsPreviousReleaseAndCounts() {
        var dir = Path.Combine(Path.GetTempPath(), "audit-rc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(dir, "audit"));
        File.WriteAllText(Path.Combine(dir, "audit", "release-counts.yml"), """
2025-1:
  empty-scope: 10
2025-2:
  empty-scope: 23
  no-latest: 3898
2026-1:
  empty-scope: 52
""");
        try {
            var counts = AuditReleaseCounts.Load(dir);
            Assert.Equal("2025-2", counts.PreviousRelease("2026-1"));
            Assert.Equal("2025-1", counts.PreviousRelease("2025-2"));
            Assert.Null(counts.PreviousRelease("2025-1"));
            Assert.Equal(23, counts.Count("2025-2", "empty-scope"));
            Assert.Null(counts.Count("2025-2", "taxonomy-cleanup"));
            Assert.Equal("2026-2:" + Environment.NewLine + "  a: 1" + Environment.NewLine + "  b: 2",
                AuditReleaseCounts.FormatBlock("2026-2", new[] { ("a", 1), ("b", 2) }));
        } finally {
            Directory.Delete(dir, true);
        }
    }

    // ---- AuditCommentary ----

    [Fact]
    public void Commentary_FiltersByReportAndRelease() {
        var dir = Path.Combine(Path.GetTempPath(), "audit-cmt-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(dir, "audit"));
        File.WriteAllText(Path.Combine(dir, "audit", "commentary.yml"), """
- report: failed-assessments
  release: 2025-2
  scope: report
  title: Note A
  markdown: about this release
- report: failed-assessments
  release: any
  scope: report
  markdown: carried forward
- report: failed-assessments
  release: 2026-1
  scope: report
  markdown: future only
""");
        try {
            var commentary = AuditCommentary.Load(dir);
            var entries = commentary.ForReport("failed-assessments", "2025-2");
            Assert.Equal(2, entries.Count); // the 2025-2 entry and the "any" entry, not the 2026-1 one
            Assert.Contains(entries, e => e.Title == "Note A");
            Assert.Contains(entries, e => e.Markdown.Contains("carried forward"));
            Assert.DoesNotContain(entries, e => e.Markdown.Contains("future only"));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Commentary_MissingFileIsEmpty() {
        var commentary = AuditCommentary.Load(Path.Combine(Path.GetTempPath(), "no-such-" + Guid.NewGuid().ToString("N")));
        Assert.Null(commentary.SourcePath);
        Assert.Empty(commentary.ForReport("failed-assessments", "2025-2"));
    }

    // ---- --limit runs and the output folder ----

    [Fact]
    public void OutputDir_LimitedRunDefaultsToItsOwnFolder() {
        var reports = Path.Combine(Path.GetTempPath(), "reports");
        var cwd = Path.GetTempPath();
        Assert.Equal(Path.Combine(reports, "redlist-audit-2026"),
            RedlistAuditSiteCommand.ResolveOutputDir(reports, null, limited: false, cwd));
        Assert.Equal(Path.Combine(reports, "redlist-audit-2026-limited"),
            RedlistAuditSiteCommand.ResolveOutputDir(reports, null, limited: true, cwd));
        // No reports_dir configured: ./reports under the current directory.
        Assert.Equal(Path.Combine(cwd, "reports", "redlist-audit-2026-limited"),
            RedlistAuditSiteCommand.ResolveOutputDir(null, "  ", limited: true, cwd));
        // An explicit --output wins, limited or not.
        var explicitDir = Path.Combine(cwd, "mine");
        Assert.Equal(explicitDir, RedlistAuditSiteCommand.ResolveOutputDir(reports, explicitDir, limited: true, cwd));
    }

    private static AuditDocument Doc(long? rowLimit, params AuditReport[] reports) => new() {
        Release = "2026-1",
        GeneratedAt = "2026-09-30",
        Reports = reports,
        PreviousRelease = "2025-2",
        ReleaseCounts = AuditReleaseCounts.Empty,
        RowLimit = rowLimit,
    };

    private static AuditReport EmptyReport(string id, string? familyId = null) => new() {
        Id = id, Title = "Empty " + id, Summary = "x", DataSourceLabel = "src", FamilyId = familyId,
    };

    private static string TempDir(string prefix) {
        var dir = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void Page_LimitedRunCarriesNoticeAndTitlePrefix() {
        var html = AuditPageLayout.Page(Doc(5000), "Demo", null, "<p>body</p>");
        Assert.Contains("class=\"limited-notice\"", html);
        Assert.Contains("<code>--limit 5000</code>", html);
        Assert.Contains("<title>Partial results · Demo · ", html);
        // The notice comes before the page body, so it is the first thing on the page.
        Assert.True(html.IndexOf("limited-notice", StringComparison.Ordinal) < html.IndexOf("<p>body</p>", StringComparison.Ordinal));

        var full = AuditPageLayout.Page(Doc(null), "Demo", null, "<p>body</p>");
        Assert.DoesNotContain("limited-notice", full);
        Assert.DoesNotContain("Partial results", full);
    }

    [Fact]
    public void Write_LimitedRun_EveryPageHasTheNoticeAndNoCountsFileIsWritten() {
        var dir = TempDir("audit-lim-");
        try {
            AuditSiteRenderer.Write(Doc(200, SampleReport(), EmptyReport("col-a", "col"), EmptyReport("col-b", "col")), dir);

            var pages = Directory.GetFiles(dir, "*.html");
            // index, demo, demo-list, col-a, col-b and the col-crosscheck entry page
            Assert.Equal(6, pages.Length);
            foreach (var page in pages) {
                Assert.Contains("class=\"limited-notice\"", File.ReadAllText(page));
            }
            Assert.False(File.Exists(Path.Combine(dir, AuditSiteRenderer.ReleaseCountsFileName)));
            // A partial count beside the previous release's full count would read as a change.
            Assert.DoesNotContain("Since 2025-2", File.ReadAllText(Path.Combine(dir, "index.html")));
            // An empty report says only that the rows checked had nothing.
            var empty = File.ReadAllText(Path.Combine(dir, "col-a.html"));
            Assert.Contains("in the rows checked", empty);
            Assert.DoesNotContain("in the current release", empty);
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Write_FullRun_WritesCountsFileAndSinceColumn() {
        var dir = TempDir("audit-full-");
        try {
            AuditSiteRenderer.Write(Doc(null, SampleReport(), EmptyReport("empty")), dir);
            var counts = File.ReadAllText(Path.Combine(dir, AuditSiteRenderer.ReleaseCountsFileName));
            Assert.Contains("2026-1:", counts);
            Assert.Contains("demo: 1", counts);
            Assert.Contains("Since 2025-2", File.ReadAllText(Path.Combine(dir, "index.html")));
            Assert.Contains("in the current release", File.ReadAllText(Path.Combine(dir, "empty.html")));
            Assert.DoesNotContain("limited-notice", File.ReadAllText(Path.Combine(dir, "index.html")));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    // A report that drops to zero rows, or is skipped, must not leave its old list and CSV in the
    // folder looking current. Once a folder has a manifest, only files on it are removed, so the
    // owner's own files are kept even when they look like the generator's.
    [Fact]
    public void Write_RemovesGeneratedFilesThisRunDidNotWrite() {
        var dir = TempDir("audit-stale-");
        try {
            AuditSiteRenderer.Write(Doc(null, SampleReport(), EmptyReport("gone")), dir);
            Assert.True(File.Exists(Path.Combine(dir, "demo-list.html")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "demo.csv")));
            var manifest = File.ReadAllLines(Path.Combine(dir, AuditSiteRenderer.ManifestFileName));
            Assert.Contains("demo-list.html", manifest);
            Assert.Contains("csv/demo.csv", manifest);
            Assert.Contains("assets/audit.css", manifest);

            File.WriteAllText(Path.Combine(dir, "notes.html"), "<!doctype html><p>my notes</p>");
            File.WriteAllText(Path.Combine(dir, "CNAME"), "audit.example.org\n");
            File.WriteAllText(Path.Combine(dir, "csv", "mine.csv"), "name,value\nx,1\n");
            // A hand-made CSV with an id column, and a hand-made page that copies the stylesheet link.
            File.WriteAllText(Path.Combine(dir, "csv", "mine2.csv"), "id,name\n1,x\n");
            File.WriteAllText(Path.Combine(dir, "mine.html"),
                "<!doctype html>\n<head>\n" + AuditPageLayout.StylesheetLink + "\n</head>");

            var logged = new List<string>();
            var result = AuditSiteRenderer.Write(Doc(null, EmptyReport("demo")), dir, logged.Add);

            Assert.True(File.Exists(Path.Combine(dir, "demo.html")));
            Assert.False(File.Exists(Path.Combine(dir, "demo-list.html")));
            Assert.False(File.Exists(Path.Combine(dir, "csv", "demo.csv")));
            Assert.False(File.Exists(Path.Combine(dir, "gone.html")));
            Assert.True(File.Exists(Path.Combine(dir, "notes.html")));
            Assert.True(File.Exists(Path.Combine(dir, "CNAME")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "mine.csv")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "mine2.csv")));
            Assert.True(File.Exists(Path.Combine(dir, "mine.html")));
            Assert.Equal(new[] { "csv/demo.csv", "demo-list.html", "gone.html" }, result.Removed);
            Assert.Empty(result.Kept);
            Assert.Contains(logged, l => l.Contains("removed csv/demo.csv"));

            manifest = File.ReadAllLines(Path.Combine(dir, AuditSiteRenderer.ManifestFileName));
            Assert.DoesNotContain("demo-list.html", manifest);
            Assert.DoesNotContain("mine.html", manifest);
            Assert.Contains("demo.html", manifest);
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    // A folder written before the manifest existed: the generator's files are recognised by their
    // content, once. An "id" header alone is not enough for a CSV; its first row id must start with
    // the file's own report id.
    [Fact]
    public void Write_FolderWithoutManifest_RemovesOnlyFilesWithTheGeneratorsMarks() {
        var dir = TempDir("audit-legacy-");
        try {
            Directory.CreateDirectory(Path.Combine(dir, "csv"));
            void Put(string relative, string content) => File.WriteAllText(Path.Combine(dir, relative), content);
            Put("demo-g-mammalia.html", "<!doctype html>\n<head>\n" + AuditPageLayout.StylesheetLink + "\n</head>");
            Put("notes.html", "<!doctype html><p>my notes</p>");
            Put("CNAME", "audit.example.org\n");
            Put(Path.Combine("csv", "old.csv"), "\uFEFFid,scientificName\nold:123,Panthera leo\n");
            Put(Path.Combine("csv", "quoted.csv"), "id,name\r\n\"quoted:1:a, b\",x\r\n");
            Put(Path.Combine("csv", "mine2.csv"), "id,name\n1,x\n");
            Put(Path.Combine("csv", "header-only.csv"), "id,name\n");
            Put(Path.Combine("csv", "other.csv"), "id,name\ndemo:1,x\n");

            var result = AuditSiteRenderer.Write(Doc(null, SampleReport()), dir);

            Assert.Equal(new[] { "csv/old.csv", "csv/quoted.csv", "demo-g-mammalia.html" }, result.Removed);
            Assert.True(File.Exists(Path.Combine(dir, "notes.html")));
            Assert.True(File.Exists(Path.Combine(dir, "CNAME")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "mine2.csv")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "header-only.csv")));
            Assert.True(File.Exists(Path.Combine(dir, "csv", "other.csv")));
            Assert.True(File.Exists(Path.Combine(dir, AuditSiteRenderer.ManifestFileName)));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    // The command passes prune: false when a producer failed, so that producer's pages from the
    // earlier run are kept. They stay on the manifest, so the next run without errors removes them.
    [Fact]
    public void Write_PruneFalse_KeepsEarlierFilesAndStillListsThem() {
        var dir = TempDir("audit-keep-");
        try {
            AuditSiteRenderer.Write(Doc(null, SampleReport(), EmptyReport("col-a", "col"), EmptyReport("col-b", "col")), dir);

            var kept = AuditSiteRenderer.Write(Doc(null, SampleReport()), dir, prune: false);
            Assert.Equal(new[] { "col-a.html", "col-b.html", "col-crosscheck.html" }, kept.Kept);
            Assert.Empty(kept.Removed);
            Assert.True(File.Exists(Path.Combine(dir, "col-a.html")));
            Assert.True(File.Exists(Path.Combine(dir, "col-crosscheck.html")));
            Assert.Contains("col-a.html", File.ReadAllLines(Path.Combine(dir, AuditSiteRenderer.ManifestFileName)));

            var next = AuditSiteRenderer.Write(Doc(null, SampleReport()), dir);
            Assert.Equal(new[] { "col-a.html", "col-b.html", "col-crosscheck.html" }, next.Removed);
            Assert.False(File.Exists(Path.Combine(dir, "col-a.html")));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    // A --limit run into the full run's folder (an explicit --output) leaves no full-run counts
    // file beside its partial pages.
    [Fact]
    public void Write_LimitedRunOverFullFolder_RemovesReleaseCounts() {
        var dir = TempDir("audit-counts-");
        try {
            AuditSiteRenderer.Write(Doc(null, SampleReport()), dir);
            Assert.True(File.Exists(Path.Combine(dir, AuditSiteRenderer.ReleaseCountsFileName)));
            var result = AuditSiteRenderer.Write(Doc(100, SampleReport()), dir);
            Assert.Equal(new[] { AuditSiteRenderer.ReleaseCountsFileName }, result.Removed);
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Write_IgnoresManifestEntriesOutsideTheFolder() {
        var parent = TempDir("audit-outside-");
        try {
            var site = Path.Combine(parent, "site");
            Directory.CreateDirectory(site);
            var outside = Path.Combine(parent, "outside.html");
            var absolute = Path.Combine(parent, "absolute.html");
            File.WriteAllText(outside, "x");
            File.WriteAllText(absolute, "x");
            File.WriteAllText(Path.Combine(site, AuditSiteRenderer.ManifestFileName),
                "../outside.html\n" + absolute + "\n" + AuditSiteRenderer.ManifestFileName + "\n");

            var result = AuditSiteRenderer.Write(Doc(null, SampleReport()), site);

            Assert.Empty(result.Removed);
            Assert.True(File.Exists(outside));
            Assert.True(File.Exists(absolute));
            Assert.True(File.Exists(Path.Combine(site, AuditSiteRenderer.ManifestFileName)));
        } finally {
            Directory.Delete(parent, recursive: true);
        }
    }

    // The failed-assessments producer reads every row whatever --limit says: the notice names it as
    // the exception, and its empty state speaks for the whole release.
    [Fact]
    public void LimitedNotice_NamesReportsThatIgnoreTheLimit() {
        var failed = new AuditReport {
            Id = "failed-assessments", Title = "Historical assessments missing from the API",
            Summary = "x", DataSourceLabel = "src", IgnoresRowLimit = true,
        };
        var withException = AuditPageLayout.LimitedNotice(Doc(5000, SampleReport(), failed));
        Assert.Contains("Every report except \u201C<a href=\"failed-assessments.html\">Historical assessments missing from the API</a>\u201D checked at most 5,000 database rows", withException);

        var withoutException = AuditPageLayout.LimitedNotice(Doc(5000, SampleReport()));
        Assert.Contains("Every report on this site checked at most 5,000 database rows", withoutException);
        Assert.Equal("", AuditPageLayout.LimitedNotice(Doc(null, SampleReport(), failed)));

        var dir = TempDir("audit-ignores-");
        try {
            AuditSiteRenderer.Write(Doc(5000, failed), dir);
            var page = File.ReadAllText(Path.Combine(dir, "failed-assessments.html"));
            Assert.Contains("in the current release", page);
            Assert.DoesNotContain("in the rows checked", page);
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void JoinWithAnd_JoinsLikeASentence() {
        Assert.Equal("", HtmlText.JoinWithAnd(Array.Empty<string>()));
        Assert.Equal("a", HtmlText.JoinWithAnd(new[] { "a" }));
        Assert.Equal("a and b", HtmlText.JoinWithAnd(new[] { "a", "b" }));
        Assert.Equal("a, b and c", HtmlText.JoinWithAnd(new[] { "a", "b", "c" }));
    }
}
