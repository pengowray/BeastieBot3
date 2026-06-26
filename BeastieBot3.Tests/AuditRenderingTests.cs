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
            new AuditFinding { ScientificName = "Aus, bus", Detail = "has \"quote\"" },
        };
        var csv = AuditCsvWriter.Write(columns, findings);
        var lines = csv.Replace("\r", "").Split('\n');
        Assert.Equal("name,detail", lines[0]);
        Assert.Equal("\"Aus, bus\",\"has \"\"quote\"\"\"", lines[1]);
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
}
