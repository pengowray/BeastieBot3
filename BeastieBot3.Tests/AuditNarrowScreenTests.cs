using System;
using System.IO;
using System.Text.RegularExpressions;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Rendering;

namespace BeastieBot3.Tests;

// The audit site on a phone. The report index and the list of crosscheck pages were tables that
// made a 375 px screen scroll sideways (the index was 500 px wide). Below 640 px audit.css shows each
// row as a block, with the column headings hidden from view, so each count carries its unit and
// each change its "Since <release>:" heading in a span that wider screens hide.
public class AuditNarrowScreenTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "audit-narrow-" + Guid.NewGuid().ToString("N"));

    public AuditNarrowScreenTests() {
        Directory.CreateDirectory(Path.Combine(_dir, "rules", "audit"));
        Directory.CreateDirectory(Path.Combine(_dir, "site"));
    }

    public void Dispose() {
        try { Directory.Delete(_dir, recursive: true); } catch { /* a locked temp dir is not a test failure */ }
    }

    private static AuditReport Report(string id, int count, string? familyId = null, int familyRank = 0) => new() {
        Id = id, Title = "Title " + id, Summary = "x", DataSourceLabel = "src", HeadlineCount = count,
        FamilyId = familyId, FamilyRank = familyRank, FamilyScope = familyId is null ? null : "Scope " + id,
    };

    // A full run (no --limit) with release 2025-2's counts, so the index has a "Since 2025-2" column.
    private string WriteSite(params AuditReport[] reports) {
        File.WriteAllText(AuditReleaseCounts.PathFor(Path.Combine(_dir, "rules")),
            "2025-2:\n  one: 1\n  many: 9\n  col-a: 2\n");
        var doc = new AuditDocument {
            Release = "2026-1",
            GeneratedAt = "2026-10-03",
            Reports = reports,
            PreviousRelease = "2025-2",
            ReleaseCounts = AuditReleaseCounts.Load(Path.Combine(_dir, "rules")),
        };
        var site = Path.Combine(_dir, "site");
        AuditSiteRenderer.Write(doc, site);
        return site;
    }

    [Fact]
    public void Index_CountCarriesItsUnit_SingularForOne() {
        var site = WriteSite(Report("one", 1), Report("many", 1234));
        var index = File.ReadAllText(Path.Combine(site, "index.html"));
        Assert.Contains("<td class=\"count\">1<span class=\"stack-label\"> row</span></td>", index);
        Assert.Contains("<td class=\"count\">1,234<span class=\"stack-label\"> rows</span></td>", index);
    }

    [Fact]
    public void Index_ChangeCarriesItsHeading_AndAnUnknownChangeStaysEmpty() {
        var site = WriteSite(Report("one", 1), Report("many", 12), Report("new", 3));
        var index = File.ReadAllText(Path.Combine(site, "index.html"));
        Assert.Contains("<th class=\"since\">Since 2025-2</th>", index);
        Assert.Contains("<td class=\"since \"><span class=\"stack-label\">Since 2025-2: </span>unchanged</td>", index);
        Assert.Contains("<td class=\"since up\"><span class=\"stack-label\">Since 2025-2: </span>up from 9</td>", index);
        // No count for 2025-2: the cell is empty, and a narrow screen hides it (td.since:empty).
        Assert.Contains("<td class=\"since \"></td>", index);
    }

    [Fact]
    public void CrosscheckPageList_CountCarriesItsUnit_AndItsChangeItsHeading() {
        var site = WriteSite(Report("col-a", 1, "col", 1), Report("col-b", 5, "col", 2));
        var page = File.ReadAllText(Path.Combine(site, "col-a.html"));
        Assert.Contains("<table class=\"summary family\">", page);
        Assert.Contains("<td class=\"num\">1<span class=\"stack-label\"> name</span></td>", page);
        Assert.Contains("<td class=\"num\">5<span class=\"stack-label\"> names</span></td>", page);
        Assert.Contains("<td class=\"since down\"><span class=\"stack-label\">Since 2025-2: </span>down from 2</td>", page);
    }

    private static string NarrowBlock() {
        var css = AuditAssets.Css;
        var start = css.IndexOf("@media (max-width: 640px) {", StringComparison.Ordinal);
        Assert.True(start >= 0, "No narrow-screen block in audit.css");
        var end = css.IndexOf("\n}", start, StringComparison.Ordinal);
        return css[start..end];
    }

    [Fact]
    public void Css_StackLabelsShowOnlyOnANarrowScreen() {
        var css = AuditAssets.Css;
        var narrow = NarrowBlock();
        Assert.Matches(@"(?m)^\.stack-label \{ display: none; \}", css);
        Assert.Contains(".stack-label { display: inline; }", narrow);
        // Printing keeps the columns, so it keeps the labels hidden.
        Assert.DoesNotContain("stack-label", css[css.IndexOf("@media print", StringComparison.Ordinal)..]);
    }

    [Fact]
    public void Css_NarrowScreenStacksBothTables_AndKeepsTheirHeadingsForScreenReaders() {
        var narrow = NarrowBlock();
        foreach (var table in new[] { "table.index", "table.summary.family" }) {
            Assert.Matches(Regex.Escape(table) + @" tr[,\s][^{]*\{ display: flex; flex-wrap: wrap;", narrow);
            Assert.Matches(Regex.Escape(table) + @" thead[,\s][^{]*\{ position: absolute; width: 1px; height: 1px; overflow: hidden;", narrow);
            Assert.Contains(table + " td.since:empty", narrow);
        }
        // The headings are hidden from view only, never with display: none.
        Assert.DoesNotMatch(@"thead[^{]*\{[^}]*display: none", narrow);
        // td.kind is 1% wide on a wide screen; as a block in a stacked row it must size to its badge.
        Assert.Contains("table.index td.kind", narrow);
    }
}
