using System.IO;
using BeastieBot3.Iucn;

namespace BeastieBot3.Tests;

// Pins where iucn report-synonym-formatting writes its CSV: beside the Markdown report unless
// --csv-output names a path. It used to resolve its own folder, so --markdown-output moved only
// the Markdown file and left the CSV in reports_dir.
public class SynonymFormattingReportPathTests {
    [Fact]
    public void DefaultCsv_GoesBesideTheMarkdownReport() {
        var markdown = Path.GetFullPath(Path.Combine("some-folder", "custom-name.md"));
        var csv = IucnSynonymFormattingReportCommand.ResolveCsvPath(null, markdown, "20260930-120000");
        Assert.Equal(Path.GetDirectoryName(markdown), Path.GetDirectoryName(csv));
        Assert.Equal("iucn-synonym-formatting-20260930-120000.csv", Path.GetFileName(csv));
    }

    [Fact]
    public void ExplicitCsvPath_Wins() {
        var markdown = Path.GetFullPath(Path.Combine("some-folder", "report.md"));
        var explicitCsv = Path.Combine("elsewhere", "out.csv");
        var csv = IucnSynonymFormattingReportCommand.ResolveCsvPath(explicitCsv, markdown, "20260930-120000");
        Assert.Equal(Path.GetFullPath(explicitCsv), csv);
    }

    [Fact]
    public void BlankCsvPath_CountsAsNotGiven() {
        var markdown = Path.GetFullPath(Path.Combine("some-folder", "report.md"));
        var csv = IucnSynonymFormattingReportCommand.ResolveCsvPath("  ", markdown, "20260930-120000");
        Assert.Equal(Path.GetDirectoryName(markdown), Path.GetDirectoryName(csv));
    }
}
