using System;
using System.IO;
using System.Linq;
using BeastieBot3.Sprat;

namespace BeastieBot3.Tests;

// Pins the offline half of `sprat download`: the form it posts, finding the form's submit URL in
// the report page, reading the DDMMYYYY-HHMMSS stamp in the site's file names (not sortable as
// text), and the check that a download is the two-header-row "Select ALL" report the importer needs.
public class SpratReportDownloadTests {
    [Fact]
    public void FindSubmitAction_ReturnsTheFormActionWithItsSessionId() {
        var html = "<div><form id=\"reportForm\" class=\"form-horizontal\" " +
            "action=\"/sprat-public/action/report-submit;jsessionid=B3764CF9\" method=\"post\" enctype=\"multipart/form-data\">";
        Assert.Equal("/sprat-public/action/report-submit;jsessionid=B3764CF9", SpratReportDownload.FindSubmitAction(html));
    }

    [Fact]
    public void FindSubmitAction_ReturnsNullForAPageWithoutTheReportForm() {
        Assert.Null(SpratReportDownload.FindSubmitAction("<html><h1>Page Is Unavailable</h1></html>"));
        Assert.Null(SpratReportDownload.FindSubmitAction("<form action=\"/search\">"));
    }

    [Fact]
    public void BuildFormFields_SelectsEveryColumnOfEveryGroup() {
        var fields = SpratReportDownload.BuildFormFields();
        Assert.Contains(fields, f => f.Key == "reportType" && f.Value == "CSV");
        Assert.Contains(fields, f => f.Key == "generate-csv");
        // 3 name/id columns + 9 EPBC + 3 documents + 7 taxonomy + 9 state/IUCN + 17 presence.
        Assert.Equal(48, fields.Count(f => f.Key == "csvReport.selectedColumns"));
        Assert.Equal(5, fields.Count(f => f.Key.EndsWith("_SELECTED") && !f.Key.StartsWith('_') && f.Value == "true"));
        Assert.Contains(fields, f => f.Key == "csvReport.selectedColumns" && f.Value == "THREAT_ICUN_RED_LIST_STATUS");
    }

    [Theory]
    [InlineData("01102026-014425-report.csv", 2026, 10, 1, 1, 44, 25)]
    [InlineData("/data/sprat/25062026-070407-report.csv", 2026, 6, 25, 7, 4, 7)]
    public void ReadReportStamp_ReadsDayMonthYearFirst(string name, int y, int mo, int d, int h, int mi, int s) {
        Assert.Equal(new DateTime(y, mo, d, h, mi, s), SpratReportDownload.ReadReportStamp(name));
    }

    [Theory]
    [InlineData("report.csv")]
    [InlineData("sprat-report-download.csv.part")]
    [InlineData("32132026-014425-report.csv")]
    public void ReadReportStamp_ReturnsNullForOtherNames(string name) {
        Assert.Null(SpratReportDownload.ReadReportStamp(name));
    }

    [Fact]
    public void FindNewestReport_UsesTheDateInTheNameNotTextOrder() {
        var dir = Directory.CreateTempSubdirectory("sprat-download-test").FullName;
        try {
            // Sorted as text, 25062026 comes after 01102026.
            File.WriteAllText(Path.Combine(dir, "25062026-070407-report.csv"), "june");
            File.WriteAllText(Path.Combine(dir, "01102026-014425-report.csv"), "october");
            File.WriteAllText(Path.Combine(dir, "notes-report.csv"), "not a report");
            Assert.Equal("01102026-014425-report.csv", Path.GetFileName(SpratReportDownload.FindNewestReport(dir)));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static string Csv(string firstCell, Func<string, bool> keepHeader, int dataRows) {
        var headers = SpratColumns.HeaderMap.Keys.Where(keepHeader).Append("Listed Name").Append("Listed Name").ToList();
        var lines = new[] {
            string.Join(",", headers.Select((_, i) => i == 0 ? $"\"{firstCell}\"" : "\"Taxonomic Data\"")),
            string.Join(",", headers.Select(h => $"\"{h}\"")),
        }.Concat(Enumerable.Range(1, dataRows).Select(r => string.Join(",", headers.Select((_, i) => $"\"r{r}c{i}\""))));
        return string.Join("\n", lines) + "\n";
    }

    [Fact]
    public void CheckReport_AcceptsTheReportAndCountsTaxa() {
        var check = SpratReportDownload.CheckReport(new StringReader(Csv("Current Name and SPRAT ID", _ => true, 3)));
        Assert.True(check.IsValid, check.Problem);
        Assert.Equal(3, check.Taxa);
    }

    [Fact]
    public void CheckReport_RejectsAPageThatIsNotTheReport() {
        var check = SpratReportDownload.CheckReport(new StringReader("<html><body>Page Is Unavailable</body></html>\n"));
        Assert.False(check.IsValid);
    }

    [Fact]
    public void CheckReport_ListsColumnsTheImporterNeeds() {
        var check = SpratReportDownload.CheckReport(new StringReader(Csv("Current Name and SPRAT ID", h => h != "EPBC Threat Status", 3)));
        Assert.False(check.IsValid);
        Assert.Equal(new[] { "EPBC Threat Status" }, check.MissingColumns);
    }

    [Fact]
    public void CheckReport_RejectsAReportWithNoRows() {
        Assert.False(SpratReportDownload.CheckReport(new StringReader(Csv("Current Name and SPRAT ID", _ => true, 0))).IsValid);
    }

    [Fact]
    public void SameContent_ComparesBytes() {
        var dir = Directory.CreateTempSubdirectory("sprat-download-test").FullName;
        try {
            var a = Path.Combine(dir, "a.csv");
            var b = Path.Combine(dir, "b.csv");
            var c = Path.Combine(dir, "c.csv");
            File.WriteAllText(a, "\"x\",\"y\"\n");
            File.WriteAllText(b, "\"x\",\"y\"\n");
            File.WriteAllText(c, "\"x\",\"z\"\n");
            Assert.True(SpratReportDownload.SameContent(a, b));
            Assert.False(SpratReportDownload.SameContent(a, c));
        } finally {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void WriteLineUnwrapped_KeepsALongPathOnOneLine() {
        var writer = new StringWriter();
        var console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings {
            Ansi = Spectre.Console.AnsiSupport.No,
            Out = new Spectre.Console.AnsiConsoleOutput(writer),
        });
        console.Profile.Width = 40;
        var line = "SPRAT_csv=/a/very/long/folder/name/that/goes/past/forty/columns/01102026-014425-report.csv";
        BeastieBot3.Infrastructure.ConsoleSize.WriteLineUnwrapped(console, line);
        Assert.Equal(line + Environment.NewLine, writer.ToString());
    }
}
