using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

// Generates output files for chart groups:
//   Per group:  .tab (tabular data) and .wikitext (embedding snippet)
//   Once:       shared .Bar.chart definition (reused via |data= override)
//               summary.txt with all group statistics
//
// Uses System.Text.Json for output (no ORM, no Mustache — the JSON structure is
// fixed and simple enough that templates would add complexity without benefit).

namespace BeastieBot3.WikipediaLists;

internal static class ChartOutputWriter {
    private static readonly JsonSerializerOptions JsonOptions = new() {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Base filename without extension, e.g. "IUCN Red List mammals 2025-2".
    /// </summary>
    public static string BaseFileName(ChartGroupResult result) =>
        $"IUCN Red List {result.ChartName} {result.DatasetVersion}";

    // ==================== .tab file ====================

    public static string BuildTabJson(ChartGroupResult result) {
        var description = $"IUCN Red List assessment counts for {result.ChartName} by conservation status category";

        var notes = new List<string>();
        notes.Add($"Source: IUCN Red List of Threatened Species, version {result.DatasetVersion}.");
        notes.Add("Counts include global assessments of full species only (excludes subspecies, varieties, and regional assessments).");
        notes.Add("CR excludes species tagged as possibly extinct CR(PE) or possibly extinct in the wild CR(PEW), which are shown separately.");
        if (result.LrCdMerged > 0) {
            notes.Add($"NT includes {result.LrCdMerged} species assessed as Lower Risk/conservation dependent (LR/cd).");
        }

        var fields = new List<object> {
            new {
                name = "category",
                type = "string",
                title = new Dictionary<string, string> { ["en"] = "Red List category" }
            },
            new {
                name = "count",
                type = "number",
                title = new Dictionary<string, string> { ["en"] = "Number of species" }
            }
        };

        var data = new List<object[]>();
        foreach (var c in result.Counts) {
            data.Add(new object[] { c.Code, c.Count });
        }

        var tab = new Dictionary<string, object> {
            ["license"] = "CC0-1.0",
            ["description"] = new Dictionary<string, string> { ["en"] = description },
            ["sources"] = string.Join(" ", notes),
            ["schema"] = new Dictionary<string, object> { ["fields"] = fields },
            ["data"] = data,
        };

        return JsonSerializer.Serialize(tab, JsonOptions);
    }

    // ==================== Shared .chart file ====================

    /// <summary>
    /// Filename for the single shared chart definition, e.g. "IUCN Red List species.Bar.chart".
    /// All per-group wikitext snippets reference this via {{#chart:}} with |data= override.
    /// </summary>
    public static string SharedChartFileName => "IUCN Red List species.Bar.chart";

    public static string BuildSharedChartJson(string datasetVersion) {
        var chart = new Dictionary<string, object> {
            ["license"] = "CC0-1.0",
            ["version"] = 1,
            ["type"] = "bar",
            ["title"] = new Dictionary<string, string> {
                ["en"] = $"Number of species by IUCN Red List category ({datasetVersion})"
            },
            ["xAxis"] = new Dictionary<string, object> {
                ["title"] = new Dictionary<string, string> { ["en"] = "Red List category" },
            },
            ["yAxis"] = new Dictionary<string, object> {
                ["title"] = new Dictionary<string, string> { ["en"] = "Number of species" },
                ["format"] = false,
            },
        };

        return JsonSerializer.Serialize(chart, JsonOptions);
    }

    // ==================== .wikitext file ====================

    public static string BuildWikitext(ChartGroupResult result) {
        var baseName = BaseFileName(result);
        var sb = new StringBuilder();

        // Chart invocation with image frame — uses shared chart definition with |data= override
        sb.AppendLine("{{image frame");
        sb.AppendLine($"|content={{{{#chart:{SharedChartFileName}|data={baseName}.tab}}}} [[commons:Data:{baseName}.tab|'''Raw data''']]");
        sb.AppendLine("|max-width=400");
        sb.AppendLine("|align=right");
        sb.AppendLine("|pos=bottom");

        // Caption
        sb.Append($"|caption='''IUCN Red List status of {result.ChartName}''' ({result.DatasetVersion})");
        sb.AppendLine();

        // Summary statistics
        var total = result.TotalAssessed;
        var crPe = result.Counts.FirstOrDefault(c => c.Code == "CR(PE)")?.Count ?? 0;
        var crPew = result.Counts.FirstOrDefault(c => c.Code == "CR(PEW)")?.Count ?? 0;
        var crPure = result.Counts.FirstOrDefault(c => c.Code == "CR")?.Count ?? 0;
        var crTotal = crPure + crPe + crPew;
        var ex = result.Counts.FirstOrDefault(c => c.Code == "EX")?.Count ?? 0;
        var ew = result.Counts.FirstOrDefault(c => c.Code == "EW")?.Count ?? 0;
        var en = result.Counts.FirstOrDefault(c => c.Code == "EN")?.Count ?? 0;
        var vu = result.Counts.FirstOrDefault(c => c.Code == "VU")?.Count ?? 0;
        var nt = result.Counts.FirstOrDefault(c => c.Code == "NT")?.Count ?? 0;
        var lc = result.Counts.FirstOrDefault(c => c.Code == "LC")?.Count ?? 0;
        var dd = result.Counts.FirstOrDefault(c => c.Code == "DD")?.Count ?? 0;

        var threatened = crTotal + en + vu;
        var extantAssessed = total - ex;

        sb.AppendLine($"* {FormatNum(total)} species assessed");
        sb.AppendLine($"* {FormatNum(threatened)} [[threatened species|threatened]] (CR, EN, VU)");

        if (crPe > 0 || crPew > 0) {
            sb.Append($"* CR includes {FormatNum(crPe)} [[possibly extinct]]");
            if (crPew > 0) {
                sb.Append($" and {FormatNum(crPew)} possibly extinct in the wild");
            }
            sb.AppendLine(" (shown separately in chart)");
        }

        if (result.LrCdMerged > 0) {
            sb.AppendLine($"* NT includes {FormatNum(result.LrCdMerged)} [[conservation dependent]] (LR/cd) species");
        }

        sb.AppendLine("}}");
        sb.AppendLine();
        sb.AppendLine($"<!-- Generated by BeastieBot3 from IUCN Red List version {result.DatasetVersion} -->");

        return sb.ToString();
    }

    // ==================== Write per-group files ====================

    /// <summary>
    /// Writes .tab and .wikitext for a single chart group.
    /// The shared .chart file is written separately via <see cref="WriteSharedChart"/>.
    /// </summary>
    public static void WriteGroupFiles(ChartGroupResult result, string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var baseName = BaseFileName(result);

        var tabPath = Path.Combine(outputDirectory, $"{baseName}.tab");
        File.WriteAllText(tabPath, BuildTabJson(result), Encoding.UTF8);

        var wikitextPath = Path.Combine(outputDirectory, $"{baseName}.wikitext");
        File.WriteAllText(wikitextPath, BuildWikitext(result), Encoding.UTF8);
    }

    /// <summary>
    /// Writes the single shared .Bar.chart definition (once per run).
    /// </summary>
    public static void WriteSharedChart(string datasetVersion, string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var chartPath = Path.Combine(outputDirectory, SharedChartFileName);
        File.WriteAllText(chartPath, BuildSharedChartJson(datasetVersion), Encoding.UTF8);
    }

    // ==================== Summary file ====================

    public static void WriteSummary(IReadOnlyList<ChartGroupResult> results, string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var sb = new StringBuilder();

        var version = results.Count > 0 ? results[0].DatasetVersion : "unknown";
        sb.AppendLine($"IUCN Red List Chart Generation Summary");
        sb.AppendLine($"Dataset version: {version}");
        sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        sb.AppendLine($"Groups: {results.Count}");
        sb.AppendLine();

        // Header
        sb.AppendLine(string.Format(
            "{0,-22} {1,8} {2,6} {3,6} {4,7} {5,8} {6,6} {7,6} {8,6} {9,6} {10,6} {11,6}",
            "Group", "Total", "EX", "EW", "CR(PE)", "CR(PEW)", "CR", "EN", "VU", "NT", "LC", "DD"));
        sb.AppendLine(new string('-', 107));

        foreach (var result in results) {
            var c = result.Counts.ToDictionary(x => x.Code, x => x.Count);
            sb.AppendLine(string.Format(
                "{0,-22} {1,8} {2,6} {3,6} {4,7} {5,8} {6,6} {7,6} {8,6} {9,6} {10,6} {11,6}",
                result.ChartName,
                FormatNum(result.TotalAssessed),
                FormatNum(GetCount(c, "EX")),
                FormatNum(GetCount(c, "EW")),
                FormatNum(GetCount(c, "CR(PE)")),
                FormatNum(GetCount(c, "CR(PEW)")),
                FormatNum(GetCount(c, "CR")),
                FormatNum(GetCount(c, "EN")),
                FormatNum(GetCount(c, "VU")),
                FormatNum(GetCount(c, "NT")),
                FormatNum(GetCount(c, "LC")),
                FormatNum(GetCount(c, "DD"))));
        }

        sb.AppendLine();

        // Notes
        sb.AppendLine("Notes:");
        sb.AppendLine("  - Counts include global assessments of full species only.");
        sb.AppendLine("  - CR excludes CR(PE) and CR(PEW); all bars are mutually exclusive.");

        var anyLrCd = results.Any(r => r.LrCdMerged > 0);
        if (anyLrCd) {
            sb.AppendLine("  - NT includes Lower Risk/conservation dependent (LR/cd) species:");
            foreach (var r in results.Where(r => r.LrCdMerged > 0)) {
                sb.AppendLine($"      {r.ChartName}: {FormatNum(r.LrCdMerged)} LR/cd merged into NT");
            }
        }

        sb.AppendLine();
        sb.AppendLine("Files generated:");
        sb.AppendLine($"  Shared chart: {SharedChartFileName}");
        foreach (var result in results) {
            var baseName = BaseFileName(result);
            sb.AppendLine($"  {baseName}.tab");
            sb.AppendLine($"  {baseName}.wikitext");
        }

        var summaryPath = Path.Combine(outputDirectory, "summary.txt");
        File.WriteAllText(summaryPath, sb.ToString(), Encoding.UTF8);
    }

    private static int GetCount(Dictionary<string, int> counts, string code) =>
        counts.TryGetValue(code, out var c) ? c : 0;

    private static string FormatNum(int n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
