using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

// Generates three output files per chart group:
//   1. .tab  — Wikimedia Commons tabular data JSON (for Data: namespace)
//   2. .chart — Extension:Chart bar chart definition JSON
//   3. .wikitext — wikitext snippet to replace Wikipedia templates like {{IUCN mammal chart}}
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

    // ==================== .chart file ====================

    public static string BuildChartJson(ChartGroupResult result) {
        var baseName = BaseFileName(result);
        var titleText = $"IUCN Red List status of {result.ChartName} ({result.DatasetVersion})";

        var chart = new Dictionary<string, object> {
            ["license"] = "CC0-1.0",
            ["version"] = 1,
            ["type"] = "bar",
            ["source"] = $"{baseName}.tab",
            ["title"] = new Dictionary<string, string> { ["en"] = titleText },
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

        // Chart invocation with image frame
        sb.AppendLine("{{image frame");
        sb.AppendLine($"|content={{{{#chart:{baseName}.Bar.chart}}}} [[commons:Data:{baseName}.tab|'''Raw data''']]");
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

    // ==================== Write all files ====================

    public static void WriteAll(ChartGroupResult result, string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var baseName = BaseFileName(result);

        var tabPath = Path.Combine(outputDirectory, $"{baseName}.tab");
        File.WriteAllText(tabPath, BuildTabJson(result), Encoding.UTF8);

        var chartPath = Path.Combine(outputDirectory, $"{baseName}.Bar.chart");
        File.WriteAllText(chartPath, BuildChartJson(result), Encoding.UTF8);

        var wikitextPath = Path.Combine(outputDirectory, $"{baseName}.wikitext");
        File.WriteAllText(wikitextPath, BuildWikitext(result), Encoding.UTF8);
    }

    private static string FormatNum(int n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
