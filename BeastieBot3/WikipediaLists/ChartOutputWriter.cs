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
    /// Base filename with version for .tab data files, e.g. "IUCN Red List mammals 2025-2".
    /// </summary>
    public static string BaseFileName(ChartGroupResult result) =>
        $"IUCN Red List {result.ChartName} {result.DatasetVersion}";

    /// <summary>
    /// Base filename without version for .wikitext files, e.g. "IUCN Red List mammals".
    /// Wikitext files are updated in place and reference the versioned .tab file.
    /// </summary>
    public static string WikitextFileName(ChartGroupResult result) =>
        $"IUCN Red List {result.ChartName}";

    // ==================== .tab file ====================

    public static string BuildTabJson(ChartGroupResult result) {
        var description = $"IUCN Red List assessment counts for {result.ChartName} by conservation status category (version {result.DatasetVersion})";

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
    /// Filename for the single shared chart definition.
    /// All per-group wikitext snippets reference this via {{#chart:}} with |data= override.
    /// </summary>
    public static string SharedChartFileName => "IUCN Red List species.Bar.chart";

    public static string BuildSharedChartJson() {
        var chart = new Dictionary<string, object> {
            ["license"] = "CC0-1.0",
            ["version"] = 1,
            ["type"] = "bar",
            ["title"] = new Dictionary<string, string> {
                ["en"] = "Number of species by IUCN Red List category"
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

        // Extract all counts
        var ex = CountFor(result, "EX");
        var ew = CountFor(result, "EW");
        var crPe = CountFor(result, "CR(PE)");
        var crPew = CountFor(result, "CR(PEW)");
        var crPure = CountFor(result, "CR");
        var en = CountFor(result, "EN");
        var vu = CountFor(result, "VU");
        var nt = CountFor(result, "NT");
        var lc = CountFor(result, "LC");
        var dd = CountFor(result, "DD");

        var crTotal = crPure + crPe + crPew;
        var total = result.TotalAssessed;
        var threatened = crTotal + en + vu;
        var threatenedUpper = threatened + dd;
        var notThreatened = nt + lc;

        // Chart invocation with image frame — uses shared chart definition with |data= override
        sb.AppendLine("{{image frame");
        sb.AppendLine($"|content={{{{#chart:{SharedChartFileName}|data={baseName}.tab}}}} [[commons:Data:{baseName}.tab|'''Raw data''']]");
        sb.AppendLine("|max-width=400");
        sb.AppendLine("|align=right");
        sb.AppendLine("|pos=bottom");

        // Caption title
        var chartTitle = char.ToUpper(result.ChartName[0]) + result.ChartName[1..];
        sb.AppendLine($"|caption='''{chartTitle} species''' (IUCN, {result.DatasetVersion})");

        // Total assessed
        if (result.Comprehensive) {
            sb.AppendLine($"* {Fmt(total)} species assessed (comprehensively assessed group)");
        } else {
            sb.AppendLine($"* {Fmt(total)} species assessed");
        }

        // Extinct and extinct in the wild
        if (ex > 0 || ew > 0) {
            var extinctTotal = ex + ew;
            if (ex > 0 && ew > 0) {
                sb.AppendLine($"* {Fmt(extinctTotal)} assessed as [[extinction|extinct]] (EX) or [[extinct in the wild]] (EW):");
                sb.AppendLine($"** {Fmt(ex)} [[extinct]] <small>(EX)</small>{{{{efn|Extinct (EX) as defined by the IUCN: no reasonable doubt that the last individual has died. Includes species declared extinct since the Red List began.|group=ic}}}}");
                sb.AppendLine($"** {Fmt(ew)} [[extinct in the wild]] <small>(EW)</small>");
            } else if (ex > 0) {
                sb.AppendLine($"* {Fmt(ex)} assessed as [[extinction|extinct]] <small>(EX)</small>{{{{efn|Extinct (EX) as defined by the IUCN: no reasonable doubt that the last individual has died. Includes species declared extinct since the Red List began.|group=ic}}}}");
            } else {
                sb.AppendLine($"* {Fmt(ew)} assessed as [[extinct in the wild]] <small>(EW)</small>");
            }
        }

        // Threatened: CR + EN + VU (with DD upper estimate)
        if (threatened > 0) {
            if (dd > 0) {
                sb.AppendLine($"* {Fmt(threatened)} to {Fmt(threatenedUpper)} [[threatened species|threatened]]{{{{efn|Threatened comprises CR, EN, and VU. Upper estimate additionally includes [[data deficient|Data Deficient]] (DD) species, which may prove to be threatened once assessed.|group=ic}}}}");
            } else {
                sb.AppendLine($"* {Fmt(threatened)} [[threatened species|threatened]] (CR, EN, VU)");
            }
        }

        // CR detail with PE/PEW breakdown
        if (crTotal > 0) {
            sb.Append($"** {Fmt(crTotal)} [[critically endangered]] <small>(CR)</small>");
            if (crPe > 0 || crPew > 0) {
                var peDetails = new List<string>();
                if (crPe > 0) {
                    peDetails.Add($"{Fmt(crPe)} [[possibly extinct]]");
                }
                if (crPew > 0) {
                    peDetails.Add($"{Fmt(crPew)} possibly extinct in the wild");
                }
                sb.Append($", including {string.Join(" and ", peDetails)} (shown separately in chart)");
            }
            sb.AppendLine();
        }

        // EN and VU
        if (en > 0) {
            sb.AppendLine($"** {Fmt(en)} [[endangered species|endangered]] <small>(EN)</small>");
        }
        if (vu > 0) {
            sb.AppendLine($"** {Fmt(vu)} [[vulnerable species|vulnerable]] <small>(VU)</small>");
        }

        // Not threatened
        if (notThreatened > 0) {
            sb.Append($"* {Fmt(notThreatened)} not threatened at present");
            if (result.LrCdMerged > 0) {
                sb.AppendLine($"{{{{efn|[[Near threatened]] (NT) and [[Least concern]] (LC). NT includes {Fmt(result.LrCdMerged)} species assessed as [[conservation dependent|Lower Risk/conservation dependent]] (LR/cd).|group=ic}}}}");
            } else {
                sb.AppendLine("{{efn|[[Near threatened]] (NT) and [[Least concern]] (LC).|group=ic}}");
            }
        }

        // Data Deficient
        if (dd > 0) {
            sb.AppendLine($"* {Fmt(dd)} [[data deficient]] <small>(DD)</small>");
        }

        // Footnotes
        sb.AppendLine("----");
        sb.AppendLine("<small>{{notelist|group=ic}}</small>");

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

        var tabPath = Path.Combine(outputDirectory, $"{BaseFileName(result)}.tab");
        File.WriteAllText(tabPath, BuildTabJson(result), Encoding.UTF8);

        var wikitextPath = Path.Combine(outputDirectory, $"{WikitextFileName(result)}.wikitext");
        File.WriteAllText(wikitextPath, BuildWikitext(result), Encoding.UTF8);
    }

    /// <summary>
    /// Writes the single shared .Bar.chart definition (once per run).
    /// </summary>
    public static void WriteSharedChart(string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var chartPath = Path.Combine(outputDirectory, SharedChartFileName);
        File.WriteAllText(chartPath, BuildSharedChartJson(), Encoding.UTF8);
    }

    // ==================== Summary file ====================

    public static void WriteSummary(IReadOnlyList<ChartGroupResult> results, string outputDirectory) {
        Directory.CreateDirectory(outputDirectory);
        var sb = new StringBuilder();

        var version = results.Count > 0 ? results[0].DatasetVersion : "unknown";
        sb.AppendLine("IUCN Red List Chart Generation Summary");
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
                Fmt(result.TotalAssessed),
                Fmt(GetCount(c, "EX")),
                Fmt(GetCount(c, "EW")),
                Fmt(GetCount(c, "CR(PE)")),
                Fmt(GetCount(c, "CR(PEW)")),
                Fmt(GetCount(c, "CR")),
                Fmt(GetCount(c, "EN")),
                Fmt(GetCount(c, "VU")),
                Fmt(GetCount(c, "NT")),
                Fmt(GetCount(c, "LC")),
                Fmt(GetCount(c, "DD"))));
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
                sb.AppendLine($"      {r.ChartName}: {Fmt(r.LrCdMerged)} LR/cd merged into NT");
            }
        }

        sb.AppendLine();
        sb.AppendLine("Files generated:");
        sb.AppendLine($"  Shared chart: {SharedChartFileName}");
        foreach (var result in results) {
            sb.AppendLine($"  {BaseFileName(result)}.tab");
            sb.AppendLine($"  {WikitextFileName(result)}.wikitext");
        }

        var summaryPath = Path.Combine(outputDirectory, "summary.txt");
        File.WriteAllText(summaryPath, sb.ToString(), Encoding.UTF8);
    }

    private static int CountFor(ChartGroupResult result, string code) =>
        result.Counts.FirstOrDefault(c => c.Code == code)?.Count ?? 0;

    private static int GetCount(Dictionary<string, int> counts, string code) =>
        counts.TryGetValue(code, out var c) ? c : 0;

    private static string Fmt(int n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
