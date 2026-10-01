using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;

// The offline half of `sprat download`: the report form's fields, finding the form's submit URL in
// the report page, checking a downloaded CSV is the "Select ALL" report SpratImporter expects, and
// picking the newest report already in the folder. Kept free of HTTP so it can be tested against a
// saved page and CSV.

namespace BeastieBot3.Sprat;

internal static class SpratReportDownload {
    public const string ReportPageUrl = "https://environment.gov.au/sprat-public/action/report";

    // The site answers 403 to every non-browser user agent tried (curl, "BeastieBot3/1.0", and
    // "Mozilla/5.0 (compatible; ...)"), so the default is a desktop Chrome string.
    public const string DefaultUserAgent =
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

    // First cell of the report's category row (the first of its two header rows).
    public const string FirstCategoryHeader = "Current Name and SPRAT ID";

    // The "Current Name and SPRAT ID" columns, which the form always selects.
    private static readonly string[] CommonColumns = { "TAXON_ID", "CURRENT_SCIENTIFIC_NAME", "COMMON_NAME" };

    // The form's column groups, in form order, each with every option of its multi-select list.
    // Selecting all of them is what clicking "Select ALL" in every group does.
    private static readonly (string Group, string[] Columns)[] ColumnGroups = {
        ("EPBC_ACT_THREAT_SPEC", new[] {
            "EPBC_THREAT_STATUS", "EPBC_MIGRATORY_STATUS", "EPBC_MIG_BONN_STATUS", "EPBC_MIG_CAMBA_STATUS",
            "EPBC_MIG_JAMBA_STATUS", "EPBC_MIG_ROKAMBA_STATUS", "EPBC_MARINE_STATUS", "EPBC_CETACEAN_STATUS",
            "FPAL_STATUS",
        }),
        ("EPBC_ACT_DOCUMENTS", new[] { "CONSERVATION_ADVICE", "RECOVERY_PLAN", "RECOVERY_PLAN_DESCISION" }),
        ("TAXONOMY_DATA", new[] {
            "TAXON_KINGDOM", "TAXON_PHYLUM", "TAXON_CLASS", "TAXON_ORDER", "TAXON_FAMILY", "TAXON_GENUS", "TAXON_GROUP",
        }),
        ("STATE_GOV_THREAT_SPEC", new[] {
            "THREAT_ACT_NCA_STATUS", "THREAT_NSW_TSC_STATUS", "THREAT_NT_TPWCA_STATUS", "THREAT_QLD_NCA_STATUS",
            "THREAT_SA_NPW_STATUS", "THREAT_TAS_TSCA_STATUS", "THREAT_VIC_FFGA_STATUS", "THREAT_WA_WCAPR_STATUS",
            "THREAT_ICUN_RED_LIST_STATUS",
        }),
        ("STATE_GOV_OCEANIC_PREC", new[] {
            "ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA",
            "ACI", "CKI", "CI", "CSI", "JBT", "NFI", "HMI", "AAT", "CMA",
        }),
    };

    /// <summary>
    /// The multipart fields a browser posts for a CSV report with every column selected, including
    /// the Spring "_field" markers that sit beside each list and checkbox. Order matters only for
    /// the column order of the CSV, which follows the form.
    /// </summary>
    public static IReadOnlyList<KeyValuePair<string, string>> BuildFormFields() {
        var fields = new List<KeyValuePair<string, string>> { new("reportType", "CSV") };
        foreach (var column in CommonColumns) {
            fields.Add(new("csvReport.selectedColumns", column));
        }
        fields.Add(new("_csvReport.selectedColumns", "1"));

        foreach (var (group, columns) in ColumnGroups) {
            foreach (var column in columns) {
                fields.Add(new("csvReport.selectedColumns", column));
            }
            fields.Add(new("_csvReport.selectedColumns", "1"));
            fields.Add(new($"csvReport.{group}_SELECTED", "true"));
            fields.Add(new($"_csvReport.{group}_SELECTED", "on"));
        }

        fields.Add(new("generate-csv", "Generate Report"));
        return fields;
    }

    private static readonly Regex SubmitActionPattern = new(
        @"<form\b[^>]*\baction=""(?<action>[^""]*report-submit[^""]*)""",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The report form's action URL, which carries the session id (";jsessionid=...") as well as
    /// the cookie. Null when the page has no report form.
    /// </summary>
    public static string? FindSubmitAction(string html) {
        var match = SubmitActionPattern.Match(html);
        return match.Success ? System.Net.WebUtility.HtmlDecode(match.Groups["action"].Value) : null;
    }

    private static readonly Regex ReportFileNamePattern = new(
        @"^(?<day>\d{2})(?<month>\d{2})(?<year>\d{4})-(?<hour>\d{2})(?<minute>\d{2})(?<second>\d{2})-report\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The time stamped in a report file name like "01102026-014425-report.csv" (DDMMYYYY-HHMMSS).
    /// The site writes the hour on a 12-hour clock with no AM/PM, so two reports from the same day
    /// can sort in the wrong order; callers break ties by file modification time.
    /// </summary>
    public static DateTime? ReadReportStamp(string fileName) {
        var match = ReportFileNamePattern.Match(Path.GetFileName(fileName));
        if (!match.Success) {
            return null;
        }
        int Part(string name) => int.Parse(match.Groups[name].Value, CultureInfo.InvariantCulture);
        try {
            return new DateTime(Part("year"), Part("month"), Part("day"), Part("hour"), Part("minute"), Part("second"));
        } catch (ArgumentOutOfRangeException) {
            return null;
        }
    }

    /// <summary>
    /// The newest report CSV in <paramref name="directory"/> by the date in its file name, then by
    /// modification time. Null when the folder has none.
    /// </summary>
    public static string? FindNewestReport(string directory) {
        if (!Directory.Exists(directory)) {
            return null;
        }
        return Directory.EnumerateFiles(directory, "*-report.csv")
            .Select(path => (Path: path, Stamp: ReadReportStamp(path)))
            .Where(f => f.Stamp is not null)
            .OrderByDescending(f => f.Stamp!.Value.Date)
            .ThenByDescending(f => File.GetLastWriteTimeUtc(f.Path))
            .Select(f => f.Path)
            .FirstOrDefault();
    }

    /// <summary>
    /// The result of reading a downloaded report: the number of taxa, or a problem worded to follow
    /// "Not the SPRAT report CSV:" (or the missing columns, when that is the problem).
    /// </summary>
    public sealed record ReportCheck(int Taxa, string? Problem, IReadOnlyList<string> MissingColumns) {
        public bool IsValid => Problem is null;
    }

    /// <summary>
    /// Checks that a CSV is the SPRAT "Select ALL" report: a category row starting with
    /// "Current Name and SPRAT ID", then a column-name row with every column SpratImporter maps to
    /// a fixed name (SpratColumns.HeaderMap). Counts the data rows.
    /// </summary>
    public static ReportCheck CheckReport(TextReader text) {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture) {
            BadDataFound = null,
            MissingFieldFound = null,
            TrimOptions = TrimOptions.None,
            DetectColumnCountChanges = false,
            HasHeaderRecord = false,
        };
        using var csv = new CsvReader(text, config);

        if (!csv.Read()) {
            return new ReportCheck(0, "the file is empty", Array.Empty<string>());
        }
        var firstCell = (csv.GetField(0) ?? string.Empty).Trim();
        if (!string.Equals(firstCell, FirstCategoryHeader, StringComparison.OrdinalIgnoreCase)) {
            return new ReportCheck(0, $"the first header row does not start with \"{FirstCategoryHeader}\"", Array.Empty<string>());
        }

        if (!csv.Read()) {
            return new ReportCheck(0, "the file has no second header row", Array.Empty<string>());
        }
        // Parser.Record, not TryGetField: with MissingFieldFound off, TryGetField past the last
        // column returns true with null, so a loop on it never ends.
        var headers = new HashSet<string>(
            (csv.Parser.Record ?? Array.Empty<string>()).Select(h => (h ?? string.Empty).Trim()),
            StringComparer.OrdinalIgnoreCase);
        var missing = SpratColumns.HeaderMap.Keys.Where(k => !headers.Contains(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
        if (missing.Count > 0) {
            return new ReportCheck(0, "missing columns", missing);
        }

        var taxa = 0;
        while (csv.Read()) {
            taxa++;
        }
        return taxa == 0
            ? new ReportCheck(0, "the file has no taxa after the header rows", Array.Empty<string>())
            : new ReportCheck(taxa, null, Array.Empty<string>());
    }

    public static ReportCheck CheckReport(string path) {
        using var reader = new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        return CheckReport(reader);
    }

    public static bool SameContent(string pathA, string pathB) {
        var a = new FileInfo(pathA);
        var b = new FileInfo(pathB);
        if (!a.Exists || !b.Exists || a.Length != b.Length) {
            return false;
        }
        using var streamA = a.OpenRead();
        using var streamB = b.OpenRead();
        var bufferA = new byte[81920];
        var bufferB = new byte[81920];
        while (true) {
            var readA = streamA.ReadAtLeast(bufferA, bufferA.Length, throwOnEndOfStream: false);
            var readB = streamB.ReadAtLeast(bufferB, bufferB.Length, throwOnEndOfStream: false);
            if (readA != readB || !bufferA.AsSpan(0, readA).SequenceEqual(bufferB.AsSpan(0, readB))) {
                return false;
            }
            if (readA == 0) {
                return true;
            }
        }
    }
}
