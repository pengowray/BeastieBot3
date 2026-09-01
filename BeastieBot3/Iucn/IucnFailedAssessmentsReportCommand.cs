using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Reports assessment ids the IUCN API errored on and has not served since. Reads the API cache's
// failed_requests joined with the backlog; outputs Markdown + CSV.
//
// Until August 2026 this was dominated by HTTP 500 on assessments with an empty geographic-scope
// array: across the whole cache the split was clean (scoped 200, scope-less 500). IUCN fixed that
// server-side without filling the scope in, so those ids now download and leave failed_requests,
// and the blank scope itself is reported by the audit site's empty-scope page. What remains here is
// HTTP 404: ids a taxon's own assessment list carries that the API says do not exist.
// Run via: iucn api report-failed-assessments

namespace BeastieBot3.Iucn;

[CommandInfo("iucn api report-failed-assessments", CommandKind.ReadOnly,
    "List assessment ids /api/v4/assessment/{id} keeps failing on, with their HTTP status, latest flag and SIS id. Outputs Markdown and CSV.",
    Examples = new[] {
        "iucn api report-failed-assessments",
        "iucn api report-failed-assessments -o failed.md --csv-output failed.csv"
    })]
public sealed class IucnFailedAssessmentsReportCommand : Command<IucnFailedAssessmentsReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }

        [CommandOption("-o|--output <PATH>")]
        [Description("Output path for the Markdown report. Defaults to a timestamped file in the reports directory.")]
        public string? OutputPath { get; init; }

        [CommandOption("--csv-output <PATH>")]
        [Description("Output path for the companion CSV. Defaults to the same directory as the Markdown report.")]
        public string? CsvOutputPath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN API cache not found:[/] {cachePath}");
            return -1;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]API cache database:[/] {cachePath}");

        var ro = new SqliteConnectionStringBuilder { DataSource = cachePath, Mode = SqliteOpenMode.ReadOnly };
        using var connection = new SqliteConnection(ro.ConnectionString);
        connection.Open();

        if (!ObjectExists(connection, "failed_requests")) {
            AnsiConsole.MarkupLine("[red]failed_requests table not found.[/] Run cache-assessments first.");
            return -2;
        }

        var rows = Query(connection, cancellationToken);
        AnsiConsole.MarkupLineInterpolated($"[grey]Failed assessment downloads:[/] {rows.Count:N0}");

        var fallbackBaseDir = Path.GetDirectoryName(cachePath) ?? Environment.CurrentDirectory;
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");

        var mdPath = ReportPathResolver.ResolveFilePath(
            paths, settings.OutputPath, explicitDirectory: null,
            fallbackBaseDirectory: fallbackBaseDir,
            defaultFileName: $"iucn-failed-assessments-{timestamp}.md");
        var csvPath = settings.CsvOutputPath
            ?? Path.Combine(Path.GetDirectoryName(mdPath) ?? ".", $"iucn-failed-assessments-{timestamp}.csv");
        var csvDir = Path.GetDirectoryName(csvPath);
        if (!string.IsNullOrEmpty(csvDir)) Directory.CreateDirectory(csvDir);

        File.WriteAllText(mdPath, BuildMarkdown(cachePath, rows), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]Markdown report written to:[/] {mdPath}");
        File.WriteAllText(csvPath, BuildCsv(rows), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]CSV report written to:[/] {csvPath}");
        return 0;
    }

    private static List<FailedRow> Query(SqliteConnection connection, CancellationToken cancellationToken) {
        const string sql = @"
SELECT f.entity_id, f.last_status, f.attempt_count, f.last_error, f.last_attempt_at, f.next_attempt_after,
       b.sis_id, b.root_sis_id, b.latest, b.year_published
FROM failed_requests f
LEFT JOIN taxa_assessment_backlog b ON b.assessment_id = CAST(f.entity_id AS INTEGER)
WHERE f.endpoint = 'assessment'
ORDER BY (f.last_status IS NULL), f.last_status DESC, b.latest, b.sis_id";

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;

        var rows = new List<FailedRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            long.TryParse(reader.GetString(0), out var assessmentId);
            rows.Add(new FailedRow(
                assessmentId,
                reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? (long?)null : reader.GetInt64(6),
                reader.IsDBNull(7) ? (long?)null : reader.GetInt64(7),
                reader.IsDBNull(8) ? (bool?)null : reader.GetInt64(8) != 0,
                reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9)));
        }
        return rows;
    }

    private static string BuildMarkdown(string cachePath, List<FailedRow> rows) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Failed Assessment Downloads");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **Cache database:** `{EscapeMd(cachePath)}`");
        sb.AppendLine($"- **Failed assessment downloads:** {rows.Count:N0}");
        sb.AppendLine();
        sb.AppendLine("Assessment ids `/api/v4/assessment/{id}` errored on and has not served since. **HTTP 404** means the id appears in");
        sb.AppendLine("the taxon's own assessment list but the API reports it as not found; those seen so far are historical assessments,");
        sb.AppendLine("so the taxon's current assessment is unaffected.");
        sb.AppendLine();
        sb.AppendLine("**HTTP 500** used to dominate this list: until August 2026 the API errored on every assessment with an empty");
        sb.AppendLine("geographic-scope array, and across the whole cache the split was clean (assessments with a scope returned 200,");
        sb.AppendLine("scope-less ones 500). That fault was fixed without the scope being filled in, so those ids now download and drop off");
        sb.AppendLine("this list; the blank scope itself is reported by `redlist audit-site` on its empty-scope page.");
        sb.AppendLine();

        if (rows.Count == 0) {
            sb.AppendLine("No failed assessment downloads recorded.");
            return sb.ToString();
        }

        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine("| HTTP status | latest | non-latest | unknown | total |");
        sb.AppendLine("| --- | ---: | ---: | ---: | ---: |");
        foreach (var group in rows.GroupBy(r => r.HttpStatus).OrderByDescending(g => g.Count())) {
            var latest = group.Count(r => r.Latest == true);
            var nonLatest = group.Count(r => r.Latest == false);
            var unknown = group.Count(r => r.Latest is null);
            sb.AppendLine($"| {(group.Key?.ToString() ?? "(none)")} | {latest:N0} | {nonLatest:N0} | {unknown:N0} | {group.Count():N0} |");
        }
        sb.AppendLine();

        sb.AppendLine("## Failed assessments");
        sb.AppendLine();
        sb.AppendLine("| Assessment | Status | Latest | SIS id | Year | Attempts | URL |");
        sb.AppendLine("| --- | ---: | --- | ---: | ---: | ---: | --- |");
        foreach (var r in rows) {
            var latest = r.Latest is null ? "?" : (r.Latest.Value ? "yes" : "no");
            var sis = r.SisId?.ToString(CultureInfo.InvariantCulture) ?? "";
            var url = $"https://www.iucnredlist.org/species/{sis}/{r.AssessmentId}";
            sb.AppendLine($"| {r.AssessmentId} | {(r.HttpStatus?.ToString() ?? "")} | {latest} | {EscapeMd(sis)} | {r.YearPublished?.ToString() ?? ""} | {r.AttemptCount} | {url} |");
        }
        sb.AppendLine();
        return sb.ToString();
    }

    private static string BuildCsv(List<FailedRow> rows) {
        var sb = new StringBuilder();
        sb.AppendLine("assessment_id,http_status,latest,sis_id,root_sis_id,year_published,attempt_count,last_attempt_at,next_attempt_after,last_error,iucn_url");
        foreach (var r in rows) {
            var sis = r.SisId?.ToString(CultureInfo.InvariantCulture) ?? "";
            var url = $"https://www.iucnredlist.org/species/{sis}/{r.AssessmentId}";
            sb.AppendLine(string.Join(",",
                Csv(r.AssessmentId.ToString(CultureInfo.InvariantCulture)),
                Csv(r.HttpStatus?.ToString(CultureInfo.InvariantCulture) ?? ""),
                Csv(r.Latest is null ? "" : (r.Latest.Value ? "true" : "false")),
                Csv(sis),
                Csv(r.RootSisId?.ToString(CultureInfo.InvariantCulture) ?? ""),
                Csv(r.YearPublished?.ToString(CultureInfo.InvariantCulture) ?? ""),
                Csv(r.AttemptCount.ToString(CultureInfo.InvariantCulture)),
                Csv(r.LastAttemptAt ?? ""),
                Csv(r.NextAttemptAfter ?? ""),
                Csv(r.LastError ?? ""),
                Csv(url)));
        }
        return sb.ToString();
    }

    private static bool ObjectExists(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    private static string EscapeMd(string value) =>
        value.Length == 0 ? value : value.Replace("|", "\\|").Replace("\r", " ").Replace("\n", " ");

    private static string Csv(string value) =>
        value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r')
            ? $"\"{value.Replace("\"", "\"\"")}\""
            : value;

    private sealed record FailedRow(
        long AssessmentId, int? HttpStatus, int AttemptCount, string? LastError, string? LastAttemptAt,
        string? NextAttemptAfter, long? SisId, long? RootSisId, bool? Latest, int? YearPublished);
}
