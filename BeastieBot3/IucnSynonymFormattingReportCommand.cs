using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class IucnSynonymFormattingReportCommand : Command<IucnSynonymFormattingReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the IUCN API cache SQLite database (Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }

        [CommandOption("--markdown-output <PATH>")]
        [Description("Optional Markdown output path. Defaults to Reports:output_dir or <cache dir>/data-analysis.")]
        public string? MarkdownOutputPath { get; init; }

        [CommandOption("--csv-output <PATH>")]
        [Description("Optional CSV output path. Defaults to the same directory as the Markdown report.")]
        public string? CsvOutputPath { get; init; }

        [CommandOption("--output-dir <DIR>")]
        [Description("Write both outputs into this directory (overrides Reports:output_dir).")]
        public string? OutputDirectory { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Limit the number of cached taxa rows scanned (0 = all).")]
        public long? Limit { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;

        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        string cachePath;
        try {
            cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }

        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLine($"[red]IUCN API cache not found at:[/] {Markup.Escape(cachePath)}");
            return -2;
        }

        var generatedAt = DateTimeOffset.Now;
        var fallbackBaseDir = Path.GetDirectoryName(cachePath) ?? Environment.CurrentDirectory;
        string markdownPath;
        string csvPath;
        try {
            var stamp = generatedAt.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
            markdownPath = ReportPathResolver.ResolveFilePath(
                paths,
                settings.MarkdownOutputPath,
                settings.OutputDirectory,
                fallbackBaseDirectory: fallbackBaseDir,
                defaultFileName: $"iucn-synonym-formatting-{stamp}.md");
            csvPath = ReportPathResolver.ResolveFilePath(
                paths,
                settings.CsvOutputPath,
                settings.OutputDirectory,
                fallbackBaseDirectory: fallbackBaseDir,
                defaultFileName: $"iucn-synonym-formatting-{stamp}.csv");
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to resolve output paths:[/] {Markup.Escape(ex.Message)}");
            return -3;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = cachePath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        if (!TableExists(connection, "taxa")) {
            AnsiConsole.MarkupLine("[red]taxa table not found in the API cache database.[/]");
            return -4;
        }

        AnsiConsole.MarkupLine("[grey]Scanning cached synonyms...[/]");
        var scanResult = ScanSynonyms(connection, settings.Limit, cancellationToken);

        var markdown = BuildMarkdownReport(cachePath, scanResult, generatedAt);
        try {
            WriteTextFile(markdownPath, markdown);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to write Markdown report:[/] {Markup.Escape(ex.Message)}");
            return -5;
        }

        try {
            WriteCsvFile(csvPath, scanResult.Issues);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to write CSV export:[/] {Markup.Escape(ex.Message)}");
            return -6;
        }

        AnsiConsole.MarkupLine($"[green]Markdown report:[/] {Markup.Escape(markdownPath)}");
        AnsiConsole.MarkupLine($"[green]CSV export:[/] {Markup.Escape(csvPath)}");
        AnsiConsole.MarkupLine($"[grey]Taxa rows scanned:[/] {scanResult.TaxaRows:N0}");
        AnsiConsole.MarkupLine($"[grey]Synonyms inspected:[/] {scanResult.SynonymsScanned:N0}");
        AnsiConsole.MarkupLine($"[grey]Synonyms with issues:[/] {scanResult.Issues.Count:N0}");
        if (scanResult.JsonFailures > 0) {
            AnsiConsole.MarkupLine($"[yellow]JSON parse failures:[/] {scanResult.JsonFailures:N0}");
        }

        return 0;
    }

    private static bool TableExists(SqliteConnection connection, string tableName) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", tableName);
        return command.ExecuteScalar() is not null;
    }

    private static SynonymScanResult ScanSynonyms(SqliteConnection connection, long? limit, CancellationToken cancellationToken) {
        var result = new SynonymScanResult();
        using var command = connection.CreateCommand();
        var sql = new StringBuilder("SELECT root_sis_id, json FROM taxa ORDER BY root_sis_id");
        if (limit.HasValue && limit.Value > 0) {
            sql.Append(" LIMIT @limit");
            command.Parameters.AddWithValue("@limit", limit.Value);
        }
        command.CommandText = sql.ToString();
        command.CommandTimeout = 0;

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            result.TaxaRows++;

            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }

            var json = reader.GetString(1);
            try {
                using var document = JsonDocument.Parse(json);
                var root = document.RootElement;
                if (!root.TryGetProperty("taxon", out var taxonElement) || taxonElement.ValueKind != JsonValueKind.Object) {
                    continue;
                }

                var taxonName = ExtractTaxonName(taxonElement);
                var assessmentId = ExtractPrimaryAssessmentId(root);
                if (!taxonElement.TryGetProperty("synonyms", out var synonymsElement) || synonymsElement.ValueKind != JsonValueKind.Array) {
                    continue;
                }

                foreach (var synonymElement in synonymsElement.EnumerateArray()) {
                    cancellationToken.ThrowIfCancellationRequested();
                    var synonym = ExtractSynonymName(synonymElement);
                    if (synonym is null) {
                        continue;
                    }

                    result.SynonymsScanned++;
                    var analysis = SynonymFormattingAnalyzer.Analyze(synonym);
                    if (!analysis.HasIssues) {
                        continue;
                    }

                    var normalized = analysis.NormalizedValue;
                    if (string.Equals(synonym, normalized, StringComparison.Ordinal)) {
                        normalized = string.Empty;
                    }

                    var record = new SynonymIssueRecord(
                        rootSisId,
                        assessmentId,
                        taxonName,
                        synonym,
                        string.IsNullOrEmpty(normalized) ? null : normalized,
                        analysis.Issues);

                    result.Issues.Add(record);
                    foreach (var issueKind in analysis.Issues) {
                        result.IncrementIssueCount(issueKind);
                    }
                }
            }
            catch (JsonException ex) {
                result.JsonFailures++;
                AnsiConsole.MarkupLineInterpolated($"[yellow]Skipping root SIS {rootSisId}: {Markup.Escape(ex.Message)}[/]");
            }
        }

        return result;
    }

    private static string? ExtractSynonymName(JsonElement element) {
        switch (element.ValueKind) {
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Object:
                if (element.TryGetProperty("name", out var nameElement) && nameElement.ValueKind == JsonValueKind.String) {
                    return nameElement.GetString();
                }
                break;
        }
        return null;
    }

    private static string? ExtractTaxonName(JsonElement taxonElement) {
        var candidates = new[] { "taxon_name", "scientific_name", "taxon_scientific_name", "name" };
        foreach (var property in candidates) {
            if (TryGetString(taxonElement, property) is { } value && value.Length > 0) {
                return value.Trim();
            }
        }
        return null;
    }

    private static long? ExtractPrimaryAssessmentId(JsonElement root) {
        if (!root.TryGetProperty("assessments", out var assessmentsElement) || assessmentsElement.ValueKind != JsonValueKind.Array) {
            return null;
        }

        long? latest = null;
        foreach (var assessment in assessmentsElement.EnumerateArray()) {
            var id = TryGetInt64(assessment, "assessment_id");
            if (id is null) {
                continue;
            }

            var isLatest = TryGetBoolean(assessment, "latest");
            if (isLatest == true) {
                return id;
            }

            latest ??= id;
        }

        return latest;
    }

    private static long? TryGetInt64(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var property)) {
            return null;
        }

        return property.ValueKind switch {
            JsonValueKind.Number when property.TryGetInt64(out var number) => number,
            JsonValueKind.String when long.TryParse(property.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null
        };
    }

    private static bool? TryGetBoolean(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var property)) {
            return null;
        }

        return property.ValueKind switch {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(property.GetString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static string? TryGetString(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var property)) {
            return null;
        }

        return property.ValueKind switch {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => null,
            _ => property.ToString()
        };
    }

    private static string BuildMarkdownReport(string cachePath, SynonymScanResult result, DateTimeOffset generatedAt) {
        var builder = new StringBuilder();
        builder.AppendLine("# IUCN Synonym Formatting Report");
        builder.AppendLine();
        builder.AppendLine($"- Generated: {generatedAt:O}");
        builder.AppendLine($"- API cache: `{EscapeMarkdown(cachePath)}`");
        builder.AppendLine($"- Taxa rows scanned: {result.TaxaRows:N0}");
        builder.AppendLine($"- Synonyms inspected: {result.SynonymsScanned:N0}");
        builder.AppendLine($"- Synonyms with formatting issues: {result.Issues.Count:N0}");
        if (result.JsonFailures > 0) {
            builder.AppendLine($"- JSON parse failures: {result.JsonFailures:N0}");
        }
        builder.AppendLine();

        if (result.Issues.Count == 0) {
            builder.AppendLine("No synonym formatting anomalies were detected.");
            return builder.ToString();
        }

        if (result.IssueCounts.Count > 0) {
            builder.AppendLine("## Issue summary");
            builder.AppendLine("| Issue | Count |");
            builder.AppendLine("| --- | --- |");
            foreach (var entry in result.IssueCounts
                         .OrderBy(kvp => GetIssueSortOrder(kvp.Key))
                         .ThenBy(kvp => GetIssueLabel(kvp.Key), StringComparer.Ordinal)) {
                builder.AppendLine($"| {EscapeMarkdown(GetIssueLabel(entry.Key))} | {entry.Value:N0} |");
            }
            builder.AppendLine();
        }

        builder.AppendLine("## Detailed entries");
        var groupedIssues = GroupIssuesByKind(result.Issues);
        foreach (var group in groupedIssues
                 .OrderBy(kvp => GetIssueSortOrder(kvp.Key))
                 .ThenBy(kvp => GetIssueLabel(kvp.Key), StringComparer.Ordinal)) {
            builder.AppendLine($"### {EscapeMarkdown(GetIssueLabel(group.Key))} ({group.Value.Count:N0})");
            foreach (var issue in group.Value
                         .OrderBy(r => r.RootSisId)
                         .ThenBy(r => r.RawSynonym, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(FormatIssueEntry(issue));
            }
            builder.AppendLine();
        }

        builder.AppendLine("Markdown report lists every synonym that currently contains double spaces, stray whitespace, or embedded markup. Use the CSV export for spreadsheet filtering when editing source data.");
        return builder.ToString();
    }

    private static string? BuildIucnUrl(long rootSisId, long? assessmentId) {
        if (assessmentId is null) {
            return null;
        }
        return $"https://www.iucnredlist.org/species/{rootSisId}/{assessmentId}";
    }

    private static void WriteTextFile(string path, string content) {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        File.WriteAllText(path, content, Encoding.UTF8);
    }

    private static void WriteCsvFile(string path, IReadOnlyList<SynonymIssueRecord> issues) {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }

        var builder = new StringBuilder();
        builder.AppendLine("root_sis_id,taxon_name,synonym,issues,suggested,assessment_id,iucn_url");
        foreach (var issue in issues
                     .OrderBy(r => r.RootSisId)
                     .ThenBy(r => r.RawSynonym, StringComparer.OrdinalIgnoreCase)) {
            var issueText = FormatIssueList(issue.IssueKinds);
            var url = BuildIucnUrl(issue.RootSisId, issue.PrimaryAssessmentId) ?? string.Empty;
            var assessmentIdText = issue.PrimaryAssessmentId?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            WriteCsvRow(builder, new[] {
                issue.RootSisId.ToString(CultureInfo.InvariantCulture),
                issue.TaxonName ?? string.Empty,
                issue.RawSynonym,
                issueText,
                issue.NormalizedSynonym ?? string.Empty,
                assessmentIdText,
                url
            });
        }

        File.WriteAllText(path, builder.ToString(), Encoding.UTF8);
    }

    private static void WriteCsvRow(StringBuilder builder, IReadOnlyList<string> columns) {
        for (var i = 0; i < columns.Count; i++) {
            if (i > 0) {
                builder.Append(',');
            }
            builder.Append(EscapeCsv(columns[i]));
        }
        builder.AppendLine();
    }

    private static Dictionary<SynonymIssueKind, List<SynonymIssueRecord>> GroupIssuesByKind(IEnumerable<SynonymIssueRecord> issues) {
        var grouped = new Dictionary<SynonymIssueKind, List<SynonymIssueRecord>>();
        foreach (var issue in issues) {
            foreach (var kind in issue.IssueKinds.Distinct()) {
                if (!grouped.TryGetValue(kind, out var list)) {
                    list = new List<SynonymIssueRecord>();
                    grouped[kind] = list;
                }
                list.Add(issue);
            }
        }
        return grouped;
    }

    private static string EscapeCsv(string value) {
        if (value.IndexOfAny(new[] { ',', '\"', '\n', '\r' }) < 0) {
            return value;
        }

        var escaped = value.Replace("\"", "\"\"");
        return $"\"{escaped}\"";
    }

    private static string FormatIssueEntry(SynonymIssueRecord issue) {
        var sisId = EscapeMarkdown(issue.RootSisId.ToString(CultureInfo.InvariantCulture));
        var taxon = EscapeMarkdown(issue.TaxonName ?? "(unknown)");
        var synonym = WrapInlineCode(issue.RawSynonym);
        var issueText = EscapeMarkdown(FormatIssueList(issue.IssueKinds));
        var parts = new List<string> {
            $"synonym: {synonym}",
            $"issues: {issueText}"
        };

        if (!string.IsNullOrEmpty(issue.NormalizedSynonym)) {
            parts.Add($"suggested: {EscapeMarkdown(issue.NormalizedSynonym)}");
        }

        if (BuildIucnUrl(issue.RootSisId, issue.PrimaryAssessmentId) is { } url) {
            parts.Add($"IUCN: {EscapeMarkdown(url)}");
        }

        return $"- SIS {sisId} ('{taxon}') — {string.Join("; ", parts)}";
    }

    private static string WrapInlineCode(string value) {
        if (value is null) {
            return "``";
        }

        var sanitized = value
            .Replace("\r", "\\r")
            .Replace("\n", "\\n");

        if (!sanitized.Contains('`')) {
            return $"`{sanitized}`";
        }

        var fenceLength = 2;
        while (sanitized.Contains(new string('`', fenceLength))) {
            fenceLength++;
        }

        var fence = new string('`', fenceLength);
        return $"{fence} {sanitized} {fence}";
    }

    private static string EscapeMarkdown(string value) {
        if (string.IsNullOrEmpty(value)) {
            return string.Empty;
        }

        var sanitized = value
            .Replace("\r", "\\r")
            .Replace("\n", "\\n");
        return sanitized;
    }

    private static string FormatIssueList(IReadOnlyList<SynonymIssueKind> issues) {
        if (issues.Count == 0) {
            return string.Empty;
        }
        var labels = issues
            .Select(GetIssueLabel)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        return string.Join("; ", labels);
    }

    private static string GetIssueLabel(SynonymIssueKind kind) => kind switch {
        SynonymIssueKind.EmptyOrWhitespace => "blank or whitespace-only synonym",
        SynonymIssueKind.LeadingTrailingWhitespace => "leading/trailing whitespace",
        SynonymIssueKind.RepeatedSpaces => "double spaces inside name",
        SynonymIssueKind.SpecialWhitespace => "non-breaking or control whitespace",
        SynonymIssueKind.HtmlMarkup => "contains HTML markup",
        _ => kind.ToString()
    };

    private static int GetIssueSortOrder(SynonymIssueKind kind) => kind switch {
        SynonymIssueKind.HtmlMarkup => 0,
        SynonymIssueKind.SpecialWhitespace => 1,
        SynonymIssueKind.LeadingTrailingWhitespace => 2,
        SynonymIssueKind.RepeatedSpaces => 3,
        SynonymIssueKind.EmptyOrWhitespace => 4,
        _ => 100 + (int)kind
    };

    private sealed class SynonymScanResult {
        public long TaxaRows { get; set; }
        public long SynonymsScanned { get; set; }
        public int JsonFailures { get; set; }
        public List<SynonymIssueRecord> Issues { get; } = new();
        public Dictionary<SynonymIssueKind, long> IssueCounts { get; } = new();

        public void IncrementIssueCount(SynonymIssueKind kind) {
            if (IssueCounts.TryGetValue(kind, out var current)) {
                IssueCounts[kind] = current + 1;
            }
            else {
                IssueCounts[kind] = 1;
            }
        }
    }

    private sealed record SynonymIssueRecord(
        long RootSisId,
        long? PrimaryAssessmentId,
        string? TaxonName,
        string RawSynonym,
        string? NormalizedSynonym,
        IReadOnlyList<SynonymIssueKind> IssueKinds
    );

    private enum SynonymIssueKind {
        EmptyOrWhitespace,
        LeadingTrailingWhitespace,
        RepeatedSpaces,
        SpecialWhitespace,
        HtmlMarkup
    }

    private static class SynonymFormattingAnalyzer {
        public static SynonymAnalysis Analyze(string value) {
            if (value is null) {
                throw new ArgumentNullException(nameof(value));
            }

            var issues = new List<SynonymIssueKind>();
            if (string.IsNullOrWhiteSpace(value)) {
                issues.Add(SynonymIssueKind.EmptyOrWhitespace);
                return new SynonymAnalysis(string.Empty, issues);
            }

            if (!string.Equals(value, value.Trim(), StringComparison.Ordinal)) {
                issues.Add(SynonymIssueKind.LeadingTrailingWhitespace);
            }

            if (HasRepeatedSpaces(value)) {
                issues.Add(SynonymIssueKind.RepeatedSpaces);
            }

            if (ContainsSpecialWhitespace(value)) {
                issues.Add(SynonymIssueKind.SpecialWhitespace);
            }

            if (ContainsHtmlMarkup(value)) {
                issues.Add(SynonymIssueKind.HtmlMarkup);
            }

            var normalized = NormalizeWhitespace(value);
            return new SynonymAnalysis(normalized, issues);
        }

        private static bool HasRepeatedSpaces(string value) {
            var previousSpace = false;
            foreach (var ch in value) {
                if (ch == ' ') {
                    if (previousSpace) {
                        return true;
                    }
                    previousSpace = true;
                }
                else {
                    previousSpace = false;
                }
            }
            return false;
        }

        private static bool ContainsSpecialWhitespace(string value) {
            foreach (var ch in value) {
                if (ch is '\u00A0' or '\u2007' or '\u202F' or '\u2009' or '\t' or '\r' or '\n') {
                    return true;
                }
            }
            return false;
        }

        private static bool ContainsHtmlMarkup(string value) {
            var start = value.IndexOf('<');
            while (start >= 0 && start < value.Length - 1) {
                var end = value.IndexOf('>', start + 1);
                if (end < 0) {
                    break;
                }

                var length = end - start - 1;
                if (length > 0 && length <= 64) {
                    return true;
                }

                start = value.IndexOf('<', end + 1);
            }
            return false;
        }

        private static string NormalizeWhitespace(string value) {
            var trimmed = value.Trim();
            if (trimmed.Length == 0) {
                return string.Empty;
            }

            var builder = new StringBuilder(trimmed.Length);
            var previousWasSpace = false;
            foreach (var ch in trimmed) {
                var normalized = ch switch {
                    '\u00A0' => ' ',
                    '\u2007' => ' ',
                    '\u202F' => ' ',
                    '\u2009' => ' ',
                    '\t' => ' ',
                    '\r' => ' ',
                    '\n' => ' ',
                    _ => ch
                };

                if (char.IsWhiteSpace(normalized)) {
                    if (previousWasSpace) {
                        continue;
                    }
                    builder.Append(' ');
                    previousWasSpace = true;
                }
                else {
                    builder.Append(normalized);
                    previousWasSpace = false;
                }
            }

            return builder.ToString();
        }

        internal sealed class SynonymAnalysis {
            public SynonymAnalysis(string normalizedValue, IReadOnlyList<SynonymIssueKind> issues) {
                NormalizedValue = normalizedValue;
                Issues = issues;
            }

            public string NormalizedValue { get; }
            public IReadOnlyList<SynonymIssueKind> Issues { get; }
            public bool HasIssues => Issues.Count > 0;
        }
    }
}
