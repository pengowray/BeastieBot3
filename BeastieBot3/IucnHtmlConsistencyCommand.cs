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
using System.Text.RegularExpressions;

namespace BeastieBot3;

public sealed class IucnHtmlConsistencyCommand : Command<IucnHtmlConsistencyCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Explicit SQLite database path. Overrides paths.ini Datastore:IUCN_sqlite_from_cvs.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of assessments to inspect (0 = all).")]
        public long Limit { get; init; }

        [CommandOption("--max-samples <COUNT>")]
        [Description("Maximum number of mismatch samples to display per field.")]
        public int MaxSamples { get; init; } = 5;
    }

    private static readonly string[] HtmlFields = { "rationale", "habitat", "threats", "population", "range", "useTrade" };

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        if (settings.MaxSamples <= 0) {
            AnsiConsole.MarkupLine("[red]--max-samples must be greater than zero.[/]");
            return -1;
        }

        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string databasePath;
        try {
            databasePath = IucnTextUtilities.ResolveDatabasePath(settings.DatabasePath, paths);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(databasePath)) {
            AnsiConsole.MarkupLine($"[red]IUCN SQLite database not found at:[/] {Markup.Escape(databasePath)}");
            return -3;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        if (!ObjectExists(connection, "view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]view_assessments_html_taxonomy_html not found.[/] Re-run the importer to create the joined view.");
            return -4;
        }

        if (!ObjectExists(connection, "assessments", "table")) {
            AnsiConsole.MarkupLine("[red]assessments table not found.[/]");
            return -5;
        }

        var viewColumns = GetColumnSet(connection, "view_assessments_html_taxonomy_html");
        var plainColumns = GetColumnSet(connection, "assessments");

        var requiredViewColumns = new[] { "assessmentId", "redlist_version" }.Concat(HtmlFields);
        var missingView = requiredViewColumns.Where(col => !viewColumns.Contains(col)).ToList();
        if (missingView.Count > 0) {
            AnsiConsole.MarkupLine("[red]Required columns missing from view:[/] " + string.Join(", ", missingView.Select(Markup.Escape)));
            return -6;
        }

        var missingPlain = HtmlFields.Where(col => !plainColumns.Contains(col)).ToList();
        if (missingPlain.Count > 0) {
            AnsiConsole.MarkupLine("[red]Required columns missing from assessments table:[/] " + string.Join(", ", missingPlain.Select(Markup.Escape)));
            return -7;
        }

        var results = InspectHtmlFields(connection, settings.MaxSamples, settings.Limit, cancellationToken);
        RenderResults(results, databasePath, settings.Limit);

        return results.HasErrors ? -8 : 0;
    }

    private static HtmlVerificationResults InspectHtmlFields(SqliteConnection connection, int maxSamples, long rowLimit, CancellationToken cancellationToken) {
        var fieldAliases = HtmlFields.Select(name => new HtmlFieldAlias(name, $"{name}_html", $"{name}_plain")).ToList();

        var selectParts = new List<string> {
            "    v.assessmentId",
            "    v.redlist_version"
        };

        foreach (var field in fieldAliases) {
            selectParts.Add($"    v.{field.FieldName} AS {field.HtmlAlias}");
            selectParts.Add($"    p.{field.FieldName} AS {field.PlainAlias}");
        }

        var sql = new StringBuilder();
        sql.AppendLine("SELECT");
        sql.AppendLine(string.Join(",\n", selectParts));
        sql.AppendLine("FROM view_assessments_html_taxonomy_html v");
        sql.AppendLine("JOIN assessments p ON p.assessmentId = v.assessmentId AND p.redlist_version = v.redlist_version");
        sql.AppendLine("ORDER BY v.assessmentId");
        if (rowLimit > 0) {
            sql.AppendLine("LIMIT @limit");
        }

        using var command = connection.CreateCommand();
        command.CommandText = sql.ToString();
        command.CommandTimeout = 0;
        if (rowLimit > 0) {
            command.Parameters.AddWithValue("@limit", rowLimit);
        }

        using var reader = command.ExecuteReader();
        var ordinals = new HtmlOrdinals(reader, fieldAliases);
        var results = new HtmlVerificationResults(fieldAliases.Select(f => f.FieldName), maxSamples);

        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            results.TotalRows++;
            var assessmentId = reader.GetString(ordinals.AssessmentId);

            foreach (var field in fieldAliases) {
                var htmlValue = GetNullableString(reader, ordinals.GetHtml(field));
                var plainValue = GetNullableString(reader, ordinals.GetPlain(field));
                results.FieldStats[field.FieldName].Register(assessmentId, htmlValue, plainValue);
            }
        }

        return results;
    }

    private static void RenderResults(HtmlVerificationResults results, string databasePath, long limit) {
        AnsiConsole.MarkupLine($"[bold]IUCN HTML Consistency Check[/] ({Markup.Escape(Path.GetFileName(databasePath))})");
        if (limit > 0) {
            AnsiConsole.MarkupLine($"[grey]Row limit:[/] {limit:N0}");
        }
        AnsiConsole.MarkupLine($"[grey]Rows processed:[/] {results.TotalRows:N0}");
        AnsiConsole.MarkupLine(string.Empty);

        foreach (var field in HtmlFields) {
            var stats = results.FieldStats[field];
            var compared = stats.RowsCompared;
            var mismatchText = FormatMismatch(stats.MismatchCount, compared);
            var details = new List<string>();
            if (stats.HtmlNullOnlyCount > 0) {
                details.Add($"html-null/plain-value {stats.HtmlNullOnlyCount:N0}");
            }
            if (stats.PlainNullOnlyCount > 0) {
                details.Add($"plain-null/html-value {stats.PlainNullOnlyCount:N0}");
            }
            if (stats.BothNullCount > 0 && stats.BothNullCount == compared) {
                details.Add("all null");
            }
            var suffix = details.Count > 0 ? " (" + string.Join(", ", details) + ")" : string.Empty;
            AnsiConsole.MarkupLine($"- {field}: Mismatches: {mismatchText}{suffix}");
            if (stats.Samples.Count > 0) {
                PrintSamples("  Sample mismatches", stats.Samples);
            }
        }
    }

    private static void PrintSamples(string label, IReadOnlyList<MismatchSample> samples) {
        if (samples.Count == 0) {
            return;
        }

        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(label)}[/]");

        foreach (var sample in samples) {
            var builder = new StringBuilder();

            AppendSampleField(builder, "assessmentId", sample.AssessmentId);

            if (!string.IsNullOrEmpty(sample.Observed)) {
                AppendSampleField(builder, "observed", sample.Observed);
            }

            if (!string.IsNullOrEmpty(sample.Expected)) {
                AppendSampleField(builder, "expected", sample.Expected);
            }

            foreach (var (detailLabel, detailValue) in ParseAdditionalDetails(sample.Additional)) {
                AppendSampleField(builder, detailLabel, detailValue);
            }

            builder.AppendLine();
            AnsiConsole.Markup(builder.ToString());
        }
    }

    private static void AppendSampleField(StringBuilder builder, string label, string? value) {
        builder.Append("[grey]").Append(Markup.Escape(label)).AppendLine(":[/]");

        if (value is null) {
            builder.AppendLine("  (null)");
            return;
        }

        if (value.Length == 0) {
            builder.AppendLine("  (empty)");
            return;
        }

        var escaped = Markup.Escape(value);
        var lines = escaped.Split('\n');
        foreach (var line in lines) {
            builder.Append("  ");
            builder.AppendLine(line.Length == 0 ? "(empty)" : line);
        }
    }

    private static IEnumerable<(string Label, string Value)> ParseAdditionalDetails(string? additional) {
        if (string.IsNullOrWhiteSpace(additional)) {
            yield break;
        }

        var segments = additional.Split(" | ", StringSplitOptions.RemoveEmptyEntries);
        foreach (var segmentRaw in segments) {
            var segment = segmentRaw.Trim();
            if (segment.Length == 0) {
                continue;
            }

            var approxIndex = segment.IndexOf('≈');
            var equalsIndex = segment.IndexOf('=');
            var hasApprox = approxIndex >= 0;
            var hasEquals = equalsIndex >= 0;

            var separatorIndex = -1;
            var separator = '\0';

            if (hasApprox && hasEquals) {
                if (approxIndex < equalsIndex) {
                    separatorIndex = approxIndex;
                    separator = '≈';
                }
                else {
                    separatorIndex = equalsIndex;
                    separator = '=';
                }
            }
            else if (hasApprox) {
                separatorIndex = approxIndex;
                separator = '≈';
            }
            else if (hasEquals) {
                separatorIndex = equalsIndex;
                separator = '=';
            }

            if (separatorIndex >= 0) {
                var label = segment[..separatorIndex].Trim();
                var remainder = segment[(separatorIndex + 1)..];
                remainder = separator == '≈' ? $"≈ {remainder.TrimStart()}" : remainder.TrimStart();
                if (remainder.Length == 0) {
                    remainder = separator == '≈' ? "≈" : string.Empty;
                }
                yield return (label.Length == 0 ? "detail" : label, remainder);
            }
            else {
                yield return (segment, "(present)");
            }
        }
    }

    private sealed record HtmlFieldAlias(string FieldName, string HtmlAlias, string PlainAlias);

    private sealed class HtmlOrdinals {
        private readonly Dictionary<string, int> _htmlOrdinals;
        private readonly Dictionary<string, int> _plainOrdinals;

        public HtmlOrdinals(SqliteDataReader reader, IEnumerable<HtmlFieldAlias> aliases) {
            AssessmentId = reader.GetOrdinal("assessmentId");
            RedlistVersion = reader.GetOrdinal("redlist_version");
            _htmlOrdinals = aliases.ToDictionary(a => a.FieldName, a => reader.GetOrdinal(a.HtmlAlias), StringComparer.OrdinalIgnoreCase);
            _plainOrdinals = aliases.ToDictionary(a => a.FieldName, a => reader.GetOrdinal(a.PlainAlias), StringComparer.OrdinalIgnoreCase);
        }

        public int AssessmentId { get; }
        public int RedlistVersion { get; }
        public int GetHtml(HtmlFieldAlias alias) => _htmlOrdinals[alias.FieldName];
        public int GetPlain(HtmlFieldAlias alias) => _plainOrdinals[alias.FieldName];
    }

    private sealed class HtmlVerificationResults {
        public HtmlVerificationResults(IEnumerable<string> fieldNames, int maxSamples) {
            FieldStats = fieldNames.ToDictionary(name => name, name => new FieldComparisonStats(name, maxSamples), StringComparer.OrdinalIgnoreCase);
        }

        public long TotalRows { get; set; }
        public Dictionary<string, FieldComparisonStats> FieldStats { get; }
        public bool HasErrors => FieldStats.Values.Any(f => f.MismatchCount > 0);
    }

    private sealed class FieldComparisonStats {
        private readonly int _maxSamples;

        public FieldComparisonStats(string fieldName, int maxSamples) {
            FieldName = fieldName;
            _maxSamples = maxSamples;
        }

        public string FieldName { get; }
        public long RowsCompared { get; private set; }
        public long MismatchCount { get; private set; }
        public long BothNullCount { get; private set; }
        public long HtmlNullOnlyCount { get; private set; }
        public long PlainNullOnlyCount { get; private set; }
        public List<MismatchSample> Samples { get; } = new();

        public void Register(string assessmentId, string? htmlValue, string? plainValue) {
            RowsCompared++;
            var htmlIsNull = htmlValue is null;
            var plainIsNull = plainValue is null;

            if (htmlIsNull && plainIsNull) {
                BothNullCount++;
                return;
            }

            if (htmlIsNull && !plainIsNull) {
                HtmlNullOnlyCount++;
            }
            else if (!htmlIsNull && plainIsNull) {
                PlainNullOnlyCount++;
            }

            var htmlExact = IucnTextUtilities.ConvertHtmlToExactPlainText(htmlValue);
            var plainExact = IucnTextUtilities.NormalizePlainTextExact(plainValue);

            if (IucnTextUtilities.NormalizedEquals(htmlExact, plainExact)) {
                return;
            }

            MismatchCount++;
            if (Samples.Count < _maxSamples) {
                var observed = IucnTextUtilities.ShortenForDisplay(htmlValue);
                var expected = IucnTextUtilities.ShortenForDisplay(plainValue);
                var htmlNormalized = NormalizeForComparison(htmlExact);
                var plainNormalized = NormalizeForComparison(plainExact);
                var info = BuildMismatchInfo(htmlExact, plainExact, htmlNormalized, plainNormalized);
                Samples.Add(new MismatchSample(assessmentId, observed, expected, info));
            }
        }

        private static string? BuildMismatchInfo(string? recreatedPlain, string? storedPlain, string? recreatedNormalized, string? storedNormalized) {
            if (recreatedPlain is null && storedPlain is null) {
                return null;
            }

            var parts = new List<string>();

            AppendDifferenceParts(parts, recreatedPlain, "recreated", storedPlain, "plain", includeCharCodes: true);

            var normalizedMatch = IucnTextUtilities.NormalizedEquals(recreatedNormalized, storedNormalized);
            if (normalizedMatch) {
                parts.Add("normalized=match");
            }
            else {
                AppendDifferenceParts(parts, recreatedNormalized, "recreated-norm", storedNormalized, "stored-norm", includeCharCodes: false);
            }

            return parts.Count > 0 ? string.Join(" | ", parts) : null;
        }

        private static void AppendDifferenceParts(List<string> parts, string? left, string leftLabel, string? right, string rightLabel, bool includeCharCodes) {
            if (left is null && right is null) {
                parts.Add($"{leftLabel}=(null)");
                parts.Add($"{rightLabel}=(null)");
                return;
            }

            if (left is null) {
                parts.Add($"{leftLabel}=(null)");
                parts.Add($"{rightLabel}≈{BuildSegment(right ?? string.Empty, 0)}");
                return;
            }

            if (right is null) {
                parts.Add($"{leftLabel}≈{BuildSegment(left, 0)}");
                parts.Add($"{rightLabel}=(null)");
                return;
            }

            var diffIndex = FindFirstDifferenceIndex(left, right);
            parts.Add($"{leftLabel}≈{BuildSegment(left, diffIndex)}");
            parts.Add($"{rightLabel}≈{BuildSegment(right, diffIndex)}");

            if (!includeCharCodes) {
                return;
            }

            if (diffIndex >= 0 && diffIndex < left.Length && diffIndex < right.Length) {
                parts.Add($"char-codes={FormatCharCode(left, diffIndex)}/{FormatCharCode(right, diffIndex)}");
            }
            else if (left.Length != right.Length) {
                parts.Add("length-mismatch");
            }
        }

        private static int FindFirstDifferenceIndex(string left, string right) {
            var max = Math.Min(left.Length, right.Length);
            for (var i = 0; i < max; i++) {
                if (left[i] != right[i]) {
                    return i;
                }
            }

            return left.Length == right.Length ? Math.Max(0, max - 1) : max;
        }

        private static string BuildSegment(string value, int differenceIndex, int radius = 32) {
            if (value.Length == 0) {
                return "(empty)";
            }

            var center = differenceIndex;
            if (center < 0) {
                center = 0;
            }
            else if (center >= value.Length) {
                center = value.Length - 1;
            }

            var start = Math.Max(0, center - radius);
            var end = Math.Min(value.Length, center + radius);
            var segment = value[start..end];
            var prefix = start > 0 ? "…" : string.Empty;
            var suffix = end < value.Length ? "…" : string.Empty;
            return prefix + MakeVisible(segment) + suffix;
        }

        private static string MakeVisible(string value) {
            var builder = new StringBuilder(value.Length);
            foreach (var ch in value) {
                switch (ch) {
                    case '\n':
                        builder.Append("\\n");
                        break;
                    case '\r':
                        builder.Append("\\r");
                        break;
                    case '\t':
                        builder.Append("\\t");
                        break;
                    case '\u00A0':
                        builder.Append("\\u00A0");
                        break;
                    default:
                        if (char.IsControl(ch)) {
                            builder.Append("\\u");
                            builder.Append(((int)ch).ToString("X4", CultureInfo.InvariantCulture));
                        }
                        else {
                            builder.Append(ch);
                        }
                        break;
                }
            }
            return builder.ToString();
        }

        private static string FormatCharCode(string value, int index) {
            if (index >= value.Length) {
                return "END";
            }

            var ch = value[index];
            if (char.IsHighSurrogate(ch) && index + 1 < value.Length && char.IsLowSurrogate(value[index + 1])) {
                var codePoint = char.ConvertToUtf32(ch, value[index + 1]);
                return $"U+{codePoint:X4}";
            }

            return $"U+{((int)ch):X4}";
        }
    }

    private sealed record MismatchSample(string AssessmentId, string? Observed, string? Expected, string? Additional);

    private static bool ObjectExists(SqliteConnection connection, string name, string type) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = @type AND name = @name LIMIT 1";
        command.Parameters.AddWithValue("@type", type);
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    private static HashSet<string> GetColumnSet(SqliteConnection connection, string tableOrView) {
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info(\"{tableOrView.Replace("\"", "\"\"")}\");";
        using var reader = command.ExecuteReader();
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (reader.Read()) {
            columns.Add(reader.GetString(1));
        }
        return columns;
    }

    private static string? GetNullableString(SqliteDataReader reader, int ordinal) {
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static string FormatMismatch(long mismatches, long total) {
        if (total == 0) {
            return "-";
        }
        if (mismatches == 0) {
            return $"0 (0.000% of {total:N0})";
        }
        var percentage = (double)mismatches / total * 100d;
        return $"{mismatches:N0} ({percentage:F3}% of {total:N0})";
    }

    private static readonly Regex WhitespaceRegex = new("\\s+", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    public static string? NormalizeForComparison(string? value) {
        if (value is null) {
            return null;
        }
        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return string.Empty;
        }
        return WhitespaceRegex.Replace(trimmed, " ");
    }

}
