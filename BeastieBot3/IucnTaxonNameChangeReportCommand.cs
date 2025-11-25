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

/// <summary>
/// Generate a report of IUCN taxa where the taxon_scientific_name has changed across assessments sharing the same SIS taxon ID.
/// Results: (IUCN 2025-02)
///  - SIS IDs with detected name changes: 0
///  - No taxon_scientific_name changes were detected for shared SIS taxon IDs.
/// 
/// Notes:
/// It's likely not possible to have taxon_scientific_name change independently per assessment, so will need to find another way to identify taxon name changes.
/// 
/// IUCN amend previous assessments with new taxonomy. The documentation of the changes appears in the "errata" field of old assessments (listed as "Amendment" on the website).
/// 
/// Example: 
/// https://www.iucnredlist.org/species/195459/126665723 (2010 assessment)
/// Mobula alfredi, which was Manta alfredi when this assessment was made, but there's no database field to give the taxon name that was used when the assessment was made in 2010.
/// It just has the amendment text: 
/// 
/// This amended version of the 2010 assessment was created to update the scientific name: previously on the Red List as Manta alfredi, this species has now been moved to the genus Mobula.
/// 
/// another example from the JSON:
/// 
/// ...`"errata":[{"reason":"This amended version of the 2009 assessment was created to update the scientific name: previously on the Red List as \u003cem\u003eBarbus trispilopleura\u003c/em\u003e, this species has now been moved to \u003cem\u003eEnteromius\u003c/em\u003e."}]`...
/// 
/// This report generator does not look for errata text, only changes in the taxon_scientific_name field (and finds none).
/// </summary>
public sealed class IucnTaxonNameChangeReportCommand : Command<IucnTaxonNameChangeReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the IUCN API cache SQLite database (Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }

        [CommandOption("--output <PATH>")]
        [Description("Write the generated report to this path. Defaults to ./iucn-name-changes-<timestamp>.md.")]
        public string? OutputPath { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Limit the number of cached taxa rows scanned (primarily for testing). 0 = all rows.")]
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

        string outputPath;
        try {
            outputPath = ResolveOutputPath(settings.OutputPath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to resolve output path:[/] {Markup.Escape(ex.Message)}");
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

        AnsiConsole.MarkupLine("[grey]Scanning cached taxa JSON...[/]");
        var scanResult = ScanAssessments(connection, settings.Limit, cancellationToken);

        var changes = scanResult.Histories
            .Values
            .Where(history => history.HasMultipleNames)
            .OrderByDescending(history => history.DistinctNames.Count)
            .ThenBy(history => history.SisId)
            .ToList();

        var reportContent = BuildReport(cachePath, scanResult, changes);

        try {
            var directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrEmpty(directory)) {
                Directory.CreateDirectory(directory);
            }
            File.WriteAllText(outputPath, reportContent, Encoding.UTF8);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to write report:[/] {Markup.Escape(ex.Message)}");
            return -5;
        }

        AnsiConsole.MarkupLine($"[green]Report saved to:[/] {Markup.Escape(outputPath)}");
        AnsiConsole.MarkupLine($"[grey]Taxa rows scanned:[/] {scanResult.TaxaRows:N0}");
        AnsiConsole.MarkupLine($"[grey]Assessments inspected:[/] {scanResult.AssessmentCount:N0}");
        AnsiConsole.MarkupLine($"[grey]SIS IDs with name changes:[/] {changes.Count:N0}");
        if (scanResult.JsonFailures > 0) {
            AnsiConsole.MarkupLine($"[yellow]JSON parse failures:[/] {scanResult.JsonFailures:N0}");
        }

        return 0;
    }

    private static string ResolveOutputPath(string? requestedPath) {
        if (!string.IsNullOrWhiteSpace(requestedPath)) {
            return Path.GetFullPath(requestedPath);
        }

        var fileName = $"iucn-name-changes-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.md";
        return Path.Combine(Environment.CurrentDirectory, fileName);
    }

    private static bool TableExists(SqliteConnection connection, string tableName) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", tableName);
        return command.ExecuteScalar() is not null;
    }

    private static NameChangeScanResult ScanAssessments(SqliteConnection connection, long? limit, CancellationToken cancellationToken) {
        var result = new NameChangeScanResult();

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
                if (!root.TryGetProperty("assessments", out var assessmentsElement) || assessmentsElement.ValueKind != JsonValueKind.Array) {
                    continue;
                }

                foreach (var assessment in assessmentsElement.EnumerateArray()) {
                    cancellationToken.ThrowIfCancellationRequested();

                    var sisTaxonId = TryGetInt64(assessment, "sis_taxon_id") ?? rootSisId;
                    var snapshot = CreateSnapshot(assessment);
                    if (snapshot is null) {
                        continue;
                    }

                    var history = result.GetOrAddHistory(sisTaxonId);
                    history.AddAssessment(rootSisId, snapshot);
                    result.AssessmentCount++;
                }
            }
            catch (JsonException ex) {
                result.JsonFailures++;
                AnsiConsole.MarkupLineInterpolated($"[yellow]Skipping root SIS {rootSisId}: {Markup.Escape(ex.Message)}[/]");
            }
        }

        return result;
    }

    private static AssessmentNameSnapshot? CreateSnapshot(JsonElement assessment) {
        var assessmentId = TryGetInt64(assessment, "assessment_id");
        var scientificName = TryGetString(assessment, "taxon_scientific_name");
        var normalizedName = NormalizeName(scientificName);
        var year = TryGetInt32(assessment, "year_published");
        var yearText = TryGetString(assessment, "year_published");
        var latest = TryGetBoolean(assessment, "latest") ?? false;
        var category = TryGetString(assessment, "red_list_category_code");
        var url = TryGetString(assessment, "url");

        if (assessmentId is null && scientificName is null && year is null) {
            return null;
        }

        return new AssessmentNameSnapshot(assessmentId, year, yearText, latest, category, scientificName, normalizedName, url);
    }

    private static string BuildReport(string cachePath, NameChangeScanResult scanResult, IReadOnlyList<SisNameHistory> changes) {
        var builder = new StringBuilder();
        builder.AppendLine("# IUCN Taxon Scientific Name Change Report");
        builder.AppendLine();
        builder.AppendLine($"- Generated: {DateTimeOffset.Now:O}");
        builder.AppendLine($"- Cache database: `{EscapeMarkdown(cachePath)}`");
        builder.AppendLine($"- Taxa rows scanned: {scanResult.TaxaRows:N0}");
        builder.AppendLine($"- Assessments inspected: {scanResult.AssessmentCount:N0}");
        builder.AppendLine($"- SIS IDs with detected name changes: {changes.Count:N0}");
        if (scanResult.JsonFailures > 0) {
            builder.AppendLine($"- JSON parse failures: {scanResult.JsonFailures:N0}");
        }
        builder.AppendLine();

        if (changes.Count == 0) {
            builder.AppendLine("No taxon_scientific_name changes were detected for shared SIS taxon IDs.");
            return builder.ToString();
        }

        foreach (var history in changes) {
            builder.AppendLine($"## SIS {history.SisId}");
            if (history.RootSisIds.Count > 0) {
                var roots = string.Join(", ", history.RootSisIds.OrderBy(id => id));
                builder.AppendLine($"Root SIS IDs encountered: {roots}");
            }
            if (history.DistinctNames.Count > 0) {
                var distinct = history.DistinctNames
                    .OrderBy(name => name, StringComparer.Ordinal)
                    .Select(EscapeMarkdown);
                builder.AppendLine($"Distinct names ({history.DistinctNames.Count}): {string.Join(", ", distinct)}");
            }
            if (history.HasMissingNames) {
                builder.AppendLine("_One or more assessments returned an empty or missing name._");
            }
            builder.AppendLine();
            builder.AppendLine("| Year | Assessment ID | Latest | Category | Name | URL |");
            builder.AppendLine("| --- | --- | --- | --- | --- | --- |");

            foreach (var snapshot in history.Assessments
                         .OrderBy(a => a.Year ?? int.MaxValue)
                         .ThenBy(a => a.AssessmentId ?? long.MaxValue)) {
                var yearText = snapshot.Year?.ToString(CultureInfo.InvariantCulture) ?? snapshot.YearText ?? string.Empty;
                var assessmentId = snapshot.AssessmentId?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                var latestText = snapshot.Latest ? "yes" : string.Empty;
                var categoryText = snapshot.Category ?? string.Empty;
                var nameText = snapshot.ScientificName ?? "(null)";
                var urlText = snapshot.Url ?? string.Empty;

                builder.AppendLine($"| {EscapeMarkdown(yearText)} | {EscapeMarkdown(assessmentId)} | {EscapeMarkdown(latestText)} | {EscapeMarkdown(categoryText)} | {EscapeMarkdown(nameText)} | {EscapeMarkdown(urlText)} |");
            }

            builder.AppendLine();
        }

        return builder.ToString();
    }

    private static string EscapeMarkdown(string value) {
        if (value.Length == 0) {
            return value;
        }
        var sanitized = value.Replace("|", "\\|").Replace("\r", " ").Replace("\n", " ");
        return sanitized;
    }

    private static string? NormalizeName(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }
        return value.Trim();
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

    private static int? TryGetInt32(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var property)) {
            return null;
        }

        return property.ValueKind switch {
            JsonValueKind.Number when property.TryGetInt32(out var number) => number,
            JsonValueKind.String when int.TryParse(property.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
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

    private sealed record AssessmentNameSnapshot(
        long? AssessmentId,
        int? Year,
        string? YearText,
        bool Latest,
        string? Category,
        string? ScientificName,
        string? NormalizedName,
        string? Url
    );

    private sealed class SisNameHistory {
        public SisNameHistory(long sisId) {
            SisId = sisId;
        }

        public long SisId { get; }
        public HashSet<long> RootSisIds { get; } = new();
        public List<AssessmentNameSnapshot> Assessments { get; } = new();
        public HashSet<string> DistinctNames { get; } = new(StringComparer.Ordinal);
        public bool HasMissingNames { get; private set; }
        public bool HasMultipleNames => DistinctNames.Count > 1;

        public void AddAssessment(long rootSisId, AssessmentNameSnapshot snapshot) {
            RootSisIds.Add(rootSisId);
            Assessments.Add(snapshot);
            if (snapshot.NormalizedName is { Length: > 0 } normalized) {
                DistinctNames.Add(normalized);
            }
            else {
                HasMissingNames = true;
            }
        }
    }

    private sealed class NameChangeScanResult {
        private readonly Dictionary<long, SisNameHistory> _histories = new();

        public Dictionary<long, SisNameHistory> Histories => _histories;
        public long TaxaRows { get; set; }
        public long AssessmentCount { get; set; }
        public int JsonFailures { get; set; }

        public SisNameHistory GetOrAddHistory(long sisId) {
            if (!_histories.TryGetValue(sisId, out var history)) {
                history = new SisNameHistory(sisId);
                _histories[sisId] = history;
            }
            return history;
        }
    }
}
