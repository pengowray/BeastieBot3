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
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;

// Generates a phylogenetically grouped report of all cached taxa where no
// assessment in the JSON carries "latest": true.  These are typically species
// that have been removed, delisted, or reclassified on the IUCN Red List.
// Outputs Markdown (grouped by taxonomy) and a companion CSV.
// Run via: iucn api report-no-latest

namespace BeastieBot3.Iucn;

public sealed class IucnNoCurrentAssessmentReportCommand : Command<IucnNoCurrentAssessmentReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }

        [CommandOption("-o|--output <PATH>")]
        [Description("Output path for the Markdown report. Defaults to a timestamped file in the reports directory.")]
        public string? OutputPath { get; init; }

        [CommandOption("--csv-output <PATH>")]
        [Description("Output path for the companion CSV. Defaults to same directory as the Markdown report.")]
        public string? CsvOutputPath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Limit the number of taxa rows scanned (for testing).")]
        public long? Limit { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");

        // Open the store so EnsureSchema runs the migration (adds/renames the
        // has_latest_flag_in_assessments column, backfills, creates indexes).
        // We keep the same connection for queries to avoid WAL visibility gaps.
        using var store = IucnApiCacheStore.Open(cachePath);

        // Grab the underlying connection via a lightweight read query to verify tables.
        var builder = new SqliteConnectionStringBuilder {
            DataSource = cachePath,
            Mode = SqliteOpenMode.ReadWrite
        };
        using var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        if (!TableExists(connection, "taxa") || !TableExists(connection, "taxa_assessment_backlog")) {
            AnsiConsole.MarkupLine("[red]Required tables not found. Run cache-taxa first to populate the API cache.[/]");
            return -1;
        }

        AnsiConsole.MarkupLine("[grey]Scanning for taxa with no latest assessment flag...[/]");
        var (taxa, skippedCount) = ScanTaxaWithNoLatestFlag(connection, settings.Limit);
        AnsiConsole.MarkupLine($"[grey]Found {taxa.Count:N0} taxa with no latest assessment.[/]");
        if (skippedCount > 0) {
            AnsiConsole.MarkupLine($"[yellow]Skipped {skippedCount:N0} taxa where the backlog was stale (JSON actually contains latest=true).[/]");
        }

        if (taxa.Count == 0) {
            AnsiConsole.MarkupLine("[green]All cached taxa have a latest assessment.[/]");
            return 0;
        }

        // Group phylogenetically
        var grouped = taxa
            .OrderBy(t => SortKey(t.Taxonomy.KingdomName))
            .ThenBy(t => SortKey(t.Taxonomy.ClassName))
            .ThenBy(t => SortKey(t.Taxonomy.OrderName))
            .ThenBy(t => SortKey(t.Taxonomy.FamilyName))
            .ThenBy(t => SortKey(t.Taxonomy.ScientificName))
            .ToList();

        // Resolve output paths
        var fallbackBaseDir = Path.GetDirectoryName(cachePath) ?? Environment.CurrentDirectory;
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");

        var mdPath = ReportPathResolver.ResolveFilePath(
            paths,
            settings.OutputPath,
            explicitDirectory: null,
            fallbackBaseDirectory: fallbackBaseDir,
            defaultFileName: $"iucn-no-latest-assessment-{timestamp}.md");

        var csvPath = settings.CsvOutputPath
            ?? Path.Combine(Path.GetDirectoryName(mdPath) ?? ".", $"iucn-no-latest-assessment-{timestamp}.csv");
        var csvDir = Path.GetDirectoryName(csvPath);
        if (!string.IsNullOrEmpty(csvDir)) {
            Directory.CreateDirectory(csvDir);
        }

        // Build and write Markdown report
        var markdown = BuildMarkdownReport(cachePath, grouped);
        File.WriteAllText(mdPath, markdown, Encoding.UTF8);
        AnsiConsole.MarkupLine($"[green]Markdown report written to:[/] {Markup.Escape(mdPath)}");

        // Build and write CSV
        var csv = BuildCsvReport(grouped);
        File.WriteAllText(csvPath, csv, Encoding.UTF8);
        AnsiConsole.MarkupLine($"[green]CSV report written to:[/] {Markup.Escape(csvPath)}");

        // Summary to console
        PrintConsoleSummary(grouped);

        return 0;
    }

    /// <summary>
    /// Query taxa whose backlog contains no latest=1 entry (fast indexed scan),
    /// then verify each candidate against the actual JSON to eliminate false
    /// positives from stale backlog data.
    /// </summary>
    private static (List<TaxonReportRow> Results, int SkippedCount) ScanTaxaWithNoLatestFlag(
        SqliteConnection connection, long? limit) {

        using var command = connection.CreateCommand();

        // NOT EXISTS is fast thanks to idx_assessment_backlog_taxa_latest(taxa_id, latest).
        var sql = @"SELECT t.root_sis_id, t.json FROM taxa t
WHERE NOT EXISTS (
    SELECT 1 FROM taxa_assessment_backlog b WHERE b.taxa_id = t.id AND b.latest = 1
)
ORDER BY t.root_sis_id";

        if (limit.HasValue && limit.Value > 0) {
            sql += " LIMIT @limit";
            command.Parameters.AddWithValue("@limit", limit.Value);
        }

        command.CommandText = sql;
        command.CommandTimeout = 0;

        var results = new List<TaxonReportRow>();
        var skipped = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var rootSisId = reader.GetInt64(0);
            var json = reader.IsDBNull(1) ? null : reader.GetString(1);

            // Safety: verify the JSON itself has no assessment with latest=true.
            // If it does, the backlog is stale — skip rather than report a false positive.
            if (json is not null && JsonHasLatestAssessment(json)) {
                skipped++;
                continue;
            }

            var taxonomy = json is not null
                ? IucnTaxaTaxonomyExtractor.Extract(json) ?? EmptyTaxonomy(rootSisId)
                : EmptyTaxonomy(rootSisId);

            AssessmentBrief? mostRecent = null;
            if (json is not null) {
                mostRecent = ExtractMostRecentAssessment(json, rootSisId);
            }

            results.Add(new TaxonReportRow(rootSisId, taxonomy, mostRecent));
        }

        return (results, skipped);
    }

    /// <summary>
    /// Returns true if any assessment in the cached JSON has "latest": true.
    /// </summary>
    private static bool JsonHasLatestAssessment(string json) {
        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("assessments", out var assessments) ||
                assessments.ValueKind != JsonValueKind.Array) {
                return false;
            }

            foreach (var assessment in assessments.EnumerateArray()) {
                if (!assessment.TryGetProperty("latest", out var latestProp)) {
                    continue;
                }
                if (latestProp.ValueKind == JsonValueKind.True) {
                    return true;
                }
                if (latestProp.ValueKind == JsonValueKind.String &&
                    bool.TryParse(latestProp.GetString(), out var parsed) && parsed) {
                    return true;
                }
            }

            return false;
        }
        catch (JsonException) {
            return false;
        }
    }

    private static AssessmentBrief? ExtractMostRecentAssessment(string json, long rootSisId) {
        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("assessments", out var assessments) || assessments.ValueKind != JsonValueKind.Array) {
                return null;
            }

            AssessmentBrief? best = null;
            foreach (var assessment in assessments.EnumerateArray()) {
                var assessmentId = TryGetLong(assessment, "assessment_id");
                var year = TryGetInt(assessment, "year_published");
                var category = TryGetString(assessment, "red_list_category_code");
                var url = TryGetString(assessment, "url");

                if (assessmentId is null) {
                    continue;
                }

                var brief = new AssessmentBrief(assessmentId.Value, year, category, url, rootSisId);
                if (best is null || (brief.YearPublished ?? 0) > (best.YearPublished ?? 0)) {
                    best = brief;
                }
            }

            return best;
        }
        catch (JsonException) {
            return null;
        }
    }

    private static string BuildMarkdownReport(string cachePath, List<TaxonReportRow> taxa) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Taxa With No Latest Assessment");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **Cache database:** `{EscapeMarkdown(cachePath)}`");
        sb.AppendLine($"- **Taxa with no latest assessment:** {taxa.Count:N0}");
        sb.AppendLine();
        sb.AppendLine("These are species in the IUCN API cache where no assessment has `\"latest\": true`. ");
        sb.AppendLine("They may have been removed from the Red List, merged into another taxon, or reclassified.");
        sb.AppendLine();

        // Summary statistics by class
        var byClass = taxa
            .GroupBy(t => t.Taxonomy.ClassName ?? "(unknown class)")
            .OrderByDescending(g => g.Count())
            .ToList();

        sb.AppendLine("## Summary by Class");
        sb.AppendLine();
        sb.AppendLine("| Class | Count |");
        sb.AppendLine("| --- | ---: |");
        foreach (var group in byClass) {
            sb.AppendLine($"| {EscapeMarkdown(group.Key)} | {group.Count():N0} |");
        }
        sb.AppendLine();

        // Summary statistics by order (top 30)
        var byOrder = taxa
            .GroupBy(t => t.Taxonomy.OrderName ?? "(unknown order)")
            .OrderByDescending(g => g.Count())
            .Take(30)
            .ToList();

        sb.AppendLine("## Top Orders");
        sb.AppendLine();
        sb.AppendLine("| Order | Count |");
        sb.AppendLine("| --- | ---: |");
        foreach (var group in byOrder) {
            sb.AppendLine($"| {EscapeMarkdown(group.Key)} | {group.Count():N0} |");
        }
        sb.AppendLine();

        // Detailed listing grouped by Kingdom > Class > Order > Family
        sb.AppendLine("## Detailed Listing");
        sb.AppendLine();

        string? currentKingdom = null;
        string? currentClass = null;
        string? currentOrder = null;
        string? currentFamily = null;

        foreach (var row in taxa) {
            var kingdom = row.Taxonomy.KingdomName ?? "(unknown kingdom)";
            var className = row.Taxonomy.ClassName ?? "(unknown class)";
            var order = row.Taxonomy.OrderName ?? "(unknown order)";
            var family = row.Taxonomy.FamilyName ?? "(unknown family)";

            if (!string.Equals(kingdom, currentKingdom, StringComparison.Ordinal)) {
                currentKingdom = kingdom;
                currentClass = null;
                currentOrder = null;
                currentFamily = null;
                sb.AppendLine($"### {EscapeMarkdown(kingdom)}");
                sb.AppendLine();
            }

            if (!string.Equals(className, currentClass, StringComparison.Ordinal)) {
                currentClass = className;
                currentOrder = null;
                currentFamily = null;
                sb.AppendLine($"#### {EscapeMarkdown(className)}");
                sb.AppendLine();
            }

            if (!string.Equals(order, currentOrder, StringComparison.Ordinal)) {
                currentOrder = order;
                currentFamily = null;
                sb.AppendLine($"##### {EscapeMarkdown(order)}");
                sb.AppendLine();
            }

            if (!string.Equals(family, currentFamily, StringComparison.Ordinal)) {
                currentFamily = family;
                sb.AppendLine($"**{EscapeMarkdown(family)}**");
                sb.AppendLine();
            }

            // Build the species line
            var name = row.Taxonomy.ScientificName ?? $"SIS {row.RootSisId}";
            var commonName = row.Taxonomy.CommonName;
            var parts = new List<string>();
            parts.Add($"*{EscapeMarkdown(name)}*");

            if (!string.IsNullOrWhiteSpace(commonName)) {
                parts.Add($"({EscapeMarkdown(commonName)})");
            }

            if (row.MostRecentAssessment is { } assessment) {
                var assessmentParts = new List<string>();
                if (assessment.Category is not null) {
                    assessmentParts.Add(assessment.Category);
                }
                if (assessment.YearPublished.HasValue) {
                    assessmentParts.Add(assessment.YearPublished.Value.ToString(CultureInfo.InvariantCulture));
                }
                var url = assessment.Url ?? $"https://www.iucnredlist.org/species/{assessment.RootSisId}/{assessment.AssessmentId}";
                var assessmentLabel = assessmentParts.Count > 0 ? string.Join(" ", assessmentParts) : $"#{assessment.AssessmentId}";
                parts.Add($"\u2014 [{EscapeMarkdown(assessmentLabel)}]({url})");
            }

            sb.AppendLine($"- {string.Join(" ", parts)}");
        }

        sb.AppendLine();
        return sb.ToString();
    }

    private static string BuildCsvReport(List<TaxonReportRow> taxa) {
        var sb = new StringBuilder();
        sb.AppendLine("root_sis_id,scientific_name,common_name,kingdom,phylum,class,order,family,genus,species,last_assessment_year,last_category,iucn_url");

        foreach (var row in taxa) {
            var t = row.Taxonomy;
            var a = row.MostRecentAssessment;
            var url = a?.Url ?? (a is not null ? $"https://www.iucnredlist.org/species/{a.RootSisId}/{a.AssessmentId}" : "");
            sb.AppendLine(string.Join(",",
                CsvEscape(row.RootSisId.ToString(CultureInfo.InvariantCulture)),
                CsvEscape(t.ScientificName ?? ""),
                CsvEscape(t.CommonName ?? ""),
                CsvEscape(t.KingdomName ?? ""),
                CsvEscape(t.PhylumName ?? ""),
                CsvEscape(t.ClassName ?? ""),
                CsvEscape(t.OrderName ?? ""),
                CsvEscape(t.FamilyName ?? ""),
                CsvEscape(t.GenusName ?? ""),
                CsvEscape(t.SpeciesName ?? ""),
                CsvEscape(a?.YearPublished?.ToString(CultureInfo.InvariantCulture) ?? ""),
                CsvEscape(a?.Category ?? ""),
                CsvEscape(url)));
        }

        return sb.ToString();
    }

    private static void PrintConsoleSummary(List<TaxonReportRow> taxa) {
        AnsiConsole.WriteLine();
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Class");
        table.AddColumn(new TableColumn("Count").RightAligned());

        var byClass = taxa
            .GroupBy(t => t.Taxonomy.ClassName ?? "(unknown)")
            .OrderByDescending(g => g.Count())
            .Take(15);

        foreach (var group in byClass) {
            table.AddRow(Markup.Escape(group.Key), group.Count().ToString("N0"));
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"[grey]Total taxa with no latest assessment:[/] {taxa.Count:N0}");
    }

    private static TaxaTaxonomyInfo EmptyTaxonomy(long sisId) =>
        new(sisId, null, null, null, null, null, null, null, null, null);

    private static string SortKey(string? value) => value ?? "\uFFFF";

    private static string EscapeMarkdown(string value) =>
        value.Length == 0 ? value : value.Replace("|", "\\|").Replace("\r", " ").Replace("\n", " ");

    private static string CsvEscape(string value) {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r')) {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }
        return value;
    }

    private static bool TableExists(SqliteConnection connection, string tableName) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", tableName);
        return command.ExecuteScalar() is not null;
    }

    private static long? TryGetLong(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) {
            return null;
        }
        return prop.ValueKind switch {
            JsonValueKind.Number => prop.GetInt64(),
            JsonValueKind.String when long.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static int? TryGetInt(JsonElement element, string propertyName) {
        if (!element.TryGetProperty(propertyName, out var prop)) {
            return null;
        }
        return prop.ValueKind switch {
            JsonValueKind.Number => prop.GetInt32(),
            JsonValueKind.String when int.TryParse(prop.GetString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static string? TryGetString(JsonElement element, string propertyName) {
        return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private sealed record TaxonReportRow(long RootSisId, TaxaTaxonomyInfo Taxonomy, AssessmentBrief? MostRecentAssessment);

    private sealed record AssessmentBrief(long AssessmentId, int? YearPublished, string? Category, string? Url, long RootSisId);
}
