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

[CommandInfo("iucn api report-no-latest", CommandKind.ReadOnly,
    "Report all cached taxa with no current (latest) assessment, grouped phylogenetically. Outputs Markdown and CSV.",
    Examples = new[] {
        "iucn api report-no-latest",
        "iucn api report-no-latest --limit 500",
        "iucn api report-no-latest -o report.md --csv-output report.csv"
    })]
public sealed class IucnNoCurrentAssessmentReportCommand : Command<IucnNoCurrentAssessmentReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }

        [CommandOption("-d|--database <PATH>")]
        [Description("Override path to the CSV-imported IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs). It gives the current taxa for the same-name and IUCN synonym columns.")]
        public string? DatabasePath { get; init; }

        [CommandOption("-o|--output <PATH>")]
        [Description("Output path for the Markdown report. Defaults to a timestamped file in the reports directory.")]
        public string? OutputPath { get; init; }

        [CommandOption("--csv-output <PATH>")]
        [Description("Output path for the companion CSV. Defaults to same directory as the Markdown report.")]
        public string? CsvOutputPath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("List at most N taxa in the report: the first N without a current assessment, in SIS id order. For test runs; no limit by default.")]
        public long? Limit { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);

        AnsiConsole.MarkupLine($"[grey]API cache database:[/] {Markup.Escape(cachePath)}");
        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLine("[red]API cache database not found.[/] Run [yellow]iucn api cache-taxa[/] first to populate it.");
            return -1;
        }

        // Read-only: the scan needs only the taxa table and taxa_assessment_backlog.latest.
        using var connection = OpenReadOnly(cachePath);

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

        var sameNameTaxa = LoadSameNameTaxa(paths, settings.DatabasePath, connection);
        if (sameNameTaxa is not null) {
            taxa = taxa
                .Select(t => t.Old is null ? t : t with { SameName = sameNameTaxa.SameName(t.Old), ViaSynonym = sameNameTaxa.ViaSynonym(t.Old) })
                .ToList();
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
        var markdown = BuildMarkdownReport(cachePath, grouped, matchesChecked: sameNameTaxa is not null);
        File.WriteAllText(mdPath, markdown, Encoding.UTF8);
        AnsiConsole.MarkupLine($"[green]Markdown report written to:[/] {Markup.Escape(mdPath)}");

        // Build and write CSV
        var csv = BuildCsvReport(grouped, matchesChecked: sameNameTaxa is not null);
        File.WriteAllText(csvPath, csv, Encoding.UTF8);
        AnsiConsole.MarkupLine($"[green]CSV report written to:[/] {Markup.Escape(csvPath)}");

        // Summary to console
        PrintConsoleSummary(grouped, matchesChecked: sameNameTaxa is not null);

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
            IucnOldTaxon? old = null;
            if (json is not null) {
                mostRecent = ExtractMostRecentAssessment(json, rootSisId);
                old = IucnSameNameTaxa.OldTaxonFromJson(rootSisId, json);
            }

            results.Add(new TaxonReportRow(rootSisId, taxonomy, mostRecent, old));
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

    private static string BuildMarkdownReport(string cachePath, List<TaxonReportRow> taxa, bool matchesChecked) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Taxa With No Latest Assessment");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **Cache database:** `{EscapeMarkdown(cachePath)}`");
        sb.AppendLine($"- **Taxa with no latest assessment:** {taxa.Count:N0}");
        if (matchesChecked) {
            var (sameName, viaSynonymOnly) = MatchCounts(taxa);
            sb.AppendLine($"- **{MdSameNameCountLabel}:** {sameName:N0}");
            sb.AppendLine($"- **{MdViaSynonymCountLabel}:** {viaSynonymOnly:N0}");
            sb.AppendLine($"- **{MdNeitherCountLabel}:** {taxa.Count - sameName - viaSynonymOnly:N0}");
        }
        sb.AppendLine();
        sb.AppendLine("These are species in the IUCN API cache where no assessment has `\"latest\": true`. ");
        sb.AppendLine("They may have been removed from the Red List, merged into another taxon, or reclassified.");
        sb.AppendLine();
        sb.AppendLine(matchesChecked ? MdMatchesIntro : MdMatchesNotChecked);
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

            sb.AppendLine($"- {string.Join(" ", parts)}{MarkdownMatches(row)}");
        }

        sb.AppendLine();
        return sb.ToString();
    }

    private static string BuildCsvReport(List<TaxonReportRow> taxa, bool matchesChecked) {
        var sb = new StringBuilder();
        sb.AppendLine("root_sis_id,scientific_name,common_name,kingdom,phylum,class,order,family,genus,species,last_assessment_year,last_category,iucn_url"
            + (matchesChecked ? $",{CsvSameNameColumn},{CsvViaSynonymColumn}" : ""));

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
                CsvEscape(url))
                + (matchesChecked
                    ? "," + CsvEscape(Describe(row.SameName, row.Old)) + "," + CsvEscape(Describe(row.ViaSynonym, row.Old))
                    : ""));
        }

        return sb.ToString();
    }

    private static void PrintConsoleSummary(List<TaxonReportRow> taxa, bool matchesChecked) {
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
        if (matchesChecked) {
            var (sameName, viaSynonymOnly) = MatchCounts(taxa);
            AnsiConsole.MarkupLine($"[grey]{ConsoleSameNameLabel}:[/] {sameName:N0}");
            AnsiConsole.MarkupLine($"[grey]{ConsoleViaSynonymLabel}:[/] {viaSynonymOnly:N0}");
        }
    }

    // ---- Current taxa with the same name, or listing the name as an IUCN synonym ----

    private const string MdSameNameCountLabel = "Taxa with the same name as a current taxon";
    private const string MdViaSynonymCountLabel = "Taxa whose name is an IUCN synonym of a current taxon (not counted above)";
    private const string MdNeitherCountLabel = "Taxa with no current taxon found by name or IUCN synonym";
    private const string MdMatchesIntro =
        "A current taxon here is one in the CSV export, in the same kingdom as the listed taxon, with a current assessment in the same scope as the listed taxon's most recent assessment. " +
        "After a taxon's own assessment, \"same name as\" is followed by each current taxon with the same scientific name, and \"IUCN synonym of\" by each current taxon that has the listed taxon's name in its IUCN synonym list. " +
        "Each one links to its current assessment and shows its SIS id, its name and authority when either is written differently, and its category and year.";
    private const string MdMatchesNotChecked = "The listed taxa were not checked for current taxa with the same name or an IUCN synonym, because the IUCN CSV database was not found.";
    private const string MdSameNameLead = "; same name as ";
    private const string MdViaSynonymLead = "; IUCN synonym of ";
    private const string CsvSameNameColumn = "same_name_as_current_taxon";
    private const string CsvViaSynonymColumn = "iucn_synonym_of_current_taxon";
    private const string ConsoleSameNameLabel = MdSameNameCountLabel;
    private const string ConsoleViaSynonymLabel = MdViaSynonymCountLabel;

    // Old ids with a same-name match, and old ids with no same-name match but a synonym match.
    private static (int SameName, int ViaSynonymOnly) MatchCounts(IEnumerable<TaxonReportRow> taxa) {
        var sameName = 0;
        var viaSynonymOnly = 0;
        foreach (var row in taxa) {
            if (row.SameName.Count > 0) {
                sameName++;
            } else if (row.ViaSynonym.Count > 0) {
                viaSynonymOnly++;
            }
        }
        return (sameName, viaSynonymOnly);
    }

    private static string Describe(IReadOnlyList<IucnSameNameMatch> matches, IucnOldTaxon? old) =>
        old is null ? "" : IucnSameNameTaxa.DescribeAll(matches, old);

    // The suffix on a taxon's Markdown line: each match linked to its current assessment.
    private static string MarkdownMatches(TaxonReportRow row) {
        if (row.Old is null) {
            return "";
        }
        var sb = new StringBuilder();
        AppendMarkdownMatches(sb, MdSameNameLead, row.SameName, row.Old);
        AppendMarkdownMatches(sb, MdViaSynonymLead, row.ViaSynonym, row.Old);
        return sb.ToString();
    }

    private static void AppendMarkdownMatches(StringBuilder sb, string lead, IReadOnlyList<IucnSameNameMatch> matches, IucnOldTaxon old) {
        if (matches.Count == 0) {
            return;
        }
        sb.Append(lead);
        if (matches.Count > 1) {
            sb.Append(IucnSameNameTaxa.SeveralPrefix(matches.Count));
        }
        sb.Append(string.Join(IucnSameNameTaxa.Separator,
            matches.Select(m => $"[{EscapeMarkdownLinkText(IucnSameNameTaxa.Describe(m, old))}]({m.Url})")));
    }

    private static string EscapeMarkdownLinkText(string value) =>
        EscapeMarkdown(value).Replace("[", "\\[").Replace("]", "\\]");

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

    private static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    // The current taxa from the CSV export and the synonyms IUCN lists for them. Null, with a
    // message, when the CSV database is missing; the report is then written without the two columns.
    private static IucnSameNameTaxa? LoadSameNameTaxa(PathsService paths, string? databaseOverride, SqliteConnection apiCache) {
        string csvPath;
        try {
            csvPath = paths.ResolveIucnDatabasePath(databaseOverride, "--database");
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{ex.Message} {ColumnsLeftOut}[/]");
            return null;
        }
        if (!File.Exists(csvPath)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{CsvMissingLine(csvPath)}[/]");
            return null;
        }
        using var csv = OpenReadOnly(csvPath);
        if (!TableExists(csv, "taxonomy_html") || !TableExists(csv, "assessments_html")) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]CSV export database has no imported taxa: {csvPath}. {ColumnsLeftOut}[/]");
            return null;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN CSV database:[/] {csvPath}");
        AnsiConsole.MarkupLine("[grey]Reading current taxa and their IUCN synonyms...[/]");
        return IucnSameNameTaxa.Load(csv, apiCache);
    }

    private static string CsvMissingLine(string path) =>
        $"CSV export database not found: {path}. {ColumnsLeftOut}";

    private const string ColumnsLeftOut = "Report written without same-name and IUCN synonym matches.";

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

    private sealed record TaxonReportRow(long RootSisId, TaxaTaxonomyInfo Taxonomy, AssessmentBrief? MostRecentAssessment, IucnOldTaxon? Old) {
        public IReadOnlyList<IucnSameNameMatch> SameName { get; init; } = Array.Empty<IucnSameNameMatch>();
        public IReadOnlyList<IucnSameNameMatch> ViaSynonym { get; init; } = Array.Empty<IucnSameNameMatch>();
    }

    private sealed record AssessmentBrief(long AssessmentId, int? YearPublished, string? Category, string? Url, long RootSisId);
}
