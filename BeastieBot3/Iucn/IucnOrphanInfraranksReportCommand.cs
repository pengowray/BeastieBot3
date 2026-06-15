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

// Lists assessed infraspecific taxa (subspecies/varieties) whose PARENT species has no
// species-level assessment in the CSV. These are "orphans": the IUCN API can't reach them
// through its discovery paths — an unassessed parent appears in neither the CSV species list
// (so cache-taxa never queues it) nor the family-page listings (which return only assessed
// species, so discover-by-family never sees it), so its taxon.infrarank_taxa is never read.
// They're recoverable only by their own sis_id, which only the CSV enumerates (cache-infraranks
// --from-csv). Reads the CSV-imported IUCN DB; outputs Markdown (grouped by taxonomy) + CSV.
// Run via: iucn report-orphan-infraranks

namespace BeastieBot3.Iucn;

[CommandInfo("iucn report-orphan-infraranks", CommandKind.ReadOnly,
    "List assessed subspecies/varieties whose parent species is unassessed — the infraspecific taxa the IUCN API can't discover on its own (only reachable by their CSV sis_id). Outputs Markdown and CSV.",
    Examples = new[] {
        "iucn report-orphan-infraranks",
        "iucn report-orphan-infraranks -o orphans.md --csv-output orphans.csv"
    })]
public sealed class IucnOrphanInfraranksReportCommand : Command<IucnOrphanInfraranksReportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Override path to the CSV-imported IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
        public string? DatabasePath { get; init; }

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
        var dbPath = paths.ResolveIucnDatabasePath(settings.DatabasePath);

        if (!File.Exists(dbPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN CSV database not found:[/] {dbPath}");
            AnsiConsole.MarkupLine("Import it first with [yellow]iucn import[/].");
            return -1;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN CSV database:[/] {dbPath}");

        var ro = new SqliteConnectionStringBuilder { DataSource = dbPath, Mode = SqliteOpenMode.ReadOnly };
        using var connection = new SqliteConnection(ro.ConnectionString);
        connection.Open();

        if (!ObjectExists(connection, "view_assessments_html_taxonomy_html")) {
            AnsiConsole.MarkupLine("[red]view_assessments_html_taxonomy_html not found.[/] Re-run [yellow]iucn import --force[/] to rebuild the view.");
            return -2;
        }

        AnsiConsole.MarkupLine("[grey]Finding infraspecific taxa whose parent species is unassessed…[/]");
        var orphans = Query(connection, cancellationToken);
        AnsiConsole.MarkupLineInterpolated($"[grey]Orphan infraspecific taxa:[/] {orphans.Count:N0}");

        var fallbackBaseDir = Path.GetDirectoryName(dbPath) ?? Environment.CurrentDirectory;
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");

        var mdPath = ReportPathResolver.ResolveFilePath(
            paths, settings.OutputPath, explicitDirectory: null,
            fallbackBaseDirectory: fallbackBaseDir,
            defaultFileName: $"iucn-orphan-infraranks-{timestamp}.md");

        var csvPath = settings.CsvOutputPath
            ?? Path.Combine(Path.GetDirectoryName(mdPath) ?? ".", $"iucn-orphan-infraranks-{timestamp}.csv");
        var csvDir = Path.GetDirectoryName(csvPath);
        if (!string.IsNullOrEmpty(csvDir)) Directory.CreateDirectory(csvDir);

        File.WriteAllText(mdPath, BuildMarkdown(dbPath, orphans), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]Markdown report written to:[/] {mdPath}");
        File.WriteAllText(csvPath, BuildCsv(orphans), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]CSV report written to:[/] {csvPath}");

        PrintSummary(orphans);
        return 0;
    }

    private static List<OrphanRow> Query(SqliteConnection connection, CancellationToken cancellationToken) {
        // An infraspecific taxon (infraType set) is an orphan when no species-rank row
        // (infraType empty AND subpopulationName empty) shares its genus + species.
        const string sql = @"
SELECT i.taxonId, i.assessmentId, i.scientificName, i.infraType, i.infraName, i.subpopulationName,
       i.redlistCategory, i.kingdomName, i.phylumName, i.className, i.orderName, i.familyName,
       i.genusName, i.speciesName
FROM view_assessments_html_taxonomy_html i
WHERE i.infraType IS NOT NULL AND TRIM(i.infraType) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM view_assessments_html_taxonomy_html p
    WHERE p.genusName = i.genusName
      AND p.speciesName = i.speciesName
      AND (p.infraType IS NULL OR TRIM(p.infraType) = '')
      AND (p.subpopulationName IS NULL OR TRIM(p.subpopulationName) = '')
  )
ORDER BY i.kingdomName, i.className, i.orderName, i.familyName, i.scientificName, i.taxonId";

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;

        var rows = new List<OrphanRow>();
        var seen = new HashSet<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var taxonId = reader.GetInt64(0);
            if (!seen.Add(taxonId)) continue; // one row per taxon (a taxon may have regional + global rows)
            rows.Add(new OrphanRow(
                taxonId,
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                Str(reader, 2), Str(reader, 3), Str(reader, 4), Str(reader, 5), Str(reader, 6),
                Str(reader, 7), Str(reader, 8), Str(reader, 9), Str(reader, 10), Str(reader, 11),
                Str(reader, 12), Str(reader, 13)));
        }
        return rows;
    }

    private static string BuildMarkdown(string dbPath, List<OrphanRow> orphans) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Orphan Infraspecific Taxa (not API-discoverable)");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **Source CSV database:** `{EscapeMd(dbPath)}`");
        sb.AppendLine($"- **Orphan infraspecific taxa:** {orphans.Count:N0}");
        sb.AppendLine();
        sb.AppendLine("Assessed subspecies/varieties whose **parent species has no species-level assessment** in the");
        sb.AppendLine("Red List. The IUCN API cannot discover these on its own: an unassessed parent species appears");
        sb.AppendLine("in neither the CSV species list (so `cache-taxa` never queues it) nor the family-page listings");
        sb.AppendLine("(which return only assessed species, so `discover-by-family` never sees it), so the parent's");
        sb.AppendLine("`taxon.infrarank_taxa` — where these would be listed — is never read. They are recoverable only");
        sb.AppendLine("by their own SIS id, which only the CSV enumerates (`iucn api cache-infraranks --from-csv`).");
        sb.AppendLine();

        if (orphans.Count == 0) {
            sb.AppendLine("No orphan infraspecific taxa found — every assessed subspecies/variety has an assessed parent species.");
            return sb.ToString();
        }

        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine("| Breakdown | Count |");
        sb.AppendLine("| --- | ---: |");
        foreach (var (label, count) in orphans.GroupBy(o => o.InfraType ?? "(none)").Select(g => (g.Key, g.Count())).OrderByDescending(x => x.Item2)) {
            sb.AppendLine($"| infraType: {EscapeMd(label)} | {count:N0} |");
        }
        foreach (var (label, count) in orphans.GroupBy(o => o.KingdomName ?? "(unknown)").Select(g => (g.Key, g.Count())).OrderByDescending(x => x.Item2)) {
            sb.AppendLine($"| kingdom: {EscapeMd(label)} | {count:N0} |");
        }
        sb.AppendLine();

        sb.AppendLine("## By class");
        sb.AppendLine();
        sb.AppendLine("| Class | Count |");
        sb.AppendLine("| --- | ---: |");
        foreach (var group in orphans.GroupBy(o => o.ClassName ?? "(unknown)").OrderByDescending(g => g.Count())) {
            sb.AppendLine($"| {EscapeMd(group.Key)} | {group.Count():N0} |");
        }
        sb.AppendLine();

        sb.AppendLine("## Detailed listing");
        sb.AppendLine();
        string? curKingdom = null, curClass = null, curOrder = null, curFamily = null;
        foreach (var o in orphans) {
            var kingdom = o.KingdomName ?? "(unknown kingdom)";
            var className = o.ClassName ?? "(unknown class)";
            var order = o.OrderName ?? "(unknown order)";
            var family = o.FamilyName ?? "(unknown family)";

            if (kingdom != curKingdom) { curKingdom = kingdom; curClass = curOrder = curFamily = null; sb.AppendLine($"### {EscapeMd(kingdom)}"); sb.AppendLine(); }
            if (className != curClass) { curClass = className; curOrder = curFamily = null; sb.AppendLine($"#### {EscapeMd(className)}"); sb.AppendLine(); }
            if (order != curOrder) { curOrder = order; curFamily = null; sb.AppendLine($"##### {EscapeMd(order)}"); sb.AppendLine(); }
            if (family != curFamily) { curFamily = family; sb.AppendLine($"**{EscapeMd(family)}**"); sb.AppendLine(); }

            var name = o.ScientificName ?? $"SIS {o.TaxonId}";
            var status = string.IsNullOrWhiteSpace(o.RedlistCategory) ? "" : $" — {EscapeMd(o.RedlistCategory!)}";
            var parent = $"{o.GenusName} {o.SpeciesName}".Trim();
            var url = $"https://www.iucnredlist.org/species/{o.TaxonId}/{(o.AssessmentId?.ToString(CultureInfo.InvariantCulture) ?? "")}";
            sb.AppendLine($"- [*{EscapeMd(name)}*]({url}){status}  ·  parent species *{EscapeMd(parent)}* (unassessed)");
        }
        sb.AppendLine();
        return sb.ToString();
    }

    private static string BuildCsv(List<OrphanRow> orphans) {
        var sb = new StringBuilder();
        sb.AppendLine("taxon_id,assessment_id,scientific_name,infra_type,infra_name,red_list_category,parent_species,kingdom,phylum,class,order,family,genus,species,subpopulation,iucn_url");
        foreach (var o in orphans) {
            var parent = $"{o.GenusName} {o.SpeciesName}".Trim();
            var url = $"https://www.iucnredlist.org/species/{o.TaxonId}/{(o.AssessmentId?.ToString(CultureInfo.InvariantCulture) ?? "")}";
            sb.AppendLine(string.Join(",",
                Csv(o.TaxonId.ToString(CultureInfo.InvariantCulture)),
                Csv(o.AssessmentId?.ToString(CultureInfo.InvariantCulture) ?? ""),
                Csv(o.ScientificName ?? ""), Csv(o.InfraType ?? ""), Csv(o.InfraName ?? ""),
                Csv(o.RedlistCategory ?? ""), Csv(parent),
                Csv(o.KingdomName ?? ""), Csv(o.PhylumName ?? ""), Csv(o.ClassName ?? ""),
                Csv(o.OrderName ?? ""), Csv(o.FamilyName ?? ""), Csv(o.GenusName ?? ""), Csv(o.SpeciesName ?? ""),
                Csv(o.SubpopulationName ?? ""), Csv(url)));
        }
        return sb.ToString();
    }

    private static void PrintSummary(List<OrphanRow> orphans) {
        if (orphans.Count == 0) {
            AnsiConsole.MarkupLine("[green]No orphan infraspecific taxa found.[/]");
            return;
        }
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("infraType");
        table.AddColumn(new TableColumn("Count").RightAligned());
        foreach (var g in orphans.GroupBy(o => o.InfraType ?? "(none)").OrderByDescending(g => g.Count())) {
            table.AddRow(Markup.Escape(g.Key), g.Count().ToString("N0"));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated($"[grey]Total orphan infraspecific taxa:[/] {orphans.Count:N0}");
    }

    private static string? Str(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

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

    private sealed record OrphanRow(
        long TaxonId, long? AssessmentId, string? ScientificName, string? InfraType, string? InfraName,
        string? SubpopulationName, string? RedlistCategory, string? KingdomName, string? PhylumName,
        string? ClassName, string? OrderName, string? FamilyName, string? GenusName, string? SpeciesName);
}
