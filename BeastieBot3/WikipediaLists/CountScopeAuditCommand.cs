using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// Diagnostic report that audits the IUCN "count scopes" per taxa group, so the headline
// list number, the chart total, and the raw body row count can be reconciled at a glance.
//
// For each taxa group it partitions every matching row in view_assessments_html_taxonomy_html
// into four mutually-exclusive, exhaustive buckets:
//   Canonical : global, species-rank, non-subpopulation  (== chart total == list headline count)
//   Regional  : species-rank, non-subpopulation, but NOT global scope (e.g. Europe, Mediterranean)
//   Infra     : subspecies + varieties (infraType set)
//   Subpop    : subpopulation/regional-population assessments (subpopulationName set)
// Canonical + Regional + Infra + Subpop == raw body rows for the group.
//
// With --compare it runs the Canonical count on BOTH the CSV dataset and the API projection
// side-by-side, so the user can see exactly where the two data sources diverge.
//
// Usage:
//   iucn count-scopes
//   iucn count-scopes --group mammals --group birds
//   iucn count-scopes --compare

namespace BeastieBot3.WikipediaLists;

[CommandInfo("iucn count-scopes", CommandKind.ReadOnly,
    "Audit IUCN count scopes per taxa group: the canonical global-species count (= chart total = list headline) vs the regional, infraspecific (subspecies/variety) and subpopulation rows excluded from it. Pass --compare to diff the CSV and API datasets.",
    Reason = "Reads the IUCN dataset(s) read-only and writes a Markdown count-scope audit report.",
    Examples = new[] {
        "iucn count-scopes",
        "iucn count-scopes --group mammals --group birds",
        "iucn count-scopes --compare",
        "iucn count-scopes --dataset api"
    })]
internal sealed class CountScopeAuditCommand : Command<CountScopeAuditCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--dataset <SOURCE>")]
        [Description("Which IUCN dataset to audit: 'csv' (default) or 'api' (the projection). Ignored when --compare is set.")]
        public string? Dataset { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Override the database path within the chosen dataset.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--taxa-config <FILE>")]
        [Description("Path to taxa-groups.yml. Defaults to rules/taxa-groups.yml.")]
        public string? TaxaConfigPath { get; init; }

        [CommandOption("--group <ID>")]
        [Description("Audit only specific taxa groups (repeatable). Omit to audit all groups plus an all-taxa total.")]
        public string[]? GroupIds { get; init; }

        [CommandOption("--compare")]
        [Description("Run the canonical count on BOTH the CSV dataset and the API projection and show the delta.")]
        public bool Compare { get; init; }

        [CommandOption("-o|--output <PATH>")]
        [Description("Output path for the Markdown report. Defaults to a timestamped file in the reports directory.")]
        public string? OutputPath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();

        var taxaConfigPath = ResolveTaxaConfigPath(paths, settings.TaxaConfigPath);
        var taxaGroups = LoadTaxaGroups(taxaConfigPath);

        // Build the audit target list: requested groups, or all groups + a no-filter "(all taxa)" row.
        var targets = BuildTargets(taxaGroups, settings.GroupIds);
        if (targets.Count == 0) {
            if (settings.GroupIds is { Length: > 0 }) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]No taxa groups matched:[/] {string.Join(", ", settings.GroupIds)}");
                AnsiConsole.MarkupLine("[grey]Available groups:[/] " + string.Join(", ", taxaGroups.Keys.OrderBy(k => k)));
            }
            return 1;
        }

        return settings.Compare
            ? RunCompare(paths, settings, targets, cancellationToken)
            : RunSingle(paths, settings, targets, cancellationToken);
    }

    // ==================== single-dataset audit ====================

    private int RunSingle(PathsService paths, Settings settings, List<AuditTarget> targets, CancellationToken ct) {
        var dbPath = IucnDatasetResolver.Resolve(paths, settings.Dataset, settings.DatabasePath);
        var dataset = string.IsNullOrWhiteSpace(settings.Dataset) ? "csv" : settings.Dataset.Trim().ToLowerInvariant();

        AnsiConsole.MarkupLineInterpolated($"[grey]Dataset:[/] {dataset}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Database:[/] {dbPath}");

        var rows = new List<(AuditTarget Target, ScopePartition Counts)>();
        using (var conn = OpenReadOnly(dbPath)) {
            var version = ScalarString(conn, "SELECT DISTINCT redlist_version FROM import_metadata LIMIT 1") ?? "unknown";
            AnsiConsole.MarkupLineInterpolated($"[grey]Version:[/] {version}");
            AnsiConsole.WriteLine();

            foreach (var target in targets) {
                ct.ThrowIfCancellationRequested();
                rows.Add((target, QueryPartition(conn, target.Filters)));
            }
        }

        var table = new Table().Border(TableBorder.Rounded)
            .AddColumn("Taxa group")
            .AddColumn(new TableColumn("Canonical").RightAligned())
            .AddColumn(new TableColumn("+Regional").RightAligned())
            .AddColumn(new TableColumn("+Infra").RightAligned())
            .AddColumn(new TableColumn("+Subpop").RightAligned())
            .AddColumn(new TableColumn("Raw body").RightAligned());
        foreach (var (target, c) in rows) {
            table.AddRow(
                Markup.Escape(target.Label),
                c.Canonical.ToString("N0"),
                Dim(c.Regional), Dim(c.Infra), Dim(c.Subpop),
                c.Raw.ToString("N0"));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine("[grey]Canonical = global species-rank count = list headline / percentage denominator = chart total. The other three columns are excluded from it.[/]");
        AnsiConsole.MarkupLine("[grey]Legacy LR/cd + LR/nt fold into NT and LR/lc into LC, so every global species lands in a bar. A residual gap would only appear if a dataset carried categories with no bar (e.g. Not Evaluated).[/]");

        var md = BuildSingleMarkdown(dataset, dbPath, rows);
        WriteReport(paths, settings.OutputPath, "iucn-count-scopes", md);
        return 0;
    }

    // ==================== CSV-vs-API comparison ====================

    private int RunCompare(PathsService paths, Settings settings, List<AuditTarget> targets, CancellationToken ct) {
        var csvPath = paths.ResolveIucnDatabasePath(null);
        var apiPath = paths.ResolveIucnApiProjectedPath(null);
        if (!File.Exists(apiPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]API projection not found:[/] {apiPath}");
            AnsiConsole.MarkupLine("Build it first with [yellow]iucn api project-view[/] (after [yellow]iucn api cache-all[/]).");
            return 1;
        }

        AnsiConsole.MarkupLineInterpolated($"[grey]CSV:[/] {csvPath}");
        AnsiConsole.MarkupLineInterpolated($"[grey]API:[/] {apiPath}");
        AnsiConsole.WriteLine();

        var rows = new List<(AuditTarget Target, ScopePartition Csv, ScopePartition Api)>();
        using (var csv = OpenReadOnly(csvPath))
        using (var api = OpenReadOnly(apiPath)) {
            foreach (var target in targets) {
                ct.ThrowIfCancellationRequested();
                rows.Add((target, QueryPartition(csv, target.Filters), QueryPartition(api, target.Filters)));
            }
        }

        var table = new Table().Border(TableBorder.Rounded)
            .AddColumn("Taxa group")
            .AddColumn(new TableColumn("CSV canon.").RightAligned())
            .AddColumn(new TableColumn("API canon.").RightAligned())
            .AddColumn(new TableColumn("Δ").RightAligned())
            .AddColumn(new TableColumn("CSV infra").RightAligned());
        foreach (var (target, csv, api) in rows) {
            var delta = api.Canonical - csv.Canonical;
            table.AddRow(
                Markup.Escape(target.Label),
                csv.Canonical.ToString("N0"),
                api.Canonical.ToString("N0"),
                DeltaMarkup(delta),
                Dim(csv.Infra));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine("[grey]Δ = API − CSV. API is species-only (no Infra) and omits delisted taxa with no current assessment — see [yellow]iucn api report-no-latest[/].[/]");

        var md = BuildCompareMarkdown(csvPath, apiPath, rows);
        WriteReport(paths, settings.OutputPath, "iucn-count-scopes-compare", md);
        return 0;
    }

    // ==================== the partition query ====================

    // Conditions mirror TaxonFilterSql.GlobalSpeciesPredicate, split so the report can attribute
    // each excluded bucket. The four CASE arms are mutually exclusive and exhaustive.
    private const string SpeciesNoSubpop =
        "(v.infraType IS NULL OR v.infraType = '') AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '')";
    private const string GlobalScope =
        "(v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')";
    private const string IsInfra = "(v.infraType IS NOT NULL AND TRIM(v.infraType) <> '')";
    private const string IsSubpopNotInfra =
        "(v.infraType IS NULL OR TRIM(v.infraType) = '') AND (v.subpopulationName IS NOT NULL AND TRIM(v.subpopulationName) <> '')";

    private static ScopePartition QueryPartition(SqliteConnection conn, List<TaxonFilterDefinition>? filters) {
        var parameters = new List<SqliteParameter>();
        var sql = new StringBuilder();
        sql.AppendLine("SELECT");
        sql.AppendLine($"  SUM(CASE WHEN {SpeciesNoSubpop} AND {GlobalScope} THEN 1 ELSE 0 END) AS canonical,");
        sql.AppendLine($"  SUM(CASE WHEN {SpeciesNoSubpop} AND NOT ({GlobalScope}) THEN 1 ELSE 0 END) AS regional,");
        sql.AppendLine($"  SUM(CASE WHEN {IsInfra} THEN 1 ELSE 0 END) AS infra,");
        sql.AppendLine($"  SUM(CASE WHEN {IsSubpopNotInfra} THEN 1 ELSE 0 END) AS subpop,");
        sql.AppendLine("  COUNT(*) AS raw");
        sql.AppendLine("FROM view_assessments_html_taxonomy_html v");
        sql.AppendLine("WHERE 1=1");
        if (filters != null) {
            for (var i = 0; i < filters.Count; i++) {
                TaxonFilterSql.AppendFilter(sql, parameters, filters[i], i, paramPrefix: "a");
            }
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql.ToString();
        cmd.CommandTimeout = 0;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return new ScopePartition(0, 0, 0, 0, 0);
        return new ScopePartition(
            GetInt(reader, 0), GetInt(reader, 1), GetInt(reader, 2), GetInt(reader, 3), GetInt(reader, 4));
    }

    private sealed record ScopePartition(long Canonical, long Regional, long Infra, long Subpop, long Raw);

    private sealed record AuditTarget(string Label, List<TaxonFilterDefinition>? Filters);

    // ==================== markdown ====================

    private static string BuildSingleMarkdown(string dataset, string dbPath, List<(AuditTarget Target, ScopePartition Counts)> rows) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Count-Scope Audit");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **Dataset:** `{dataset}`");
        sb.AppendLine($"- **Database:** `{dbPath}`");
        sb.AppendLine();
        sb.AppendLine("`Canonical` is the global, species-rank, non-subpopulation count — the number used for the list headline prose, the DD/threatened percentage denominator, and the bar-chart total. The remaining columns are the rows excluded from it; the four sum to `Raw body`.");
        sb.AppendLine();
        sb.AppendLine("> Note: legacy `Lower Risk/conservation dependent` + `Lower Risk/near threatened` fold into NT and `Lower Risk/least concern` into LC, so every global species lands in a chart bar and the chart total equals `Canonical`. A residual gap would only appear if a dataset carried categories with no bar (e.g. `Not Evaluated`).");
        sb.AppendLine();
        sb.AppendLine("| Taxa group | Canonical | +Regional | +Infra | +Subpop | Raw body |");
        sb.AppendLine("| --- | ---: | ---: | ---: | ---: | ---: |");
        foreach (var (target, c) in rows) {
            sb.AppendLine($"| {Esc(target.Label)} | {c.Canonical:N0} | {c.Regional:N0} | {c.Infra:N0} | {c.Subpop:N0} | {c.Raw:N0} |");
        }
        sb.AppendLine();
        return sb.ToString();
    }

    private static string BuildCompareMarkdown(string csvPath, string apiPath, List<(AuditTarget Target, ScopePartition Csv, ScopePartition Api)> rows) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN Count-Scope Audit — CSV vs API");
        sb.AppendLine();
        sb.AppendLine($"- **Generated:** {DateTimeOffset.Now:O}");
        sb.AppendLine($"- **CSV:** `{csvPath}`");
        sb.AppendLine($"- **API projection:** `{apiPath}`");
        sb.AppendLine();
        sb.AppendLine("`Canonical` = global species-rank count on each dataset. The API projection is species-only (no subspecies/varieties) and omits delisted taxa lacking a current assessment, so it normally runs slightly under CSV. `CSV infra` is the subspecies/variety count the API has none of.");
        sb.AppendLine();
        sb.AppendLine("| Taxa group | CSV canonical | API canonical | Δ (API−CSV) | CSV infra |");
        sb.AppendLine("| --- | ---: | ---: | ---: | ---: |");
        foreach (var (target, csv, api) in rows) {
            sb.AppendLine($"| {Esc(target.Label)} | {csv.Canonical:N0} | {api.Canonical:N0} | {api.Canonical - csv.Canonical:+0;-0;0} | {csv.Infra:N0} |");
        }
        sb.AppendLine();
        return sb.ToString();
    }

    private void WriteReport(PathsService paths, string? explicitOutput, string stem, string markdown) {
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");
        var mdPath = ReportPathResolver.ResolveFilePath(
            paths, explicitOutput, explicitDirectory: null, fallbackBaseDirectory: Environment.CurrentDirectory,
            defaultFileName: $"{stem}-{timestamp}.md");
        File.WriteAllText(mdPath, markdown, Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]Report written:[/] {mdPath}");
    }

    // ==================== helpers ====================

    private static List<AuditTarget> BuildTargets(Dictionary<string, TaxaGroupDefinition> groups, string[]? requested) {
        var targets = new List<AuditTarget>();
        if (requested is { Length: > 0 }) {
            var wanted = new HashSet<string>(requested, StringComparer.OrdinalIgnoreCase);
            foreach (var (id, def) in groups.Where(kv => wanted.Contains(kv.Key)).OrderBy(kv => kv.Key)) {
                targets.Add(new AuditTarget(id, def.Filters));
            }
            return targets;
        }

        targets.Add(new AuditTarget("(all taxa)", null));
        foreach (var (id, def) in groups.OrderBy(kv => kv.Key)) {
            targets.Add(new AuditTarget(id, def.Filters));
        }
        return targets;
    }

    private static SqliteConnection OpenReadOnly(string path) {
        var csb = new SqliteConnectionStringBuilder { DataSource = Path.GetFullPath(path), Mode = SqliteOpenMode.ReadOnly };
        var conn = new SqliteConnection(csb.ConnectionString);
        conn.Open();
        return conn;
    }

    private static long GetInt(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? 0L : Convert.ToInt64(reader.GetValue(ordinal));

    private static string Dim(long n) => n == 0 ? "[grey]0[/]" : n.ToString("N0");

    private static string DeltaMarkup(long delta) => delta switch {
        0 => "[grey]0[/]",
        < 0 => $"[red]{delta:N0}[/]",
        _ => $"[green]+{delta:N0}[/]",
    };

    private static string Esc(string s) => s.Replace("|", "\\|");

    private static Dictionary<string, TaxaGroupDefinition> LoadTaxaGroups(string path) {
        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
        using var reader = File.OpenText(path);
        var file = deserializer.Deserialize<TaxaGroupsFile>(reader);
        return file?.Groups ?? new();
    }

    private static string ResolveTaxaConfigPath(PathsService paths, string? explicitPath) {
        if (!string.IsNullOrWhiteSpace(explicitPath)) return Path.GetFullPath(explicitPath);
        var candidates = new[] {
            Path.Combine(paths.BaseDirectory, "rules", "taxa-groups.yml"),
            Path.Combine(AppContext.BaseDirectory, "rules", "taxa-groups.yml"),
        };
        foreach (var candidate in candidates) {
            if (File.Exists(candidate)) return candidate;
        }
        throw new FileNotFoundException("taxa-groups.yml not found. Pass --taxa-config explicitly.");
    }

    private static string? ScalarString(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        try { return cmd.ExecuteScalar() as string; } catch { return null; }
    }
}
