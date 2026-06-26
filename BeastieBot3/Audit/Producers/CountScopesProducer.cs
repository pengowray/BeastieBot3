using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Data.Sqlite;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using BeastieBot3.Audit.Model;
using BeastieBot3.WikipediaLists;

// Methodology reconciliation: for each taxa group, the canonical global species-rank count (the
// list headline and chart total) and the regional, infraspecific, and subpopulation rows excluded
// from it. Mirrors CountScopeAuditCommand's partition, reusing TaxonFilterSql for the group filters.

namespace BeastieBot3.Audit.Producers;

internal sealed class CountScopesProducer : IAuditReportProducer {
    public string Id => "count-scopes";

    private const string SpeciesNoSubpop =
        "(v.infraType IS NULL OR v.infraType = '') AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '')";
    private const string GlobalScope = "(v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')";
    private const string IsInfra = "(v.infraType IS NOT NULL AND TRIM(v.infraType) <> '')";
    private const string IsSubpopNotInfra =
        "(v.infraType IS NULL OR TRIM(v.infraType) = '') AND (v.subpopulationName IS NOT NULL AND TRIM(v.subpopulationName) <> '')";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "view_assessments_html_taxonomy_html")) {
            return null;
        }

        var groups = LoadTaxaGroups(ctx);
        if (groups is null) {
            return null;
        }

        var findings = new List<AuditFinding>();
        // "(all taxa)" first.
        findings.Add(Build("(all taxa)", QueryPartition(conn, null)));
        foreach (var (id, def) in groups.OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase)) {
            ctx.Ct.ThrowIfCancellationRequested();
            findings.Add(Build(id, QueryPartition(conn, def.Filters)));
        }

        var ordered = findings
            .OrderByDescending(f => f.Field == "(all taxa)")
            .ThenByDescending(f => long.TryParse(f.Get("canonical")?.Replace(",", ""), out var n) ? n : 0)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Count reconciliation by taxa group",
            Tier = AuditReportTier.Methodology,
            Breakage = BreakageClass.Advisory,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Summary =
                "For each taxa group this splits every assessment row into four buckets that sum to the raw body count: " +
                "canonical (global, species rank), regional (species rank, non-global scope), infraspecific (subspecies and varieties), and subpopulation. " +
                "The canonical count is the figure used for a list headline, a percentage denominator, and a bar-chart total. " +
                "This page exists so those numbers can be reconciled at a glance.",
            Columns = new List<AuditColumn> {
                AuditColumns.Custom("taxaGroup", "Taxa group", AuditColumnType.Text),
                AuditColumns.Custom("canonical", "Canonical", AuditColumnType.Number, "Global species-rank count. Equals the list headline and chart total."),
                AuditColumns.Custom("regional", "+Regional", AuditColumnType.Number),
                AuditColumns.Custom("infra", "+Infra", AuditColumnType.Number),
                AuditColumns.Custom("subpop", "+Subpop", AuditColumnType.Number),
                AuditColumns.Custom("rawBody", "Raw body", AuditColumnType.Number),
            },
            Findings = ordered,
            HeadlineCount = ordered.Count,
        };
    }

    private static AuditFinding Build(string label, (long Canonical, long Regional, long Infra, long Subpop, long Raw) p) {
        var f = new AuditFinding {
            ReportId = "count-scopes",
            DataSource = "csv",
            Field = label,
            CurrentValue = p.Canonical.ToString("N0"),
            SuggestedValue = p.Raw.ToString("N0"),
            IssueType = "scope-partition",
            Detail = $"Canonical {p.Canonical:N0}; +Regional {p.Regional:N0}; +Infra {p.Infra:N0}; +Subpop {p.Subpop:N0}; raw body {p.Raw:N0}.",
        };
        f.Extra["taxaGroup"] = label;
        f.Extra["canonical"] = p.Canonical.ToString("N0");
        f.Extra["regional"] = p.Regional.ToString("N0");
        f.Extra["infra"] = p.Infra.ToString("N0");
        f.Extra["subpop"] = p.Subpop.ToString("N0");
        f.Extra["rawBody"] = p.Raw.ToString("N0");
        if (p.Canonical == p.Raw) {
            f.Notes.Add("No rows excluded for this group (no regional, infraspecific, or subpopulation rows).");
        }
        return f;
    }

    private static (long, long, long, long, long) QueryPartition(SqliteConnection conn, List<TaxonFilterDefinition>? filters) {
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
        foreach (var p in parameters) {
            cmd.Parameters.Add(p);
        }
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) {
            return (0, 0, 0, 0, 0);
        }
        return (Get(reader, 0), Get(reader, 1), Get(reader, 2), Get(reader, 3), Get(reader, 4));
    }

    private static long Get(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? 0L : Convert.ToInt64(reader.GetValue(ordinal));

    private static Dictionary<string, TaxaGroupDefinition>? LoadTaxaGroups(AuditContext ctx) {
        var candidates = new[] {
            Path.Combine(ctx.Paths.BaseDirectory, "rules", "taxa-groups.yml"),
            Path.Combine(AppContext.BaseDirectory, "rules", "taxa-groups.yml"),
        };
        var path = candidates.FirstOrDefault(File.Exists);
        if (path is null) {
            return null;
        }
        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
        using var reader = File.OpenText(path);
        var file = deserializer.Deserialize<TaxaGroupsFile>(reader);
        return file?.Groups ?? new Dictionary<string, TaxaGroupDefinition>();
    }
}
