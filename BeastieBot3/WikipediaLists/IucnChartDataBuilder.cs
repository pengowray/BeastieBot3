using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Data.Sqlite;

// Queries the IUCN SQLite database for aggregate species counts per Red List
// category, filtered by taxonomic group. Used by ChartGeneratorCommand to
// produce bar chart data for Wikipedia. Counts only full species with global
// scope, excluding subspecies, varieties, and regional/subpopulation assessments.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Status categories in Wikipedia display order: EX → DD.
/// CR is split into CR(PE), CR(PEW), and "pure" CR (mutually exclusive).
/// LR/cd is merged into NT.
/// </summary>
internal static class ChartStatusOrder {
    /// <summary>
    /// Ordered status entries for the bar chart. Each entry defines a database
    /// category filter and how CR sub-flags are handled.
    /// </summary>
    public static readonly IReadOnlyList<ChartStatusEntry> Entries = new List<ChartStatusEntry> {
        new("EX", "Extinct", "Extinct"),
        new("EW", "Extinct in the Wild", "Extinct in the Wild"),
        new("CR(PE)", "Critically Endangered (PE)", "Critically Endangered", PeFilter: "true", PewFilter: null),
        new("CR(PEW)", "Critically Endangered (PEW)", "Critically Endangered", PeFilter: null, PewFilter: "true"),
        new("CR", "Critically Endangered", "Critically Endangered", PeFilter: "false", PewFilter: "false"),
        new("EN", "Endangered", "Endangered"),
        new("VU", "Vulnerable", "Vulnerable"),
        // NT absorbs LR/cd — the count method handles both categories.
        new("NT", "Near Threatened", "Near Threatened", MergesLrCd: true),
        new("LC", "Least Concern", "Least Concern"),
        new("DD", "Data Deficient", "Data Deficient"),
    };
}

/// <param name="Code">Short code for display (e.g. "CR(PE)").</param>
/// <param name="Label">Human-readable label for chart axis/legend.</param>
/// <param name="DbCategory">Value in redlistCategory column.</param>
/// <param name="PeFilter">If set, filter possiblyExtinct to this value.</param>
/// <param name="PewFilter">If set, filter possiblyExtinctInTheWild to this value.</param>
/// <param name="MergesLrCd">When true, also count "Lower Risk/conservation dependent" rows.</param>
internal sealed record ChartStatusEntry(
    string Code,
    string Label,
    string DbCategory,
    string? PeFilter = null,
    string? PewFilter = null,
    bool MergesLrCd = false);

/// <summary>
/// Result of counting species for one chart group.
/// </summary>
internal sealed class ChartGroupResult {
    public required string GroupId { get; init; }
    public required string ChartName { get; init; }
    public required string DatasetVersion { get; init; }
    public required bool Comprehensive { get; init; }
    public required string? TemplateName { get; init; }
    public required string? Caption { get; init; }

    /// <summary>Ordered status counts matching <see cref="ChartStatusOrder.Entries"/>.</summary>
    public required IReadOnlyList<StatusCount> Counts { get; init; }

    /// <summary>Number of LR/cd species merged into the NT count. Zero if none.</summary>
    public required int LrCdMerged { get; init; }

    public int TotalAssessed => Counts.Sum(c => c.Count);
}

internal sealed record StatusCount(string Code, string Label, int Count);

internal sealed class IucnChartDataBuilder : IDisposable {
    private readonly SqliteConnection _connection;

    public IucnChartDataBuilder(string databasePath) {
        if (string.IsNullOrWhiteSpace(databasePath))
            throw new ArgumentException("Database path was not provided.", nameof(databasePath));

        var fullPath = Path.GetFullPath(databasePath);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"IUCN SQLite database not found: {fullPath}", fullPath);

        var builder = new SqliteConnectionStringBuilder {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadOnly
        };
        _connection = new SqliteConnection(builder.ToString());
        _connection.Open();
    }

    public string GetDatasetVersion() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT redlist_version FROM import_metadata LIMIT 1";
        var result = cmd.ExecuteScalar();
        return result?.ToString() ?? "unknown";
    }

    /// <summary>
    /// Count global species per status category for a given set of taxonomic filters.
    /// </summary>
    /// <param name="filters">Taxonomic filters (kingdom, class, order, etc.). Null or empty means all species.</param>
    public ChartGroupResult BuildCounts(
        string groupId,
        string chartName,
        bool comprehensive,
        string? templateName,
        string? caption,
        List<TaxonFilterDefinition>? filters) {

        var version = GetDatasetVersion();
        var counts = new List<StatusCount>();
        var lrCdMerged = 0;

        foreach (var entry in ChartStatusOrder.Entries) {
            var count = CountSpecies(entry, filters);

            if (entry.MergesLrCd) {
                var lrCdCount = CountLrCd(filters);
                lrCdMerged = lrCdCount;
                count += lrCdCount;
            }

            counts.Add(new StatusCount(entry.Code, entry.Label, count));
        }

        return new ChartGroupResult {
            GroupId = groupId,
            ChartName = chartName,
            DatasetVersion = version,
            Comprehensive = comprehensive,
            TemplateName = templateName,
            Caption = caption,
            Counts = counts,
            LrCdMerged = lrCdMerged,
        };
    }

    private int CountSpecies(ChartStatusEntry entry, List<TaxonFilterDefinition>? filters) {
        var parameters = new List<SqliteParameter>();
        var sql = new StringBuilder();
        sql.AppendLine("SELECT COUNT(*) FROM view_assessments_html_taxonomy_html v");
        sql.AppendLine("WHERE (v.infraType IS NULL OR v.infraType = '')");
        sql.AppendLine("  AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '')");
        sql.AppendLine("  AND (v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')");

        var catParam = new SqliteParameter("@category", entry.DbCategory);
        sql.AppendLine($"  AND v.redlistCategory = @category");
        parameters.Add(catParam);

        if (entry.PeFilter is not null) {
            var peParam = new SqliteParameter("@pe", entry.PeFilter);
            sql.AppendLine($"  AND IFNULL(v.possiblyExtinct, 'false') = @pe");
            parameters.Add(peParam);
        }

        if (entry.PewFilter is not null) {
            var pewParam = new SqliteParameter("@pew", entry.PewFilter);
            sql.AppendLine($"  AND IFNULL(v.possiblyExtinctInTheWild, 'false') = @pew");
            parameters.Add(pewParam);
        }

        AppendTaxonFilters(sql, parameters, filters);

        return ExecuteCount(sql.ToString(), parameters);
    }

    private int CountLrCd(List<TaxonFilterDefinition>? filters) {
        var parameters = new List<SqliteParameter>();
        var sql = new StringBuilder();
        sql.AppendLine("SELECT COUNT(*) FROM view_assessments_html_taxonomy_html v");
        sql.AppendLine("WHERE (v.infraType IS NULL OR v.infraType = '')");
        sql.AppendLine("  AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '')");
        sql.AppendLine("  AND (v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')");
        sql.AppendLine("  AND v.redlistCategory = @category");
        parameters.Add(new SqliteParameter("@category", "Lower Risk/conservation dependent"));

        AppendTaxonFilters(sql, parameters, filters);

        return ExecuteCount(sql.ToString(), parameters);
    }

    private static void AppendTaxonFilters(StringBuilder sql, List<SqliteParameter> parameters, List<TaxonFilterDefinition>? filters) {
        if (filters is null) return;

        for (var i = 0; i < filters.Count; i++) {
            var filter = filters[i];

            if (!string.IsNullOrWhiteSpace(filter.System)) {
                var p = new SqliteParameter($"@sys_{i}", $"%{filter.System}%");
                sql.AppendLine($"  AND v.systems LIKE @sys_{i}");
                parameters.Add(p);
                continue;
            }

            var column = filter.Rank?.Trim().ToLowerInvariant() switch {
                "kingdom" => "kingdomName",
                "phylum" => "phylumName",
                "class" => "className",
                "order" => "orderName",
                "family" => "familyName",
                "genus" => "genusName",
                _ => null
            };
            if (column is null) continue;

            if (filter.Values is { Count: > 0 }) {
                var orClauses = new List<string>();
                for (var j = 0; j < filter.Values.Count; j++) {
                    var val = NormalizeFilterValue(filter.Rank, filter.Values[j]);
                    if (string.IsNullOrWhiteSpace(val)) continue;
                    var p = new SqliteParameter($"@f_{column}_{i}_{j}", val);
                    orClauses.Add($"v.{column} = @f_{column}_{i}_{j}");
                    parameters.Add(p);
                }
                if (orClauses.Count > 0)
                    sql.AppendLine($"  AND ({string.Join(" OR ", orClauses)})");
            } else {
                var val = NormalizeFilterValue(filter.Rank, filter.Value);
                if (string.IsNullOrWhiteSpace(val)) continue;
                var p = new SqliteParameter($"@f_{column}_{i}", val);
                sql.AppendLine($"  AND v.{column} = @f_{column}_{i}");
                parameters.Add(p);
            }
        }
    }

    private static string? NormalizeFilterValue(string? rank, string? value) {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return rank?.Trim().ToLowerInvariant() switch {
            "kingdom" or "phylum" or "class" or "order" or "family" => value.Trim().ToUpperInvariant(),
            _ => value.Trim()
        };
    }

    private int ExecuteCount(string sql, List<SqliteParameter> parameters) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public void Dispose() => _connection.Dispose();
}
