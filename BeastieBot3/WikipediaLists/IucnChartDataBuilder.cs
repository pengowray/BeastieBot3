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
        // NT absorbs legacy LR/cd (footnoted) and LR/nt (silent direct equivalent).
        new("NT", "Near Threatened", "Near Threatened", MergesLrCd: true,
            ExtraCategories: new[] { "Lower Risk/near threatened" }),
        // LC absorbs legacy LR/lc (silent direct equivalent).
        new("LC", "Least Concern", "Least Concern",
            ExtraCategories: new[] { "Lower Risk/least concern" }),
        new("DD", "Data Deficient", "Data Deficient"),
    };
}

/// <param name="Code">Short code for display (e.g. "CR(PE)").</param>
/// <param name="Label">Human-readable label for chart axis/legend.</param>
/// <param name="DbCategory">Value in redlistCategory column.</param>
/// <param name="PeFilter">If set, filter possiblyExtinct to this value.</param>
/// <param name="PewFilter">If set, filter possiblyExtinctInTheWild to this value.</param>
/// <param name="MergesLrCd">When true, also count "Lower Risk/conservation dependent" rows (tracked
/// separately for the LR/cd footnote).</param>
/// <param name="ExtraCategories">Additional legacy redlistCategory values folded into this code with no
/// footnote — direct equivalents of the modern category (e.g. "Lower Risk/near threatened" → NT,
/// "Lower Risk/least concern" → LC), matching the Wikipedia list status mapping.</param>
internal sealed record ChartStatusEntry(
    string Code,
    string Label,
    string DbCategory,
    string? PeFilter = null,
    string? PewFilter = null,
    bool MergesLrCd = false,
    IReadOnlyList<string>? ExtraCategories = null);

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
        sql.AppendLine($"WHERE {TaxonFilterSql.GlobalSpeciesPredicate()}");

        // Main category plus any silently-folded legacy equivalents (LR/nt, LR/lc).
        var catNames = new List<string> { "@category" };
        parameters.Add(new SqliteParameter("@category", entry.DbCategory));
        if (entry.ExtraCategories is { Count: > 0 }) {
            for (var i = 0; i < entry.ExtraCategories.Count; i++) {
                var name = $"@extracat{i}";
                catNames.Add(name);
                parameters.Add(new SqliteParameter(name, entry.ExtraCategories[i]));
            }
        }
        sql.AppendLine($"  AND v.redlistCategory IN ({string.Join(", ", catNames)})");

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
        sql.AppendLine($"WHERE {TaxonFilterSql.GlobalSpeciesPredicate()}");
        sql.AppendLine("  AND v.redlistCategory = @category");
        parameters.Add(new SqliteParameter("@category", "Lower Risk/conservation dependent"));

        AppendTaxonFilters(sql, parameters, filters);

        return ExecuteCount(sql.ToString(), parameters);
    }

    private static void AppendTaxonFilters(StringBuilder sql, List<SqliteParameter> parameters, List<TaxonFilterDefinition>? filters) {
        if (filters is null) return;

        for (var i = 0; i < filters.Count; i++) {
            TaxonFilterSql.AppendFilter(sql, parameters, filters[i], i);
        }
    }

    /// <summary>
    /// Per-child status breakdown computed in ONE GROUP BY scan: for a parent filter set and a child
    /// rank, returns childKey -&gt; ordered <see cref="StatusCount"/>[] (full EX..DD in
    /// <see cref="ChartStatusOrder"/> order). Uses the SAME scope / species-only / no-subpop predicate
    /// and the SAME CR(PE)/CR(PEW)/pure-CR split and LR/cd-&gt;NT merge as <see cref="BuildCounts"/>, so
    /// per-status numbers match the chart by construction. This one method backs both the parent
    /// summary table and the web taxa-grouping counts endpoint.
    /// </summary>
    /// <param name="parentFilters">Filters selecting the parent's scope (exclude-aware).</param>
    /// <param name="childRank">Rank whose column is grouped (kingdom/phylum/class/order/family/genus).</param>
    /// <param name="curatedChildren">Display keys to seed as all-zero rows (in desired order) so unlisted
    /// children still appear. Pass normalized values (UPPERCASE for kingdom..family). If null, only
    /// non-empty children appear.</param>
    /// <param name="memberToGroup">Optional normalized raw-value -&gt; display-key map that folds several
    /// classes/orders into one supergroup row (e.g. MYXINI + PETROMYZONTI -&gt; "Jawless fishes"). Values
    /// absent from the map key on their own raw value.</param>
    public Dictionary<string, IReadOnlyList<StatusCount>> BuildChildBreakdown(
        List<TaxonFilterDefinition>? parentFilters,
        string childRank,
        IReadOnlyList<string>? curatedChildren = null,
        IReadOnlyDictionary<string, string>? memberToGroup = null) {

        var column = TaxonFilterSql.ResolveColumn(childRank)
            ?? throw new ArgumentException($"Unknown child rank '{childRank}'.", nameof(childRank));

        var parameters = new List<SqliteParameter>();
        var sql = new StringBuilder();
        sql.AppendLine($"SELECT v.{column} AS childVal, v.redlistCategory, v.possiblyExtinct, v.possiblyExtinctInTheWild, COUNT(*) AS n");
        sql.AppendLine("FROM view_assessments_html_taxonomy_html v");
        sql.AppendLine($"WHERE {TaxonFilterSql.GlobalSpeciesPredicate()}");
        if (parentFilters != null) {
            for (var i = 0; i < parentFilters.Count; i++) {
                TaxonFilterSql.AppendFilter(sql, parameters, parentFilters[i], i, paramPrefix: "b");
            }
        }
        sql.AppendLine($"GROUP BY v.{column}, v.redlistCategory, v.possiblyExtinct, v.possiblyExtinctInTheWild");

        // childKey -> (chart code -> count). Ordinal compare keeps DB-uppercase keys exact.
        var acc = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);

        // Seed curated children first so zero-count children still render, in the requested order.
        if (curatedChildren != null) {
            foreach (var c in curatedChildren) {
                if (!string.IsNullOrWhiteSpace(c) && !acc.ContainsKey(c)) {
                    acc[c] = new Dictionary<string, int>(StringComparer.Ordinal);
                }
            }
        }

        using (var cmd = _connection.CreateCommand()) {
            cmd.CommandText = sql.ToString();
            foreach (var p in parameters) cmd.Parameters.Add(p);
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) {
                var rawVal = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
                var category = reader.IsDBNull(1) ? null : reader.GetString(1);
                var pe = reader.IsDBNull(2) ? null : reader.GetString(2);
                var pew = reader.IsDBNull(3) ? null : reader.GetString(3);
                var n = reader.GetInt32(4);

                var code = ResolveChartCode(category, pe, pew);
                if (code is null) continue; // categories outside the 10 chart bars (LR/nt, LR/lc, NE, ...)

                var key = memberToGroup != null && memberToGroup.TryGetValue(rawVal, out var g) ? g : rawVal;
                if (string.IsNullOrWhiteSpace(key)) continue; // NULL/blank child column: not attributable to a child

                if (!acc.TryGetValue(key, out var bucket)) {
                    bucket = new Dictionary<string, int>(StringComparer.Ordinal);
                    acc[key] = bucket;
                }
                bucket[code] = bucket.GetValueOrDefault(code) + n;
            }
        }

        var result = new Dictionary<string, IReadOnlyList<StatusCount>>(StringComparer.Ordinal);
        foreach (var (key, bucket) in acc) {
            result[key] = ChartStatusOrder.Entries
                .Select(e => new StatusCount(e.Code, e.Label, bucket.GetValueOrDefault(e.Code)))
                .ToList();
        }
        return result;
    }

    /// <summary>
    /// Map a raw (category, possiblyExtinct, possiblyExtinctInTheWild) triple to its
    /// <see cref="ChartStatusOrder"/> code, mirroring CountSpecies/CountLrCd exactly (IFNULL-&gt;'false'
    /// for the PE/PEW flags; legacy LR/cd and LR/nt fold into NT, LR/lc into LC via each entry's
    /// ExtraCategories). Returns null for categories outside the 10 bars.
    /// </summary>
    private static string? ResolveChartCode(string? category, string? pe, string? pew) {
        if (string.IsNullOrEmpty(category)) return null;
        if (category == "Lower Risk/conservation dependent") return "NT"; // MergesLrCd
        var peEff = string.IsNullOrEmpty(pe) ? "false" : pe;
        var pewEff = string.IsNullOrEmpty(pew) ? "false" : pew;
        foreach (var e in ChartStatusOrder.Entries) {
            var matchesCategory = e.DbCategory == category
                || (e.ExtraCategories?.Contains(category) ?? false);
            if (!matchesCategory) continue;
            if (e.PeFilter != null && e.PeFilter != peEff) continue;
            if (e.PewFilter != null && e.PewFilter != pewEff) continue;
            return e.Code;
        }
        return null;
    }

    private int ExecuteCount(string sql, List<SqliteParameter> parameters) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public void Dispose() => _connection.Dispose();
}
