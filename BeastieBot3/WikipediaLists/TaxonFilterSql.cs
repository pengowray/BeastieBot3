using System.Collections.Generic;
using System.Text;
using Microsoft.Data.Sqlite;

// Single source of truth for turning a TaxonFilterDefinition into a SQL WHERE fragment.
// Shared by IucnListQueryService (list body + counts) and IucnChartDataBuilder (chart/summary
// counts) so include/exclude/System semantics can never drift between the three call sites.
//
// Sargability (CLAUDE.md rule): every clause is an exact-match equality or NOT IN on an indexed,
// pre-normalized column value — never LOWER()/UPPER()/LIKE on the column. kingdom..family values
// are upper-cased to match the denormalized UPPERCASE columns before binding.

namespace BeastieBot3.WikipediaLists;

internal static class TaxonFilterSql {
    /// <summary>Map a YAML rank name to its denormalized column on the taxonomy view.</summary>
    public static string? ResolveColumn(string? rank) => rank?.Trim().ToLowerInvariant() switch {
        "kingdom" => "kingdomName",
        "phylum" => "phylumName",
        "class" => "className",
        "order" => "orderName",
        "family" => "familyName",
        "genus" => "genusName",
        _ => null
    };

    /// <summary>
    /// Normalize a filter value to match the column's stored casing. kingdom..family are stored
    /// UPPERCASE; genus/species mixed-case. Returns null for blank input.
    /// </summary>
    public static string? NormalizeValue(string? rank, string? value) {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return rank?.Trim().ToLowerInvariant() switch {
            "kingdom" or "phylum" or "class" or "order" or "family" => value.Trim().ToUpperInvariant(),
            _ => value.Trim()
        };
    }

    /// <summary>
    /// Append the WHERE fragment(s) for one filter: a System LIKE, or a rank include
    /// (single Value or OR-of-Values) and/or a NULL-safe Exclude (NOT IN). Each emitted line is
    /// prefixed with "  AND ". <paramref name="paramPrefix"/> keeps parameter names unique within a
    /// single command when the same loop runs more than once (e.g. "c" for the count query).
    /// </summary>
    public static void AppendFilter(
        StringBuilder sql,
        List<SqliteParameter> parameters,
        TaxonFilterDefinition filter,
        int index,
        string paramPrefix = "",
        string alias = "v") {

        // System tag (mutually exclusive with rank); LIKE on the systems field.
        if (!string.IsNullOrWhiteSpace(filter.System)) {
            var p = new SqliteParameter($"@{paramPrefix}sys_{index}", $"%{filter.System}%");
            sql.AppendLine($"  AND {alias}.systems LIKE {p.ParameterName}");
            parameters.Add(p);
            return;
        }

        var column = ResolveColumn(filter.Rank);
        if (column is null) return;

        // Include: OR-of-Values takes precedence over single Value.
        if (filter.Values is { Count: > 0 }) {
            var orClauses = new List<string>();
            for (var j = 0; j < filter.Values.Count; j++) {
                var val = NormalizeValue(filter.Rank, filter.Values[j]);
                if (string.IsNullOrWhiteSpace(val)) continue;
                var p = new SqliteParameter($"@{paramPrefix}f_{column}_{index}_{j}", val);
                orClauses.Add($"{alias}.{column} = {p.ParameterName}");
                parameters.Add(p);
            }
            if (orClauses.Count > 0)
                sql.AppendLine($"  AND ({string.Join(" OR ", orClauses)})");
        }
        else {
            var val = NormalizeValue(filter.Rank, filter.Value);
            if (!string.IsNullOrWhiteSpace(val)) {
                var p = new SqliteParameter($"@{paramPrefix}f_{column}_{index}", val);
                sql.AppendLine($"  AND {alias}.{column} = {p.ParameterName}");
                parameters.Add(p);
            }
        }

        // Exclude (NULL-safe): keep NULL-column rows, drop the listed values.
        if (filter.Exclude is { Count: > 0 }) {
            var inParams = new List<string>();
            for (var j = 0; j < filter.Exclude.Count; j++) {
                var val = NormalizeValue(filter.Rank, filter.Exclude[j]);
                if (string.IsNullOrWhiteSpace(val)) continue;
                var p = new SqliteParameter($"@{paramPrefix}x_{column}_{index}_{j}", val);
                inParams.Add(p.ParameterName);
                parameters.Add(p);
            }
            if (inParams.Count > 0)
                sql.AppendLine($"  AND ({alias}.{column} IS NULL OR {alias}.{column} NOT IN ({string.Join(", ", inParams)}))");
        }
    }
}
