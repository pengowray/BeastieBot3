using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.WikipediaLists;

internal sealed class IucnListQueryService : IDisposable {
    private readonly SqliteConnection _connection;

    public IucnListQueryService(string databasePath) {
        if (string.IsNullOrWhiteSpace(databasePath)) {
            throw new ArgumentException("Database path was not provided.", nameof(databasePath));
        }

        var fullPath = Path.GetFullPath(databasePath);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"IUCN SQLite database not found: {fullPath}", fullPath);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadOnly
        };

        _connection = new SqliteConnection(builder.ToString());
        _connection.Open();
    }

    public IReadOnlyList<IucnSpeciesRecord> QuerySpecies(
        WikipediaListDefinition definition,
        IReadOnlyCollection<RedlistStatusDescriptor> statuses,
        int? limit = null) {
        if (statuses is null || statuses.Count == 0) {
            return Array.Empty<IucnSpeciesRecord>();
        }

        var sql = BuildSql(definition, statuses, limit);
        using var command = _connection.CreateCommand();
        command.CommandText = sql.Text;
        foreach (var parameter in sql.Parameters) {
            command.Parameters.Add(parameter);
        }

        using var reader = command.ExecuteReader();
        var results = new List<IucnSpeciesRecord>();
        while (reader.Read()) {
            results.Add(ReadRecord(reader));
        }

        return results;
    }

    private (string Text, List<SqliteParameter> Parameters) BuildSql(WikipediaListDefinition definition, IReadOnlyCollection<RedlistStatusDescriptor> statuses, int? limit) {
        var parameters = new List<SqliteParameter>();
        var builder = new StringBuilder();
        builder.AppendLine("SELECT");
        builder.AppendLine("    v.taxonId,");
        builder.AppendLine("    v.assessmentId,");
        builder.AppendLine("    v.redlistCategory,");
        builder.AppendLine("    v.possiblyExtinct,");
        builder.AppendLine("    v.possiblyExtinctInTheWild,");
        builder.AppendLine("    v.scientificName AS scientificName_assessments,");
        builder.AppendLine("    v.scientificName_taxonomy,");
        builder.AppendLine("    v.kingdomName,");
        builder.AppendLine("    v.phylumName,");
        builder.AppendLine("    v.className,");
        builder.AppendLine("    v.orderName,");
        builder.AppendLine("    v.familyName,");
        builder.AppendLine("    v.genusName,");
        builder.AppendLine("    v.speciesName,");
        builder.AppendLine("    v.infraType,");
        builder.AppendLine("    v.infraName,");
        builder.AppendLine("    v.subpopulationName,");
        builder.AppendLine("    v.authority,");
        builder.AppendLine("    v.infraAuthority,");
        builder.AppendLine("    v.yearPublished");
        builder.AppendLine("FROM view_assessments_html_taxonomy_html v");
        builder.AppendLine("WHERE");
        AppendStatusClauses(builder, statuses, parameters);

        for (var i = 0; i < definition.Filters.Count; i++) {
            var filter = definition.Filters[i];
            var column = ResolveColumn(filter.Rank);
            if (column is null) {
                continue;
            }

            var normalizedValue = NormalizeFilterValue(filter.Rank, filter.Value);
            if (string.IsNullOrWhiteSpace(normalizedValue)) {
                continue;
            }

            var parameter = new SqliteParameter($"@f_{column}_{i}", normalizedValue);
            builder.AppendLine($"  AND v.{column} = {parameter.ParameterName}");
            parameters.Add(parameter);
        }

        builder.AppendLine("ORDER BY v.orderName, v.familyName, v.genusName, v.speciesName");
        if (limit.HasValue && limit.Value > 0) {
            builder.AppendLine($"LIMIT {limit.Value}");
        }

        return (builder.ToString(), parameters);
    }

    private static void AppendStatusClauses(StringBuilder builder, IReadOnlyCollection<RedlistStatusDescriptor> statuses, List<SqliteParameter> parameters) {
        var index = 0;
        foreach (var descriptor in statuses) {
            var prefix = index == 0 ? "    (" : "    OR (";
            builder.Append(prefix);
            var categoryParam = new SqliteParameter($"@status{index}", descriptor.Category);
            builder.Append($"v.redlistCategory = {categoryParam.ParameterName}");
            parameters.Add(categoryParam);

            if (descriptor.PossiblyExtinctFilter != TriStateFilter.Any) {
                var value = descriptor.PossiblyExtinctFilter == TriStateFilter.True ? "true" : "false";
                var param = new SqliteParameter($"@pe{index}", value);
                builder.Append($" AND IFNULL(v.possiblyExtinct, 'false') = {param.ParameterName}");
                parameters.Add(param);
            }

            if (descriptor.PossiblyExtinctInTheWildFilter != TriStateFilter.Any) {
                var value = descriptor.PossiblyExtinctInTheWildFilter == TriStateFilter.True ? "true" : "false";
                var param = new SqliteParameter($"@pew{index}", value);
                builder.Append($" AND IFNULL(v.possiblyExtinctInTheWild, 'false') = {param.ParameterName}");
                parameters.Add(param);
            }

            builder.Append(')');
            builder.AppendLine();
            index++;
        }
    }

    private static string? ResolveColumn(string rank) => rank?.Trim().ToLowerInvariant() switch {
        "kingdom" => "kingdomName",
        "phylum" => "phylumName",
        "class" => "className",
        "order" => "orderName",
        "family" => "familyName",
        "genus" => "genusName",
        _ => null
    };

    private static string? NormalizeFilterValue(string rank, string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        return rank?.Trim().ToLowerInvariant() switch {
            "kingdom" or "phylum" or "class" or "order" or "family" => value.Trim().ToUpperInvariant(),
            _ => value.Trim()
        };
    }

    private static IucnSpeciesRecord ReadRecord(SqliteDataReader reader) {
        var redlistCategory = reader.GetString(reader.GetOrdinal("redlistCategory"));
        var possiblyExtinct = GetStringOrNull(reader, "possiblyExtinct");
        var possiblyExtinctInTheWild = GetStringOrNull(reader, "possiblyExtinctInTheWild");
        var descriptor = IucnRedlistStatus.ResolveFromDatabase(redlistCategory, possiblyExtinct, possiblyExtinctInTheWild);

        return new IucnSpeciesRecord(
            reader.GetInt64(reader.GetOrdinal("taxonId")),
            reader.GetInt64(reader.GetOrdinal("assessmentId")),
            redlistCategory,
            descriptor.Code,
            GetStringOrNull(reader, "scientificName_assessments"),
            GetStringOrNull(reader, "scientificName_taxonomy"),
            reader.GetString(reader.GetOrdinal("kingdomName")),
            GetStringOrNull(reader, "phylumName"),
            GetStringOrNull(reader, "className"),
            GetStringOrNull(reader, "orderName"),
            GetStringOrNull(reader, "familyName"),
            reader.GetString(reader.GetOrdinal("genusName")),
            reader.GetString(reader.GetOrdinal("speciesName")),
            GetStringOrNull(reader, "infraType"),
            GetStringOrNull(reader, "infraName"),
            GetStringOrNull(reader, "subpopulationName"),
            GetStringOrNull(reader, "authority"),
            GetStringOrNull(reader, "infraAuthority"),
            possiblyExtinct,
            possiblyExtinctInTheWild,
            GetStringOrNull(reader, "yearPublished")
        );
    }

    private static string? GetStringOrNull(SqliteDataReader reader, string column) {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    public void Dispose() {
        _connection.Dispose();
    }
}

internal sealed record IucnSpeciesRecord(
    long TaxonId,
    long AssessmentId,
    string RedlistCategory,
    string StatusCode,
    string? ScientificNameAssessments,
    string? ScientificNameTaxonomy,
    string KingdomName,
    string? PhylumName,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string GenusName,
    string SpeciesName,
    string? InfraType,
    string? InfraName,
    string? SubpopulationName,
    string? Authority,
    string? InfraAuthority,
    string? PossiblyExtinct,
    string? PossiblyExtinctInTheWild,
    string? YearPublished
);
