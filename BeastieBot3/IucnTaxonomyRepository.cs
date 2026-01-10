using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class IucnTaxonomyRepository {
    private readonly SqliteConnection _connection;

    public IucnTaxonomyRepository(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public bool ObjectExists(string name, string type) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = @type AND name = @name LIMIT 1";
        command.Parameters.AddWithValue("@type", type);
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    public IEnumerable<IucnTaxonomyRow> ReadRows(long limit, CancellationToken cancellationToken) {
        var sql = @"SELECT
    v.assessmentId,
    v.taxonId,
    v.scientificName AS scientificName_assessments,
    v.scientificName_taxonomy,
    v.kingdomName,
    v.phylumName,
    v.className,
    v.orderName,
    v.familyName,
    v.genusName,
    v.speciesName,
    v.infraType,
    v.infraName,
    v.subpopulationName,
    v.authority,
    v.infraAuthority
FROM view_assessments_html_taxonomy_html v
ORDER BY v.assessmentId";

        using var command = _connection.CreateCommand();
        command.CommandText = limit > 0 ? sql + "\nLIMIT @limit" : sql;
        command.CommandTimeout = 0;
        if (limit > 0) {
            command.Parameters.AddWithValue("@limit", limit);
        }

        using var reader = command.ExecuteReader();
        var ordinals = new Ordinals(reader);

        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            yield return new IucnTaxonomyRow(
                reader.GetInt64(ordinals.AssessmentId),
                reader.GetInt64(ordinals.TaxonId),
                GetNullableString(reader, ordinals.ScientificNameAssessments),
                GetNullableString(reader, ordinals.ScientificNameTaxonomy),
                reader.GetString(ordinals.KingdomName),
                GetNullableString(reader, ordinals.PhylumName),
                GetNullableString(reader, ordinals.ClassName),
                GetNullableString(reader, ordinals.OrderName),
                GetNullableString(reader, ordinals.FamilyName),
                reader.GetString(ordinals.GenusName),
                reader.GetString(ordinals.SpeciesName),
                GetNullableString(reader, ordinals.InfraType),
                GetNullableString(reader, ordinals.InfraName),
                GetNullableString(reader, ordinals.SubpopulationName),
                GetNullableString(reader, ordinals.Authority),
                GetNullableString(reader, ordinals.InfraAuthority)
            );
        }
    }

    public IucnTaxonomyRow? GetRowByTaxonId(long? taxonId) {
        if (taxonId is null) {
            return null;
        }

        var sql = @"SELECT
    v.assessmentId,
    v.taxonId,
    v.scientificName AS scientificName_assessments,
    v.scientificName_taxonomy,
    v.kingdomName,
    v.phylumName,
    v.className,
    v.orderName,
    v.familyName,
    v.genusName,
    v.speciesName,
    v.infraType,
    v.infraName,
    v.subpopulationName,
    v.authority,
    v.infraAuthority
FROM view_assessments_html_taxonomy_html v
WHERE v.taxonId = @id
LIMIT 1";

        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;
        command.Parameters.AddWithValue("@id", taxonId.Value);

        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return null;
        }

        var ordinals = new Ordinals(reader);
        return new IucnTaxonomyRow(
            reader.GetInt64(ordinals.AssessmentId),
            reader.GetInt64(ordinals.TaxonId),
            GetNullableString(reader, ordinals.ScientificNameAssessments),
            GetNullableString(reader, ordinals.ScientificNameTaxonomy),
            reader.GetString(ordinals.KingdomName),
            GetNullableString(reader, ordinals.PhylumName),
            GetNullableString(reader, ordinals.ClassName),
            GetNullableString(reader, ordinals.OrderName),
            GetNullableString(reader, ordinals.FamilyName),
            reader.GetString(ordinals.GenusName),
            reader.GetString(ordinals.SpeciesName),
            GetNullableString(reader, ordinals.InfraType),
            GetNullableString(reader, ordinals.InfraName),
            GetNullableString(reader, ordinals.SubpopulationName),
            GetNullableString(reader, ordinals.Authority),
            GetNullableString(reader, ordinals.InfraAuthority));
    }

    private static string? GetNullableString(SqliteDataReader reader, int? ordinal) {
        if (!ordinal.HasValue) {
            return null;
        }

        return reader.IsDBNull(ordinal.Value) ? null : reader.GetString(ordinal.Value);
    }

    private sealed class Ordinals {
        public Ordinals(SqliteDataReader reader) {
            AssessmentId = reader.GetOrdinal("assessmentId");
            TaxonId = reader.GetOrdinal("taxonId");
            ScientificNameAssessments = reader.GetOrdinal("scientificName_assessments");
            ScientificNameTaxonomy = reader.GetOrdinal("scientificName_taxonomy");
            KingdomName = reader.GetOrdinal("kingdomName");
            PhylumName = GetOptionalOrdinal(reader, "phylumName");
            ClassName = GetOptionalOrdinal(reader, "className");
            OrderName = GetOptionalOrdinal(reader, "orderName");
            FamilyName = GetOptionalOrdinal(reader, "familyName");
            GenusName = reader.GetOrdinal("genusName");
            SpeciesName = reader.GetOrdinal("speciesName");
            InfraType = reader.GetOrdinal("infraType");
            InfraName = reader.GetOrdinal("infraName");
            SubpopulationName = reader.GetOrdinal("subpopulationName");
            Authority = reader.GetOrdinal("authority");
            InfraAuthority = reader.GetOrdinal("infraAuthority");
        }

        public int AssessmentId { get; }
        public int TaxonId { get; }
        public int ScientificNameAssessments { get; }
        public int ScientificNameTaxonomy { get; }
        public int KingdomName { get; }
        public int? PhylumName { get; }
        public int? ClassName { get; }
        public int? OrderName { get; }
        public int? FamilyName { get; }
        public int GenusName { get; }
        public int SpeciesName { get; }
        public int InfraType { get; }
        public int InfraName { get; }
        public int SubpopulationName { get; }
        public int Authority { get; }
        public int InfraAuthority { get; }
    }

    private static int? GetOptionalOrdinal(SqliteDataReader reader, string columnName) {
        try {
            return reader.GetOrdinal(columnName);
        } catch (IndexOutOfRangeException) {
            return null;
        }
    }
}

internal sealed record IucnTaxonomyRow(
    long AssessmentId,
    long TaxonId,
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
    string? InfraAuthority
);