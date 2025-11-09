using System;
using System.Collections.Generic;
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
    v.internalTaxonId,
    v.redlist_version,
    v.scientificName AS scientificName_assessments,
    v.""scientificName:1"" AS scientificName_taxonomy,
    v.genusName,
    v.speciesName,
    v.infraType,
    v.infraName,
    v.subpopulationName,
    v.kingdomName,
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
                reader.GetString(ordinals.AssessmentId),
                reader.GetString(ordinals.InternalTaxonId),
                reader.GetString(ordinals.RedlistVersion),
                GetNullableString(reader, ordinals.ScientificNameAssessments),
                GetNullableString(reader, ordinals.ScientificNameTaxonomy),
                reader.GetString(ordinals.GenusName),
                reader.GetString(ordinals.SpeciesName),
                GetNullableString(reader, ordinals.InfraType),
                GetNullableString(reader, ordinals.InfraName),
                GetNullableString(reader, ordinals.SubpopulationName),
                reader.GetString(ordinals.KingdomName),
                GetNullableString(reader, ordinals.Authority),
                GetNullableString(reader, ordinals.InfraAuthority)
            );
        }
    }

    private static string? GetNullableString(SqliteDataReader reader, int ordinal) {
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private sealed class Ordinals {
        public Ordinals(SqliteDataReader reader) {
            AssessmentId = reader.GetOrdinal("assessmentId");
            InternalTaxonId = reader.GetOrdinal("internalTaxonId");
            RedlistVersion = reader.GetOrdinal("redlist_version");
            ScientificNameAssessments = reader.GetOrdinal("scientificName_assessments");
            ScientificNameTaxonomy = reader.GetOrdinal("scientificName_taxonomy");
            GenusName = reader.GetOrdinal("genusName");
            SpeciesName = reader.GetOrdinal("speciesName");
            InfraType = reader.GetOrdinal("infraType");
            InfraName = reader.GetOrdinal("infraName");
            SubpopulationName = reader.GetOrdinal("subpopulationName");
            KingdomName = reader.GetOrdinal("kingdomName");
            Authority = reader.GetOrdinal("authority");
            InfraAuthority = reader.GetOrdinal("infraAuthority");
        }

        public int AssessmentId { get; }
        public int InternalTaxonId { get; }
        public int RedlistVersion { get; }
        public int ScientificNameAssessments { get; }
        public int ScientificNameTaxonomy { get; }
        public int GenusName { get; }
        public int SpeciesName { get; }
        public int InfraType { get; }
        public int InfraName { get; }
        public int SubpopulationName { get; }
        public int KingdomName { get; }
        public int Authority { get; }
        public int InfraAuthority { get; }
    }
}

internal sealed record IucnTaxonomyRow(
    string AssessmentId,
    string InternalTaxonId,
    string RedlistVersion,
    string? ScientificNameAssessments,
    string? ScientificNameTaxonomy,
    string GenusName,
    string SpeciesName,
    string? InfraType,
    string? InfraName,
    string? SubpopulationName,
    string KingdomName,
    string? Authority,
    string? InfraAuthority
);