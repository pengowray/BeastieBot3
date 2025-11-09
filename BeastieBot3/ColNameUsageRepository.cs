using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class ColNameUsageRepository {
    private readonly SqliteConnection _connection;
    private readonly string _selectColumns;

    public bool SupportsAcceptedNameLookup { get; }
    public bool UsesParentIdFallback => !SupportsAcceptedNameLookup && _selectColumns.Contains("parentID", StringComparison.OrdinalIgnoreCase);

    public ColNameUsageRepository(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        var selectInfo = DetermineSelectColumns();
        _selectColumns = selectInfo.Columns;
        SupportsAcceptedNameLookup = selectInfo.SupportsAcceptedNameLookup;
    }

    public bool ObjectExists(string name, string type) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = @type AND name = @name LIMIT 1";
        command.Parameters.AddWithValue("@type", type);
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    public IReadOnlyList<ColNameUsageEntry> FindByScientificName(string? scientificName, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return Array.Empty<ColNameUsageEntry>();
        }

    using var command = _connection.CreateCommand();
    command.CommandText = $@"SELECT {_selectColumns}
FROM nameusage
WHERE scientificName IS NOT NULL
  AND LOWER(scientificName) = @name
  AND (rank IS NULL OR LOWER(rank) = 'species');";
        command.CommandTimeout = 0;
        command.Parameters.AddWithValue("@name", scientificName.ToLowerInvariant());

        return ReadEntries(command, cancellationToken);
    }

    public IReadOnlyList<ColNameUsageEntry> FindByGenusSpecies(string? genus, string? species, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return Array.Empty<ColNameUsageEntry>();
        }

    using var command = _connection.CreateCommand();
    command.CommandText = $@"SELECT {_selectColumns}
FROM nameusage
WHERE genus IS NOT NULL
  AND specificEpithet IS NOT NULL
  AND LOWER(genus) = @genus
  AND LOWER(specificEpithet) = @species
  AND (rank IS NULL OR LOWER(rank) = 'species');";
        command.CommandTimeout = 0;
        command.Parameters.AddWithValue("@genus", genus.ToLowerInvariant());
        command.Parameters.AddWithValue("@species", species.ToLowerInvariant());

        return ReadEntries(command, cancellationToken);
    }

    public ColNameUsageEntry? GetById(string? id, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(id)) {
            return null;
        }

        using var command = _connection.CreateCommand();
    command.CommandText = $@"SELECT {_selectColumns}
FROM nameusage
WHERE id = @id
LIMIT 1;";
        command.CommandTimeout = 0;
        command.Parameters.AddWithValue("@id", id);

        var entries = ReadEntries(command, cancellationToken);
        return entries.Count > 0 ? entries[0] : null;
    }

    private static List<ColNameUsageEntry> ReadEntries(SqliteCommand command, CancellationToken cancellationToken) {
        var results = new List<ColNameUsageEntry>();
        using var reader = command.ExecuteReader();
        if (!reader.HasRows) {
            return results;
        }

        var ordinals = new ColumnOrdinals(reader);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            if (reader.IsDBNull(ordinals.Id)) {
                continue;
            }

            var scientificName = GetNullableString(reader, ordinals.ScientificName);
            if (string.IsNullOrEmpty(scientificName)) {
                continue;
            }

            results.Add(new ColNameUsageEntry(
                reader.GetString(ordinals.Id),
                scientificName,
                GetNullableString(reader, ordinals.Authorship),
                GetNullableString(reader, ordinals.Status),
                GetNullableString(reader, ordinals.Rank),
                GetNullableString(reader, ordinals.AcceptedNameUsageId)
            ));
        }

        return results;
    }

    private (string Columns, bool SupportsAcceptedNameLookup) DetermineSelectColumns() {
        const string baseColumns = "id, scientificName, authorship, status, rank";
        var columnNames = GetColumnNames();

        foreach (var candidate in new[] { "acceptedNameUsageID", "acceptedNameUsageId", "acceptedNameUsage" }) {
            if (columnNames.Contains(candidate)) {
                return ($"{baseColumns}, {candidate} AS acceptedNameUsageID", true);
            }
        }

        if (columnNames.Contains("parentID")) {
            return ($"{baseColumns}, parentID AS acceptedNameUsageID", false);
        }

        return ($"{baseColumns}, NULL AS acceptedNameUsageID", false);
    }

    private HashSet<string> GetColumnNames() {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var command = _connection.CreateCommand();
        command.CommandText = "PRAGMA table_info(nameusage);";
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (reader.IsDBNull(1)) {
                continue;
            }

            var name = reader.GetString(1);
            if (!string.IsNullOrWhiteSpace(name)) {
                result.Add(name);
            }
        }

        return result;
    }

    private sealed class ColumnOrdinals {
        public ColumnOrdinals(SqliteDataReader reader) {
            Id = reader.GetOrdinal("id");
            ScientificName = reader.GetOrdinal("scientificName");
            Authorship = GetOptionalOrdinal(reader, "authorship");
            Status = GetOptionalOrdinal(reader, "status");
            Rank = GetOptionalOrdinal(reader, "rank");
            AcceptedNameUsageId = GetOptionalOrdinal(reader, "acceptedNameUsageID");
        }

        public int Id { get; }
        public int ScientificName { get; }
        public int? Authorship { get; }
        public int? Status { get; }
        public int? Rank { get; }
        public int? AcceptedNameUsageId { get; }
    }

    private static int? GetOptionalOrdinal(SqliteDataReader reader, string columnName) {
        try {
            return reader.GetOrdinal(columnName);
        } catch (IndexOutOfRangeException) {
            return null;
        }
    }

    private static string? GetNullableString(SqliteDataReader reader, int? ordinal) {
        if (!ordinal.HasValue) {
            return null;
        }

        return reader.IsDBNull(ordinal.Value) ? null : reader.GetString(ordinal.Value);
    }
}

internal sealed record ColNameUsageEntry(
    string Id,
    string ScientificName,
    string? Authorship,
    string? Status,
    string? Rank,
    string? AcceptedNameUsageId
);
