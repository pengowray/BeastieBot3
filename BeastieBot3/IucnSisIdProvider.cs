using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class IucnSisIdProvider {
    private readonly string _databasePath;

    public IucnSisIdProvider(string databasePath) {
        _databasePath = databasePath ?? throw new ArgumentNullException(nameof(databasePath));
    }

    public IEnumerable<long> ReadSpeciesSisIds(long? limit, CancellationToken cancellationToken) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = _databasePath,
            Mode = SqliteOpenMode.ReadOnly
        };

        using var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        var sql = BuildSql(connection);
        using var command = connection.CreateCommand();
        command.CommandText = limit.HasValue ? sql + " LIMIT @limit" : sql;
        if (limit.HasValue) {
            command.Parameters.AddWithValue("@limit", limit.Value);
        }

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var internalTaxonId = reader.GetString(0);
            if (long.TryParse(internalTaxonId, out var sisId)) {
                yield return sisId;
            }
        }
    }

    private static string BuildSql(SqliteConnection connection) {
        if (ObjectExists(connection, "view_assessments_html_taxonomy_html")) {
            return @"SELECT DISTINCT internalTaxonId
FROM view_assessments_html_taxonomy_html
WHERE internalTaxonId IS NOT NULL
  AND TRIM(internalTaxonId) <> ''
  AND (subpopulationName IS NULL OR TRIM(subpopulationName) = '')
  AND (infraType IS NULL OR TRIM(infraType) = '')
ORDER BY CAST(internalTaxonId AS INTEGER)";
        }

        return @"SELECT DISTINCT internalTaxonId
FROM taxonomy
WHERE internalTaxonId IS NOT NULL
  AND TRIM(internalTaxonId) <> ''
  AND (subpopulationName IS NULL OR TRIM(subpopulationName) = '')
  AND (infraType IS NULL OR TRIM(infraType) = '')
ORDER BY CAST(internalTaxonId AS INTEGER)";
    }

    private static bool ObjectExists(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }
}
