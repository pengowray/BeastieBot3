using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Data.Sqlite;

// Enumerates SIS (Species Information Service) IDs from CSV-imported IUCN
// database (taxonomy table). Returns IDs in ascending order, supporting
// optional limit for testing. Used by IucnApiCacheTaxaCommand to determine
// which taxa to fetch from API. Yields lazily to handle ~200K species.

namespace BeastieBot3.Iucn;

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
            var sisId = reader.GetInt64(0);
            yield return sisId;
        }
    }

    /// <summary>
    /// Infraspecific (subspecies/variety) SIS ids from the CSV, in ascending order. Used to seed
    /// <c>cache-infraranks --from-csv</c> so it can reach assessed subspecies whose parent species is
    /// unassessed — those never surface via the API's species→infrarank_taxa path, and the CSV's
    /// taxonId is the only place they're enumerated.
    /// </summary>
    public IEnumerable<long> ReadInfraspecificSisIds(long? limit, CancellationToken cancellationToken) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = _databasePath,
            Mode = SqliteOpenMode.ReadOnly
        };

        using var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        var sql = BuildInfraspecificSql(connection);
        using var command = connection.CreateCommand();
        command.CommandText = limit.HasValue ? sql + " LIMIT @limit" : sql;
        if (limit.HasValue) {
            command.Parameters.AddWithValue("@limit", limit.Value);
        }

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            yield return reader.GetInt64(0);
        }
    }

    private static string BuildSql(SqliteConnection connection) {
        var table = ObjectExists(connection, "view_assessments_html_taxonomy_html")
            ? "view_assessments_html_taxonomy_html"
            : "taxonomy";
        return $@"SELECT DISTINCT taxonId
FROM {table}
WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '')
  AND (infraType IS NULL OR TRIM(infraType) = '')
ORDER BY taxonId";
    }

    private static string BuildInfraspecificSql(SqliteConnection connection) {
        var table = ObjectExists(connection, "view_assessments_html_taxonomy_html")
            ? "view_assessments_html_taxonomy_html"
            : "taxonomy";
        return $@"SELECT DISTINCT taxonId
FROM {table}
WHERE infraType IS NOT NULL AND TRIM(infraType) <> ''
ORDER BY taxonId";
    }

    private static bool ObjectExists(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }
}
