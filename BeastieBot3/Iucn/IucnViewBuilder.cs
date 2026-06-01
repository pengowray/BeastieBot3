using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

// Single definition of the `view_assessments_html_taxonomy_html` join view, shared
// by the CSV importer (IucnImporter) and the API-cache projection
// (IucnApiProjectionStore). Keeping one implementation guarantees the column set
// and the collision-aliasing (e.g. taxonomy.scientificName -> scientificName_taxonomy)
// can never drift between the two datasets — list/chart generation reads this view
// by name and must see identical columns regardless of which dataset produced it.

namespace BeastieBot3.Iucn;

internal static class IucnViewBuilder {
    /// <summary>
    /// Recreate <paramref name="viewName"/> as the assessments-LEFT-JOIN-taxonomy view:
    /// every assessment column projected as a.&lt;col&gt;, every taxonomy column projected as
    /// t.&lt;col&gt; (aliased &lt;col&gt;_taxonomy when it collides with an assessment column),
    /// dropping taxonomy's taxonId/import_id, joined on t.taxonId = a.taxonId.
    /// </summary>
    public static void RecreateJoinView(SqliteConnection connection, string viewName, string assessmentsTable, string taxonomyTable) {
        var selectStatement = BuildViewSelect(connection, assessmentsTable, taxonomyTable);

        using (var drop = connection.CreateCommand()) {
            drop.CommandText = $"DROP VIEW IF EXISTS {QuoteIdentifier(viewName)};";
            drop.ExecuteNonQuery();
        }
        using (var create = connection.CreateCommand()) {
            create.CommandText = $"CREATE VIEW {QuoteIdentifier(viewName)} AS {selectStatement};";
            create.ExecuteNonQuery();
        }
    }

    private static string BuildViewSelect(SqliteConnection connection, string assessmentsTable, string taxonomyTable) {
        var assessmentCols = GetTableColumns(connection, assessmentsTable);
        var taxonomyCols = GetTableColumns(connection, taxonomyTable);
        var assessmentSet = new HashSet<string>(assessmentCols, StringComparer.OrdinalIgnoreCase);

        var selectParts = new List<string>();
        foreach (var col in assessmentCols) {
            selectParts.Add($"a.{QuoteIdentifier(col)}");
        }
        foreach (var col in taxonomyCols) {
            if (col.Equals("taxonId", StringComparison.OrdinalIgnoreCase) ||
                col.Equals("import_id", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }
            var alias = assessmentSet.Contains(col) ? $"{col}_taxonomy" : col;
            selectParts.Add($"t.{QuoteIdentifier(col)} AS {QuoteIdentifier(alias)}");
        }

        return $"SELECT {string.Join(", ", selectParts)} FROM {QuoteIdentifier(assessmentsTable)} AS a " +
               $"LEFT JOIN {QuoteIdentifier(taxonomyTable)} AS t ON t.taxonId = a.taxonId";
    }

    private static List<string> GetTableColumns(SqliteConnection connection, string tableName) {
        var columns = new List<string>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
        using var reader = cmd.ExecuteReader();
        var nameOrdinal = reader.GetOrdinal("name");
        while (reader.Read()) {
            columns.Add(reader.GetString(nameOrdinal));
        }
        return columns;
    }

    private static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"") + "\"";
}
