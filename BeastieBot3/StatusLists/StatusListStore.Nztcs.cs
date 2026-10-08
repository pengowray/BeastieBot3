using Microsoft.Data.Sqlite;

// The status lists store's New Zealand Threat Classification System table (`statuses nztcs-import`).

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountNztcs() => Scalar("SELECT COUNT(*) FROM nztcs_assessment");

    /// Replaces every NZTCS assessment, and the NZTCS source row, in one transaction.
    public void ReplaceNztcs(IReadOnlyList<NztcsAssessment> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM nztcs_assessment");
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO nztcs_assessment(assessment_id, species_id, scientific_name, assessment_name, common_name, category, status,
                criteria, qualifiers, report_id, report_name, report_year, imported_at)
            VALUES (@assessment, @species, @scientific, @name, @common, @category, @status, @criteria, @qualifiers, @report, @report_name,
                @report_year, @imported)
            ON CONFLICT(assessment_id) DO NOTHING
            """;
        var names = new[] { "@assessment", "@species", "@scientific", "@name", "@common", "@category", "@status", "@criteria", "@qualifiers",
            "@report", "@report_name", "@report_year" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
        static object Value(object? v) => v ?? DBNull.Value;
        foreach (var row in rows) {
            p["@assessment"].Value = row.AssessmentId;
            p["@species"].Value = row.SpeciesId;
            p["@scientific"].Value = Value(row.ScientificName);
            p["@name"].Value = row.AssessmentName;
            p["@common"].Value = Value(row.CommonName);
            p["@category"].Value = Value(row.Category);
            p["@status"].Value = Value(row.Status);
            p["@criteria"].Value = Value(row.Criteria);
            p["@qualifiers"].Value = Value(row.Qualifiers);
            p["@report"].Value = Value(row.ReportId);
            p["@report_name"].Value = Value(row.ReportName);
            p["@report_year"].Value = Value(row.ReportYear);
            insert.ExecuteNonQuery();
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
