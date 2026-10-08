// The status lists store's SALVE table (`statuses salve-import`).

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountSalve() => Scalar("SELECT COUNT(*) FROM salve_assessment");

    /// Replaces every SALVE assessment, and the SALVE source row, in one transaction.
    public void ReplaceSalve(IReadOnlyList<SalveAssessment> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM salve_assessment");
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO salve_assessment(ficha_id, scientific_name, authority, common_name, taxon_group, category, possibly_extinct,
                criteria, assessed_on, doi, taxon_level, published, imported_at)
            VALUES (@id, @name, @authority, @common, @group, @category, @pe, @criteria, @assessed, @doi, @level, @published, @imported)
            ON CONFLICT(ficha_id) DO NOTHING
            """;
        var names = new[] { "@id", "@name", "@authority", "@common", "@group", "@category", "@pe", "@criteria", "@assessed", "@doi", "@level",
            "@published" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
        static object Value(object? v) => v ?? DBNull.Value;
        foreach (var row in rows) {
            p["@id"].Value = row.FichaId;
            p["@name"].Value = row.ScientificName;
            p["@authority"].Value = Value(row.Authority);
            p["@common"].Value = Value(row.CommonName);
            p["@group"].Value = Value(row.Group);
            p["@category"].Value = row.Category;
            p["@pe"].Value = row.PossiblyExtinct ? 1 : 0;
            p["@criteria"].Value = Value(row.Criteria);
            p["@assessed"].Value = Value(row.AssessedOn);
            p["@doi"].Value = Value(row.Doi);
            p["@level"].Value = Value(row.TaxonLevel);
            p["@published"].Value = row.Published ? 1 : 0;
            insert.ExecuteNonQuery();
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
