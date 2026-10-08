// The status lists store's JNCC table (`statuses jncc-import`): jncc_designation.

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountJncc() => Scalar("SELECT COUNT(*) FROM jncc_designation");

    /// Replaces every JNCC designation, and the JNCC source row, in one transaction.
    public void ReplaceJncc(IReadOnlyList<JnccDesignation> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM jncc_designation");
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO jncc_designation(row_number, taxon_version_key, scientific_name, authority, qualifier, rank, designated_name,
                common_name, category, taxon_group, kingdom, reporting_category, sort_code, designation, designation_code, status_code,
                population, iucn_version, scope, area, source, source_url, designated_on, imported_at)
            VALUES (@row, @key, @name, @authority, @qualifier, @rank, @designated, @common, @category, @group, @kingdom, @reporting,
                @sort, @designation, @code, @status, @population, @iucn, @scope, @area, @source, @url, @date, @imported)
            ON CONFLICT(row_number) DO NOTHING
            """;
        var names = new[] { "@row", "@key", "@name", "@authority", "@qualifier", "@rank", "@designated", "@common", "@category", "@group",
            "@kingdom", "@reporting", "@sort", "@designation", "@code", "@status", "@population", "@iucn", "@scope", "@area", "@source",
            "@url", "@date" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
        static object Value(object? v) => v ?? DBNull.Value;
        foreach (var row in rows) {
            p["@row"].Value = row.RowNumber;
            p["@key"].Value = row.TaxonVersionKey;
            p["@name"].Value = row.ScientificName;
            p["@authority"].Value = Value(row.Authority);
            p["@qualifier"].Value = Value(row.Qualifier);
            p["@rank"].Value = Value(row.Rank);
            p["@designated"].Value = Value(row.DesignatedName);
            p["@common"].Value = Value(row.CommonName);
            p["@category"].Value = Value(row.Category);
            p["@group"].Value = Value(row.TaxonGroup);
            p["@kingdom"].Value = Value(row.Kingdom);
            p["@reporting"].Value = row.ReportingCategory;
            p["@sort"].Value = Value(row.SortCode);
            p["@designation"].Value = row.Designation;
            p["@code"].Value = row.DesignationCode;
            p["@status"].Value = Value(row.StatusCode);
            p["@population"].Value = Value(row.Population);
            p["@iucn"].Value = Value(row.IucnVersion);
            p["@scope"].Value = Value(row.Scope);
            p["@area"].Value = Value(row.Area);
            p["@source"].Value = Value(row.Source);
            p["@url"].Value = Value(row.SourceUrl);
            p["@date"].Value = Value(row.DesignatedOn);
            insert.ExecuteNonQuery();
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
