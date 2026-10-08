using Microsoft.Data.Sqlite;

// The status lists store's ECOS tables (`statuses ecos-import`): ecos_listing and ecos_name.

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountEcos() => Scalar("SELECT COUNT(*) FROM ecos_listing");

    /// Replaces every ECOS listing and name, and the ECOS source row, in one transaction.
    public void ReplaceEcos(IReadOnlyList<EcosListing> rows, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM ecos_name");
        Execute(tx, "DELETE FROM ecos_listing");
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO ecos_listing(entity_id, species_id, scientific_name_raw, scientific_name, name_note, common_name, status,
                entity_description, listing_date, is_dps, is_foreign, range_country, species_group, itis_tsn, kingdom, family, url, imported_at)
            VALUES (@entity, @species, @raw, @name, @note, @common, @status, @description, @date, @dps, @foreign, @country, @group,
                @tsn, @kingdom, @family, @url, @imported)
            """;
        var names = new[] { "@entity", "@species", "@raw", "@name", "@note", "@common", "@status", "@description", "@date", "@dps",
            "@foreign", "@country", "@group", "@tsn", "@kingdom", "@family", "@url" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));

        using var insertName = _connection.CreateCommand();
        insertName.Transaction = tx;
        insertName.CommandText = "INSERT INTO ecos_name(entity_id, name) VALUES (@entity, @name) ON CONFLICT DO NOTHING";
        var nameEntity = insertName.Parameters.Add("@entity", SqliteType.Integer);
        var nameValue = insertName.Parameters.Add("@name", SqliteType.Text);

        static object Value(object? v) => v ?? DBNull.Value;
        static object Flag(bool? b) => b is { } v ? (v ? 1 : 0) : DBNull.Value;
        foreach (var row in rows) {
            p["@entity"].Value = row.EntityId;
            p["@species"].Value = Value(row.SpeciesId);
            p["@raw"].Value = row.ScientificNameRaw;
            p["@name"].Value = row.ScientificName;
            p["@note"].Value = Value(row.NameNote);
            p["@common"].Value = Value(row.CommonName);
            p["@status"].Value = row.Status;
            p["@description"].Value = Value(row.EntityDescription);
            p["@date"].Value = Value(row.ListingDate);
            p["@dps"].Value = Flag(row.IsDps);
            p["@foreign"].Value = Flag(row.IsForeign);
            p["@country"].Value = Value(row.RangeCountry);
            p["@group"].Value = Value(row.SpeciesGroup);
            p["@tsn"].Value = Value(row.ItisTsn);
            p["@kingdom"].Value = Value(row.Kingdom);
            p["@family"].Value = Value(row.Family);
            p["@url"].Value = row.Url;
            insert.ExecuteNonQuery();
            nameEntity.Value = row.EntityId;
            foreach (var name in row.Names) {
                nameValue.Value = name;
                insertName.ExecuteNonQuery();
            }
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
