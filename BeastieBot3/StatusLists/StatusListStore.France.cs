using Microsoft.Data.Sqlite;

// The status lists store's French tables (`statuses france-import`): france_status,
// france_status_type, france_territory and france_document from PatriNat's BDC Statuts, and
// france_taxref_name and france_taxref_link from TAXREF.

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountFranceStatuses() => Scalar("SELECT COUNT(*) FROM france_status");

    public long CountTaxrefNames() => Scalar("SELECT COUNT(*) FROM france_taxref_name");

    /// Replaces every French table, and the BDC and TAXREF rows of status_source, in one transaction.
    public void ReplaceFrance(FranceBdcData bdc, FranceTaxrefData taxref, DateTime importedAtUtc, StatusSourceInfo bdcSource,
        StatusSourceInfo taxrefSource) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM france_status");
        Execute(tx, "DELETE FROM france_document");
        Execute(tx, "DELETE FROM france_territory");
        Execute(tx, "DELETE FROM france_status_type");
        Execute(tx, "DELETE FROM france_taxref_link");
        Execute(tx, "DELETE FROM france_taxref_name");
        static object Value(object? v) => v ?? DBNull.Value;

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO france_status_type(type_code, label, type_group, stored) VALUES (@code, @label, @group, @stored)";
            var code = insert.Parameters.Add("@code", SqliteType.Text);
            var label = insert.Parameters.Add("@label", SqliteType.Text);
            var group = insert.Parameters.Add("@group", SqliteType.Text);
            var stored = insert.Parameters.Add("@stored", SqliteType.Integer);
            foreach (var type in bdc.Types) {
                code.Value = type.Code;
                label.Value = type.Label;
                group.Value = Value(type.Group);
                stored.Value = type.Stored ? 1 : 0;
                insert.ExecuteNonQuery();
            }
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO france_territory(territory_code, name, name_en, admin_level, iso3166_1, iso3166_2)
                VALUES (@code, @name, @en, @level, @iso1, @iso2)
                """;
            var p = new[] { "@code", "@name", "@en", "@level", "@iso1", "@iso2" }.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
            foreach (var territory in bdc.Territories) {
                p["@code"].Value = territory.Code;
                p["@name"].Value = territory.Name;
                p["@en"].Value = Value(territory.NameEn);
                p["@level"].Value = Value(territory.AdminLevel);
                p["@iso1"].Value = Value(territory.Iso31661);
                p["@iso2"].Value = Value(territory.Iso31662);
                insert.ExecuteNonQuery();
            }
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO france_document(cd_doc, year, title, citation, citation_html, url, bird_population)
                VALUES (@doc, @year, @title, @citation, @html, @url, @population)
                """;
            var p = new[] { "@doc", "@year", "@title", "@citation", "@html", "@url", "@population" }
                .ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
            foreach (var document in bdc.Documents) {
                p["@doc"].Value = document.CdDoc;
                p["@year"].Value = Value(document.Year);
                p["@title"].Value = Value(document.Title);
                p["@citation"].Value = Value(document.Citation);
                p["@html"].Value = Value(document.CitationHtml);
                p["@url"].Value = Value(document.Url);
                p["@population"].Value = Value(document.BirdPopulation);
                insert.ExecuteNonQuery();
            }
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO france_status(row_number, cd_nom, cd_ref, type_code, code, label, remark, criteria, adjusted_from, adjustment,
                    na_reason, population_fr, population, is_current, territory_code, name, author, kingdom, phylum, taxclass, taxorder,
                    family, cd_doc, imported_at)
                VALUES (@row, @nom, @ref, @type, @code, @label, @remark, @criteria, @from, @adjustment, @na, @population_fr, @population,
                    @current, @territory, @name, @author, @kingdom, @phylum, @class, @order, @family, @doc, @imported)
                ON CONFLICT(row_number) DO NOTHING
                """;
            var names = new[] { "@row", "@nom", "@ref", "@type", "@code", "@label", "@remark", "@criteria", "@from", "@adjustment", "@na",
                "@population_fr", "@population", "@current", "@territory", "@name", "@author", "@kingdom", "@phylum", "@class", "@order",
                "@family", "@doc" };
            var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
            insert.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));
            foreach (var row in bdc.Rows) {
                p["@row"].Value = row.RowNumber;
                p["@nom"].Value = row.CdNom;
                p["@ref"].Value = row.CdRef;
                p["@type"].Value = row.TypeCode;
                p["@code"].Value = row.Code;
                p["@label"].Value = Value(row.Label);
                p["@remark"].Value = Value(row.Remark.Text);
                p["@criteria"].Value = Value(row.Remark.Criteria);
                p["@from"].Value = Value(row.Remark.AdjustedFrom);
                p["@adjustment"].Value = Value(row.Remark.Adjustment);
                p["@na"].Value = Value(row.Remark.NaReason);
                p["@population_fr"].Value = Value(row.Remark.PopulationFr);
                p["@population"].Value = Value(row.Remark.Population);
                p["@current"].Value = row.IsCurrent is { } current ? current ? 1 : 0 : DBNull.Value;
                p["@territory"].Value = row.TerritoryCode;
                p["@name"].Value = row.Name;
                p["@author"].Value = Value(row.Author);
                p["@kingdom"].Value = Value(row.Kingdom);
                p["@phylum"].Value = Value(row.Phylum);
                p["@class"].Value = Value(row.TaxClass);
                p["@order"].Value = Value(row.TaxOrder);
                p["@family"].Value = Value(row.Family);
                p["@doc"].Value = Value(row.CdDoc);
                insert.ExecuteNonQuery();
            }
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO france_taxref_name(cd_nom, cd_ref, rank, name, author) VALUES (@nom, @ref, @rank, @name, @author)
                ON CONFLICT(cd_nom) DO NOTHING
                """;
            var p = new[] { "@nom", "@ref", "@rank", "@name", "@author" }.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
            foreach (var name in taxref.Names) {
                p["@nom"].Value = name.CdNom;
                p["@ref"].Value = name.CdRef;
                p["@rank"].Value = Value(name.Rank);
                p["@name"].Value = name.Name;
                p["@author"].Value = Value(name.Author);
                insert.ExecuteNonQuery();
            }
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO france_taxref_link(cd_nom, cd_ref, source, external_id) VALUES (@nom, @ref, @source, @id)
                ON CONFLICT DO NOTHING
                """;
            var p = new[] { "@nom", "@ref", "@source", "@id" }.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
            foreach (var link in taxref.Links) {
                p["@nom"].Value = link.CdNom;
                p["@ref"].Value = link.CdRef;
                p["@source"].Value = link.Source;
                p["@id"].Value = link.ExternalId;
                insert.ExecuteNonQuery();
            }
        }

        UpsertSource(bdcSource, tx);
        UpsertSource(taxrefSource, tx);
        tx.Commit();
    }
}
