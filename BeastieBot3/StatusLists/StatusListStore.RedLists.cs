using Microsoft.Data.Sqlite;

// The status lists store's national and subnational red lists (`statuses red-lists-import`):
// red_list_dataset, red_list_taxon and red_list_synonym, and each list's status_source row
// ('redlist:<key>').

namespace BeastieBot3.StatusLists;

/// A red_list_dataset row. FetchedAtUtc: when the import last checked the dataset;
/// ImportedAtUtc: when its rows were last replaced.
internal sealed record RedListDatasetRecord(
    string Key,
    string GbifKey,
    string Title,
    string ListName,
    string? ListNameEn,
    int? ListYear,
    string Publisher,
    string CountryCode,
    string? Region,
    string? RegionCode,
    string Licence,
    string Citation,
    string? GbifCitation,
    string? Doi,
    string? PubDate,
    string ArchiveUrl,
    string ArchiveFile,
    string ArchiveSha256,
    long ArchiveSize,
    string? Notes,
    DateTime FetchedAtUtc,
    DateTime ImportedAtUtc,
    long RowCount,
    long TaxonCount,
    long SynonymCount,
    int ReaderVersion);

internal sealed partial class StatusListStore {
    private const string DatasetColumns = """
        dataset_key, gbif_dataset_key, title, list_name, list_name_en, list_year, publisher, country_code, region, region_code, licence,
        citation, gbif_citation, doi, pub_date, archive_url, archive_file, archive_sha256, archive_size, notes, fetched_at, imported_at,
        row_count, taxon_count, synonym_count, reader_version
        """;

    public long CountRedListTaxa() => Scalar("SELECT COUNT(*) FROM red_list_taxon");

    /// False for a store written before the red lists were added, opened read-only (no schema work).
    public bool HasRedListTables() => Scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'red_list_dataset'") > 0;

    public RedListDatasetRecord? GetRedListDataset(string key) => ReadRedListDatasets("WHERE dataset_key = @key", ("@key", key)).FirstOrDefault();

    public IReadOnlyList<RedListDatasetRecord> RedListDatasets() => ReadRedListDatasets("ORDER BY dataset_key");

    /// The rows of one dataset by threat_status as written, most rows first.
    public IReadOnlyList<(string Status, string? IucnCode, long Rows)> RedListStatusCounts(string key) {
        var list = new List<(string, string?, long)>();
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT threat_status, iucn_code, COUNT(*) FROM red_list_taxon WHERE dataset_key = @key
            GROUP BY threat_status, iucn_code ORDER BY COUNT(*) DESC, threat_status
            """;
        command.Parameters.AddWithValue("@key", key);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt64(2)));
        }
        return list;
    }

    /// Replaces the dataset's rows, its red_list_dataset row and its status_source row in one
    /// transaction. The dataset row is written first: the taxon and synonym rows refer to it.
    public void ReplaceRedList(RedListDatasetRecord dataset, RedListParse parse) {
        using var tx = _connection.BeginTransaction();
        UpsertRedListDataset(tx, dataset);
        Execute(tx, "DELETE FROM red_list_synonym WHERE dataset_key = @key", ("@key", dataset.Key));
        Execute(tx, "DELETE FROM red_list_taxon WHERE dataset_key = @key", ("@key", dataset.Key));

        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = """
            INSERT INTO red_list_taxon(dataset_key, taxon_id, seq, scientific_name, canonical_name, authorship, taxon_rank, taxonomic_status,
                accepted_taxon_id, accepted_name, kingdom, phylum, taxclass, taxorder, family, genus, threat_status, iucn_code, status_label,
                country_code, locality, location_id, establishment_means, occurrence_status, event_date, source, url)
            VALUES (@key, @id, @seq, @name, @canonical, @authorship, @rank, @status, @accepted, @accepted_name, @kingdom, @phylum, @class,
                @order, @family, @genus, @threat, @code, @label, @country, @locality, @location, @means, @occurrence, @date, @source, @url)
            """;
        var names = new[] { "@id", "@seq", "@name", "@canonical", "@authorship", "@rank", "@status", "@accepted", "@accepted_name", "@kingdom",
            "@phylum", "@class", "@order", "@family", "@genus", "@threat", "@code", "@label", "@country", "@locality", "@location", "@means",
            "@occurrence", "@date", "@source", "@url" };
        var p = names.ToDictionary(n => n, n => AddParameter(insert, n), StringComparer.Ordinal);
        insert.Parameters.AddWithValue("@key", dataset.Key);
        static object Value(object? v) => v ?? DBNull.Value;
        foreach (var row in parse.Taxa) {
            p["@id"].Value = row.TaxonId;
            p["@seq"].Value = row.Seq;
            p["@name"].Value = row.ScientificName;
            p["@canonical"].Value = Value(row.CanonicalName);
            p["@authorship"].Value = Value(row.Authorship);
            p["@rank"].Value = Value(row.Rank);
            p["@status"].Value = Value(row.TaxonomicStatus);
            p["@accepted"].Value = Value(row.AcceptedTaxonId);
            p["@accepted_name"].Value = Value(row.AcceptedName);
            p["@kingdom"].Value = Value(row.Kingdom);
            p["@phylum"].Value = Value(row.Phylum);
            p["@class"].Value = Value(row.Class);
            p["@order"].Value = Value(row.Order);
            p["@family"].Value = Value(row.Family);
            p["@genus"].Value = Value(row.Genus);
            p["@threat"].Value = row.ThreatStatus;
            p["@code"].Value = Value(row.IucnCode);
            p["@label"].Value = Value(row.StatusLabel);
            p["@country"].Value = Value(row.CountryCode);
            p["@locality"].Value = Value(row.Locality);
            p["@location"].Value = Value(row.LocationId);
            p["@means"].Value = Value(row.EstablishmentMeans);
            p["@occurrence"].Value = Value(row.OccurrenceStatus);
            p["@date"].Value = Value(row.EventDate);
            p["@source"].Value = Value(row.Source);
            p["@url"].Value = Value(row.Url);
            insert.ExecuteNonQuery();
        }

        using var insertSynonym = _connection.CreateCommand();
        insertSynonym.Transaction = tx;
        insertSynonym.CommandText = """
            INSERT INTO red_list_synonym(dataset_key, taxon_id, scientific_name, canonical_name, authorship, taxonomic_status, accepted_taxon_id)
            VALUES (@key, @id, @name, @canonical, @authorship, @status, @accepted)
            ON CONFLICT(dataset_key, taxon_id) DO NOTHING
            """;
        var s = new[] { "@id", "@name", "@canonical", "@authorship", "@status", "@accepted" }
            .ToDictionary(n => n, n => AddParameter(insertSynonym, n), StringComparer.Ordinal);
        insertSynonym.Parameters.AddWithValue("@key", dataset.Key);
        foreach (var synonym in parse.Synonyms) {
            s["@id"].Value = synonym.TaxonId;
            s["@name"].Value = synonym.Name;
            s["@canonical"].Value = Value(synonym.CanonicalName);
            s["@authorship"].Value = Value(synonym.Authorship);
            s["@status"].Value = Value(synonym.TaxonomicStatus);
            s["@accepted"].Value = synonym.AcceptedTaxonId;
            insertSynonym.ExecuteNonQuery();
        }

        UpsertSource(SourceRow(dataset), tx);
        tx.Commit();
    }

    /// Records that the dataset was checked and found unchanged: its fetched_at and pub_date, and the
    /// fetched_at of its status_source row. Its rows stay as they are.
    public void MarkRedListChecked(string key, DateTime checkedAtUtc, string? pubDate) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "UPDATE red_list_dataset SET fetched_at = @at, pub_date = COALESCE(@pub, pub_date) WHERE dataset_key = @key",
            ("@at", Stamp(checkedAtUtc)), ("@pub", (object?)pubDate ?? DBNull.Value), ("@key", key));
        Execute(tx, "UPDATE status_source SET fetched_at = @at WHERE source = @source",
            ("@at", Stamp(checkedAtUtc)), ("@source", StatusSources.RedListPrefix + key));
        tx.Commit();
    }

    /// Deletes a dataset, its rows and its status_source row. False when it was not stored.
    public bool DeleteRedList(string key) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM red_list_synonym WHERE dataset_key = @key", ("@key", key));
        Execute(tx, "DELETE FROM red_list_taxon WHERE dataset_key = @key", ("@key", key));
        var deleted = Execute(tx, "DELETE FROM red_list_dataset WHERE dataset_key = @key", ("@key", key));
        Execute(tx, "DELETE FROM status_source WHERE source = @source", ("@source", StatusSources.RedListPrefix + key));
        tx.Commit();
        return deleted > 0;
    }

    /// The status_source row of a red list: title (English name when there is one), the GBIF dataset
    /// page, licence, the original list's citation, the GBIF pubDate as its version.
    internal static StatusSourceInfo SourceRow(RedListDatasetRecord dataset) =>
        new(StatusSources.RedListPrefix + dataset.Key, dataset.ListNameEn ?? dataset.ListName, GbifRegistry.DatasetPage(dataset.GbifKey),
            dataset.Licence, dataset.Citation, dataset.PubDate, dataset.FetchedAtUtc, dataset.RowCount);

    private void UpsertRedListDataset(SqliteTransaction tx, RedListDatasetRecord d) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        // ON CONFLICT ... DO UPDATE, never INSERT OR REPLACE: a replace deletes the row first, and the
        // delete would cascade to the dataset's taxa and synonyms.
        command.CommandText = $"""
            INSERT INTO red_list_dataset({DatasetColumns})
            VALUES (@key, @gbif, @title, @name, @name_en, @year, @publisher, @country, @region, @region_code, @licence, @citation,
                @gbif_citation, @doi, @pub, @url, @file, @sha, @size, @notes, @fetched, @imported, @rows, @taxa, @synonyms, @reader)
            ON CONFLICT(dataset_key) DO UPDATE SET gbif_dataset_key = excluded.gbif_dataset_key, title = excluded.title,
                list_name = excluded.list_name, list_name_en = excluded.list_name_en, list_year = excluded.list_year,
                publisher = excluded.publisher, country_code = excluded.country_code, region = excluded.region,
                region_code = excluded.region_code, licence = excluded.licence, citation = excluded.citation,
                gbif_citation = excluded.gbif_citation, doi = excluded.doi, pub_date = excluded.pub_date, archive_url = excluded.archive_url,
                archive_file = excluded.archive_file, archive_sha256 = excluded.archive_sha256, archive_size = excluded.archive_size,
                notes = excluded.notes, fetched_at = excluded.fetched_at, imported_at = excluded.imported_at, row_count = excluded.row_count,
                taxon_count = excluded.taxon_count, synonym_count = excluded.synonym_count, reader_version = excluded.reader_version
            """;
        static object Value(object? v) => v ?? DBNull.Value;
        command.Parameters.AddWithValue("@key", d.Key);
        command.Parameters.AddWithValue("@gbif", d.GbifKey);
        command.Parameters.AddWithValue("@title", d.Title);
        command.Parameters.AddWithValue("@name", d.ListName);
        command.Parameters.AddWithValue("@name_en", Value(d.ListNameEn));
        command.Parameters.AddWithValue("@year", Value(d.ListYear));
        command.Parameters.AddWithValue("@publisher", d.Publisher);
        command.Parameters.AddWithValue("@country", d.CountryCode);
        command.Parameters.AddWithValue("@region", Value(d.Region));
        command.Parameters.AddWithValue("@region_code", Value(d.RegionCode));
        command.Parameters.AddWithValue("@licence", d.Licence);
        command.Parameters.AddWithValue("@citation", d.Citation);
        command.Parameters.AddWithValue("@gbif_citation", Value(d.GbifCitation));
        command.Parameters.AddWithValue("@doi", Value(d.Doi));
        command.Parameters.AddWithValue("@pub", Value(d.PubDate));
        command.Parameters.AddWithValue("@url", d.ArchiveUrl);
        command.Parameters.AddWithValue("@file", d.ArchiveFile);
        command.Parameters.AddWithValue("@sha", d.ArchiveSha256);
        command.Parameters.AddWithValue("@size", d.ArchiveSize);
        command.Parameters.AddWithValue("@notes", Value(d.Notes));
        command.Parameters.AddWithValue("@fetched", Stamp(d.FetchedAtUtc));
        command.Parameters.AddWithValue("@imported", Stamp(d.ImportedAtUtc));
        command.Parameters.AddWithValue("@rows", d.RowCount);
        command.Parameters.AddWithValue("@taxa", d.TaxonCount);
        command.Parameters.AddWithValue("@synonyms", d.SynonymCount);
        command.Parameters.AddWithValue("@reader", d.ReaderVersion);
        command.ExecuteNonQuery();
    }

    private List<RedListDatasetRecord> ReadRedListDatasets(string tail, params (string Name, object Value)[] parameters) {
        var list = new List<RedListDatasetRecord>();
        using var command = _connection.CreateCommand();
        command.CommandText = $"SELECT {DatasetColumns} FROM red_list_dataset {tail}";
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        using var reader = command.ExecuteReader();
        string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
        while (reader.Read()) {
            list.Add(new RedListDatasetRecord(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), Text(4),
                reader.IsDBNull(5) ? null : reader.GetInt32(5), reader.GetString(6), reader.GetString(7), Text(8), Text(9), reader.GetString(10),
                reader.GetString(11), Text(12), Text(13), Text(14), reader.GetString(15), reader.GetString(16), reader.GetString(17),
                reader.GetInt64(18), Text(19), Infrastructure.StoredUtc.Parse(reader.GetString(20)) ?? DateTime.MinValue,
                Infrastructure.StoredUtc.Parse(reader.GetString(21)) ?? DateTime.MinValue, reader.GetInt64(22), reader.GetInt64(23),
                reader.GetInt64(24), reader.GetInt32(25)));
        }
        return list;
    }
}
