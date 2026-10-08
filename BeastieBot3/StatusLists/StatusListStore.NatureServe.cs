using System.Globalization;
using Microsoft.Data.Sqlite;

// The status lists store's NatureServe tables (`statuses natureserve-fetch`): natureserve_species,
// natureserve_synonym and natureserve_partition.

namespace BeastieBot3.StatusLists;

/// One name prefix of a NatureServe pass. Expected is the number of records NatureServe gave for
/// the prefix (null until its first page is read); NextPage is the next page to ask for.
internal sealed record NatureServePartition(string Prefix, long? Expected, int NextPage, bool Done);

internal sealed partial class StatusListStore {
    public long CountNatureServe() => Scalar("SELECT COUNT(*) FROM natureserve_species");

    public long CountNatureServeFetchedSince(DateTime sinceUtc) =>
        Scalar("SELECT COUNT(*) FROM natureserve_species WHERE fetched_at >= @since", ("@since", Stamp(sinceUtc)));

    public IReadOnlyList<NatureServePartition> GetPartitions() {
        var list = new List<NatureServePartition>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT prefix, expected, next_page, done FROM natureserve_partition ORDER BY prefix";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add(new NatureServePartition(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt32(2), reader.GetInt64(3) != 0));
        }
        return list;
    }

    /// Clears the partitions and the pass keys, then writes the new pass's keys and its first
    /// partition, in one transaction.
    public void StartNatureServePass(IReadOnlyDictionary<string, string?> passKeys, IEnumerable<string> passKeysToClear, string firstPrefix) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM natureserve_partition");
        foreach (var key in passKeysToClear) {
            SetState(key, null, tx);
        }
        foreach (var (key, value) in passKeys) {
            SetState(key, value, tx);
        }
        InsertPartition(tx, firstPrefix);
        tx.Commit();
    }

    /// Stores one page of a partition and moves the partition on, in one transaction, so a stopped
    /// run never leaves a partition's page count ahead of its rows. When <paramref name="splitInto"/>
    /// is given, the partition is replaced by those prefixes instead (its records were too many to
    /// page through). The page's records are stored either way.
    public void StoreNatureServePage(NatureServePartition partition, long expected, bool done, IReadOnlyList<NatureServeSpecies> rows,
        DateTime fetchedAtUtc, IReadOnlyList<string>? splitInto = null) {
        using var tx = _connection.BeginTransaction();
        UpsertNatureServe(tx, rows, fetchedAtUtc);
        if (splitInto is not null) {
            Execute(tx, "DELETE FROM natureserve_partition WHERE prefix = @prefix", ("@prefix", partition.Prefix));
            foreach (var prefix in splitInto) {
                InsertPartition(tx, prefix);
            }
        } else {
            using var command = _connection.CreateCommand();
            command.Transaction = tx;
            command.CommandText = "UPDATE natureserve_partition SET expected = @expected, next_page = @next, done = @done WHERE prefix = @prefix";
            command.Parameters.AddWithValue("@expected", expected);
            command.Parameters.AddWithValue("@next", partition.NextPage + 1);
            command.Parameters.AddWithValue("@done", done ? 1 : 0);
            command.Parameters.AddWithValue("@prefix", partition.Prefix);
            command.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// Ends a pass: deletes the given records (and, when <paramref name="deleteNotFetchedSince"/>
    /// is set, every record no page of the pass stored), clears the partitions and the pass keys,
    /// writes the completion keys and the source row, in one transaction. Returns how many records
    /// it deleted.
    public int CompleteNatureServePass(IEnumerable<long> deleteIds, DateTime? deleteNotFetchedSince,
        IReadOnlyDictionary<string, string?> completionKeys, IEnumerable<string> passKeysToClear, Func<long, StatusSourceInfo> source) {
        using var tx = _connection.BeginTransaction();
        var deleted = 0;
        if (deleteNotFetchedSince is { } since) {
            deleted += Execute(tx, "DELETE FROM natureserve_species WHERE fetched_at < @since", ("@since", Stamp(since)));
        }
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM natureserve_species WHERE element_global_id = @id";
            var id = delete.Parameters.Add("@id", SqliteType.Integer);
            foreach (var value in deleteIds) {
                id.Value = value;
                deleted += delete.ExecuteNonQuery();
            }
        }
        Execute(tx, "DELETE FROM natureserve_partition");
        foreach (var key in passKeysToClear) {
            SetState(key, null, tx);
        }
        foreach (var (key, value) in completionKeys) {
            SetState(key, value, tx);
        }
        using (var count = _connection.CreateCommand()) {
            count.Transaction = tx;
            count.CommandText = "SELECT COUNT(*) FROM natureserve_species";
            UpsertSource(source(Convert.ToInt64(count.ExecuteScalar(), CultureInfo.InvariantCulture)), tx);
        }
        tx.Commit();
        return deleted;
    }

    private void InsertPartition(SqliteTransaction tx, string prefix) =>
        Execute(tx, "INSERT INTO natureserve_partition(prefix) VALUES (@prefix) ON CONFLICT(prefix) DO NOTHING", ("@prefix", prefix));

    private void UpsertNatureServe(SqliteTransaction tx, IReadOnlyList<NatureServeSpecies> rows, DateTime fetchedAtUtc) {
        if (rows.Count == 0) {
            return;
        }
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
            INSERT INTO natureserve_species(element_global_id, unique_id, elcode, scientific_name, primary_common_name, primary_common_name_language,
                g_rank, rounded_g_rank, classification_status, kingdom, phylum, taxclass, taxorder, family, genus, informal_taxonomy, infraspecies,
                usesa_code, cosewic_code, sara_code, sara_code_raw, us_n_rank, ca_n_rank, nsx_url, last_modified, fetched_at)
            VALUES (@id, @unique, @elcode, @name, @common, @commonLang, @grank, @rounded, @classification, @kingdom, @phylum, @class, @order,
                @family, @genus, @informal, @infra, @usesa, @cosewic, @sara, @saraRaw, @us, @ca, @url, @modified, @fetched)
            ON CONFLICT(element_global_id) DO UPDATE SET unique_id = excluded.unique_id, elcode = excluded.elcode,
                scientific_name = excluded.scientific_name, primary_common_name = excluded.primary_common_name,
                primary_common_name_language = excluded.primary_common_name_language, g_rank = excluded.g_rank,
                rounded_g_rank = excluded.rounded_g_rank, classification_status = excluded.classification_status,
                kingdom = excluded.kingdom, phylum = excluded.phylum, taxclass = excluded.taxclass, taxorder = excluded.taxorder,
                family = excluded.family, genus = excluded.genus, informal_taxonomy = excluded.informal_taxonomy,
                infraspecies = excluded.infraspecies, usesa_code = excluded.usesa_code, cosewic_code = excluded.cosewic_code,
                sara_code = excluded.sara_code, sara_code_raw = excluded.sara_code_raw, us_n_rank = excluded.us_n_rank,
                ca_n_rank = excluded.ca_n_rank, nsx_url = excluded.nsx_url, last_modified = excluded.last_modified,
                fetched_at = excluded.fetched_at
            """;
        var p = new Dictionary<string, SqliteParameter>(StringComparer.Ordinal);
        foreach (var name in new[] { "@id", "@unique", "@elcode", "@name", "@common", "@commonLang", "@grank", "@rounded", "@classification",
                     "@kingdom", "@phylum", "@class", "@order", "@family", "@genus", "@informal", "@infra", "@usesa", "@cosewic", "@sara",
                     "@saraRaw", "@us", "@ca", "@url", "@modified" }) {
            p[name] = AddParameter(command, name);
        }
        command.Parameters.AddWithValue("@fetched", Stamp(fetchedAtUtc));

        using var deleteSynonyms = _connection.CreateCommand();
        deleteSynonyms.Transaction = tx;
        deleteSynonyms.CommandText = "DELETE FROM natureserve_synonym WHERE element_global_id = @id";
        var deleteId = deleteSynonyms.Parameters.Add("@id", SqliteType.Integer);
        using var insertSynonym = _connection.CreateCommand();
        insertSynonym.Transaction = tx;
        insertSynonym.CommandText = "INSERT INTO natureserve_synonym(element_global_id, name) VALUES (@id, @name) ON CONFLICT DO NOTHING";
        var synonymId = insertSynonym.Parameters.Add("@id", SqliteType.Integer);
        var synonymName = insertSynonym.Parameters.Add("@name", SqliteType.Text);

        static object Value(string? s) => s is null ? DBNull.Value : s;
        foreach (var row in rows) {
            p["@id"].Value = row.ElementGlobalId;
            p["@unique"].Value = row.UniqueId;
            p["@elcode"].Value = Value(row.Elcode);
            p["@name"].Value = row.ScientificName;
            p["@common"].Value = Value(row.PrimaryCommonName);
            p["@commonLang"].Value = Value(row.PrimaryCommonNameLanguage);
            p["@grank"].Value = Value(row.GRank);
            p["@rounded"].Value = Value(row.RoundedGRank);
            p["@classification"].Value = Value(row.ClassificationStatus);
            p["@kingdom"].Value = Value(row.Kingdom);
            p["@phylum"].Value = Value(row.Phylum);
            p["@class"].Value = Value(row.TaxClass);
            p["@order"].Value = Value(row.TaxOrder);
            p["@family"].Value = Value(row.Family);
            p["@genus"].Value = Value(row.Genus);
            p["@informal"].Value = Value(row.InformalTaxonomy);
            p["@infra"].Value = row.Infraspecies ? 1 : 0;
            p["@usesa"].Value = Value(row.UsesaCode);
            p["@cosewic"].Value = Value(row.CosewicCode);
            p["@sara"].Value = Value(row.SaraCode);
            p["@saraRaw"].Value = Value(row.SaraCodeRaw);
            p["@us"].Value = Value(row.UsNRank);
            p["@ca"].Value = Value(row.CaNRank);
            p["@url"].Value = row.NsxUrl;
            p["@modified"].Value = Value(row.LastModified);
            command.ExecuteNonQuery();

            deleteId.Value = row.ElementGlobalId;
            deleteSynonyms.ExecuteNonQuery();
            synonymId.Value = row.ElementGlobalId;
            foreach (var synonym in row.Synonyms) {
                synonymName.Value = synonym;
                insertSynonym.ExecuteNonQuery();
            }
        }
    }
}
