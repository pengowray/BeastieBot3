using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Conservation statuses from systems other than the IUCN Red List (Datastore:status_lists_sqlite),
// for the public species site:
//   status_source         one row per source: title, licence, citation, when it was last fetched;
//   status_sync_state     key/value progress of `statuses natureserve-fetch` (the pass under way);
//   natureserve_species   one row per NatureServe Explorer species, subspecies, variety or
//                         population: G rank, US and Canadian N ranks, US ESA, COSEWIC and SARA codes;
//   natureserve_synonym   the synonyms NatureServe lists for each of them;
//   natureserve_partition the name prefixes of the pass under way and how far each has got;
//   ecos_listing          one row per US Endangered Species Act listing in ECOS (a species,
//                         subspecies or population);
//   ecos_name             every name the ECOS scientific name gives, brackets read (EcosScientificName).
//
// Nothing narrative is stored: no NatureServe taxonomic comments, ranking reasons or other text.

namespace BeastieBot3.StatusLists;

internal static class StatusSources {
    public const string NatureServe = "natureserve";
    public const string Ecos = "ecos";
}

internal sealed record StatusSourceInfo(
    string Source,
    string Title,
    string Url,
    string Licence,
    string? Citation,
    string? Version,
    DateTime FetchedAtUtc,
    long RowCount);

/// One NatureServe Explorer record, as `statuses natureserve-fetch` stores it.
internal sealed record NatureServeSpecies(
    long ElementGlobalId,
    string UniqueId,
    string? Elcode,
    string ScientificName,
    string? PrimaryCommonName,
    string? PrimaryCommonNameLanguage,
    string? GRank,
    string? RoundedGRank,
    string? ClassificationStatus,
    string? Kingdom,
    string? Phylum,
    string? TaxClass,
    string? TaxOrder,
    string? Family,
    string? Genus,
    string? InformalTaxonomy,
    bool Infraspecies,
    string? UsesaCode,
    string? CosewicCode,
    string? SaraCode,
    string? SaraCodeRaw,
    string? UsNRank,
    string? CaNRank,
    string NsxUrl,
    string? LastModified,
    IReadOnlyList<string> Synonyms);

/// One name prefix of a NatureServe pass. Expected is the number of records NatureServe gave for
/// the prefix (null until its first page is read); NextPage is the next page to ask for.
internal sealed record NatureServePartition(string Prefix, long? Expected, int NextPage, bool Done);

/// One US Endangered Species Act listing from ECOS.
internal sealed record EcosListing(
    long EntityId,
    long? SpeciesId,
    string ScientificNameRaw,
    string ScientificName,
    string? NameNote,
    string? CommonName,
    string Status,
    string? EntityDescription,
    string? ListingDate,
    bool? IsDps,
    bool? IsForeign,
    string? RangeCountry,
    string? SpeciesGroup,
    long? ItisTsn,
    string? Kingdom,
    string? Family,
    string Url,
    IReadOnlyList<string> Names);

internal sealed class StatusListStore : SqliteStore {
    private StatusListStore(SqliteConnection connection) : base(connection) { }

    public static StatusListStore Open(string path) {
        var store = new StatusListStore(OpenConnection(path));
        store.EnsureSchema();
        return store;
    }

    /// Null when the file is missing. No schema work: any write fails.
    public static StatusListStore? OpenReadOnly(string path) {
        if (!File.Exists(path)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        return new StatusListStore(connection);
    }

    internal static StatusListStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new StatusListStore(connection);
        store.EnsureSchema();
        return store;
    }

    internal const string Ddl = """
        CREATE TABLE IF NOT EXISTS status_source (
            source      TEXT PRIMARY KEY,   -- 'natureserve' | 'ecos'
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,
            licence     TEXT NOT NULL,
            citation    TEXT,               -- the source's citation form, with the access date filled in
            version     TEXT,
            fetched_at  TEXT NOT NULL,      -- UTC "O": when the last full download or refresh finished
            row_count   INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS status_sync_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS natureserve_species (
            element_global_id     INTEGER PRIMARY KEY,
            unique_id             TEXT NOT NULL,     -- ELEMENT_GLOBAL.2.<element_global_id>
            elcode                TEXT,
            scientific_name       TEXT NOT NULL,     -- as NatureServe writes it: "Atriplex cordulata var. cordulata", "Ambystoma californiense pop. 1"
            primary_common_name   TEXT,
            primary_common_name_language TEXT,       -- EN, FR, ES ...
            g_rank                TEXT,              -- global rank as published: G3G4, G2T1, G3TNRQ
            rounded_g_rank        TEXT,              -- G1..G5, GH, GX, GNR, GNA, GU, or a rounded T rank (T1 ...)
            classification_status TEXT,              -- Standard, Provisional, Nonstandard
            kingdom               TEXT,
            phylum                TEXT,
            taxclass              TEXT,
            taxorder              TEXT,
            family                TEXT,
            genus                 TEXT,
            informal_taxonomy     TEXT,              -- "Animals | Vertebrates | Amphibians"
            infraspecies          INTEGER NOT NULL,  -- 1 for a subspecies, variety or population
            usesa_code            TEXT,              -- US Endangered Species Act status codes as NatureServe writes them: E, T, PE, PT, C, SAT, PSAT, DL, PDL, UR, XN; several joined by ", " ("E, XN")
            cosewic_code          TEXT,              -- COSEWIC status code: E, T, SC, X, XT, NAR, DD, Non-active/Nonactive
            sara_code             TEXT,              -- SARA status, English part ("Endangered")
            sara_code_raw         TEXT,              -- SARA status as given ("Endangered/En voie de disparition")
            us_n_rank             TEXT,              -- rounded national rank in the United States
            ca_n_rank             TEXT,              -- rounded national rank in Canada
            nsx_url               TEXT NOT NULL,     -- the record's page on NatureServe Explorer
            last_modified         TEXT,              -- NatureServe's lastModified, as given
            fetched_at            TEXT NOT NULL      -- UTC "O": when a pass last stored the record
        );
        CREATE INDEX IF NOT EXISTS natureserve_species_name ON natureserve_species(scientific_name);
        CREATE INDEX IF NOT EXISTS natureserve_species_fetched ON natureserve_species(fetched_at);
        CREATE TABLE IF NOT EXISTS natureserve_synonym (
            element_global_id INTEGER NOT NULL REFERENCES natureserve_species(element_global_id) ON DELETE CASCADE,
            name              TEXT NOT NULL,
            PRIMARY KEY (element_global_id, name)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS natureserve_synonym_name ON natureserve_synonym(name);
        CREATE TABLE IF NOT EXISTS natureserve_partition (
            prefix    TEXT PRIMARY KEY,           -- scientific name prefix ('' = every record)
            expected  INTEGER,                    -- records NatureServe gave for the prefix
            next_page INTEGER NOT NULL DEFAULT 0,
            done      INTEGER NOT NULL DEFAULT 0
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS ecos_listing (
            entity_id           INTEGER PRIMARY KEY,  -- ECOS Listed Species ID: one listed entity (species, subspecies or population)
            species_id          INTEGER,              -- ECOS Species ID, the number in the species page URL; shared by the listings of one species
            scientific_name_raw TEXT NOT NULL,        -- as ECOS writes it: "Papasula (=Sula) abbotti"
            scientific_name     TEXT NOT NULL,        -- brackets removed: "Papasula abbotti"
            name_note           TEXT,                 -- bracketed notes that are not names: "entire genus", "incl. D. cascus"
            common_name         TEXT,                 -- NULL where ECOS gives "No common name"
            status              TEXT NOT NULL,        -- Endangered, Threatened, Experimental Population, Non-Essential, Similarity of Appearance (Threatened)
            entity_description  TEXT,                 -- where the listing applies: "Wherever found", "U.S.A. (FL)"
            listing_date        TEXT,                 -- yyyy-MM-dd
            is_dps              INTEGER,              -- 1 for a distinct population segment
            is_foreign          INTEGER,              -- 1 for a species found only outside the United States
            range_country       TEXT,                 -- Domestic, Foreign, Both Domestic and Foreign
            species_group       TEXT,                 -- FWS group: Birds, Flowering Plants, Clams ...
            itis_tsn            INTEGER,              -- ITIS Taxonomic Serial Number
            kingdom             TEXT,                 -- Animal, Plant, UNKNOWN
            family              TEXT,
            url                 TEXT NOT NULL,        -- the species page on ECOS
            imported_at         TEXT NOT NULL         -- UTC "O"
        );
        CREATE INDEX IF NOT EXISTS ecos_listing_species ON ecos_listing(species_id);
        CREATE INDEX IF NOT EXISTS ecos_listing_name ON ecos_listing(scientific_name);
        CREATE TABLE IF NOT EXISTS ecos_name (
            entity_id INTEGER NOT NULL REFERENCES ecos_listing(entity_id) ON DELETE CASCADE,
            name      TEXT NOT NULL,
            PRIMARY KEY (entity_id, name)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS ecos_name_name ON ecos_name(name);
        """;

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = Ddl;
        command.ExecuteNonQuery();
    }

    internal static string Stamp(DateTime utc) => utc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    // ---- status_sync_state ----

    public string? GetState(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM status_sync_state WHERE key = @key";
        command.Parameters.AddWithValue("@key", key);
        return command.ExecuteScalar() as string;
    }

    public void SetState(string key, string? value, SqliteTransaction? tx = null) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        if (value is null) {
            command.CommandText = "DELETE FROM status_sync_state WHERE key = @key";
        } else {
            command.CommandText = "INSERT INTO status_sync_state(key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value = excluded.value";
            command.Parameters.AddWithValue("@value", value);
        }
        command.Parameters.AddWithValue("@key", key);
        command.ExecuteNonQuery();
    }

    public SqliteTransaction BeginTransaction() => _connection.BeginTransaction();

    // ---- status_source ----

    public void UpsertSource(StatusSourceInfo info, SqliteTransaction? tx = null) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
            INSERT INTO status_source(source, title, url, licence, citation, version, fetched_at, row_count)
            VALUES (@source, @title, @url, @licence, @citation, @version, @fetched, @rows)
            ON CONFLICT(source) DO UPDATE SET title = excluded.title, url = excluded.url, licence = excluded.licence,
                citation = excluded.citation, version = excluded.version, fetched_at = excluded.fetched_at, row_count = excluded.row_count
            """;
        command.Parameters.AddWithValue("@source", info.Source);
        command.Parameters.AddWithValue("@title", info.Title);
        command.Parameters.AddWithValue("@url", info.Url);
        command.Parameters.AddWithValue("@licence", info.Licence);
        command.Parameters.AddWithValue("@citation", (object?)info.Citation ?? DBNull.Value);
        command.Parameters.AddWithValue("@version", (object?)info.Version ?? DBNull.Value);
        command.Parameters.AddWithValue("@fetched", Stamp(info.FetchedAtUtc));
        command.Parameters.AddWithValue("@rows", info.RowCount);
        command.ExecuteNonQuery();
    }

    public IReadOnlyList<StatusSourceInfo> Sources() {
        var list = new List<StatusSourceInfo>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT source, title, url, licence, citation, version, fetched_at, row_count FROM status_source ORDER BY source";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add(new StatusSourceInfo(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                StoredUtc.Parse(reader.GetString(6)) ?? DateTime.MinValue, reader.GetInt64(7)));
        }
        return list;
    }

    // ---- NatureServe ----

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

    // ---- ECOS ----

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

    // ---- helpers ----

    // A parameter whose SQLite type follows the value it is given, so a number is stored as an
    // INTEGER and a string as TEXT.
    private static SqliteParameter AddParameter(SqliteCommand command, string name) {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private long Scalar(string sql, params (string Name, object Value)[] parameters) {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private int Execute(SqliteTransaction tx, string sql, params (string Name, object Value)[] parameters) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        return command.ExecuteNonQuery();
    }
}
