using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Country checklists from sources other than IUCN (Datastore:checklists_sqlite), for checking the
// countries IUCN's assessments code against independent lists (`checklists crosscheck`):
//   checklist_source  one row per source: its version, licence, where it came from, when imported;
//   checklist_area    one row per source, taxon name and area: the area as the source codes it
//                     (scheme 'iso2' for ISO 3166 alpha-2, 'tdwg3' for a TDWG level-3 region,
//                     'name' for a country name not matched to a code) and the origin it gives;
//   checklist_synonym one row per source synonym and the accepted name it leads to, for matching
//                     an IUCN name that the source treats as a synonym.
//   checklist_species one row per accepted species of a source that has subspecies lists (mdd,
//                     reptiledb), with the source's id of the species' record: the MDD id
//                     ("1000002") or the query of the Reptile Database's species page
//                     ("genus=Tachyglossus&species=aculeatus"). `site build-db` matches IUCN species
//                     to these names and links the record.
//   checklist_infraspecific  one row per subspecies a source lists under one of its species: the
//                     full name ("Tachyglossus aculeatus acanthion"), rank, authority as written, and
//                     MDD's note ("fossil", "recently extinct", "holocene").
// An import replaces all of a source's rows in one transaction.

namespace BeastieBot3.Checklists;

/// One taxon's record for one area in a source. Origin: native, introduced, endemic, vagrant,
/// extinct, uncertain, recorded (GBIF: occurrences, whatever their origin), or null when the source
/// does not say. Records: the number of occurrence records (GBIF only).
internal sealed record ChecklistArea(string ScientificName, string Area, string Scheme, string? Origin, long? Records = null);

/// A name a source gives a species: an English common name, or a synonym with its authority.
internal sealed record ChecklistName(string ScientificName, string Name, string NameType, string? Authority = null);

internal static class ChecklistNameTypes {
    public const string Common = "common";
    public const string Synonym = "synonym";
}

/// An accepted species of a source, with the source's id of its record (null when it has none).
internal sealed record ChecklistSpecies(string ScientificName, string? RecordId);

/// A subspecies (or variety) a source lists under one of its species: the full name, the rank
/// (InfraspecificNames.Subspecies or Variety), the authority as the source writes it, and a note the
/// source gives (MDD: MddSubspecies.Fossil, RecentlyExtinct, Holocene).
internal sealed record ChecklistInfraspecific(string SpeciesName, string Name, string Rank, string? Authority = null, string? Note = null);

internal sealed record ChecklistSourceInfo(string Source, string? Version, string? Licence, string? Url, DateTime? ImportedAt, long Rows, long Taxa,
    long Subspecies = 0);

internal static class ChecklistSchemes {
    public const string Iso2 = "iso2";
    public const string Tdwg3 = "tdwg3";
    public const string Name = "name";
}

internal sealed class ChecklistStore : IDisposable {
    private readonly SqliteConnection _connection;

    private ChecklistStore(SqliteConnection connection) {
        _connection = connection;
    }

    public static ChecklistStore Open(string path) {
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadWriteCreate }.ConnectionString);
        connection.Open();
        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;";
            pragma.ExecuteNonQuery();
        }
        var store = new ChecklistStore(connection);
        store.EnsureSchema();
        return store;
    }

    /// Null when the file is missing.
    public static ChecklistStore? OpenReadOnly(string path) {
        if (!File.Exists(path)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly }.ConnectionString);
        connection.Open();
        return new ChecklistStore(connection);
    }

    internal static ChecklistStore OpenFromConnection(SqliteConnection connection) {
        var store = new ChecklistStore(connection);
        store.EnsureSchema();
        return store;
    }

    private void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS checklist_source (
                source      TEXT PRIMARY KEY,
                version     TEXT,
                licence     TEXT,
                url         TEXT,
                imported_at TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS checklist_area (
                source          TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                area            TEXT NOT NULL,
                scheme          TEXT NOT NULL,
                origin          TEXT,
                PRIMARY KEY (source, scientific_name, area)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS checklist_area_name ON checklist_area(scientific_name);
            CREATE TABLE IF NOT EXISTS checklist_name (
                source          TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                name            TEXT NOT NULL,
                name_type       TEXT NOT NULL,
                authority       TEXT,
                PRIMARY KEY (source, scientific_name, name, name_type)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS gbif_species_country (
                country     TEXT NOT NULL,
                species_key INTEGER NOT NULL,
                records     INTEGER NOT NULL,
                PRIMARY KEY (country, species_key)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS gbif_country_fetch (
                country    TEXT PRIMARY KEY,
                fetched_at TEXT NOT NULL,
                species    INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS checklist_synonym (
                source   TEXT NOT NULL,
                name     TEXT NOT NULL,
                accepted TEXT NOT NULL,
                PRIMARY KEY (source, name, accepted)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS checklist_species (
                source          TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                record_id       TEXT,
                PRIMARY KEY (source, scientific_name)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS checklist_infraspecific (
                source          TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                name            TEXT NOT NULL,
                rank            TEXT NOT NULL,
                authority       TEXT,
                note            TEXT,
                PRIMARY KEY (source, scientific_name, name)
            ) WITHOUT ROWID;
            """;
        command.ExecuteNonQuery();
        using var columns = _connection.CreateCommand();
        columns.CommandText = "SELECT COUNT(*) FROM pragma_table_info('checklist_area') WHERE name = 'records'";
        if (Convert.ToInt64(columns.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
            using var alter = _connection.CreateCommand();
            alter.CommandText = "ALTER TABLE checklist_area ADD COLUMN records INTEGER";
            alter.ExecuteNonQuery();
        }
    }

    /// Replaces the source's rows. A name and area given twice keeps the first origin given; a species
    /// or subspecies given twice keeps the first row.
    public void Replace(string source, string? version, string? licence, string? url, IEnumerable<ChecklistArea> rows,
        IEnumerable<(string Name, string Accepted)>? synonyms = null, IEnumerable<ChecklistName>? names = null,
        IEnumerable<ChecklistSpecies>? species = null, IEnumerable<ChecklistInfraspecific>? infraspecific = null) {
        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = """
                DELETE FROM checklist_area WHERE source = @source; DELETE FROM checklist_synonym WHERE source = @source;
                DELETE FROM checklist_name WHERE source = @source; DELETE FROM checklist_species WHERE source = @source;
                DELETE FROM checklist_infraspecific WHERE source = @source; DELETE FROM checklist_source WHERE source = @source;
                """;
            delete.Parameters.AddWithValue("@source", source);
            delete.ExecuteNonQuery();
        }
        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO checklist_area (source, scientific_name, area, scheme, origin, records) VALUES (@source, @name, @area, @scheme, @origin, @records)";
            insert.Parameters.AddWithValue("@source", source);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var area = insert.Parameters.Add("@area", SqliteType.Text);
            var scheme = insert.Parameters.Add("@scheme", SqliteType.Text);
            var origin = insert.Parameters.Add("@origin", SqliteType.Text);
            var records = insert.Parameters.Add("@records", SqliteType.Integer);
            foreach (var row in rows) {
                name.Value = row.ScientificName;
                area.Value = row.Area;
                scheme.Value = row.Scheme;
                origin.Value = (object?)row.Origin ?? DBNull.Value;
                records.Value = (object?)row.Records ?? DBNull.Value;
                insert.ExecuteNonQuery();
            }
        }
        if (synonyms is not null) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO checklist_synonym (source, name, accepted) VALUES (@source, @name, @accepted)";
            insert.Parameters.AddWithValue("@source", source);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var accepted = insert.Parameters.Add("@accepted", SqliteType.Text);
            foreach (var (n, a) in synonyms) {
                name.Value = n;
                accepted.Value = a;
                insert.ExecuteNonQuery();
            }
        }
        if (names is not null) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO checklist_name (source, scientific_name, name, name_type, authority) VALUES (@source, @species, @name, @type, @authority)";
            insert.Parameters.AddWithValue("@source", source);
            var scientificName = insert.Parameters.Add("@species", SqliteType.Text);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var type = insert.Parameters.Add("@type", SqliteType.Text);
            var authority = insert.Parameters.Add("@authority", SqliteType.Text);
            foreach (var n in names) {
                scientificName.Value = n.ScientificName;
                name.Value = n.Name;
                type.Value = n.NameType;
                authority.Value = (object?)n.Authority ?? DBNull.Value;
                insert.ExecuteNonQuery();
            }
        }
        if (species is not null) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO checklist_species (source, scientific_name, record_id) VALUES (@source, @name, @record)";
            insert.Parameters.AddWithValue("@source", source);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var record = insert.Parameters.Add("@record", SqliteType.Text);
            foreach (var s in species) {
                name.Value = s.ScientificName;
                record.Value = (object?)s.RecordId ?? DBNull.Value;
                insert.ExecuteNonQuery();
            }
        }
        if (infraspecific is not null) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT OR IGNORE INTO checklist_infraspecific (source, scientific_name, name, rank, authority, note)
                VALUES (@source, @species, @name, @rank, @authority, @note)
                """;
            insert.Parameters.AddWithValue("@source", source);
            var speciesName = insert.Parameters.Add("@species", SqliteType.Text);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var rank = insert.Parameters.Add("@rank", SqliteType.Text);
            var authority = insert.Parameters.Add("@authority", SqliteType.Text);
            var note = insert.Parameters.Add("@note", SqliteType.Text);
            foreach (var i in infraspecific) {
                speciesName.Value = i.SpeciesName;
                name.Value = i.Name;
                rank.Value = i.Rank;
                authority.Value = (object?)i.Authority ?? DBNull.Value;
                note.Value = (object?)i.Note ?? DBNull.Value;
                insert.ExecuteNonQuery();
            }
        }
        using (var info = _connection.CreateCommand()) {
            info.Transaction = tx;
            info.CommandText = "INSERT INTO checklist_source (source, version, licence, url, imported_at) VALUES (@source, @version, @licence, @url, @at)";
            info.Parameters.AddWithValue("@source", source);
            info.Parameters.AddWithValue("@version", (object?)version ?? DBNull.Value);
            info.Parameters.AddWithValue("@licence", (object?)licence ?? DBNull.Value);
            info.Parameters.AddWithValue("@url", (object?)url ?? DBNull.Value);
            info.Parameters.AddWithValue("@at", DateTime.UtcNow.ToString("O"));
            info.ExecuteNonQuery();
        }
        tx.Commit();
    }

    public IReadOnlyList<ChecklistSourceInfo> Sources() {
        using var command = _connection.CreateCommand();
        // A store written before checklist_infraspecific existed, opened read-only, has no such table.
        var subspecies = HasTable("checklist_infraspecific")
            ? "(SELECT COUNT(*) FROM checklist_infraspecific i WHERE i.source = s.source)"
            : "0";
        command.CommandText = $"""
            SELECT s.source, s.version, s.licence, s.url, s.imported_at,
                   (SELECT COUNT(*) FROM checklist_area a WHERE a.source = s.source),
                   (SELECT COUNT(DISTINCT scientific_name) FROM checklist_area a WHERE a.source = s.source),
                   {subspecies}
            FROM checklist_source s ORDER BY s.source
            """;
        using var reader = command.ExecuteReader();
        var list = new List<ChecklistSourceInfo>();
        while (reader.Read()) {
            list.Add(new ChecklistSourceInfo(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3),
                StoredUtc.Parse(reader.GetString(4)), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7)));
        }
        return list;
    }

    /// The source's species that have subspecies lists, with their record ids. Empty for a store
    /// written before checklist_species existed.
    public IEnumerable<ChecklistSpecies> Species(string source) {
        if (!HasTable("checklist_species")) {
            yield break;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scientific_name, record_id FROM checklist_species WHERE source = @source ORDER BY scientific_name";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return new ChecklistSpecies(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
        }
    }

    /// The subspecies the source lists, by species. Empty for a store written before
    /// checklist_infraspecific existed.
    public IEnumerable<ChecklistInfraspecific> Infraspecific(string source) {
        if (!HasTable("checklist_infraspecific")) {
            yield break;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scientific_name, name, rank, authority, note FROM checklist_infraspecific WHERE source = @source ORDER BY scientific_name, name";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return new ChecklistInfraspecific(reader.GetString(0), reader.GetString(1), reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4));
        }
    }

    private bool HasTable(string table) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @table";
        command.Parameters.AddWithValue("@table", table);
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) > 0;
    }

    /// Every row of a source, by scientific name.
    public IEnumerable<ChecklistArea> Rows(string source) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scientific_name, area, scheme, origin, records FROM checklist_area WHERE source = @source ORDER BY scientific_name";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return new ChecklistArea(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4));
        }
    }

    /// The source's names (common names and synonyms), by species.
    public IEnumerable<ChecklistName> Names(string source) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scientific_name, name, name_type, authority FROM checklist_name WHERE source = @source ORDER BY scientific_name";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return new ChecklistName(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3));
        }
    }

    /// The source's synonyms: name to the accepted names it leads to.
    public IReadOnlyDictionary<string, List<string>> Synonyms(string source) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT name, accepted FROM checklist_synonym WHERE source = @source";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        var map = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        while (reader.Read()) {
            var n = reader.GetString(0);
            if (!map.TryGetValue(n, out var list)) {
                map[n] = list = [];
            }
            list.Add(reader.GetString(1));
        }
        return map;
    }

    /// When each country's GBIF counts were downloaded.
    public IReadOnlyDictionary<string, DateTime> GbifFetched() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT country, fetched_at FROM gbif_country_fetch";
        using var reader = command.ExecuteReader();
        var map = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        while (reader.Read()) {
            if (StoredUtc.Parse(reader.GetString(1)) is { } at) {
                map[reader.GetString(0)] = at;
            }
        }
        return map;
    }

    /// Replaces one country's GBIF counts (species key, records).
    public void SaveGbifCountry(string country, IReadOnlyList<(long SpeciesKey, long Records)> counts, DateTime fetchedAt) {
        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM gbif_species_country WHERE country = @country";
            delete.Parameters.AddWithValue("@country", country);
            delete.ExecuteNonQuery();
        }
        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR REPLACE INTO gbif_species_country (country, species_key, records) VALUES (@country, @key, @records)";
            insert.Parameters.AddWithValue("@country", country);
            var key = insert.Parameters.Add("@key", SqliteType.Integer);
            var records = insert.Parameters.Add("@records", SqliteType.Integer);
            foreach (var (k, r) in counts) {
                key.Value = k;
                records.Value = r;
                insert.ExecuteNonQuery();
            }
        }
        using (var done = _connection.CreateCommand()) {
            done.Transaction = tx;
            done.CommandText = "INSERT OR REPLACE INTO gbif_country_fetch (country, fetched_at, species) VALUES (@country, @at, @species)";
            done.Parameters.AddWithValue("@country", country);
            done.Parameters.AddWithValue("@at", fetchedAt.ToString("O"));
            done.Parameters.AddWithValue("@species", counts.Count);
            done.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// Every downloaded GBIF count of these species keys: key, country, records.
    public IEnumerable<(long SpeciesKey, string Country, long Records)> GbifCounts() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT species_key, country, records FROM gbif_species_country";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return (reader.GetInt64(0), reader.GetString(1), reader.GetInt64(2));
        }
    }

    public void Dispose() => _connection.Dispose();

    internal static string Count(long n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
