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
// An import replaces all of a source's rows in one transaction.

namespace BeastieBot3.Checklists;

/// One taxon's record for one area in a source. Origin: native, introduced, endemic, vagrant,
/// extinct, uncertain, or null when the source does not say.
internal sealed record ChecklistArea(string ScientificName, string Area, string Scheme, string? Origin);

internal sealed record ChecklistSourceInfo(string Source, string? Version, string? Licence, string? Url, DateTime? ImportedAt, long Rows, long Taxa);

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
            CREATE TABLE IF NOT EXISTS checklist_synonym (
                source   TEXT NOT NULL,
                name     TEXT NOT NULL,
                accepted TEXT NOT NULL,
                PRIMARY KEY (source, name, accepted)
            ) WITHOUT ROWID;
            """;
        command.ExecuteNonQuery();
    }

    /// Replaces the source's rows. A name and area given twice keeps the first origin given.
    public void Replace(string source, string? version, string? licence, string? url, IEnumerable<ChecklistArea> rows,
        IEnumerable<(string Name, string Accepted)>? synonyms = null) {
        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM checklist_area WHERE source = @source; DELETE FROM checklist_synonym WHERE source = @source; DELETE FROM checklist_source WHERE source = @source;";
            delete.Parameters.AddWithValue("@source", source);
            delete.ExecuteNonQuery();
        }
        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO checklist_area (source, scientific_name, area, scheme, origin) VALUES (@source, @name, @area, @scheme, @origin)";
            insert.Parameters.AddWithValue("@source", source);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var area = insert.Parameters.Add("@area", SqliteType.Text);
            var scheme = insert.Parameters.Add("@scheme", SqliteType.Text);
            var origin = insert.Parameters.Add("@origin", SqliteType.Text);
            foreach (var row in rows) {
                name.Value = row.ScientificName;
                area.Value = row.Area;
                scheme.Value = row.Scheme;
                origin.Value = (object?)row.Origin ?? DBNull.Value;
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
        command.CommandText = """
            SELECT s.source, s.version, s.licence, s.url, s.imported_at,
                   (SELECT COUNT(*) FROM checklist_area a WHERE a.source = s.source),
                   (SELECT COUNT(DISTINCT scientific_name) FROM checklist_area a WHERE a.source = s.source)
            FROM checklist_source s ORDER BY s.source
            """;
        using var reader = command.ExecuteReader();
        var list = new List<ChecklistSourceInfo>();
        while (reader.Read()) {
            list.Add(new ChecklistSourceInfo(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3),
                StoredUtc.Parse(reader.GetString(4)), reader.GetInt64(5), reader.GetInt64(6)));
        }
        return list;
    }

    /// Every row of a source, by scientific name.
    public IEnumerable<ChecklistArea> Rows(string source) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT scientific_name, area, scheme, origin FROM checklist_area WHERE source = @source ORDER BY scientific_name";
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            yield return new ChecklistArea(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3));
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

    public void Dispose() => _connection.Dispose();

    internal static string Count(long n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
