using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// Classifications from sources that give no countries (Datastore:classifications_sqlite, else
// classifications.sqlite in the datastore folder), for the public site's comparison of
// classifications (`classifications import`, then `site build-db`):
//   classification_source  one row per source: its version, licence, where it came from, when imported;
//   classification_node    the source's accepted species and every group above them, each with its
//                          parent: id as the source gives it (an ITIS TSN, a WoRMS AphiaID, an IPNI id
//                          for Plants of the World Online), or '<rank>:<name>' for a group the source
//                          names in a column only;
//   classification_name    the names of the species, for matching IUCN's: each accepted species' own
//                          name, and each synonym with the species it leads to; name_key is
//                          SiteNameKey.Fold of the name;
//   classification_link    an id in another scheme that the source gives a species ('iucn': AviList's
//                          BirdLife id, which is the IUCN taxon id).
// An import replaces all of a source's rows in one transaction.

namespace BeastieBot3.Classifications;

/// One taxon of a source: Rank as the source names it, lower case ("order", "superfamily").
internal sealed record ClassificationNode(string Id, string? ParentId, string Rank, string Name);

/// A name of one of the source's species: its own name, or a synonym of it.
internal sealed record ClassificationName(string Name, string NodeId, bool Synonym);

/// An id of a species in another scheme.
internal sealed record ClassificationLink(string Scheme, string Value, string NodeId);

internal sealed record ClassificationSourceInfo(string Source, string? Version, string? Licence, string? Url, DateTime? ImportedAt, long Nodes,
    long Species);

internal sealed class ClassificationStore : SqliteStore {
    private ClassificationStore(SqliteConnection connection) : base(connection) {
    }

    public static ClassificationStore Open(string path) {
        var store = new ClassificationStore(OpenConnection(path));
        store.EnsureSchema();
        return store;
    }

    /// Null when the file is missing.
    public static ClassificationStore? OpenReadOnly(string path) {
        if (!File.Exists(path)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
        connection.Open();
        return new ClassificationStore(connection);
    }

    internal static ClassificationStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new ClassificationStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS classification_source (
                source      TEXT PRIMARY KEY,
                version     TEXT,
                licence     TEXT,
                url         TEXT,
                imported_at TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS classification_node (
                source    TEXT NOT NULL,
                id        TEXT NOT NULL,
                parent_id TEXT,
                rank      TEXT NOT NULL,
                name      TEXT NOT NULL,
                PRIMARY KEY (source, id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS classification_name (
                source   TEXT NOT NULL,
                name_key TEXT NOT NULL,
                node_id  TEXT NOT NULL,
                synonym  INTEGER NOT NULL,
                PRIMARY KEY (source, name_key, node_id, synonym)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS classification_link (
                source  TEXT NOT NULL,
                scheme  TEXT NOT NULL,
                value   TEXT NOT NULL,
                node_id TEXT NOT NULL,
                PRIMARY KEY (source, scheme, value, node_id)
            ) WITHOUT ROWID;
            """;
        command.ExecuteNonQuery();
    }

    /// Replaces the source's rows. A node or name given twice keeps the first.
    public void Replace(string source, string? version, string? licence, string? url, IEnumerable<ClassificationNode> nodes,
        IEnumerable<ClassificationName> names, IEnumerable<ClassificationLink>? links = null) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, """
            DELETE FROM classification_node WHERE source = @source; DELETE FROM classification_name WHERE source = @source;
            DELETE FROM classification_link WHERE source = @source; DELETE FROM classification_source WHERE source = @source;
            """, ("@source", source));
        Insert(tx, "INSERT OR IGNORE INTO classification_node (source, id, parent_id, rank, name) VALUES (@source, @a, @b, @c, @d)", source,
            nodes.Select(n => new object?[] { n.Id, n.ParentId, n.Rank, n.Name }));
        Insert(tx, "INSERT OR IGNORE INTO classification_name (source, name_key, node_id, synonym) VALUES (@source, @a, @b, @c)", source,
            names.Select(n => new object?[] { SiteNameKey.Fold(n.Name), n.NodeId, n.Synonym ? 1 : 0 }));
        Insert(tx, "INSERT OR IGNORE INTO classification_link (source, scheme, value, node_id) VALUES (@source, @a, @b, @c)", source,
            (links ?? []).Select(l => new object?[] { l.Scheme, l.Value, l.NodeId }));
        Execute(tx, "INSERT INTO classification_source (source, version, licence, url, imported_at) VALUES (@source, @version, @licence, @url, @at)",
            ("@source", source), ("@version", version), ("@licence", licence), ("@url", url), ("@at", DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture)));
        tx.Commit();
    }

    private void Execute(SqliteTransaction tx, string sql, params (string Name, object? Value)[] parameters) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        command.ExecuteNonQuery();
    }

    // Parameters @a, @b, @c ... take each row's values in order.
    private void Insert(SqliteTransaction tx, string sql, string source, IEnumerable<object?[]> rows) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        command.Parameters.AddWithValue("@source", source);
        SqliteParameter[]? parameters = null;
        foreach (var row in rows) {
            parameters ??= [.. row.Select((_, i) => command.Parameters.Add("@" + (char)('a' + i), SqliteType.Text))];
            for (var i = 0; i < row.Length; i++) {
                parameters[i].Value = row[i] ?? DBNull.Value;
            }
            command.ExecuteNonQuery();
        }
    }

    public IReadOnlyList<ClassificationSourceInfo> Sources() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT s.source, s.version, s.licence, s.url, s.imported_at,
                   (SELECT COUNT(*) FROM classification_node n WHERE n.source = s.source),
                   (SELECT COUNT(*) FROM classification_node n WHERE n.source = s.source AND n.rank = 'species')
            FROM classification_source s ORDER BY s.source
            """;
        using var reader = command.ExecuteReader();
        var list = new List<ClassificationSourceInfo>();
        while (reader.Read()) {
            list.Add(new ClassificationSourceInfo(reader.GetString(0), Text(reader, 1), Text(reader, 2), Text(reader, 3),
                StoredUtc.Parse(reader.GetString(4)), reader.GetInt64(5), reader.GetInt64(6)));
        }
        return list;
    }

    /// The node with this id, or null.
    public ClassificationNode? Node(string source, string id) {
        _nodeCommand ??= Prepare("SELECT parent_id, rank, name FROM classification_node WHERE source = @source AND id = @id", "@source", "@id");
        _nodeCommand.Parameters["@source"].Value = source;
        _nodeCommand.Parameters["@id"].Value = id;
        using var reader = _nodeCommand.ExecuteReader();
        return reader.Read() ? new ClassificationNode(id, Text(reader, 0), reader.GetString(1), reader.GetString(2)) : null;
    }

    /// The species nodes whose own name (synonym false) or a synonym (synonym true) has this key.
    public IReadOnlyList<string> NodesNamed(string source, string nameKey, bool synonym) {
        _nameCommand ??= Prepare("SELECT node_id FROM classification_name WHERE source = @source AND name_key = @key AND synonym = @synonym",
            "@source", "@key", "@synonym");
        _nameCommand.Parameters["@source"].Value = source;
        _nameCommand.Parameters["@key"].Value = nameKey;
        _nameCommand.Parameters["@synonym"].Value = synonym ? 1 : 0;
        using var reader = _nameCommand.ExecuteReader();
        var ids = new List<string>();
        while (reader.Read()) {
            ids.Add(reader.GetString(0));
        }
        return ids;
    }

    /// Every link of a scheme: value to the species nodes it names.
    public IReadOnlyDictionary<string, List<string>> Links(string source, string scheme) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value, node_id FROM classification_link WHERE source = @source AND scheme = @scheme";
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@scheme", scheme);
        using var reader = command.ExecuteReader();
        var map = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        while (reader.Read()) {
            var value = reader.GetString(0);
            if (!map.TryGetValue(value, out var list)) {
                map[value] = list = [];
            }
            list.Add(reader.GetString(1));
        }
        return map;
    }

    private SqliteCommand? _nodeCommand;
    private SqliteCommand? _nameCommand;

    private SqliteCommand Prepare(string sql, params string[] parameters) {
        var command = _connection.CreateCommand();
        command.CommandText = sql;
        foreach (var p in parameters) {
            command.Parameters.Add(p, SqliteType.Text);
        }
        return command;
    }

    private static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);

    public override void Dispose() {
        _nodeCommand?.Dispose();
        _nameCommand?.Dispose();
        base.Dispose();
    }

    internal static string Count(long n) => n.ToString("N0", CultureInfo.InvariantCulture);
}
