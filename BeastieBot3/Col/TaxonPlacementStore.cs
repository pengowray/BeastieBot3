using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

// SQLite file beside the CoL database ("<COL_sqlite>.placement.sqlite") that keeps the Catalogue of
// Life placement (TaxonPlacement.cs) and the caches used to build it.
//
//   meta           CoL file stamp and matcher version the two caches below were filled from.
//   species_match  IUCN (kingdom, genus, species) to accepted CoL ID, or not found.
//   col_node       CoL nodes met while walking lineages (missing = 1 for IDs not in CoL).
//   placement_source  one row per IUCN database a placement was built from: its path and file
//                  stamp, the CoL stamp, the algorithm version and the thresholds used.
//   placement      the placement paths, one row per CoL node, with the anchor's IUCN ranks in
//                  their own columns so a query can ATTACH this file and join on the IUCN columns.
//
// The caches are dropped whenever the CoL file or the matcher version changes. With warm caches a
// rebuild after a new IUCN release or an algorithm change reads only this file and takes seconds.
// LoadOrBuild and Status (TaxonPlacementBuild.cs) are the entry points; Status opens this file
// read-only and does no schema work.

namespace BeastieBot3.Col;

internal sealed class TaxonPlacementStore : SqliteStore {
    /// <summary>Bump when the vote, containment or segment rules change; existing placements are then rebuilt.</summary>
    public const int AlgorithmVersion = 1;

    /// <summary>Bump when the CoL matching rules change; the match and node caches are then refilled.</summary>
    public const int MatcherVersion = 1;

    /// <summary>
    /// The version a placement is stamped with: a change to either the voting rules or the matching
    /// rules makes a stored placement out of date. Stored in placement_source.algorithm_version.
    /// </summary>
    public const int RulesVersion = AlgorithmVersion * 1000 + MatcherVersion;

    private TaxonPlacementStore(SqliteConnection connection) : base(connection) {
    }

    public static string SidecarPath(string colDatabasePath) => colDatabasePath + ".placement.sqlite";

    /// <summary>
    /// The CoL placement for an IUCN database: the stored one when it is current, otherwise built
    /// now and saved. A first build matches every IUCN species against CoL (a few minutes); later
    /// builds reuse the match cache and take seconds.
    /// </summary>
    public static TaxonPlacementIndex LoadOrBuild(
        string iucnDatabasePath,
        string colDatabasePath,
        bool force,
        IPlacementBuildProgress? progress,
        CancellationToken cancellationToken,
        IucnNotAssignedRules? notAssigned = null) =>
        TaxonPlacementBuild.Run(iucnDatabasePath, colDatabasePath,
            new PlacementRunOptions { Force = force, NotAssigned = notAssigned ?? IucnNotAssignedRules.None },
            progress, cancellationToken).Index;

    /// <summary>Whether a current placement is stored for these two databases. Read-only; builds nothing.</summary>
    public static TaxonPlacementStatus Status(string iucnDatabasePath, string colDatabasePath, IucnNotAssignedRules? notAssigned = null) =>
        TaxonPlacementBuild.Status(iucnDatabasePath, colDatabasePath, notAssigned: notAssigned);

    public static TaxonPlacementStore Open(string databasePath) {
        var connection = OpenConnection(databasePath);
        using (var pragma = connection.CreateCommand()) {
            // A rebuildable cache: no fsync per write.
            pragma.CommandText = "PRAGMA synchronous = NORMAL;";
            pragma.ExecuteNonQuery();
        }
        var store = new TaxonPlacementStore(connection);
        store.EnsureSchema();
        return store;
    }

    /// <summary>Test seam: a store over a caller-owned connection, such as an in-memory database.</summary>
    internal static TaxonPlacementStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new TaxonPlacementStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS species_match (
                kingdom TEXT NOT NULL,
                genus TEXT NOT NULL,
                species TEXT NOT NULL,
                match_kind TEXT NOT NULL,
                accepted_id TEXT,
                candidates INTEGER NOT NULL,
                iucn_class TEXT,
                iucn_order TEXT,
                iucn_family TEXT,
                PRIMARY KEY (kingdom, genus, species)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS col_node (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                name TEXT,
                rank TEXT,
                status TEXT,
                kingdom TEXT,
                missing INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS placement_source (
                source_key TEXT PRIMARY KEY,
                iucn_path TEXT NOT NULL,
                iucn_stamp TEXT NOT NULL,
                col_path TEXT,
                col_stamp TEXT NOT NULL,
                algorithm_version INTEGER NOT NULL,
                vote_threshold REAL NOT NULL,
                containment_threshold REAL NOT NULL,
                built_at TEXT NOT NULL,
                species INTEGER NOT NULL,
                matched INTEGER NOT NULL,
                paths INTEGER NOT NULL,
                build_seconds REAL
            );
            CREATE TABLE IF NOT EXISTS placement (
                source_key TEXT NOT NULL,
                span TEXT NOT NULL,
                anchor_key TEXT NOT NULL,
                seq INTEGER NOT NULL,
                kingdom TEXT NOT NULL,
                class_name TEXT,
                order_name TEXT,
                family_name TEXT,
                genus_name TEXT,
                col_id TEXT,
                name TEXT NOT NULL,
                col_rank TEXT NOT NULL,
                show_rank INTEGER NOT NULL,
                PRIMARY KEY (source_key, span, anchor_key, seq)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS ix_placement_anchor
                ON placement (source_key, span, kingdom, class_name, order_name, family_name, genus_name);
            CREATE INDEX IF NOT EXISTS ix_placement_node
                ON placement (source_key, span, name);
            """;
        command.ExecuteNonQuery();
    }

    // ---- file stamps and keys ----

    /// <summary>
    /// Length and last-write ticks of a file. With <paramref name="includeWal"/>, a non-empty
    /// "-wal" file beside it is added, because a database in WAL mode can change without its main
    /// file changing. An empty WAL is ignored, since opening a WAL database read-only can create one.
    /// </summary>
    public static string FileStamp(string path, bool includeWal = false) {
        var info = new FileInfo(path);
        var stamp = info.Exists ? $"{info.Length}:{info.LastWriteTimeUtc.Ticks}" : "missing";
        if (includeWal) {
            var wal = new FileInfo(path + "-wal");
            if (wal.Exists && wal.Length > 0) {
                stamp += $";wal={wal.Length}:{wal.LastWriteTimeUtc.Ticks}";
            }
        }
        return stamp;
    }

    /// <summary>The CoL stamp, in the same form as ColTaxonomyEnricher's cache uses.</summary>
    public static string ColStamp(string colDatabasePath) => FileStamp(colDatabasePath);

    public static string IucnStamp(string iucnDatabasePath) => FileStamp(iucnDatabasePath, includeWal: true);

    /// <summary>
    /// The IUCN file's stamp plus a hash of the rules in rules/iucn-not-assigned.yml, which change
    /// the IUCN order and family the placement votes over.
    /// </summary>
    public static string IucnStamp(string iucnDatabasePath, IucnNotAssignedRules notAssigned) =>
        notAssigned.IsEmpty
            ? IucnStamp(iucnDatabasePath)
            : IucnStamp(iucnDatabasePath) + NotAssignedStampPrefix + notAssigned.Fingerprint;

    internal const string NotAssignedStampPrefix = "|not-assigned:";

    /// <summary>The file part of a stamp from <see cref="IucnStamp(string, IucnNotAssignedRules)"/>.</summary>
    public static string IucnFileStamp(string stamp) {
        var at = stamp.IndexOf(NotAssignedStampPrefix, StringComparison.Ordinal);
        return at < 0 ? stamp : stamp[..at];
    }

    /// <summary>Identifies one IUCN database file at one moment: full path plus its stamp.</summary>
    public static string SourceKey(string iucnDatabasePath, string iucnStamp) =>
        Path.GetFullPath(iucnDatabasePath) + "|" + iucnStamp;

    // ---- caches ----

    /// <summary>
    /// Empties the match and node caches unless they were filled from this CoL file with the
    /// current matcher. Returns true when it emptied them.
    /// </summary>
    public bool ResetCachesUnlessFor(string colStamp) {
        var stored = ReadMeta("col_stamp");
        var storedMatcher = ReadMeta("matcher_version");
        var matcher = MatcherVersion.ToString(CultureInfo.InvariantCulture);
        if (stored == colStamp && storedMatcher == matcher) {
            return false;
        }
        using var transaction = _connection.BeginTransaction();
        using (var command = _connection.CreateCommand()) {
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM species_match; DELETE FROM col_node;";
            command.ExecuteNonQuery();
        }
        WriteMeta("col_stamp", colStamp, transaction);
        WriteMeta("matcher_version", matcher, transaction);
        transaction.Commit();
        return true;
    }

    public Dictionary<string, CachedSpeciesMatch> LoadSpeciesMatches() {
        var result = new Dictionary<string, CachedSpeciesMatch>(StringComparer.Ordinal);
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT kingdom, genus, species, match_kind, accepted_id, candidates, iucn_class, iucn_order, iucn_family FROM species_match";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var key = MatchKey(reader.GetString(0), reader.GetString(1), reader.GetString(2));
            var kind = Enum.TryParse<ColMatchKind>(reader.GetString(3), out var parsed) ? parsed : ColMatchKind.NotFound;
            result[key] = new CachedSpeciesMatch(
                new ColSpeciesMatch(kind, Text(reader, 4), reader.GetInt32(5)),
                Text(reader, 6), Text(reader, 7), Text(reader, 8));
        }
        return result;
    }

    public List<KeyValuePair<string, ColNodeRow?>> LoadNodes() {
        var result = new List<KeyValuePair<string, ColNodeRow?>>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT id, parent_id, name, rank, status, kingdom, missing FROM col_node";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetString(0);
            ColNodeRow? row = reader.GetInt32(6) != 0
                ? null
                : new ColNodeRow(id, Text(reader, 1), Text(reader, 2) ?? string.Empty, Text(reader, 3) ?? string.Empty, Text(reader, 4), Text(reader, 5));
            result.Add(new KeyValuePair<string, ColNodeRow?>(id, row));
        }
        return result;
    }

    public void SaveSpeciesMatches(IEnumerable<(IucnSpeciesKey Species, ColSpeciesMatch Match)> matches) {
        using var transaction = _connection.BeginTransaction();
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT OR REPLACE INTO species_match (kingdom, genus, species, match_kind, accepted_id, candidates, iucn_class, iucn_order, iucn_family)
            VALUES (@kingdom, @genus, @species, @kind, @accepted, @candidates, @class, @order, @family)
            """;
        var kingdom = command.Parameters.Add("@kingdom", SqliteType.Text);
        var genus = command.Parameters.Add("@genus", SqliteType.Text);
        var species = command.Parameters.Add("@species", SqliteType.Text);
        var kind = command.Parameters.Add("@kind", SqliteType.Text);
        var accepted = command.Parameters.Add("@accepted", SqliteType.Text);
        var candidates = command.Parameters.Add("@candidates", SqliteType.Integer);
        var cls = command.Parameters.Add("@class", SqliteType.Text);
        var order = command.Parameters.Add("@order", SqliteType.Text);
        var family = command.Parameters.Add("@family", SqliteType.Text);
        command.Prepare();
        foreach (var (s, m) in matches) {
            kingdom.Value = NormalizeKingdom(s.Kingdom);
            genus.Value = s.Genus.Trim();
            species.Value = s.Species.Trim();
            kind.Value = m.Kind.ToString();
            accepted.Value = (object?)m.AcceptedId ?? DBNull.Value;
            candidates.Value = m.Candidates;
            cls.Value = (object?)Upper(s.ClassName) ?? DBNull.Value;
            order.Value = (object?)Upper(s.OrderName) ?? DBNull.Value;
            family.Value = (object?)Upper(s.FamilyName) ?? DBNull.Value;
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    public void SaveNodes(IEnumerable<KeyValuePair<string, ColNodeRow?>> nodes) {
        using var transaction = _connection.BeginTransaction();
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT OR REPLACE INTO col_node (id, parent_id, name, rank, status, kingdom, missing)
            VALUES (@id, @parent, @name, @rank, @status, @kingdom, @missing)
            """;
        var id = command.Parameters.Add("@id", SqliteType.Text);
        var parent = command.Parameters.Add("@parent", SqliteType.Text);
        var name = command.Parameters.Add("@name", SqliteType.Text);
        var rank = command.Parameters.Add("@rank", SqliteType.Text);
        var status = command.Parameters.Add("@status", SqliteType.Text);
        var kingdom = command.Parameters.Add("@kingdom", SqliteType.Text);
        var missing = command.Parameters.Add("@missing", SqliteType.Integer);
        command.Prepare();
        foreach (var (key, row) in nodes) {
            id.Value = key;
            parent.Value = (object?)row?.ParentId ?? DBNull.Value;
            name.Value = (object?)row?.Name ?? DBNull.Value;
            rank.Value = (object?)row?.Rank ?? DBNull.Value;
            status.Value = (object?)row?.Status ?? DBNull.Value;
            kingdom.Value = (object?)row?.Kingdom ?? DBNull.Value;
            missing.Value = row is null ? 1 : 0;
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    /// <summary>Key of the match cache: kingdom upper case, genus and species as IUCN stores them.</summary>
    public static string MatchKey(string? kingdom, string genus, string species) =>
        NormalizeKingdom(kingdom) + "|" + genus.Trim() + "|" + species.Trim();

    // ---- placements ----

    /// <summary>
    /// Replaces the placement for <paramref name="source"/> with <paramref name="output"/>, and
    /// deletes placements built from earlier versions of the same IUCN file.
    /// </summary>
    public void SavePlacement(PlacementSourceRow source, PlacementBuildOutput output) {
        using var transaction = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = transaction;
            delete.CommandText = """
                DELETE FROM placement WHERE source_key IN (
                    SELECT source_key FROM placement_source WHERE iucn_path = @path OR source_key = @key);
                DELETE FROM placement WHERE source_key = @key;
                DELETE FROM placement_source WHERE iucn_path = @path OR source_key = @key;
                """;
            delete.Parameters.AddWithValue("@path", source.IucnPath);
            delete.Parameters.AddWithValue("@key", source.SourceKey);
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO placement (source_key, span, anchor_key, seq, kingdom, class_name, order_name, family_name, genus_name, col_id, name, col_rank, show_rank)
                VALUES (@key, @span, @anchor, @seq, @kingdom, @class, @order, @family, @genus, @colId, @name, @rank, @show)
                """;
            var key = insert.Parameters.Add("@key", SqliteType.Text);
            var span = insert.Parameters.Add("@span", SqliteType.Text);
            var anchorKey = insert.Parameters.Add("@anchor", SqliteType.Text);
            var seq = insert.Parameters.Add("@seq", SqliteType.Integer);
            var kingdom = insert.Parameters.Add("@kingdom", SqliteType.Text);
            var cls = insert.Parameters.Add("@class", SqliteType.Text);
            var order = insert.Parameters.Add("@order", SqliteType.Text);
            var family = insert.Parameters.Add("@family", SqliteType.Text);
            var genus = insert.Parameters.Add("@genus", SqliteType.Text);
            var colId = insert.Parameters.Add("@colId", SqliteType.Text);
            var name = insert.Parameters.Add("@name", SqliteType.Text);
            var rank = insert.Parameters.Add("@rank", SqliteType.Text);
            var show = insert.Parameters.Add("@show", SqliteType.Integer);
            insert.Prepare();
            key.Value = source.SourceKey;
            foreach (var diag in output.Anchors) {
                if (diag.Kept.Count == 0) {
                    continue;
                }
                var a = diag.Anchor;
                var columns = AnchorColumns(a);
                span.Value = a.Span.ToString();
                anchorKey.Value = a.Key;
                kingdom.Value = columns.Kingdom;
                cls.Value = (object?)columns.ClassName ?? DBNull.Value;
                order.Value = (object?)columns.OrderName ?? DBNull.Value;
                family.Value = (object?)columns.FamilyName ?? DBNull.Value;
                genus.Value = (object?)columns.GenusName ?? DBNull.Value;
                for (var i = 0; i < diag.Kept.Count; i++) {
                    var node = diag.Kept[i];
                    seq.Value = i;
                    colId.Value = node.ColId;
                    name.Value = node.Node.Name;
                    rank.Value = node.Node.ColRank;
                    show.Value = node.Node.ShowRank ? 1 : 0;
                    insert.ExecuteNonQuery();
                }
            }
        }

        using (var row = _connection.CreateCommand()) {
            row.Transaction = transaction;
            row.CommandText = """
                INSERT INTO placement_source (source_key, iucn_path, iucn_stamp, col_path, col_stamp, algorithm_version,
                    vote_threshold, containment_threshold, built_at, species, matched, paths, build_seconds)
                VALUES (@key, @path, @stamp, @colPath, @colStamp, @version, @vote, @containment, @builtAt, @species, @matched, @paths, @seconds)
                """;
            row.Parameters.AddWithValue("@key", source.SourceKey);
            row.Parameters.AddWithValue("@path", source.IucnPath);
            row.Parameters.AddWithValue("@stamp", source.IucnStamp);
            row.Parameters.AddWithValue("@colPath", (object?)source.ColPath ?? DBNull.Value);
            row.Parameters.AddWithValue("@colStamp", source.ColStamp);
            row.Parameters.AddWithValue("@version", source.AlgorithmVersion);
            row.Parameters.AddWithValue("@vote", source.VoteThreshold);
            row.Parameters.AddWithValue("@containment", source.ContainmentThreshold);
            row.Parameters.AddWithValue("@builtAt", source.BuiltAtUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
            row.Parameters.AddWithValue("@species", source.Species);
            row.Parameters.AddWithValue("@matched", source.Matched);
            row.Parameters.AddWithValue("@paths", source.Paths);
            row.Parameters.AddWithValue("@seconds", (object?)source.BuildSeconds ?? DBNull.Value);
            row.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    public TaxonPlacementIndex LoadPlacement(string sourceKey) => LoadPlacement(_connection, sourceKey);

    public PlacementSourceRow? ReadSource(string sourceKey) => ReadSource(_connection, sourceKey);

    internal static TaxonPlacementIndex LoadPlacement(SqliteConnection connection, string sourceKey) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT span, anchor_key, name, col_rank, show_rank
            FROM placement WHERE source_key = @key
            ORDER BY span, anchor_key, seq
            """;
        command.Parameters.AddWithValue("@key", sourceKey);
        var paths = new List<PlacementPath>();
        string? currentSpan = null;
        string? currentAnchor = null;
        List<PlacementNode>? nodes = null;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var span = reader.GetString(0);
            var anchor = reader.GetString(1);
            if (span != currentSpan || anchor != currentAnchor) {
                Flush();
                currentSpan = span;
                currentAnchor = anchor;
                nodes = new List<PlacementNode>();
            }
            nodes!.Add(new PlacementNode(reader.GetString(2), reader.GetString(3), reader.GetInt32(4) != 0));
        }
        Flush();
        return new TaxonPlacementIndex(paths);

        void Flush() {
            if (nodes is { Count: > 0 } && Enum.TryParse<PlacementSpan>(currentSpan, out var parsed)) {
                paths.Add(new PlacementPath(parsed, currentAnchor!, nodes));
            }
        }
    }

    internal static PlacementSourceRow? ReadSource(SqliteConnection connection, string sourceKey) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT source_key, iucn_path, iucn_stamp, col_path, col_stamp, algorithm_version, vote_threshold,
                   containment_threshold, built_at, species, matched, paths, build_seconds
            FROM placement_source WHERE source_key = @key
            """;
        command.Parameters.AddWithValue("@key", sourceKey);
        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadSourceRow(reader) : null;
    }

    /// <summary>The latest placement built from any version of this IUCN file, for saying what changed.</summary>
    internal static PlacementSourceRow? ReadSourceForPath(SqliteConnection connection, string iucnPath) {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT source_key, iucn_path, iucn_stamp, col_path, col_stamp, algorithm_version, vote_threshold,
                   containment_threshold, built_at, species, matched, paths, build_seconds
            FROM placement_source WHERE iucn_path = @path ORDER BY built_at DESC LIMIT 1
            """;
        command.Parameters.AddWithValue("@path", iucnPath);
        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadSourceRow(reader) : null;
    }

    private static PlacementSourceRow ReadSourceRow(SqliteDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetString(2),
        Text(reader, 3),
        reader.GetString(4),
        reader.GetInt32(5),
        reader.GetDouble(6),
        reader.GetDouble(7),
        StoredUtc.Parse(reader.GetString(8)) ?? DateTime.MinValue,
        reader.GetInt32(9),
        reader.GetInt32(10),
        reader.GetInt32(11),
        reader.IsDBNull(12) ? null : reader.GetDouble(12));

    /// <summary>
    /// The IUCN columns stored with each placement row: only the ranks in the anchor's key, as IUCN
    /// stores them (kingdom to family upper case, genus in its own case), so a join on the IUCN view
    /// can use plain equality.
    /// </summary>
    internal static (string Kingdom, string? ClassName, string? OrderName, string? FamilyName, string? GenusName) AnchorColumns(PlacementAnchor a) =>
        a.Span switch {
            PlacementSpan.ClassToOrder => (NormalizeKingdom(a.Kingdom), Upper(a.ClassName), Upper(a.OrderName), null, null),
            PlacementSpan.OrderToFamily => (NormalizeKingdom(a.Kingdom), Upper(a.ClassName), Upper(a.OrderName), Upper(a.FamilyName), null),
            PlacementSpan.FamilyToGenus => (NormalizeKingdom(a.Kingdom), null, null, Upper(a.FamilyName), a.GenusName?.Trim()),
            _ => throw new ArgumentOutOfRangeException(nameof(a), a.Span, null),
        };

    // ---- helpers ----

    private string? ReadMeta(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM meta WHERE key = @key";
        command.Parameters.AddWithValue("@key", key);
        return command.ExecuteScalar() as string;
    }

    private void WriteMeta(string key, string value, SqliteTransaction transaction) {
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT OR REPLACE INTO meta (key, value) VALUES (@key, @value)";
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", value);
        command.ExecuteNonQuery();
    }

    private static string NormalizeKingdom(string? kingdom) => (kingdom ?? string.Empty).Trim().ToUpperInvariant();

    private static string? Upper(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim().ToUpperInvariant();

    private static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}

/// <summary>A cached match plus the IUCN ranks it was chosen with (they break ties between several CoL targets).</summary>
internal sealed record CachedSpeciesMatch(ColSpeciesMatch Match, string? ClassName, string? OrderName, string? FamilyName);

/// <summary>One placement_source row: what a stored placement was built from.</summary>
internal sealed record PlacementSourceRow(
    string SourceKey,
    string IucnPath,
    string IucnStamp,
    string? ColPath,
    string ColStamp,
    int AlgorithmVersion,
    double VoteThreshold,
    double ContainmentThreshold,
    DateTime BuiltAtUtc,
    int Species,
    int Matched,
    int Paths,
    double? BuildSeconds);
