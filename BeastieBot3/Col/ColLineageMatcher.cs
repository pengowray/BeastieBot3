using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Taxonomy;

// Matches one IUCN species to its accepted Catalogue of Life usage and returns that usage's
// lineage (the CoL nodes from the root down to the genus). Used by TaxonPlacementStore to feed
// TaxonPlacementBuilder.
//
// Matching, species rank only:
//   1. CoL rows of rank species with the same genericName and specificEpithet and no
//      infraspecificEpithet. Accepted and provisionally accepted rows whose kingdom matches win.
//   2. Otherwise synonym and ambiguous synonym rows, followed through parentID to their accepted
//      name (this ColDP schema has no acceptedNameUsageID). Synonym rows carry no classification,
//      so the kingdom is checked on the accepted target's lineage.
//   3. Otherwise the same two steps on rows whose scientificName is exactly "Genus species".
// Misapplied names are never matches. When several targets remain, the one whose lineage contains
// the IUCN family name wins, then the order name, then the class name.
//
// Only indexed columns are queried (ID, genericName + specificEpithet, scientificName), with
// prepared commands. Nodes are memoised by CoL ID, so siblings share their ancestors' lookups; a
// caller can seed the memo from a cache and read back the nodes fetched from CoL (NewNodes). The
// CoL connection opens on first use, so a run served entirely from a cache never touches the CoL
// file. Some parentIDs in CoL 26.7 point to rows the importer dropped; a chain ends there and the
// lineage is marked as cut short.

namespace BeastieBot3.Col;

internal enum ColMatchKind {
    NotFound = 0,
    Accepted = 1,
    ProvisionallyAccepted = 2,
    Synonym = 3,
    ScientificName = 4,
}

/// <summary>One CoL nameusage row as the matcher needs it. Null in a memo means the ID is not in CoL.</summary>
internal sealed record ColNodeRow(string Id, string? ParentId, string Name, string Rank, string? Status, string? Kingdom);

/// <summary>The species' accepted CoL usage, or NotFound. Candidates counts the distinct targets in the winning step.</summary>
internal sealed record ColSpeciesMatch(ColMatchKind Kind, string? AcceptedId, int Candidates) {
    public static readonly ColSpeciesMatch NotFound = new(ColMatchKind.NotFound, null, 0);
}

/// <summary>An IUCN species as the matcher sees it: ranks as stored in IUCN.</summary>
internal sealed record IucnSpeciesKey(string Kingdom, string? ClassName, string? OrderName, string? FamilyName, string Genus, string Species);

/// <summary>A lineage and whether its chain ended at a missing parent.</summary>
internal sealed record ColLineage(IReadOnlyList<ColLineageNode> Nodes, bool CutShort);

internal sealed class ColLineageMatcher : IDisposable {
    private const int MaxSynonymHops = 5;

    private readonly Func<SqliteConnection> _openCol;
    private readonly Dictionary<string, ColNodeRow?> _nodes;
    private readonly Dictionary<string, ColLineage> _chains = new(StringComparer.Ordinal);
    private readonly List<string> _newNodeIds = new();

    private SqliteConnection? _connection;
    private SqliteCommand? _byId;
    private SqliteCommand? _byParts;
    private SqliteCommand? _byName;

    /// <param name="openCol">Opens the CoL database; called once, on the first lookup that misses the memo.</param>
    /// <param name="knownNodes">Nodes already known (for example from a cache). Null values mark IDs known to be missing.</param>
    public ColLineageMatcher(Func<SqliteConnection> openCol, IEnumerable<KeyValuePair<string, ColNodeRow?>>? knownNodes = null) {
        _openCol = openCol ?? throw new ArgumentNullException(nameof(openCol));
        _nodes = knownNodes is null
            ? new Dictionary<string, ColNodeRow?>(StringComparer.Ordinal)
            : new Dictionary<string, ColNodeRow?>(knownNodes, StringComparer.Ordinal);
    }

    /// <summary>Opens a CoL database read-only, with a large memory map for the many small lookups.</summary>
    public static SqliteConnection OpenReadOnly(string colDatabasePath) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = colDatabasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        using var pragma = connection.CreateCommand();
        pragma.CommandText = "PRAGMA mmap_size = 8589934592; PRAGMA cache_size = -262144;";
        pragma.ExecuteNonQuery();
        return connection;
    }

    /// <summary>True once the CoL database has been opened.</summary>
    public bool UsedColDatabase => _connection is not null;

    /// <summary>Queries sent to the CoL database.</summary>
    public int ColQueries { get; private set; }

    /// <summary>Nodes (or missing IDs, as null) fetched from CoL since construction, for a cache to store.</summary>
    public IEnumerable<KeyValuePair<string, ColNodeRow?>> NewNodes =>
        _newNodeIds.Select(id => new KeyValuePair<string, ColNodeRow?>(id, _nodes[id]));

    public ColSpeciesMatch Match(IucnSpeciesKey species) {
        var genus = species.Genus?.Trim() ?? string.Empty;
        var epithet = species.Species?.Trim() ?? string.Empty;
        if (genus.Length == 0 || epithet.Length == 0) {
            return ColSpeciesMatch.NotFound;
        }

        var byParts = Candidates(ByParts(), ("@g", genus), ("@s", epithet));
        var match = Choose(byParts, species, fromName: false);
        if (match.Kind != ColMatchKind.NotFound) {
            return match;
        }
        var byName = Candidates(ByName(), ("@n", genus + " " + epithet));
        return Choose(byName, species, fromName: true);
    }

    /// <summary>
    /// The lineage of a CoL usage, broad to narrow from the root, ending at the genus: the usage
    /// itself and every node of rank species or below are left out. Shared between siblings.
    /// </summary>
    public ColLineage LineageOf(string usageId) {
        // Climb to the first node above species rank (a subspecies target climbs past its species).
        var id = usageId;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (true) {
            if (!seen.Add(id)) {
                return new ColLineage(Array.Empty<ColLineageNode>(), true);
            }
            var node = Node(id);
            if (node is null) {
                return new ColLineage(Array.Empty<ColLineageNode>(), true);
            }
            if (!ColRankOrder.IsSpeciesOrBelow(node.Rank)) {
                return ChainTo(id);
            }
            if (string.IsNullOrWhiteSpace(node.ParentId)) {
                return new ColLineage(Array.Empty<ColLineageNode>(), false);
            }
            id = node.ParentId!;
        }
    }

    public void Dispose() {
        _byId?.Dispose();
        _byParts?.Dispose();
        _byName?.Dispose();
        _connection?.Dispose();
    }

    // ---- matching ----

    private ColSpeciesMatch Choose(IReadOnlyList<ColNodeRow> rows, IucnSpeciesKey species, bool fromName) {
        if (rows.Count == 0) {
            return ColSpeciesMatch.NotFound;
        }

        var accepted = rows
            .Where(r => IsAccepted(r.Status) && KingdomMatches(r, species.Kingdom))
            .ToList();
        if (accepted.Count > 0) {
            var best = Best(accepted, species);
            var kind = fromName ? ColMatchKind.ScientificName
                : string.Equals(best.Status?.Trim(), "provisionally accepted", StringComparison.OrdinalIgnoreCase)
                    ? ColMatchKind.ProvisionallyAccepted
                    : ColMatchKind.Accepted;
            return new ColSpeciesMatch(kind, best.Id, accepted.Count);
        }

        var targets = new Dictionary<string, ColNodeRow>(StringComparer.Ordinal);
        foreach (var synonym in rows.Where(r => IsSynonym(r.Status))) {
            if (AcceptedTarget(synonym) is { } target && KingdomMatches(target, species.Kingdom)) {
                targets.TryAdd(target.Id, target);
            }
        }
        if (targets.Count > 0) {
            var best = Best(targets.Values.ToList(), species);
            return new ColSpeciesMatch(fromName ? ColMatchKind.ScientificName : ColMatchKind.Synonym, best.Id, targets.Count);
        }
        return ColSpeciesMatch.NotFound;
    }

    private ColNodeRow? AcceptedTarget(ColNodeRow synonym) {
        var parentId = synonym.ParentId;
        for (var hop = 0; hop < MaxSynonymHops && !string.IsNullOrWhiteSpace(parentId); hop++) {
            var node = Node(parentId!);
            if (node is null) {
                return null;
            }
            if (IsAccepted(node.Status)) {
                return node;
            }
            parentId = node.ParentId;
        }
        return null;
    }

    private ColNodeRow Best(List<ColNodeRow> rows, IucnSpeciesKey species) {
        if (rows.Count == 1) {
            return rows[0];
        }
        return rows
            .Select(r => (Row: r, Names: LineageOf(r.Id).Nodes))
            .OrderByDescending(x => Contains(x.Names, species.FamilyName))
            .ThenByDescending(x => Contains(x.Names, species.OrderName))
            .ThenByDescending(x => Contains(x.Names, species.ClassName))
            .ThenByDescending(x => string.Equals(x.Row.Status?.Trim(), "accepted", StringComparison.OrdinalIgnoreCase))
            .ThenBy(x => x.Row.Id, StringComparer.Ordinal)
            .First().Row;
    }

    private static bool Contains(IReadOnlyList<ColLineageNode> lineage, string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return false;
        }
        var trimmed = name.Trim();
        foreach (var node in lineage) {
            if (string.Equals(node.Name, trimmed, StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }
        return false;
    }

    // The kingdom node on the target's lineage decides. A lineage cut short above the kingdom falls
    // back to the row's own kingdom column; when both are blank the row is accepted.
    private bool KingdomMatches(ColNodeRow target, string? iucnKingdom) {
        if (string.IsNullOrWhiteSpace(iucnKingdom)) {
            return true;
        }
        var kingdom = iucnKingdom.Trim();
        foreach (var node in LineageOf(target.Id).Nodes) {
            if (string.Equals(node.Rank, "kingdom", StringComparison.OrdinalIgnoreCase)) {
                return string.Equals(node.Name, kingdom, StringComparison.OrdinalIgnoreCase);
            }
        }
        return string.IsNullOrWhiteSpace(target.Kingdom)
            || string.Equals(target.Kingdom.Trim(), kingdom, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsAccepted(string? status) =>
        string.IsNullOrWhiteSpace(status) || status.Contains("accepted", StringComparison.OrdinalIgnoreCase);

    private static bool IsSynonym(string? status) =>
        status is not null && status.Contains("synonym", StringComparison.OrdinalIgnoreCase);

    // ---- lineage ----

    private ColLineage ChainTo(string id) {
        if (_chains.TryGetValue(id, out var cached)) {
            return cached;
        }

        // Walk up until a node whose chain is known, the root, a missing parent or a cycle.
        var path = new List<ColNodeRow>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        ColLineage tail = new(Array.Empty<ColLineageNode>(), false);
        string? current = id;
        while (!string.IsNullOrWhiteSpace(current)) {
            if (_chains.TryGetValue(current!, out var known)) {
                tail = known;
                break;
            }
            if (!seen.Add(current!)) {
                tail = new ColLineage(Array.Empty<ColLineageNode>(), true);
                break;
            }
            var node = Node(current!);
            if (node is null) {
                tail = new ColLineage(Array.Empty<ColLineageNode>(), true);
                break;
            }
            path.Add(node);
            current = node.ParentId;
        }

        for (var i = path.Count - 1; i >= 0; i--) {
            var node = path[i];
            var nodes = new ColLineageNode[tail.Nodes.Count + 1];
            for (var j = 0; j < tail.Nodes.Count; j++) {
                nodes[j] = tail.Nodes[j];
            }
            nodes[^1] = new ColLineageNode(node.Id, node.Name, node.Rank);
            tail = new ColLineage(nodes, tail.CutShort);
            _chains[node.Id] = tail;
        }
        return _chains.TryGetValue(id, out var result) ? result : tail;
    }

    private ColNodeRow? Node(string id) {
        if (_nodes.TryGetValue(id, out var known)) {
            return known;
        }
        var command = ById();
        command.Parameters["@id"].Value = id;
        ColQueries++;
        ColNodeRow? row = null;
        using (var reader = command.ExecuteReader()) {
            if (reader.Read()) {
                row = ReadRow(reader);
            }
        }
        Remember(id, row);
        return row;
    }

    private void Remember(string id, ColNodeRow? row) {
        if (_nodes.TryAdd(id, row)) {
            _newNodeIds.Add(id);
        }
    }

    private IReadOnlyList<ColNodeRow> Candidates(SqliteCommand command, params (string Name, string Value)[] parameters) {
        foreach (var (name, value) in parameters) {
            command.Parameters[name].Value = value;
        }
        ColQueries++;
        var rows = new List<ColNodeRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var row = ReadRow(reader);
            rows.Add(row);
            // Accepted candidates are likely targets; remember them so the lineage walk needs no query.
            if (IsAccepted(row.Status)) {
                Remember(row.Id, row);
            }
        }
        return rows;
    }

    private static ColNodeRow ReadRow(SqliteDataReader reader) {
        string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
        return new ColNodeRow(
            reader.GetString(0),
            Text(1),
            Text(2) ?? string.Empty,
            Text(3) ?? string.Empty,
            Text(4),
            Text(5));
    }

    private const string Columns = "ID, parentID, scientificName, rank, status, kingdom";

    private SqliteConnection Connection() => _connection ??= _openCol();

    private SqliteCommand ById() => _byId ??= Prepare(
        $"SELECT {Columns} FROM nameusage WHERE ID = @id LIMIT 1", "@id");

    private SqliteCommand ByParts() => _byParts ??= Prepare(
        $"SELECT {Columns} FROM nameusage WHERE genericName = @g AND specificEpithet = @s AND rank = 'species' " +
        "AND (infraspecificEpithet IS NULL OR infraspecificEpithet = '')", "@g", "@s");

    private SqliteCommand ByName() => _byName ??= Prepare(
        $"SELECT {Columns} FROM nameusage WHERE scientificName = @n " +
        "AND (infraspecificEpithet IS NULL OR infraspecificEpithet = '')", "@n");

    private SqliteCommand Prepare(string sql, params string[] parameterNames) {
        var command = Connection().CreateCommand();
        command.CommandText = sql;
        foreach (var name in parameterNames) {
            command.Parameters.Add(name, SqliteType.Text);
        }
        command.Prepare();
        return command;
    }
}
