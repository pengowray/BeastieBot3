using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

// The tree of groups (higher_taxon): finding groups, their paths, children, ranks, counts and names.

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    private const string GroupColumns = """
        h.node_id, h.parent_node_id, h.depth, h.rank, h.name, h.source, h.show_rank, h.kingdom, h.col_id,
        h.common_name_en, h.common_name_source, h.enwiki_title, h.first_pos, h.last_pos,
        h.species_count, h.infra_count, h.subpopulation_count, h.link_query
        """;

    private static GroupRow GroupAt(SqliteDataReader reader) => new(
        reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetInt32(1), reader.GetInt32(2),
        reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetInt64(6) != 0, reader.GetString(7),
        Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11),
        reader.GetInt32(12), reader.GetInt32(13), reader.GetInt32(14), reader.GetInt32(15), reader.GetInt32(16), Text(reader, 17));

    private IReadOnlyList<GroupRow> ReadGroups(string sql, params (string Name, object Value)[] parameters) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) {
            command.Parameters.AddWithValue(name, value);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<GroupRow>();
        while (reader.Read()) {
            rows.Add(GroupAt(reader));
        }
        return rows;
    }

    /// The groups with this rank and name (folded with SiteNameKey.Fold), in tree order. More than
    /// one when two kingdoms, or two IUCN classes, use the name.
    public IReadOnlyList<GroupRow> FindGroups(string rank, string name) =>
        ReadGroups($"SELECT {GroupColumns} FROM higher_taxon h WHERE h.name_key = @key AND h.rank = @rank ORDER BY h.node_id",
            ("@key", SiteNameKey.Fold(name)), ("@rank", rank.Trim().ToLowerInvariant()));

    /// Groups whose name (folded) is the text, any rank, for search. Biggest first.
    /// Also the groups that have the text as the title of their English Wikipedia article or of a
    /// redirect to it (higher_taxon_name, source 'wikipedia'), after those with the name itself.
    public IReadOnlyList<GroupHit> FindGroupsByName(string text, int limit) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        // MIN(m.how) makes SQLite take m.matched from the row with the lowest `how`: the group's own
        // name before a Wikipedia title.
        command.CommandText = $"""
            SELECT {GroupColumns}, m.matched, MIN(m.how) AS how
            FROM (
                SELECT node_id, NULL AS matched, 0 AS how FROM higher_taxon WHERE name_key = @key
                UNION ALL
                SELECT node_id, name, 1 FROM higher_taxon_name WHERE name_key = @key AND source = 'wikipedia'
            ) m
            JOIN higher_taxon h ON h.node_id = m.node_id
            GROUP BY h.node_id
            ORDER BY how, h.species_count DESC, h.node_id
            LIMIT @limit
            """;
        command.Parameters.AddWithValue("@key", SiteNameKey.Fold(text));
        command.Parameters.AddWithValue("@limit", limit);
        using var reader = command.ExecuteReader();
        var hits = new List<GroupHit>();
        while (reader.Read()) {
            hits.Add(new GroupHit(GroupAt(reader), Text(reader, GroupColumnCount)));
        }
        return hits;
    }

    // The number of columns in GroupColumns, where the columns after them start.
    private const int GroupColumnCount = 18;

    /// The group and every group above it, kingdom first.
    public IReadOnlyList<GroupRow> GetGroupPath(int nodeId) =>
        ReadGroups($"""
            WITH RECURSIVE up(id) AS (
                SELECT @id
                UNION ALL
                SELECT p.parent_node_id FROM higher_taxon p JOIN up ON p.node_id = up.id WHERE p.parent_node_id IS NOT NULL)
            SELECT {GroupColumns} FROM higher_taxon h WHERE h.node_id IN (SELECT id FROM up) ORDER BY h.depth
            """, ("@id", nodeId));

    /// The groups directly under a group, in tree order.
    public IReadOnlyList<GroupRow> GetChildGroups(int nodeId) =>
        ReadGroups($"SELECT {GroupColumns} FROM higher_taxon h WHERE h.parent_node_id = @id ORDER BY h.node_id", ("@id", nodeId));

    /// Every group inside a group (not the group itself), in tree order.
    public IReadOnlyList<GroupRow> GetGroupsWithin(GroupRow group) =>
        ReadGroups($"""
            SELECT {GroupColumns} FROM higher_taxon h
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            ORDER BY h.node_id
            """, ("@first", group.FirstPos), ("@last", group.LastPos), ("@id", group.NodeId));

    /// The ranks of the groups inside a group.
    public IReadOnlyList<GroupRank> GetRanksWithin(GroupRow group) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT h.rank, MIN(h.depth), MIN(h.source = 'col') FROM higher_taxon h
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            GROUP BY h.rank
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        command.Parameters.AddWithValue("@id", group.NodeId);
        using var reader = command.ExecuteReader();
        var ranks = new List<GroupRank>();
        while (reader.Read()) {
            ranks.Add(new GroupRank(reader.GetString(0), reader.GetInt32(1), reader.GetInt64(2) != 0));
        }
        return ranks;
    }

    public IReadOnlyList<GroupCategoryCount> GetGroupCounts(int nodeId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT category, species_count, infra_count, subpopulation_count FROM higher_taxon_count WHERE node_id = @id";
        command.Parameters.AddWithValue("@id", nodeId);
        using var reader = command.ExecuteReader();
        var rows = new List<GroupCategoryCount>();
        while (reader.Read()) {
            rows.Add(new GroupCategoryCount(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3)));
        }
        return rows;
    }

    /// The counts of several groups at once, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<GroupCategoryCount>> GetGroupCounts(IReadOnlyCollection<int> nodeIds) {
        var result = new Dictionary<int, IReadOnlyList<GroupCategoryCount>>();
        if (nodeIds.Count == 0) {
            return result;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var names = new List<string>();
        var i = 0;
        foreach (var id in nodeIds) {
            var name = "@n" + i++;
            names.Add(name);
            command.Parameters.AddWithValue(name, id);
        }
        command.CommandText = $"""
            SELECT node_id, category, species_count, infra_count, subpopulation_count FROM higher_taxon_count
            WHERE node_id IN ({string.Join(", ", names)})
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<GroupCategoryCount>();
            }
            ((List<GroupCategoryCount>)list).Add(new GroupCategoryCount(reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4)));
        }
        return result;
    }

    /// The counts of every group inside a group, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<GroupCategoryCount>> GetGroupCountsWithin(GroupRow group) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT c.node_id, c.category, c.species_count, c.infra_count, c.subpopulation_count
            FROM higher_taxon h JOIN higher_taxon_count c ON c.node_id = h.node_id
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        command.Parameters.AddWithValue("@id", group.NodeId);
        var result = new Dictionary<int, IReadOnlyList<GroupCategoryCount>>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<GroupCategoryCount>();
            }
            ((List<GroupCategoryCount>)list).Add(new GroupCategoryCount(reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4)));
        }
        return result;
    }

    /// The Catalogue of Life's English names of several groups, by node id.
    public IReadOnlyDictionary<int, IReadOnlyList<string>> GetGroupColNames(IReadOnlyCollection<int> nodeIds) {
        var result = new Dictionary<int, IReadOnlyList<string>>();
        if (nodeIds.Count == 0) {
            return result;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var names = new List<string>();
        var i = 0;
        foreach (var id in nodeIds) {
            var name = "@n" + i++;
            names.Add(name);
            command.Parameters.AddWithValue(name, id);
        }
        command.CommandText = $"""
            SELECT node_id, name FROM higher_taxon_name WHERE node_id IN ({string.Join(", ", names)}) AND source = 'col'
            ORDER BY node_id, name COLLATE NOCASE
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt32(0);
            if (!result.TryGetValue(id, out var list)) {
                result[id] = list = new List<string>();
            }
            ((List<string>)list).Add(reader.GetString(1));
        }
        return result;
    }

    /// The Catalogue of Life's English names of a group, with the caps rules applied at build time.
    public IReadOnlyList<string> GetGroupColNames(int nodeId) => GetGroupNames(nodeId, "col");

    /// The group's names from English Wikipedia: the title of its article and the titles of the
    /// redirects to it. Each is a page title on English Wikipedia.
    public IReadOnlyList<string> GetGroupWikipediaNames(int nodeId) => GetGroupNames(nodeId, "wikipedia");

    private IReadOnlyList<string> GetGroupNames(int nodeId, string source) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM higher_taxon_name WHERE node_id = @id AND source = @source ORDER BY name COLLATE NOCASE";
        command.Parameters.AddWithValue("@id", nodeId);
        command.Parameters.AddWithValue("@source", source);
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read()) {
            names.Add(reader.GetString(0));
        }
        return names;
    }
}
