using BeastieBot3.Site.Lists;
using Microsoft.Data.Sqlite;

// The queries for species from the Catalogue of Life and Wikidata that IUCN does not have
// (extra_species and the tables beside it in SiteDbSchema), for the group pages' lists.

namespace BeastieBot3.Site.Data;

/// One row of higher_taxon_extra_count: extra species under a group with these sources (1 CoL only,
/// 2 Wikidata only, 3 both), placed under a family or a genus, with or without a likely IUCN duplicate.
public sealed record ExtraSpeciesSplit(int Sources, bool UnderFamily, bool IucnLikely, int Count);

/// The extra species under a group, by source, and the group's last descendant node id.
public sealed record ExtraSpeciesCounts(int LastNodeId, int Col, int Wikidata, int Both) {
    /// The same species split by placement and likely IUCN duplicates; empty when not read.
    public IReadOnlyList<ExtraSpeciesSplit> Split { get; init; } = [];

    /// How many extra species a list with these sources has at most. Species placed under a family
    /// are left out with genera=0. When the list includes IUCN and prefers it, the species that are
    /// likely an IUCN taxon are left out, as ListSourceMerge leaves them out of the list; likely
    /// duplicates between CoL and Wikidata entries are still counted.
    public int For(ListSourceOptions sources) {
        var col = sources.Enabled.Contains(ListSource.Col);
        var wikidata = sources.Enabled.Contains(ListSource.Wikidata);
        if (Split.Count == 0) {
            return (col ? Col : 0) + (wikidata ? Wikidata : 0) + (col || wikidata ? Both : 0);
        }
        var iucnFirst = sources.Enabled.Contains(ListSource.Iucn) && sources.Order.Count > 0 && sources.Order[0] == ListSource.Iucn;
        return Split
            .Where(s => (col && (s.Sources & 1) != 0) || (wikidata && (s.Sources & 2) != 0))
            .Where(s => sources.OtherGenera || !s.UnderFamily)
            .Where(s => !(iucnFirst && s.IucnLikely))
            .Sum(s => s.Count);
    }
}

public sealed partial class SiteQueries {
    public ExtraSpeciesCounts? GetExtraSpeciesCounts(int nodeId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT last_node_id, col_count, wikidata_count, both_count FROM higher_taxon_extra WHERE node_id = @id";
        command.Parameters.AddWithValue("@id", nodeId);
        ExtraSpeciesCounts counts;
        using (var reader = command.ExecuteReader()) {
            if (!reader.Read()) {
                return null;
            }
            counts = new ExtraSpeciesCounts(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3));
        }
        command.CommandText = "SELECT sources, under_family, iucn_likely, species_count FROM higher_taxon_extra_count WHERE node_id = @id";
        var split = new List<ExtraSpeciesSplit>();
        using (var reader = command.ExecuteReader()) {
            while (reader.Read()) {
                split.Add(new ExtraSpeciesSplit(reader.GetInt32(0), reader.GetInt64(1) != 0, reader.GetInt64(2) != 0, reader.GetInt32(3)));
            }
        }
        return counts with { Split = split };
    }

    /// How many extra species a list with these sources has at most in each group inside a group,
    /// by node id, counted as ExtraSpeciesCounts.For counts them.
    public IReadOnlyDictionary<int, int> GetExtraSpeciesCountsWithin(GroupRow group, ListSourceOptions sources) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT e.node_id, e.sources, e.under_family, e.iucn_likely, e.species_count
            FROM higher_taxon h JOIN higher_taxon_extra_count e ON e.node_id = h.node_id
            WHERE h.first_pos >= @first AND h.last_pos <= @last AND h.node_id > @id
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        command.Parameters.AddWithValue("@id", group.NodeId);
        var splits = new Dictionary<int, List<ExtraSpeciesSplit>>();
        using (var reader = command.ExecuteReader()) {
            while (reader.Read()) {
                var id = reader.GetInt32(0);
                if (!splits.TryGetValue(id, out var list)) {
                    splits[id] = list = [];
                }
                list.Add(new ExtraSpeciesSplit(reader.GetInt32(1), reader.GetInt64(2) != 0, reader.GetInt64(3) != 0, reader.GetInt32(4)));
            }
        }
        return splits.ToDictionary(s => s.Key, s => new ExtraSpeciesCounts(0, 0, 0, 0) { Split = s.Value }.For(sources));
    }

    private const string ExtraColumns = """
        e.extra_id, e.sources, e.scientific_name, e.wikidata_name, h.kingdom, e.col_id, e.wikidata_qid,
        e.common_name_en, e.enwiki_title, e.node_id, e.sort_pos, e.authority
        """;

    private const string ExtraFrom = "extra_species e JOIN higher_taxon h ON h.node_id = e.node_id";

    /// The extra species placed in a group or the groups under it, in sort order. underGenusOnly leaves
    /// out the species placed under a family because IUCN does not have their genus.
    public IReadOnlyList<ExtraSpeciesRow> GetExtraSpecies(int nodeId, int lastNodeId, bool underGenusOnly = false) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var genusOnly = underGenusOnly ? " AND h.rank = 'genus'" : string.Empty;
        command.CommandText = $"SELECT {ExtraColumns} FROM {ExtraFrom} WHERE e.node_id BETWEEN @first AND @last{genusOnly} ORDER BY e.extra_id";
        command.Parameters.AddWithValue("@first", nodeId);
        command.Parameters.AddWithValue("@last", lastNodeId);
        return ReadExtras(command);
    }

    public IReadOnlyDictionary<int, ExtraSpeciesRow> GetExtraSpeciesByIds(IReadOnlyCollection<int> ids) {
        if (ids.Count == 0) {
            return new Dictionary<int, ExtraSpeciesRow>();
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {ExtraColumns} FROM {ExtraFrom} WHERE e.extra_id IN ({InList(command, "@e", ids)})";
        return ReadExtras(command).ToDictionary(e => e.ExtraId);
    }

    private static List<ExtraSpeciesRow> ReadExtras(SqliteCommand command) {
        using var reader = command.ExecuteReader();
        var rows = new List<ExtraSpeciesRow>();
        while (reader.Read()) {
            var sources = reader.GetInt32(1);
            var name = reader.GetString(2);
            var parts = name.Split(' ', 2);
            rows.Add(new ExtraSpeciesRow(
                reader.GetInt32(0), (sources & 1) != 0, (sources & 2) != 0, name, Text(reader, 3),
                parts[0], parts.Length > 1 ? parts[1] : string.Empty, reader.GetString(4),
                Text(reader, 5), reader.IsDBNull(6) ? null : "Q" + reader.GetInt64(6).ToString(System.Globalization.CultureInfo.InvariantCulture),
                Text(reader, 7), Text(reader, 8), reader.GetInt32(9), reader.GetInt32(10), Text(reader, 11)));
        }
        return rows;
    }

    /// The CoL usage and Wikidata item of each species in a group, with the names CoL and Wikidata
    /// give them when they differ from IUCN's.
    public IReadOnlyDictionary<long, IucnSourceInfo> GetIucnSourceInfo(GroupRow group) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT t.taxon_id, t.col_id, t.wikidata_qid, c.scientific_name, w.scientific_name
            FROM taxon t
            LEFT JOIN taxon_source_name c ON c.taxon_id = t.taxon_id AND c.source = 'col'
            LEFT JOIN taxon_source_name w ON w.taxon_id = t.taxon_id AND w.source = 'wikidata'
            WHERE t.tree_pos BETWEEN @first AND @last AND t.kind = 'species'
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        using var reader = command.ExecuteReader();
        var result = new Dictionary<long, IucnSourceInfo>();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            result[id] = new IucnSourceInfo(id, Text(reader, 1), Text(reader, 2), Text(reader, 3), Text(reader, 4));
        }
        return result;
    }

    /// The overlaps of the extra species in a group, and of the IUCN taxa in it.
    public IReadOnlyList<ExtraOverlapRow> GetExtraOverlaps(GroupRow group, int lastNodeId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT o.extra_id, o.taxon_id, o.other_extra_id, o.reason, o.likely
            FROM extra_overlap o JOIN extra_species e ON e.extra_id = o.extra_id
            WHERE e.node_id BETWEEN @first_node AND @last_node
            UNION
            SELECT o.extra_id, o.taxon_id, o.other_extra_id, o.reason, o.likely
            FROM extra_overlap o JOIN taxon t ON t.taxon_id = o.taxon_id
            WHERE t.tree_pos BETWEEN @first AND @last
            ORDER BY 1, 2, 3
            """;
        command.Parameters.AddWithValue("@first_node", group.NodeId);
        command.Parameters.AddWithValue("@last_node", lastNodeId);
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        using var reader = command.ExecuteReader();
        var rows = new List<ExtraOverlapRow>();
        while (reader.Read()) {
            rows.Add(new ExtraOverlapRow(reader.GetInt32(0), Long(reader, 1), reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.GetString(3), reader.GetInt64(4) != 0));
        }
        return rows;
    }

    /// IUCN taxa by id, for overlaps with taxa outside a group.
    public IReadOnlyDictionary<long, OverlapTaxon> GetOverlapTaxa(IReadOnlyCollection<long> ids) {
        if (ids.Count == 0) {
            return new Dictionary<long, OverlapTaxon>();
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT taxon_id, scientific_name, col_id, wikidata_qid FROM taxon WHERE taxon_id IN ({InList(command, "@t", ids)})";
        using var reader = command.ExecuteReader();
        var result = new Dictionary<long, OverlapTaxon>();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            result[id] = new OverlapTaxon(id, reader.GetString(1), Text(reader, 2), Text(reader, 3));
        }
        return result;
    }

    private static string InList<T>(SqliteCommand command, string prefix, IReadOnlyCollection<T> values) {
        var names = new List<string>();
        var i = 0;
        foreach (var value in values) {
            var name = prefix + i++;
            names.Add(name);
            command.Parameters.AddWithValue(name, value);
        }
        return string.Join(", ", names);
    }
}
