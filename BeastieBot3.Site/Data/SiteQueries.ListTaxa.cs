using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;
using static BeastieBot3.Site.Data.ReaderValues;

// The taxa of a group for its lists, and the areas and regions that a list can be compared with.

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    /// The taxa of a group that have a latest global assessment, in tree order, of the given kinds. A
    /// subpopulation with no English name of its own has its species' name.
    public IReadOnlyList<ListTaxonRow> GetListTaxa(GroupRow group, IReadOnlyCollection<string> kinds) =>
        [.. ListTaxa(group, kinds, area: null).Select(t => t.Row)];

    /// The group's taxa of these kinds whose latest global assessment codes the area, with the record.
    public IReadOnlyList<(ListTaxonRow Row, AreaRecord Record)> GetListTaxaInArea(GroupRow group, IReadOnlyCollection<string> kinds, string area) =>
        [.. ListTaxa(group, kinds, area).Select(t => (t.Row, t.Record!))];

    /// The group's taxa with an assessment in the IUCN region, each with its latest one there (the
    /// one IUCN flags latest, else the newest) in place of the global one.
    public IReadOnlyList<ListTaxonRow> GetListTaxaInRegion(GroupRow group, IReadOnlyCollection<string> kinds, string region) =>
        [.. ListTaxa(group, kinds, area: null, region).Select(t => t.Row)];

    /// The area's records for these taxa; taxa with none are left out.
    public IReadOnlyDictionary<long, AreaRecord> GetAreaRecords(string area, IReadOnlyCollection<long> taxonIds) {
        var records = new Dictionary<long, AreaRecord>();
        if (taxonIds.Count == 0) {
            return records;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT taxon_id, origin, presence, endemic FROM taxon_area
            WHERE area = @area AND taxon_id IN (SELECT value FROM json_each(@ids))
            """;
        command.Parameters.AddWithValue("@area", area);
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            records[reader.GetInt64(0)] = new AreaRecord((AreaOrigin)reader.GetInt32(1), (AreaPresence)reader.GetInt32(2), reader.GetInt64(3) != 0);
        }
        return records;
    }

    private sealed record RegionCache(SiteSnapshot Snapshot, IReadOnlyList<(string Region, int Taxa)> Regions);
    private volatile RegionCache? _regionCache;

    /// The IUCN regions with assessments ("Europe"), with how many taxa have one, most first; read
    /// once per database file.
    public IReadOnlyList<(string Region, int Taxa)> Regions() {
        var snapshot = _db.Snapshot;
        if (_regionCache is { } known && ReferenceEquals(known.Snapshot, snapshot)) {
            return known.Regions;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT scope, COUNT(DISTINCT taxon_id) FROM assessment
            WHERE scope <> 'Global' AND TRIM(scope) <> ''
            GROUP BY scope ORDER BY 2 DESC, 1
            """;
        using var reader = command.ExecuteReader();
        var regions = new List<(string, int)>();
        while (reader.Read()) {
            regions.Add((reader.GetString(0), reader.GetInt32(1)));
        }
        if (snapshot is not null) {
            _regionCache = new RegionCache(snapshot, regions);
        }
        return regions;
    }

    private sealed record AreaCache(SiteSnapshot Snapshot, AreaNames Names);
    private volatile AreaCache? _areaCache;

    /// The area names, read once per database file.
    public AreaNames AreaNames() {
        var snapshot = _db.Snapshot;
        if (_areaCache is { } known && ReferenceEquals(known.Snapshot, snapshot)) {
            return known.Names;
        }
        var names = new AreaNames(GetAreas());
        if (snapshot is not null) {
            _areaCache = new AreaCache(snapshot, names);
        }
        return names;
    }

    /// The countries and areas the latest global assessments code (the area table), for choosing one.
    public IReadOnlyList<AreaName> GetAreas() {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT code, name, country FROM area ORDER BY name";
        using var reader = command.ExecuteReader();
        var areas = new List<AreaName>();
        while (reader.Read()) {
            areas.Add(new AreaName(reader.GetString(0), reader.GetString(1), Text(reader, 2)));
        }
        return areas;
    }

    private List<(ListTaxonRow Row, AreaRecord? Record)> ListTaxa(GroupRow group, IReadOnlyCollection<string> kinds, string? area, string? region = null) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        var kindNames = new List<string>();
        var i = 0;
        foreach (var kind in kinds) {
            var name = "@k" + i++;
            kindNames.Add(name);
            command.Parameters.AddWithValue(name, kind);
        }
        if (kindNames.Count == 0) {
            return [];
        }
        command.CommandText = $"""
            SELECT t.taxon_id, t.scientific_name, t.kind, t.kingdom, t.genus, t.species_epithet, t.infra_rank, t.infra_name,
                   t.subpopulation_name, COALESCE(t.common_name_en, p.common_name_en), t.list_article_title, t.list_parent_article_title,
                   t.parent_taxon_id, t.node_id, t.tree_pos, a.assessment_id, a.category, a.possibly_extinct, a.possibly_extinct_in_the_wild,
                   a.year_published, t.authority{(area is null ? "" : ", x.origin, x.presence, x.endemic")}
            FROM taxon t {(region is null ? "LEFT JOIN assessment a ON a.assessment_id = t.latest_global_assessment_id" : """
                JOIN assessment a ON a.assessment_id = (SELECT r.assessment_id FROM assessment r WHERE r.taxon_id = t.taxon_id AND r.scope = @region
                    ORDER BY r.is_latest DESC, r.year_published DESC, r.assessment_id DESC LIMIT 1)
                """)}
            LEFT JOIN taxon p ON p.taxon_id = t.parent_taxon_id AND t.kind = 'subpopulation'
            {(area is null ? "" : "JOIN taxon_area x ON x.taxon_id = t.taxon_id AND x.area = @area")}
            WHERE t.tree_pos BETWEEN @first AND @last AND t.kind IN ({string.Join(", ", kindNames)})
            ORDER BY t.tree_pos
            """;
        command.Parameters.AddWithValue("@first", group.FirstPos);
        command.Parameters.AddWithValue("@last", group.LastPos);
        if (area is not null) {
            command.Parameters.AddWithValue("@area", area);
        }
        if (region is not null) {
            command.Parameters.AddWithValue("@region", region);
        }
        using var reader = command.ExecuteReader();
        var rows = new List<(ListTaxonRow, AreaRecord?)>();
        while (reader.Read()) {
            rows.Add((new ListTaxonRow(
                reader.GetInt64(0), reader.GetString(1), reader.GetString(2), Text(reader, 3), Text(reader, 4), Text(reader, 5),
                Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11),
                Long(reader, 12), reader.GetInt32(13), reader.GetInt32(14), Long(reader, 15), Text(reader, 16),
                !reader.IsDBNull(17) && reader.GetInt64(17) != 0, !reader.IsDBNull(18) && reader.GetInt64(18) != 0,
                reader.IsDBNull(19) ? null : reader.GetInt32(19), Text(reader, 20)),
                area is null ? null : new AreaRecord((AreaOrigin)reader.GetInt32(21), (AreaPresence)reader.GetInt32(22), reader.GetInt64(23) != 0)));
        }
        return rows;
    }
}
