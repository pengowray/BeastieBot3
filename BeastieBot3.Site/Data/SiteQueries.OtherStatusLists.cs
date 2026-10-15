using Microsoft.Data.Sqlite;
using static BeastieBot3.Site.Data.ReaderValues;

// The lists other than the IUCN Red List (other_status) whose statuses the status update page and the
// species list maker can show beside IUCN's, and the statuses of chosen taxa in one of them.

namespace BeastieBot3.Site.Data;

/// One list of other_status: a system that is one list (ListKey null, "au-epbc"), one list of a system
/// with several (other_status_list: ListKey "jncc-redlist-gb", with the list's country, name and
/// order), or NatureServe's national ranks in one country (Country "US"). Taxa: how many taxa have a
/// status in it. Area: the population or area that at least 90% of its rows give ("Great Britain"),
/// which a status need not repeat; null when there is none.
public sealed record OtherStatusListCount(string System, string? ListKey, string? Country, string? ListCountry, string? ListName,
    int ListSortOrder, int Taxa, string? Area);

/// A taxon's status in one list. Population: the population or area the status applies to, or null
/// for the whole taxon. SourceId: the record's id in the source (the SPRAT taxon id for EPBC).
public sealed record ChosenStatus(long TaxonId, string Status, string? StatusCode, string? Population, string SourceId);

public sealed partial class SiteQueries {
    private sealed record StatusListCache(SiteSnapshot Snapshot, IReadOnlyList<OtherStatusListCount> Lists);
    private volatile StatusListCache? _statusListCache;

    /// Every list of other_status, NatureServe's state and province ranks left out (a list per state
    /// is more than the choice needs); read once per database file. Empty for a database without the
    /// tables.
    public IReadOnlyList<OtherStatusListCount> GetOtherStatusListCounts() {
        var snapshot = _db.Snapshot;
        if (_statusListCache is { } known && ReferenceEquals(known.Snapshot, snapshot)) {
            return known.Lists;
        }
        var lists = ReadOtherStatusListCounts();
        if (snapshot is not null) {
            _statusListCache = new StatusListCache(snapshot, lists);
        }
        return lists;
    }

    private IReadOnlyList<OtherStatusListCount> ReadOtherStatusListCounts() {
        using var connection = _db.OpenConnection();
        var named = new Dictionary<string, (string? Country, string Name, int Order)>(StringComparer.Ordinal);
        var populations = new Dictionary<(string, string?, string?), List<(string? Population, int Rows)>>();
        var result = new List<OtherStatusListCount>();
        try {
            using (var command = connection.CreateCommand()) {
                command.CommandText = "SELECT list_key, country, name, sort_order FROM other_status_list";
                using var reader = command.ExecuteReader();
                while (reader.Read()) {
                    named[reader.GetString(0)] = (Text(reader, 1), reader.GetString(2), reader.GetInt32(3));
                }
            }
            using (var command = connection.CreateCommand()) {
                command.CommandText = $"""
                    SELECT system, list_key, {CountryColumn}, population, COUNT(*)
                    FROM other_status
                    WHERE system <> 'natureserve-subnational'
                    GROUP BY 1, 2, 3, 4
                    """;
                using var reader = command.ExecuteReader();
                while (reader.Read()) {
                    var key = (reader.GetString(0), Text(reader, 1), Text(reader, 2));
                    if (!populations.TryGetValue(key, out var rows)) {
                        populations[key] = rows = [];
                    }
                    rows.Add((Text(reader, 3), reader.GetInt32(4)));
                }
            }
            using (var command = connection.CreateCommand()) {
                command.CommandText = $"""
                    SELECT system, list_key, {CountryColumn}, COUNT(DISTINCT taxon_id)
                    FROM other_status
                    WHERE system <> 'natureserve-subnational'
                    GROUP BY 1, 2, 3
                    """;
                using var reader = command.ExecuteReader();
                while (reader.Read()) {
                    var (system, listKey, country) = (reader.GetString(0), Text(reader, 1), Text(reader, 2));
                    var list = listKey is not null && named.TryGetValue(listKey, out var l) ? l : ((string? Country, string Name, int Order)?)null;
                    var rows = populations.GetValueOrDefault((system, listKey, country)) ?? [];
                    var total = rows.Sum(r => r.Rows);
                    var area = rows.Where(r => r.Population is not null).OrderByDescending(r => r.Rows).FirstOrDefault();
                    result.Add(new OtherStatusListCount(system, listKey, country, list?.Country, list?.Name, list?.Order ?? 0,
                        reader.GetInt32(3), area.Population is not null && area.Rows * 10 >= total * 9 ? area.Population : null));
                }
            }
        } catch (SqliteException) {
            return [];
        }
        return result;
    }

    // NatureServe's national ranks are one list per country; every other system gives its country
    // through the system or the list.
    private const string CountryColumn = "CASE WHEN system = 'natureserve-national' THEN country END";

    // The rows of other_status in one list (OtherStatusListCount): its system, and its list key or
    // country when it has one. AddOtherStatusListParameters binds them. other_status has no index on
    // system, so a query that filters a set of taxa by it scans the table once (200,000 rows in
    // October 2026, about 30 ms) and looks the taxa up by primary key.
    private const string OtherStatusListPredicate =
        "system = @system AND (@list IS NULL OR list_key = @list) AND (@country IS NULL OR country = @country)";

    private static void AddOtherStatusListParameters(SqliteCommand command, OtherStatusListCount list) {
        command.Parameters.AddWithValue("@system", list.System);
        command.Parameters.AddWithValue("@list", (object?)list.ListKey ?? DBNull.Value);
        command.Parameters.AddWithValue("@country", (object?)list.Country ?? DBNull.Value);
    }

    /// The statuses of these taxa in the list, by taxon id: the status of the whole taxon first, then
    /// those of populations by name. Taxa with no status in the list are left out.
    public IReadOnlyDictionary<long, IReadOnlyList<ChosenStatus>> GetStatusesIn(OtherStatusListCount list, IReadOnlyCollection<long> taxonIds) {
        var statuses = new Dictionary<long, List<ChosenStatus>>();
        if (taxonIds.Count == 0) {
            return new Dictionary<long, IReadOnlyList<ChosenStatus>>();
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT taxon_id, status, status_code, population, source_id
            FROM other_status
            WHERE taxon_id IN (SELECT value FROM json_each(@ids)) AND {OtherStatusListPredicate}
            ORDER BY taxon_id, population IS NOT NULL, population, source_id
            """;
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        AddOtherStatusListParameters(command, list);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            if (!statuses.TryGetValue(id, out var rows)) {
                statuses[id] = rows = [];
            }
            rows.Add(new ChosenStatus(id, reader.GetString(1), Text(reader, 2), Text(reader, 3), reader.GetString(4)));
        }
        return statuses.ToDictionary(p => p.Key, p => (IReadOnlyList<ChosenStatus>)p.Value);
    }

    /// How many of these taxa have a status in each list, by (system, list key, country) as in
    /// OtherStatusListCount; lists with none are left out.
    public IReadOnlyDictionary<(string System, string? ListKey, string? Country), int> CountTaxaInLists(IReadOnlyCollection<long> taxonIds) {
        var counts = new Dictionary<(string, string?, string?), int>();
        if (taxonIds.Count == 0) {
            return counts;
        }
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT system, list_key, {CountryColumn}, COUNT(DISTINCT taxon_id)
            FROM other_status
            WHERE taxon_id IN (SELECT value FROM json_each(@ids)) AND system <> 'natureserve-subnational'
            GROUP BY 1, 2, 3
            """;
        command.Parameters.AddWithValue("@ids", "[" + string.Join(',', taxonIds.Distinct()) + "]");
        try {
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                counts[(reader.GetString(0), Text(reader, 1), Text(reader, 2))] = reader.GetInt32(3);
            }
        } catch (SqliteException) {
            return counts;
        }
        return counts;
    }
}
