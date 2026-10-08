using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // JNCC's Conservation Designations for UK Taxa: each taxon (taxon version key) goes to the taxon with
    // its UKSI recommended name in its kingdom, else the one taxon one of its names as designated finds,
    // else the one taxon whose IUCN synonyms include it. Its designations for the UK, Great Britain or a
    // UK country become one row per list and area (JnccLists): a red list keeps one category per season,
    // the newest; another list joins its statuses ("Schedule 5, section 9.4b; Schedule 5, section 9.5a").
    // Returns the lists the rows use, for other_status_list.
    private static List<OtherStatusList> ReadJncc(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var designations = new List<JnccRow>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT taxon_version_key, scientific_name, designated_name, kingdom, designation_code, designation, status_code,
                       population, area, source, designated_on
                FROM jncc_designation
                WHERE scope IN ('uk', 'country') AND scientific_name IS NOT NULL
                ORDER BY taxon_version_key, designated_on DESC
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                designations.Add(new JnccRow(reader.GetString(0), reader.GetString(1), Text(reader, 2), Text(reader, 3), reader.GetString(4),
                    Text(reader, 5) ?? reader.GetString(4), Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10)));
            }
        }
        stats.JnccDesignations = designations.Count;
        var (fetched, version, citation) = JnccSource(connection);

        var records = designations.GroupBy(d => d.TaxonVersionKey)
            .Select(g => new JnccTaxon(g.Key, JnccName(g.First().ScientificName), g.First().Kingdom,
                g.Select(d => d.DesignatedName).OfType<string>().Select(JnccName).Distinct(StringComparer.Ordinal).ToList(), g.ToList()))
            .ToList();
        var matches = StatusListMatcher.OnePerTaxon(records, index, r => StatusListNameIndex.Kingdom(r.Kingdom), r => r.Name, r => r.OtherNames);
        var lists = new Dictionary<string, OtherStatusList>();
        foreach (var (taxon, record, _) in matches) {
            var classified = record.Rows
                .Select(r => (Row: r, Classified: JnccLists.Classify(r.Code, r.Designation, r.StatusCode, r.Area, r.Population)))
                .Where(c => c.Classified is not null)
                .Select(c => (c.Row, c.Classified!.List, c.Classified.Status, c.Classified.Section))
                .ToList();
            foreach (var group in classified.GroupBy(c => (c.List.Key, Area: c.Row.Area ?? ""))) {
                var list = group.First().List;
                var isRedList = list.Key.StartsWith("jncc-redlist", StringComparison.Ordinal);
                // A red list: one row per season (the status's bracketed season), the newest designation.
                // A law: one row per schedule, its sections on the line under it. Any other list: one
                // row, its statuses joined.
                var shown = isRedList
                    ? group.GroupBy(c => Regex.Match(c.Status, @" \((breeding|non-breeding)\)$").Value)
                        .Select(season => season.OrderByDescending(c => c.Row.DesignatedOn, StringComparer.Ordinal).First())
                        .Select(c => (c.Status, Section: (string?)null, Report: c.Row.Source))
                        .ToList()
                    : group.Any(c => c.Section is not null) || list.SortOrder is >= 40 and < 50
                        ? group.GroupBy(c => c.Status)
                            .Select(schedule => (schedule.Key,
                                Section: JnccLists.Sections(schedule.Select(c => c.Section).OfType<string>().Distinct(StringComparer.Ordinal).ToList()),
                                Report: (string?)null))
                            .ToList()
                        : [(string.Join("; ", group.Select(c => c.Status).Distinct(StringComparer.Ordinal)), Section: (string?)null,
                            Report: list.Key == "jncc-rarity" ? group.First().Row.Source : null)];
                foreach (var (status, section, report) in shown) {
                    taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Jncc, status, null,
                        SiteBuildRules.OtherListedName(group.First().Row.DesignatedName ?? record.Name, taxon.ScientificName),
                        SiteBuildRules.NullIfBlank(group.Key.Area), OtherStatusSources.Jncc, record.TaxonVersionKey,
                        StatusLists.JnccDesignations.ResourcePageUrl, null, report, ListKey: list.Key, Qualifier: section));
                    stats.JnccRows++;
                }
                lists.TryAdd(list.Key, JnccLists.ToListRow(list, fetched, version, citation));
            }
        }
        stats.JnccMatched = matches.Count;
        return lists.Values.ToList();
    }

    // "Bombus (Thoracobombus) humilis" -> "Bombus humilis": IUCN names have no subgenus.
    private static string JnccName(string name) => Regex.Replace(name.Trim(), @"^(\S+) \([A-Z][a-z]+\) ", "$1 ");

    // The download date, the date in the workbook's file name and the attribution JNCC asks for.
    private static (string? Fetched, string? Version, string? Citation) JnccSource(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT fetched_at, version, citation FROM status_source WHERE source = 'jncc'";
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return (null, null, null);
        }
        var fetched = Text(reader, 0) is { } at && StoredUtc.Parse(at) is { } utc ? utc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : null;
        var version = Text(reader, 1) is { } file && Regex.Match(file, @"(\d{4})(\d{2})(\d{2})") is { Success: true } m
            ? $"{m.Groups[1].Value}-{m.Groups[2].Value}-{m.Groups[3].Value}"
            : Text(reader, 1);
        return (fetched, version, Text(reader, 2));
    }

    private sealed record JnccRow(string TaxonVersionKey, string ScientificName, string? DesignatedName, string? Kingdom, string Code,
        string Designation, string? StatusCode, string? Population, string? Area, string? Source, string? DesignatedOn);

    private sealed record JnccTaxon(string TaxonVersionKey, string Name, string? Kingdom, IReadOnlyList<string> OtherNames,
        IReadOnlyList<JnccRow> Rows);
}
