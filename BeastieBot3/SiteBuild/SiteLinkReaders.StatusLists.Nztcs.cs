using System.Globalization;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // NZTCS: each current assessment with a scientific name (the store leaves informal names out)
    // goes to the one taxon with that name in any kingdom (NZTCS gives no kingdom), else the one
    // taxon whose IUCN synonyms include it. A taxon gets one assessment: matches by name first.
    private static void ReadNztcs(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var rows = new List<(long Id, string Name, string Status, string? Report)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT assessment_id, scientific_name, category, status, report_name
                FROM nztcs_assessment
                WHERE scientific_name IS NOT NULL
                ORDER BY assessment_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (StatusLists.NztcsApi.StatusText(Text(reader, 2), Text(reader, 3)) is { } status) {
                    rows.Add((reader.GetInt64(0), reader.GetString(1), status, Text(reader, 4)));
                }
            }
        }
        stats.NztcsAssessments = rows.Count;
        var matches = StatusListMatcher.OnePerTaxon(rows, index, _ => null, r => r.Name);
        foreach (var (taxon, row, _) in matches) {
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Nztcs, row.Status, null,
                SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), null, OtherStatusSources.Nztcs,
                row.Id.ToString(CultureInfo.InvariantCulture), StatusLists.NztcsApi.AssessmentUrl(row.Id), null, row.Report));
        }
        stats.NztcsMatched = matches.Count;
    }
}
