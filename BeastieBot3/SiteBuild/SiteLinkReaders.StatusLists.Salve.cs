using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // SALVE: each current assessment goes to the animal taxon with its name, else the one animal
    // taxon whose IUCN synonyms include it; one assessment per taxon, matches by name first.
    private static void ReadSalve(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var rows = new List<(string Id, string Name, string Code, string Status, string? AssessedOn, string? Doi)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT ficha_id, scientific_name, category, possibly_extinct, assessed_on, doi
                FROM salve_assessment
                ORDER BY ficha_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var possiblyExtinct = reader.GetInt64(3) == 1;
                if (OtherStatusSystems.SalveLabel(reader.GetString(2), possiblyExtinct) is { } status) {
                    var code = reader.GetString(2).ToUpperInvariant() + (possiblyExtinct && reader.GetString(2) == "CR" ? "(PE)" : "");
                    rows.Add((reader.GetString(0), reader.GetString(1), code, status, Text(reader, 4), Text(reader, 5)));
                }
            }
        }
        stats.SalveAssessments = rows.Count;
        var matches = StatusListMatcher.OnePerTaxon(rows, index, _ => "ANIMALIA", r => r.Name);
        foreach (var (taxon, row, _) in matches) {
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Salve, row.Status, row.Code,
                SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), null, OtherStatusSources.Salve, row.Id,
                StatusLists.SalveApi.AssessmentUrl(row.Id, row.Doi), row.AssessedOn));
        }
        stats.SalveMatched = matches.Count;
    }
}
