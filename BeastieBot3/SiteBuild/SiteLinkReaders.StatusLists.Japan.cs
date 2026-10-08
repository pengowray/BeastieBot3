using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // Japan's Red List (`statuses japan-import`): each taxon goes to the site taxon with its scientific
    // name in its kingdom (any kingdom for algae, which have none), else the one taxon whose IUCN
    // synonyms include it; one taxon a row, the whole-taxon rows first. A threatened local population
    // (LP) is a row of its own with the population's place, in Japanese, as the area it applies to; no
    // taxon has both. The status is the category in English, with the list's Japanese term under it,
    // and the list's edition in "Published in".
    private static void ReadJapan(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var rows = new List<JapanRow>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT row_id, scientific_name, kingdom, category, category_ja, population, list_version, list_year, source_url
                FROM japan_listing
                ORDER BY CASE WHEN population IS NULL THEN 0 ELSE 1 END, row_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                rows.Add(new JapanRow(reader.GetInt64(0), reader.GetString(1), Text(reader, 2), reader.GetString(3), Text(reader, 4),
                    Text(reader, 5), Text(reader, 6), reader.IsDBNull(7) ? null : reader.GetInt32(7), Text(reader, 8)));
            }
        }
        stats.JapanRows = rows.Count;
        stats.JapanFetched = SourceFetched(connection, OtherStatusSources.Japan);

        // Whole-taxon rows: one per taxon. Local populations: every population of a matched name.
        var whole = rows.Where(r => r.Population is null).ToList();
        var matches = StatusListMatcher.OnePerTaxon(whole, index, r => StatusListNameIndex.Kingdom(r.Kingdom), r => r.Name);
        foreach (var (taxon, row, _) in matches) {
            Add(taxon, row);
        }
        foreach (var population in rows.Where(r => r.Population is not null)) {
            var kingdom = StatusListNameIndex.Kingdom(population.Kingdom);
            if ((index.Find(kingdom, population.Name) ?? index.FindByIucnSynonym(kingdom, population.Name)) is { } taxon) {
                Add(taxon, population);
            }
        }
        stats.JapanMatched = matches.Count;

        void Add(SiteTaxon taxon, JapanRow row) {
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.JapanMoe, JapanCategory(row.Category), row.Category,
                SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), row.Population is { } place ? "Local population: " + place : null,
                OtherStatusSources.Japan,
                row.RowId.ToString(CultureInfo.InvariantCulture), row.SourceUrl, null,
                Report: row.Version is { } version ? (version.Contains(row.Year?.ToString(CultureInfo.InvariantCulture) ?? "~", StringComparison.Ordinal) || row.Year is null ? version : $"{version} ({row.Year})") : null,
                Qualifier: row.CategoryJa));
            stats.JapanSiteRows++;
        }
    }

    // A category of Japan's Red List in English: IA is CR, IB is EN, II is VU; I (not split) is CR or EN.
    internal static string JapanCategory(string code) => code switch {
        "EX" => "Extinct",
        "EW" => "Extinct in the Wild",
        "CR" => "Critically Endangered (IA)",
        "EN" => "Endangered (IB)",
        "CR+EN" => "Critically Endangered or Endangered (I)",
        "VU" => "Vulnerable (II)",
        "NT" => "Near Threatened",
        "DD" => "Data Deficient",
        "LP" => "Threatened local population",
        _ => code,
    };

    private sealed record JapanRow(long RowId, string Name, string? Kingdom, string Category, string? CategoryJa, string? Population,
        string? Version, int? Year, string? SourceUrl);
}
