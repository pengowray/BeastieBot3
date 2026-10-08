using System.Globalization;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ECOS: each listing goes to the taxon with its scientific name, else the one taxon that another
    // name its brackets give names ("Papasula (=Sula) abbotti" gives Sula abbotti), else the one taxon
    // whose IUCN synonyms include its name. A taxon can get several listings: one per population.
    private static void ReadEcos(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var names = new Dictionary<long, List<string>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_id, name FROM ecos_name";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var id = reader.GetInt64(0);
                if (!names.TryGetValue(id, out var list)) {
                    names[id] = list = new List<string>();
                }
                list.Add(reader.GetString(1));
            }
        }
        using var listings = connection.CreateCommand();
        listings.CommandText = """
            SELECT entity_id, scientific_name, kingdom, status, entity_description, listing_date, url
            FROM ecos_listing
            ORDER BY entity_id
            """;
        using var row = listings.ExecuteReader();
        while (row.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            stats.EcosListings++;
            var entityId = row.GetInt64(0);
            var scientificName = row.GetString(1);
            var kingdom = StatusListNameIndex.Kingdom(Text(row, 2));
            var taxon = index.Find(kingdom, scientificName)
                ?? (names.TryGetValue(entityId, out var others) ? others.Select(n => index.Find(kingdom, n)).FirstOrDefault(t => t is not null) : null)
                ?? index.FindByIucnSynonym(kingdom, scientificName);
            if (taxon is null) {
                continue;
            }
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Esa, SiteBuildRules.CollapseWhitespace(row.GetString(3)), null,
                SiteBuildRules.OtherListedName(scientificName, taxon.ScientificName), SiteBuildRules.EcosAppliesTo(Text(row, 4)),
                OtherStatusSources.Ecos, entityId.ToString(CultureInfo.InvariantCulture), row.GetString(6), Text(row, 5)));
            stats.EcosMatched++;
        }
    }
}
