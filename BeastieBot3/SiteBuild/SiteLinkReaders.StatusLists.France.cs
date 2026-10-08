using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // France (`statuses france-import`, PatriNat's BDC Statuts): the national red list's current rows
    // (france_status.is_current) for metropolitan France and each overseas territory, and the national
    // and overseas protection lists. Each TAXREF accepted name (cd_ref) goes to the site taxon whose
    // IUCN taxon id TAXREF links to it (a link on the accepted name first), else to the taxon with one of
    // its TAXREF names in its kingdom, else of an IUCN synonym. Regional red lists and regional and
    // departmental protection are stored but not shown.
    private static void ReadFrance(SqliteConnection connection, IReadOnlyDictionary<long, SiteTaxon> siteTaxa, StatusListNameIndex index,
        SiteBuildStats stats, CancellationToken cancellationToken) {
        var groups = new Dictionary<long, FranceTaxon>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT s.row_number, s.cd_nom, s.cd_ref, s.type_code, s.code, s.label, s.criteria, s.population,
                       COALESCE(t.name_en, t.name), s.name, s.kingdom, d.title, d.year, d.citation
                FROM france_status s
                JOIN france_territory t ON t.territory_code = s.territory_code
                LEFT JOIN france_document d ON d.cd_doc = s.cd_doc
                WHERE (s.type_code = 'LRN' AND s.is_current = 1) OR s.type_code IN ('PN', 'POM')
                ORDER BY s.cd_ref, s.type_code, s.row_number
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var cdRef = reader.GetInt64(2);
                if (!groups.TryGetValue(cdRef, out var group)) {
                    groups[cdRef] = group = new FranceTaxon(cdRef, new List<FranceRow>(), new List<string>(), new List<(long, bool)>());
                }
                group.Rows.Add(new FranceRow(reader.GetInt64(0), reader.GetInt64(1), reader.GetString(3), reader.GetString(4), Text(reader, 5),
                    Text(reader, 6), Text(reader, 7), reader.GetString(8), reader.GetString(9), Text(reader, 10), Text(reader, 11),
                    reader.IsDBNull(12) ? null : reader.GetInt32(12), Text(reader, 13)));
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT cd_ref, name, cd_nom = cd_ref FROM france_taxref_name ORDER BY cd_ref, cd_nom = cd_ref DESC, cd_nom";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (groups.TryGetValue(reader.GetInt64(0), out var group)) {
                    group.Names.Add(reader.GetString(1));
                }
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT cd_ref, external_id, cd_nom = cd_ref FROM france_taxref_link
                WHERE source IN ('IUCN Red List', 'IUCN Red List > BirdLife')
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (groups.TryGetValue(reader.GetInt64(0), out var group)
                    && long.TryParse(reader.GetString(1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var iucnId)) {
                    group.IucnIds.Add((iucnId, reader.GetInt64(2) == 1));
                }
            }
        }
        stats.FranceRows = groups.Values.Sum(g => g.Rows.Count);
        stats.FranceFetched = SourceFetched(connection, OtherStatusSources.France);

        var taken = new HashSet<long>();
        var byName = new List<FranceTaxon>();
        foreach (var group in groups.Values) {
            var linked = group.IucnIds.OrderByDescending(l => l.OnAccepted).Select(l => siteTaxa.GetValueOrDefault(l.Id))
                .FirstOrDefault(t => t is not null && t.Kind != SiteTaxonKind.Subpopulation && !taken.Contains(t.TaxonId));
            if (linked is not null) {
                taken.Add(linked.TaxonId);
                AddFrance(linked, group, stats);
                stats.FranceByTaxrefLink++;
            } else {
                byName.Add(group);
            }
        }
        var matches = StatusListMatcher.OnePerTaxon(byName.Where(g => g.Names.Count > 0 || g.Rows.Count > 0), index,
            g => StatusListNameIndex.Kingdom(g.Rows[0].Kingdom), g => g.Names.FirstOrDefault() ?? g.Rows[0].Name, g => g.Names.Skip(1));
        foreach (var (taxon, group, _) in matches) {
            if (taken.Add(taxon.TaxonId)) {
                AddFrance(taxon, group, stats);
                stats.FranceByName++;
            }
        }
    }

    private static void AddFrance(SiteTaxon taxon, FranceTaxon group, SiteBuildStats stats) {
        foreach (var row in group.Rows) {
            var redList = row.Type == "LRN";
            var place = row.Population is { } population ? $"{row.Place}, {FrancePopulation(population)}" : row.Place;
            var status = redList ? FranceRedListCategory(row.Code) : "Protected";
            var second = redList
                ? row.Criteria is { } criteria ? "Criteria: " + criteria : null
                : FranceArticle(row.Label);
            // The red list's chapter, or the order with its date ("Arrêté interministériel du 23 avril 2007 fixant la liste des
            // insectes protégés ..."), else the list's title in the status label.
            var report = redList
                ? row.DocumentTitle is { } title ? (row.DocumentYear is { } year ? $"{title} ({year})" : title) : null
                : row.DocumentCitation ?? (row.Label is { } label && label.LastIndexOf(':') is > 0 and var colon ? label[..colon].Trim() : row.Label);
            taxon.OtherStatuses.Add(new OtherStatus(redList ? OtherStatusSystems.FranceRedList : OtherStatusSystems.FranceProtection, status,
                redList ? row.Code : null, SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), place, OtherStatusSources.France,
                row.RowNumber.ToString(CultureInfo.InvariantCulture), null, null, report,
                Qualifier: second, ListedUnder: null));
            stats.FranceSiteRows++;
        }
    }

    // A red list category in English; CR* is critically endangered and possibly extinct (or regionally extinct).
    private static string FranceRedListCategory(string code) => code switch {
        "CR*" => "Critically Endangered (possibly extinct)",
        _ => IucnCategoryName(code) ?? (code == "NE" ? "Not Evaluated" : code),
    };

    private static string FrancePopulation(string population) => population switch {
        "breeding" => "breeding",
        "wintering" => "wintering",
        "visiting" => "on passage",
        _ => population,
    };

    // "Liste des oiseaux protégés ... : Article 3" -> "Article 3".
    private static string? FranceArticle(string? label) {
        if (label is null) {
            return null;
        }
        var colon = label.LastIndexOf(':');
        var article = colon >= 0 ? label[(colon + 1)..].Trim() : null;
        return string.IsNullOrEmpty(article) ? null : article;
    }

    private sealed record FranceTaxon(long CdRef, List<FranceRow> Rows, List<string> Names, List<(long Id, bool OnAccepted)> IucnIds);

    private sealed record FranceRow(long RowNumber, long CdNom, string Type, string Code, string? Label, string? Criteria, string? Population,
        string Place, string Name, string? Kingdom, string? DocumentTitle, int? DocumentYear, string? DocumentCitation);
}
