using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // NatureServe: each record goes to the taxon with its scientific name, else the one taxon that one
    // of its NatureServe synonyms names, else the one taxon whose IUCN synonyms include its name. A
    // taxon gets one record: Standard records are tried before Provisional and Nonstandard ones, and
    // a match by name before any match by a synonym.
    private static void ReadNatureServe(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var records = new List<NatureServeRecord>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT element_global_id, scientific_name, kingdom, g_rank, rounded_g_rank, cosewic_code, sara_code, nsx_url
                FROM natureserve_species
                ORDER BY CASE classification_status WHEN 'Standard' THEN 0 ELSE 1 END, element_global_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                records.Add(new NatureServeRecord(reader.GetInt64(0), reader.GetString(1), Text(reader, 2), Text(reader, 3),
                    Text(reader, 4), Text(reader, 5), Text(reader, 6), reader.GetString(7)));
            }
        }
        var synonyms = new Dictionary<long, List<string>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT element_global_id, name FROM natureserve_synonym";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var id = reader.GetInt64(0);
                if (!synonyms.TryGetValue(id, out var list)) {
                    synonyms[id] = list = new List<string>();
                }
                list.Add(reader.GetString(1));
            }
        }
        stats.NatureServeRecords = records.Count;
        var nations = ReadNatureServeLocalRanks(connection,
            "SELECT element_global_id, nation_code, NULL, rounded_n_rank, native, exotic FROM natureserve_nation", "natureserve_nation");
        var subnations = ReadNatureServeLocalRanks(connection,
            "SELECT element_global_id, nation_code, subnation_code, rounded_s_rank, native, exotic FROM natureserve_subnation", "natureserve_subnation");
        if (nations.Count == 0) {
            stats.Warnings.Add("The status lists store has no NatureServe national or state ranks: run statuses natureserve-fetch --restart once.");
        }

        var matches = StatusListMatcher.OnePerTaxon(records, index, r => StatusListNameIndex.Kingdom(r.Kingdom),
            r => r.ScientificName, r => synonyms.GetValueOrDefault(r.ElementGlobalId));
        stats.NatureServeByName = matches.Count(m => m.Kind == StatusListMatchKind.Name);
        stats.NatureServeBySynonym = matches.Count(m => m.Kind == StatusListMatchKind.OtherName);
        stats.NatureServeByIucnSynonym = matches.Count(m => m.Kind == StatusListMatchKind.IucnSynonym);

        foreach (var (taxon, record, _) in matches) {
            var sourceId = record.ElementGlobalId.ToString(CultureInfo.InvariantCulture);
            var listedName = SiteBuildRules.OtherListedName(record.ScientificName, taxon.ScientificName);
            if (OtherStatusSystems.NatureServeRankMeaning(record.RoundedGRank) is not null) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.NatureServeGlobal, record.GRank ?? record.RoundedGRank!,
                    record.RoundedGRank, listedName, null, OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.NatureServeRanks++;
            }
            if (OtherStatusSystems.CosewicLabel(record.CosewicCode) is { } cosewic) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Cosewic, cosewic, null, listedName, null,
                    OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.CosewicStatuses++;
            }
            if (SiteBuildRules.NullIfBlank(record.SaraCode) is { } sara) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Sara, sara, null, listedName, null,
                    OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.SaraStatuses++;
            }
            foreach (var rank in nations.GetValueOrDefault(record.ElementGlobalId) ?? []) {
                if (LocalRankShown(rank)) {
                    taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.NatureServeNational, rank.Rank, rank.Rank, listedName, null,
                        OtherStatusSources.NatureServe, sourceId, record.Url, null, Country: rank.Nation, Qualifier: LocalRankQualifier(rank)));
                    stats.NatureServeNationalRanks++;
                }
            }
            foreach (var rank in subnations.GetValueOrDefault(record.ElementGlobalId) ?? []) {
                if (LocalRankShown(rank)) {
                    taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.NatureServeSubnational, rank.Rank, rank.Rank, listedName,
                        NatureServePlaces.Name(rank.Nation, rank.Subnation!), OtherStatusSources.NatureServe, sourceId, record.Url, null,
                        Country: rank.Nation, Qualifier: LocalRankQualifier(rank)));
                    stats.NatureServeSubnationalRanks++;
                }
            }
        }
    }

    // A national or subnational rank is shown when it has a rank (1 to 5, H, X, U), or when it is not
    // applicable because the taxon is exotic there. Unranked (NR) and other not applicable ranks
    // (accidental, hybrid, not a reproducing population) are left out, as GNR and GNA are.
    private static bool LocalRankShown(NatureServeLocalRank rank) =>
        OtherStatusSystems.IsRankedLocally(rank.Rank) || (LocalRankQualifier(rank) is not null && rank.Rank.Contains("NA", StringComparison.Ordinal));

    // "exotic" when NatureServe says the taxon is exotic there and not native.
    private static string? LocalRankQualifier(NatureServeLocalRank rank) =>
        rank.Exotic == true && rank.Native != true ? "exotic" : null;

    // natureserve_nation or natureserve_subnation by record; empty when the store does not have the table.
    private static Dictionary<long, List<NatureServeLocalRank>> ReadNatureServeLocalRanks(SqliteConnection connection, string sql, string table) {
        var ranks = new Dictionary<long, List<NatureServeLocalRank>>();
        if (DelimitedTableImporter.GetTableColumns(connection, table) is null) {
            return ranks;
        }
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (Text(reader, 3) is not { } rank) {
                continue;
            }
            var id = reader.GetInt64(0);
            if (!ranks.TryGetValue(id, out var list)) {
                ranks[id] = list = new List<NatureServeLocalRank>();
            }
            list.Add(new NatureServeLocalRank(reader.GetString(1), Text(reader, 2), rank,
                reader.IsDBNull(4) ? null : reader.GetInt64(4) == 1, reader.IsDBNull(5) ? null : reader.GetInt64(5) == 1));
        }
        return ranks;
    }

    private sealed record NatureServeLocalRank(string Nation, string? Subnation, string Rank, bool? Native, bool? Exotic);

    private sealed record NatureServeRecord(long ElementGlobalId, string ScientificName, string? Kingdom, string? GRank,
        string? RoundedGRank, string? CosewicCode, string? SaraCode, string Url);
}
