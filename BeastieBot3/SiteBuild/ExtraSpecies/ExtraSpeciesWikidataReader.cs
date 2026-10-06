using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;

// Reads `wikidata sweep-taxa`'s table (wikidata_taxon_sweep in the Wikidata cache) for the extra
// species: every species item with a plain binomial name, with its kingdom and family worked out by
// following parent taxon (P171) through the non-species items, since the sweep stores no kingdom and
// genus names are reused across kingdoms (Ficus is a plant and a sea snail).

namespace BeastieBot3.SiteBuild.ExtraSpecies;

internal static class ExtraSpeciesWikidataReader {
    private const long KingdomRank = 36732;
    private const long FamilyRank = 35409;
    private const int MaxFamilySteps = 6;
    private const int MaxKingdomSteps = 60;

    /// Instance of (P31) values that make an item something other than a living species: fossil
    /// taxon, synonym, unavailable combination, original combination, extinct taxon.
    internal static readonly IReadOnlySet<long> LeftOutInstances = new HashSet<long> { 23038290, 1040689, 17487588, 14594740, 98961713 };

    /// Whether the cache has the sweep's table, and when the sweep last finished. Null when there is
    /// no table; a warning names what is missing.
    public static bool HasSweep(string cachePath, out string? finished, out string? warning) {
        finished = null;
        warning = null;
        using var connection = SiteLinkReaders.OpenReadOnly(cachePath);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM pragma_table_info('wikidata_taxon_sweep')";
        var columns = new HashSet<string>(StringComparer.Ordinal);
        using (var reader = command.ExecuteReader()) {
            while (reader.Read()) {
                columns.Add(reader.GetString(0));
            }
        }
        if (columns.Count == 0) {
            warning = "The Wikidata cache has no taxon sweep, so no species from Wikidata were added. To add them, run wikidata sweep-taxa.";
            return false;
        }
        if (!columns.Contains("instance_of")) {
            warning = "The Wikidata taxon sweep is from before instance of (P31) was stored, so fossil taxa and synonym items could not be left out. Run wikidata sweep-taxa --restart.";
        }
        using var state = connection.CreateCommand();
        state.CommandText = "SELECT value FROM wikidata_sync_state WHERE key = @key";
        state.Parameters.AddWithValue("@key", WikidataCacheStore.TaxonSweepCompletedKey);
        finished = state.ExecuteScalar() as string;
        return true;
    }

    /// The species items. genusFilter: only items whose genus is one of these names, or whose family
    /// (found through P171) is one of familyFilter (upper case), are returned.
    public static List<WikidataSpeciesRow> Read(string cachePath, IReadOnlySet<string> genusFilter, IReadOnlySet<string> familyFilter,
        ExtraSpeciesStats stats, CancellationToken ct) {
        using var connection = SiteLinkReaders.OpenReadOnly(cachePath);
        var hasInstance = HasColumn(connection, "instance_of");

        // Every item that is not a species: genera, families, clades, kingdoms.
        var higher = new Dictionary<long, (string Name, long? Rank, long[] Parents)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"SELECT qid, taxon_name, rank_qid, parent_qids FROM wikidata_taxon_sweep WHERE rank_qid IS NULL OR rank_qid <> {WikidataTaxonSweep.SpeciesRank}";
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                higher[reader.GetInt64(0)] = (reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetInt64(2),
                    Numbers(reader.IsDBNull(3) ? null : reader.GetString(3)));
            }
        }
        ct.ThrowIfCancellationRequested();

        var kingdoms = new Dictionary<long, string?>();
        string? KingdomOf(long qid, int depth) {
            if (kingdoms.TryGetValue(qid, out var known)) {
                return known;
            }
            if (depth > MaxKingdomSteps || !higher.TryGetValue(qid, out var item)) {
                return null;
            }
            kingdoms[qid] = null; // guards against loops in the parent graph
            string? found = item.Rank == KingdomRank ? item.Name.ToUpperInvariant() : null;
            if (found is null) {
                // An item with two parents in different kingdoms is left without a kingdom.
                var fromParents = item.Parents.Select(p => KingdomOf(p, depth + 1)).Where(k => k is not null).Distinct().ToList();
                found = fromParents.Count == 1 ? fromParents[0] : null;
            }
            kingdoms[qid] = found;
            return found;
        }
        string? FamilyOf(long[] parents) {
            var frontier = parents;
            for (var step = 0; step < MaxFamilySteps && frontier.Length > 0; step++) {
                var next = new List<long>();
                foreach (var p in frontier) {
                    if (!higher.TryGetValue(p, out var item)) {
                        continue;
                    }
                    if (item.Rank == FamilyRank) {
                        return item.Name.ToUpperInvariant();
                    }
                    next.AddRange(item.Parents);
                }
                frontier = next.ToArray();
            }
            return null;
        }

        var rows = new List<WikidataSpeciesRow>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                SELECT qid, taxon_name, parent_qids, col_ids, iucn_taxon_ids, enwiki_title, label_en, {(hasInstance ? "instance_of" : "NULL")}
                FROM wikidata_taxon_sweep WHERE rank_qid = {WikidataTaxonSweep.SpeciesRank}
                """;
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            var n = 0;
            while (reader.Read()) {
                if (++n % 100_000 == 0) {
                    ct.ThrowIfCancellationRequested();
                }
                stats.WikidataRead++;
                if (!reader.IsDBNull(7) && Numbers(reader.GetString(7)).Any(LeftOutInstances.Contains)) {
                    stats.WikidataLeftOutByInstance++;
                    continue;
                }
                if (ExtraSpeciesNameRules.SplitBinomial(reader.GetString(1)) is not { } name) {
                    stats.WikidataNotBinomial++;
                    continue;
                }
                var parents = Numbers(reader.IsDBNull(2) ? null : reader.GetString(2));
                string? family = null;
                if (!genusFilter.Contains(name.Genus)) {
                    family = FamilyOf(parents);
                    if (family is null || !familyFilter.Contains(family)) {
                        stats.WikidataUnplaced++;
                        continue;
                    }
                }
                family ??= FamilyOf(parents);
                var kingdom = parents.Select(p => KingdomOf(p, 0)).Where(k => k is not null).Distinct().ToList();
                rows.Add(new WikidataSpeciesRow(
                    reader.GetInt64(0), name.Genus, name.Epithet,
                    Words(reader.IsDBNull(3) ? null : reader.GetString(3)),
                    Numbers(reader.IsDBNull(4) ? null : reader.GetString(4)),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    kingdom.Count == 1 ? kingdom[0] : null,
                    family));
            }
        }
        return rows;
    }

    private static bool HasColumn(SqliteConnection connection, string column) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM pragma_table_info('wikidata_taxon_sweep') WHERE name = @name";
        command.Parameters.AddWithValue("@name", column);
        return Convert.ToInt64(command.ExecuteScalar()) > 0;
    }

    private static long[] Numbers(string? text) =>
        string.IsNullOrWhiteSpace(text)
            ? []
            : text.Split(' ', StringSplitOptions.RemoveEmptyEntries).Select(s => long.TryParse(s, out var n) ? n : 0).Where(n => n > 0).ToArray();

    private static IReadOnlyList<string> Words(string? text) =>
        string.IsNullOrWhiteSpace(text) ? [] : text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
}
