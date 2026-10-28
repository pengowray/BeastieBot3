using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The seafood guides' ratings for `site build-db --seafood-guides` (seafood_rating in the status lists
// store). Each scientific name of a rating goes to the one taxon with that name in any kingdom, else
// the one taxon whose IUCN synonyms include it; a rating naming several species goes to each of them.
// Names of a genus or group ("Sepia spp.") go to no taxon.

namespace BeastieBot3.SiteBuild;

internal sealed record SiteSeafoodRating(
    long TaxonId,
    string Guide,
    string RatingId,
    string Ratings,
    string? Color,
    string CommonName,
    string? ListedName,
    string? Region,
    string? WaterBody,
    string? Method,
    string? Production,
    string? Fishery,
    string? Note,
    string Url,
    string? Modified);

/// Counts for one guide: ratings in the store, ratings matched to at least one taxon, taxa given a
/// rating, and the species names that matched no taxon.
internal sealed class SeafoodGuideStats {
    public int Ratings;
    public int MatchedRatings;
    public int Taxa;
    public SortedSet<string> UnmatchedNames { get; } = new(StringComparer.Ordinal);
}

internal static partial class SiteLinkReaders {
    public static List<SiteSeafoodRating> ReadSeafoodGuides(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        if (DelimitedTableImporter.GetTableColumns(connection, "seafood_rating") is null) {
            stats.Warnings.Add($"The status lists store {path} has no seafood guide ratings: run seafood goodfish-import and seafood seafoodwatch-import.");
            return [];
        }
        var index = new StatusListNameIndex(taxa.Values);
        var names = new Dictionary<(string Guide, string Id), List<string>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT guide, rating_id, scientific_name FROM seafood_rating_name
                WHERE scientific_name IS NOT NULL ORDER BY guide, rating_id, position
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var key = (reader.GetString(0), reader.GetString(1));
                if (!names.TryGetValue(key, out var list)) {
                    names[key] = list = [];
                }
                list.Add(reader.GetString(2));
            }
        }

        var rows = new List<SiteSeafoodRating>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT guide, rating_id, ratings, color, common_name, region, water_body, method, production, fishery, note, url, modified
                FROM seafood_rating ORDER BY guide, rating_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var guide = reader.GetString(0);
                var id = reader.GetString(1);
                if (!stats.SeafoodGuides.TryGetValue(guide, out var guideStats)) {
                    stats.SeafoodGuides[guide] = guideStats = new SeafoodGuideStats();
                }
                guideStats.Ratings++;
                var matched = new Dictionary<long, string>();
                foreach (var name in names.GetValueOrDefault((guide, id)) ?? []) {
                    if ((index.Find(null, name) ?? index.FindByIucnSynonym(null, name)) is { } taxon) {
                        matched.TryAdd(taxon.TaxonId, name);
                    } else {
                        guideStats.UnmatchedNames.Add(name);
                    }
                }
                if (matched.Count > 0) {
                    guideStats.MatchedRatings++;
                }
                var modified = Text(reader, 12) is { } m && StoredUtc.Parse(m) is { } at
                    ? at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                    : null;
                foreach (var (taxonId, name) in matched) {
                    rows.Add(new SiteSeafoodRating(taxonId, guide, id, reader.GetString(2), Text(reader, 3), reader.GetString(4),
                        SiteBuildRules.OtherListedName(name, taxa[taxonId].ScientificName), Text(reader, 5), Text(reader, 6), Text(reader, 7),
                        Text(reader, 8), Text(reader, 9), Text(reader, 10), reader.GetString(11), modified));
                }
            }
        }
        foreach (var (guide, guideStats) in stats.SeafoodGuides) {
            guideStats.Taxa = rows.Where(r => r.Guide == guide).Select(r => r.TaxonId).Distinct().Count();
            if (guideStats.UnmatchedNames.Count > 0) {
                stats.Warnings.Add($"{guideStats.UnmatchedNames.Count:N0} species names in {guide}'s ratings match no taxon: "
                    + string.Join(", ", guideStats.UnmatchedNames));
            }
        }
        stats.GoodFishFetched = SourceFetched(connection, StatusLists.StatusSources.GoodFish);
        stats.SeafoodWatchFetched = SourceFetched(connection, StatusLists.StatusSources.SeafoodWatch);
        return rows;
    }

    public static void WriteSeafoodRatings(SiteDbWriter writer, IEnumerable<SiteSeafoodRating> rows) =>
        writer.InsertRows("""
            INSERT INTO seafood_rating (taxon_id, guide, rating_id, ratings, color, common_name, listed_name, region, water_body, method,
                production, fishery, note, url, modified)
            VALUES (@t, @g, @i, @r, @c, @n, @l, @re, @w, @m, @p, @f, @no, @u, @mo)
            """,
            ["@t", "@g", "@i", "@r", "@c", "@n", "@l", "@re", "@w", "@m", "@p", "@f", "@no", "@u", "@mo"],
            rows.Select(r => new object?[] { r.TaxonId, r.Guide, r.RatingId, r.Ratings, r.Color, r.CommonName, r.ListedName, r.Region,
                r.WaterBody, r.Method, r.Production, r.Fishery, r.Note, r.Url, r.Modified }));
}
