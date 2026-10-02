using System.Net;
using Microsoft.Data.Sqlite;

// Reads the IUCN CSV export database (IUCN_<release>.sqlite) for `site build-db`: every taxon in
// taxonomy_html, and the latest assessments in assessments_html. Only the columns the site needs are
// selected; the narrative columns (rationale, habitat, threats ...) are never read.
//
// Authority of an infraspecific taxon. The CSV has an authority and an infraAuthority column; in
// 2026-1 infraAuthority is empty in all 3,438 infraspecific rows, and authority is the infraspecific
// taxon's own authority: it differs from its species' authority in 1,960 of the 2,082 non-nominate
// infraspecific taxa whose species is in the CSV (Panthera pardus ssp. kotiya "Deraniyagala, 1956",
// species "(Linnaeus, 1758)"), and it equals the API's taxon.authority for the infraspecific
// taxon. So authority is stored, and infraAuthority is only used when authority is empty.

namespace BeastieBot3.SiteBuild;

internal static class SiteIucnCsvReader {
    public static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    /// The Red List version of the export ("2026-1"). A database holds one release (iucn import
    /// enforces it); more than one is an error.
    public static string ReadRelease(SqliteConnection csv) {
        using var command = csv.CreateCommand();
        command.CommandText = "SELECT DISTINCT redlist_version FROM import_metadata WHERE redlist_version IS NOT NULL AND redlist_version <> ''";
        using var reader = command.ExecuteReader();
        var versions = new List<string>();
        while (reader.Read()) {
            versions.Add(reader.GetString(0).Trim());
        }
        return versions.Count switch {
            1 => versions[0],
            0 => throw new InvalidOperationException("The IUCN database names no Red List version in import_metadata."),
            _ => throw new InvalidOperationException($"The IUCN database holds more than one Red List version: {string.Join(", ", versions)}."),
        };
    }

    /// Every taxon of taxonomy_html in taxon id order, or the first `limit` of them.
    public static List<SiteTaxon> ReadTaxa(SqliteConnection csv, int? limit, CancellationToken cancellationToken) {
        using var command = csv.CreateCommand();
        command.CommandText = """
            SELECT taxonId, scientificName, kingdomName, phylumName, className, orderName, familyName, genusName,
                   speciesName, infraType, infraName, infraAuthority, subpopulationName, authority
            FROM taxonomy_html
            ORDER BY taxonId
            """ + (limit is { } n ? " LIMIT @limit" : string.Empty);
        if (limit is { } max) {
            command.Parameters.AddWithValue("@limit", max);
        }
        using var reader = command.ExecuteReader();
        var taxa = new List<SiteTaxon>();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var scientificName = SiteBuildRules.CleanName(Text(reader, 1));
            if (scientificName.Length == 0) {
                continue;
            }
            var subpopulation = SiteBuildRules.NullIfBlank(Text(reader, 12));
            var kind = SiteBuildRules.KindOf(Text(reader, 9), subpopulation);
            var authority = Decode(Text(reader, 13)) ?? Decode(Text(reader, 11));
            taxa.Add(new SiteTaxon {
                TaxonId = reader.GetInt64(0),
                ScientificName = scientificName,
                Kind = kind,
                Kingdom = SiteBuildRules.NullIfBlank(Text(reader, 2)),
                Phylum = SiteBuildRules.NullIfBlank(Text(reader, 3)),
                ClassName = SiteBuildRules.NullIfBlank(Text(reader, 4)),
                OrderName = SiteBuildRules.NullIfBlank(Text(reader, 5)),
                Family = SiteBuildRules.NullIfBlank(Text(reader, 6)),
                Genus = SiteBuildRules.NullIfBlank(Text(reader, 7)),
                SpeciesEpithet = SiteBuildRules.NullIfBlank(Text(reader, 8)),
                InfraRank = SiteBuildRules.InfraRankMarker(scientificName),
                InfraName = SiteBuildRules.NullIfBlank(Text(reader, 10)),
                SubpopulationName = subpopulation,
                Authority = authority,
            });
        }
        return taxa;
    }

    private sealed record CsvRow(long AssessmentId, long TaxonId, string? Category, string? Criteria, string? Year, string? Date,
        string? CriteriaVersion, string? Trend, string? PossiblyExtinct, string? PossiblyExtinctInTheWild, IReadOnlyList<string> Regions);

    /// The assessments of the given taxa, all of them latest for their scope (the export holds
    /// current assessments only); scopes are assigned per taxon by SiteBuildRules.AssignCsvScopes.
    /// Rows with no scope are left out and counted; the API has no scope for them either.
    public static List<SiteAssessment> ReadAssessments(SqliteConnection csv, IReadOnlyDictionary<long, SiteTaxon> taxa,
        SiteBuildStats stats, CancellationToken cancellationToken) {
        var byTaxon = new Dictionary<long, List<CsvRow>>();
        using (var command = csv.CreateCommand()) {
            command.CommandText = """
                SELECT assessmentId, taxonId, redlistCategory, redlistCriteria, yearPublished, assessmentDate,
                       criteriaVersion, populationTrend, possiblyExtinct, possiblyExtinctInTheWild, scopes
                FROM assessments_html
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var taxonId = reader.GetInt64(1);
                if (!taxa.ContainsKey(taxonId)) {
                    continue;
                }
                stats.CsvAssessments++;
                if (!byTaxon.TryGetValue(taxonId, out var list)) {
                    byTaxon[taxonId] = list = new List<CsvRow>();
                }
                list.Add(new CsvRow(reader.GetInt64(0), taxonId, Text(reader, 2), Text(reader, 3), Text(reader, 4), Text(reader, 5),
                    Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9), SiteBuildRules.CsvRegions(Text(reader, 10))));
            }
        }

        var rows = new List<SiteAssessment>();
        foreach (var list in byTaxon.Values) {
            // Row order decides ties between regions; sort so a rebuild assigns the same scopes.
            list.Sort((a, b) => a.AssessmentId.CompareTo(b.AssessmentId));
            var scopes = SiteBuildRules.AssignCsvScopes(list.Select(r => r.Regions).ToList());
            for (var i = 0; i < list.Count; i++) {
                var row = list[i];
                if (scopes[i] is not { } scope) {
                    stats.CsvAssessmentsNoScope++;
                    continue;
                }
                var category = SiteBuildRules.CategoryCodeFromCsv(row.Category);
                if (category is null) {
                    stats.CsvAssessmentsUnknownCategory++;
                    stats.Warnings.Add($"Assessment {row.AssessmentId} has a category the build does not know: '{row.Category}'. Stored as written.");
                    category = SiteBuildRules.NullIfBlank(row.Category);
                    if (category is null) {
                        continue;
                    }
                }
                rows.Add(new SiteAssessment {
                    AssessmentId = row.AssessmentId,
                    TaxonId = row.TaxonId,
                    Scope = scope,
                    IsLatest = true,
                    Category = category,
                    PossiblyExtinct = SiteBuildRules.CsvFlag(row.PossiblyExtinct),
                    PossiblyExtinctInTheWild = SiteBuildRules.CsvFlag(row.PossiblyExtinctInTheWild),
                    Criteria = SiteBuildRules.NullIfBlank(row.Criteria),
                    CriteriaVersion = SiteBuildRules.CriteriaVersion(row.CriteriaVersion),
                    YearPublished = SiteBuildRules.Year(row.Year),
                    AssessmentDate = SiteBuildRules.UtcDate(row.Date),
                    PopulationTrend = SiteBuildRules.NullIfBlank(row.Trend),
                    FromCsv = true,
                });
            }
        }
        return rows;
    }

    private static string? Decode(string? text) {
        var trimmed = SiteBuildRules.NullIfBlank(text);
        return trimmed is null ? null : SiteBuildRules.NullIfBlank(WebUtility.HtmlDecode(trimmed));
    }

    private static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
