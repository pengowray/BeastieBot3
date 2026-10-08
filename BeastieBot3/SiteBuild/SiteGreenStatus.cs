using System.Globalization;
using System.Text.Json;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;

// IUCN Green Status of Species assessments for `site build-db`: the API cache's green_status table
// (`iucn api green-status`), one row per taxon (its latest assessment), without IUCN's justification
// text.

namespace BeastieBot3.SiteBuild;

/// One taxon's Green Status assessment, as the site database's green_status row holds it.
/// Scores are whole percentages; null when IUCN gives none.
internal sealed record SiteGreenStatus(
    long TaxonId,
    long? RedListAssessmentId,
    string Url,
    string AssessmentDate,
    int? PublishedYear,
    int? RedListYear,
    SiteGreenStatusMetric Recovery,
    SiteGreenStatusMetric Legacy,
    SiteGreenStatusMetric Dependence,
    SiteGreenStatusMetric Gain,
    SiteGreenStatusMetric Potential,
    string? Assessors,
    string? Reviewers,
    string? Contributors,
    string? Facilitators,
    string? Compilers,
    string CitationJson);

/// A category with a score and its range (Species Recovery Score, or a Conservation Impact Metric).
internal sealed record SiteGreenStatusMetric(string? Category, int? Best, int? Min, int? Max);

internal static class SiteGreenStatusReader {
    /// The latest Green Status assessment of each taxon in taxa that the API cache has. A cache with
    /// no green_status table gives none, and a warning.
    public static List<SiteGreenStatus> Read(string apiCachePath, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = SiteLinkReaders.OpenReadOnly(apiCachePath);
        if (Infrastructure.DelimitedTableImporter.GetTableColumns(connection, "green_status") is null) {
            stats.Warnings.Add($"The IUCN API cache {apiCachePath} has no Green Status assessments: run iucn api green-status.");
            return [];
        }
        var rows = new List<SiteGreenStatus>();
        // The Red List assessment's year published: from its cached payload, else from the taxon
        // record that lists it.
        using var redListYear = connection.CreateCommand();
        redListYear.CommandText = Infrastructure.DelimitedTableImporter.GetTableColumns(connection, "taxa_assessment_backlog") is null
            ? "SELECT json_extract(json, '$.year_published') FROM assessments WHERE assessment_id = @id"
            : """
              SELECT COALESCE((SELECT json_extract(json, '$.year_published') FROM assessments WHERE assessment_id = @id),
                              (SELECT year_published FROM taxa_assessment_backlog WHERE assessment_id = @id))
              """;
        var redListYearId = redListYear.Parameters.Add("@id", SqliteType.Integer);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT sis_id, assessment_date, red_list_assessment_id, json, first_seen_version, baseline
            FROM green_status
            ORDER BY sis_id, assessment_date DESC
            """;
        using var reader = command.ExecuteReader();
        long? lastSisId = null;
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var sisId = reader.GetInt64(0);
            if (sisId == lastSisId) {
                continue;
            }
            lastSisId = sisId;
            stats.GreenStatusRecords++;
            if (!taxa.TryGetValue(sisId, out var taxon)) {
                continue;
            }
            var redListId = reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2);
            int? rlYear = null;
            if (redListId is { } rlId) {
                redListYearId.Value = rlId;
                if (redListYear.ExecuteScalar() is { } value and not DBNull
                    && int.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), out var y)) {
                    rlYear = y;
                }
            }
            var publishedYear = !reader.IsDBNull(5) && reader.GetInt64(5) == 0 && !reader.IsDBNull(4) ? ReleaseYear(reader.GetString(4)) : null;
            if (FromJson(taxon, reader.GetString(1), redListId, reader.GetString(3), publishedYear, rlYear) is { } row) {
                rows.Add(row);
            }
        }
        stats.GreenStatusTaxa = rows.Count;
        reader.Close();
        using var fetched = connection.CreateCommand();
        fetched.CommandText = "SELECT MAX(last_seen_at) FROM green_status";
        if (fetched.ExecuteScalar() is string last && Infrastructure.StoredUtc.Parse(last) is { } at) {
            stats.GreenStatusFetched = at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
        return rows;
    }

    /// The site row for one API record (the green_status table's json).
    internal static SiteGreenStatus? FromJson(SiteTaxon taxon, string assessmentDate, long? redListAssessmentId, string json,
        int? publishedYear, int? redListYear) {
        using var document = JsonDocument.Parse(json);
        var r = document.RootElement;
        var url = Text(r, "url");
        if (url is null || !DateOnly.TryParse(assessmentDate, CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)) {
            return null;
        }
        var assessors = Text(r, "assessor_names");
        var parts = new IucnCitationParts {
            TaxonId = taxon.TaxonId,
            AssessmentId = redListAssessmentId ?? 0,
            Year = date.Year,
            ScientificName = taxon.ScientificName,
            SubpopulationName = taxon.SubpopulationName,
            Authors = Authors(assessors, out var etAl),
            AuthorsEtAl = etAl,
        };
        return new SiteGreenStatus(taxon.TaxonId, redListAssessmentId, url, date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            publishedYear, redListYear,
            Metric(r, "species_recovery_category", "species_recovery_score"),
            Metric(r, "conservation_legacy_category", "conservation_legacy"),
            Metric(r, "conservation_dependence_category", "conservation_dependence"),
            Metric(r, "conservation_gain_category", "conservation_gain"),
            Metric(r, "recovery_potential_category", "recovery_potential"),
            assessors, Text(r, "reviewer_names"), Text(r, "contributors"), Text(r, "facilitators"), Text(r, "compilers"),
            parts.ToJson());
    }

    /// The year of a Red List version ("2026-1" gives 2026); null for anything else.
    internal static int? ReleaseYear(string version) =>
        version.Length >= 4 && int.TryParse(version.AsSpan(0, 4), NumberStyles.None, CultureInfo.InvariantCulture, out var year) ? year : null;

    // The assessors as citation authors, read as the Red List's assessor credits are: split into
    // names, then each read as a person, an organisation or a name kept as published.
    private static List<CitationAuthor> Authors(string? names, out bool etAl) {
        etAl = false;
        if (string.IsNullOrWhiteSpace(names)) {
            return [];
        }
        var text = names.Trim();
        if (text.EndsWith("et al.", StringComparison.OrdinalIgnoreCase)) {
            etAl = true;
            text = text[..^"et al.".Length].TrimEnd(' ', ',', '&');
        }
        return CreditNameSplitter.Split(text).Select(n => IucnAuthorNameParser.Parse(n).Author).ToList();
    }

    private static SiteGreenStatusMetric Metric(JsonElement r, string categoryProperty, string scorePrefix) =>
        new(Text(r, categoryProperty), Percent(r, scorePrefix + "_best"), Percent(r, scorePrefix + "_minimum"), Percent(r, scorePrefix + "_maximum"));

    // "22%" or "-42%" as a whole number; null for anything else.
    internal static int? Percent(JsonElement r, string property) {
        if (Text(r, property) is not { } text) {
            return null;
        }
        var number = text.TrimEnd('%').Trim();
        return decimal.TryParse(number, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var value)
            ? (int)Math.Round(value, MidpointRounding.AwayFromZero)
            : null;
    }

    private static string? Text(JsonElement r, string property) =>
        r.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()!.Trim()
            : null;
}
