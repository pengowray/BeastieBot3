using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// Cached taxa with no assessment flagged latest=true. These commonly correspond to records that
// were removed, merged, or reclassified, so only historical assessments remain. Mirrors the scan in
// IucnNoCurrentAssessmentReportCommand and reuses IucnTaxaTaxonomyExtractor for taxonomy.

namespace BeastieBot3.Audit.Producers;

internal sealed class NoLatestAssessmentProducer : IAuditReportProducer {
    public string Id => "no-latest";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa") || !AuditContext.ObjectExists(conn, "taxa_assessment_backlog")) {
            return null;
        }

        var findings = Scan(conn, ctx);

        var byClass = findings.GroupBy(f => f.Class ?? "(unspecified)").OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Taxa with no current assessment",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.Advisory,
            DataSourceLabel = "IUCN API",
            Summary =
                "Each row is a taxon from the IUCN API where none of its assessments is flagged as the current (latest) one. " +
                "This commonly happens when a taxon was removed from the Red List, merged into another taxon, or reclassified, so only historical assessments remain. " +
                "The most recent assessment is shown for context. This covers the taxa retrieved from the API, which may not be every taxon in the release. " +
                "The point of interest for these records is how they appear on the Red List website rather than the missing current flag on its own. " +
                "Many do not come up through search on iucnredlist.org, yet each remains reachable through its direct species URL. " +
                "Some of these assessment pages carry a note such as \"(This concept is no longer recognised)\", but it is easy to overlook, and other pages carry no such note at all.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName(),
                AuditColumns.CommonName(),
                AuditColumns.Class(),
                AuditColumns.Order(),
                AuditColumns.Family(),
                AuditColumns.Status("Last status"),
                AuditColumns.Year("Last assessed"),
                AuditColumns.TaxonId(),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By class", Headers = new[] { "Class", "Count" }, Rows = byClass, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static IReadOnlyList<AuditFinding> Scan(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT t.root_sis_id, t.json FROM taxa t
WHERE NOT EXISTS (
    SELECT 1 FROM taxa_assessment_backlog b WHERE b.taxa_id = t.id AND b.latest = 1
)
ORDER BY t.root_sis_id";

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + "\nLIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var findings = new List<AuditFinding>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }
            var json = reader.GetString(1);

            if (JsonHasLatest(json)) {
                continue; // stale-backlog false positive
            }

            var taxonomy = IucnTaxaTaxonomyExtractor.Extract(json);
            var (assessmentId, year, code, url) = MostRecent(json);
            var statusCode = AuditMapping.CodeFromCode(code);

            var finding = new AuditFinding {
                ReportId = "no-latest",
                Key = $"{rootSisId}",
                TaxonId = rootSisId,
                AssessmentId = assessmentId,
                RedlistUrl = !string.IsNullOrEmpty(url) ? url : IucnUrls.Species(rootSisId, assessmentId),
                ScientificName = taxonomy?.ScientificName ?? $"SIS {rootSisId}",
                CommonName = taxonomy?.CommonName,
                Kingdom = taxonomy?.KingdomName,
                Phylum = taxonomy?.PhylumName,
                Class = taxonomy?.ClassName,
                Order = taxonomy?.OrderName,
                Family = taxonomy?.FamilyName,
                StatusCode = statusCode,
                StatusCategory = AuditMapping.CategoryText(code),
                YearPublished = year,
                Latest = false,
                DataSource = "iucn-api",
                Field = "latest",
                CurrentValue = "no latest assessment",
                IssueType = "no-latest-assessment",
                Detail = $"No assessment is flagged current. Most recent assessment: {statusCode ?? "unknown"}{(string.IsNullOrEmpty(year) ? "" : $" ({year})")}.",
            };
            finding.Notes.Add("The taxon may have been removed, merged, or reclassified.");
            findings.Add(finding);
        }

        return findings
            .OrderBy(f => f.Kingdom, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Family, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool JsonHasLatest(string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
                return false;
            }
            foreach (var a in arr.EnumerateArray()) {
                if (a.ValueKind == JsonValueKind.Object && a.TryGetProperty("latest", out var latest) &&
                    (latest.ValueKind == JsonValueKind.True ||
                     (latest.ValueKind == JsonValueKind.String && string.Equals(latest.GetString(), "true", StringComparison.OrdinalIgnoreCase)))) {
                    return true;
                }
            }
        } catch (JsonException) {
            return false;
        }
        return false;
    }

    private static (long? AssessmentId, string? Year, string? Code, string? Url) MostRecent(string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
                return (null, null, null, null);
            }
            long? bestId = null;
            string? bestYear = null, bestCode = null, bestUrl = null;
            var bestYearNum = int.MinValue;
            foreach (var a in arr.EnumerateArray()) {
                if (a.ValueKind != JsonValueKind.Object || !a.TryGetProperty("assessment_id", out var idp)) {
                    continue;
                }
                long? id = idp.ValueKind switch {
                    JsonValueKind.Number => idp.GetInt64(),
                    JsonValueKind.String when long.TryParse(idp.GetString(), out var n) => n,
                    _ => (long?)null,
                };
                if (id is null) {
                    continue;
                }
                var year = a.TryGetProperty("year_published", out var yp)
                    ? (yp.ValueKind == JsonValueKind.String ? yp.GetString() : yp.ValueKind == JsonValueKind.Number ? yp.GetRawText() : null)
                    : null;
                var yearNum = int.TryParse(year, out var yn) ? yn : int.MinValue;
                if (bestId is null || yearNum > bestYearNum) {
                    bestYearNum = yearNum;
                    bestId = id;
                    bestYear = year;
                    bestCode = a.TryGetProperty("red_list_category_code", out var cp) && cp.ValueKind == JsonValueKind.String ? cp.GetString() : null;
                    bestUrl = a.TryGetProperty("url", out var up) && up.ValueKind == JsonValueKind.String ? up.GetString() : null;
                }
            }
            return (bestId, bestYear, bestCode, bestUrl);
        } catch (JsonException) {
            return (null, null, null, null);
        }
    }
}
