using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;

// Assessment downloads the IUCN API consistently returns HTTP 500 for. Mirrors the query in
// IucnFailedAssessmentsReportCommand. These all carry an empty geographic-scope array; each taxon
// still has a valid scoped assessment, so there is no coverage gap, but the API and website cannot
// serve these particular records.

namespace BeastieBot3.Audit.Producers;

internal sealed class FailedAssessmentsProducer : IAuditReportProducer {
    public string Id => "failed-assessments";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "failed_requests")) {
            return null;
        }

        var findings = Query(conn, ctx);

        var byStatus = findings
            .GroupBy(f => f.Get("httpStatus") ?? "unknown")
            .OrderBy(g => g.Key)
            .Select(g => new[] { g.Key, g.Count(x => x.Latest == true).ToString("N0"), g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Assessment records the API cannot serve (empty geographic scope)",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.Breaking,
            DataSourceLabel = "IUCN API cache",
            Summary =
                "Each row is an assessment that the public API endpoint /api/v4/assessment/{id} returns HTTP 500 for. " +
                "Across the cache the pattern is consistent: these records carry an empty geographic-scope array, while assessments that carry at least one scope serialise normally. " +
                "On the website the region for these renders as a bare ampersand with no text. Each affected taxon also has a valid scoped assessment, so there is no coverage gap. " +
                "Repairing or removing the empty-scope record would let the API and website serve it.",
            Columns = new List<AuditColumn> {
                AuditColumns.AssessmentId(),
                AuditColumns.TaxonId(),
                AuditColumns.Custom("httpStatus", "HTTP status", AuditColumnType.Number),
                AuditColumns.Latest(),
                AuditColumns.Year(),
                AuditColumns.Custom("attemptCount", "Attempts", AuditColumnType.Number),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
                AuditColumns.Custom("lastError", "Last error", AuditColumnType.LongText),
                AuditColumns.Custom("rootSisId", "Root SIS id", AuditColumnType.Number),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By HTTP status", Headers = new[] { "HTTP status", "Latest", "Total" }, Rows = byStatus, NumericColumns = new[] { 1, 2 } },
            },
        };
    }

    private static IReadOnlyList<AuditFinding> Query(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT f.entity_id, f.last_status, f.attempt_count, f.last_error, f.last_attempt_at, f.next_attempt_after,
       b.sis_id, b.root_sis_id, b.latest, b.year_published
FROM failed_requests f
LEFT JOIN taxa_assessment_backlog b ON b.assessment_id = CAST(f.entity_id AS INTEGER)
WHERE f.endpoint = 'assessment'
ORDER BY (f.last_status IS NULL), f.last_status DESC, b.latest, b.sis_id";

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;

        var findings = new List<AuditFinding>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var entityId = reader.IsDBNull(0) ? null : reader.GetString(0);
            long? assessmentId = long.TryParse(entityId, NumberStyles.Integer, CultureInfo.InvariantCulture, out var aid) ? aid : null;
            int? httpStatus = reader.IsDBNull(1) ? null : reader.GetInt32(1);
            var attempts = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
            var lastError = reader.IsDBNull(3) ? null : reader.GetString(3);
            var lastAttemptAt = reader.IsDBNull(4) ? null : reader.GetString(4);
            var nextAttemptAfter = reader.IsDBNull(5) ? null : reader.GetString(5);
            long? sisId = reader.IsDBNull(6) ? null : reader.GetInt64(6);
            long? rootSisId = reader.IsDBNull(7) ? null : reader.GetInt64(7);
            bool? latest = reader.IsDBNull(8) ? null : reader.GetInt64(8) != 0;
            var year = reader.IsDBNull(9) ? null : reader.GetValue(9)?.ToString();

            var is500 = httpStatus == 500;
            var finding = new AuditFinding {
                ReportId = "failed-assessments",
                Key = entityId,
                TaxonId = sisId,
                AssessmentId = assessmentId,
                RedlistUrl = IucnUrls.Species(sisId, assessmentId),
                YearPublished = year,
                Latest = latest,
                DataSource = "iucn-api-cache",
                Field = "assessment download",
                CurrentValue = httpStatus?.ToString(CultureInfo.InvariantCulture),
                IssueType = is500 ? "api-empty-scope-500" : "api-download-failure",
                SeverityTier = (latest == true ? 10 : 0) + (is500 ? 5 : 3),
                Detail = is500
                    ? "Empty geographic-scope array. /api/v4/assessment/{id} returns HTTP 500 and the website shows the region as a bare ampersand."
                    : $"Assessment download returned HTTP {(httpStatus?.ToString(CultureInfo.InvariantCulture) ?? "no response")}.",
            };
            finding.Extra["httpStatus"] = httpStatus?.ToString(CultureInfo.InvariantCulture);
            finding.Extra["attemptCount"] = attempts.ToString(CultureInfo.InvariantCulture);
            finding.Extra["rootSisId"] = AuditMapping.LongToString(rootSisId);
            finding.Extra["lastError"] = lastError;
            finding.Extra["lastAttemptAt"] = lastAttemptAt;
            finding.Extra["nextAttemptAfter"] = nextAttemptAfter;
            if (is500) {
                finding.Notes.Add("Phantom scope-less duplicate of the taxon's real per-scope assessments, so no coverage gap.");
            }
            findings.Add(finding);
        }

        return findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.TaxonId)
            .ToList();
    }
}
