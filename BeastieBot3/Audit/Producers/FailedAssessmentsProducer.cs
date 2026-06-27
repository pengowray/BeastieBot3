using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;

// Assessment downloads the IUCN API consistently returns HTTP 500 for. Mirrors the query in
// IucnFailedAssessmentsReportCommand. These all carry an empty geographic-scope array; each taxon
// still has a valid scoped assessment, so there is no coverage gap, but the API and website cannot
// serve these particular records. The API has no usable payload for them, so the species name and
// taxonomy shown here are filled in from the offline CSV export (joined by taxonId), which still
// carries each taxon and its real scoped assessment.

namespace BeastieBot3.Audit.Producers;

internal sealed class FailedAssessmentsProducer : IAuditReportProducer {
    public string Id => "failed-assessments";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "failed_requests")) {
            return null;
        }

        var raw = ReadRaw(conn, ctx);
        var (taxa, assess) = LoadCsv(ctx, raw.Select(r => r.SisId).OfType<long>());

        var findings = raw
            .Select(r => Build(r, taxa, assess))
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.TaxonId)
            .ToList();

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
            DataSourceLabel = "IUCN API, with species and taxonomy from the CSV export",
            Summary =
                "Each row is an assessment that the public API endpoint /api/v4/assessment/{id} returns HTTP 500 for. " +
                "The pattern is consistent: these records carry an empty geographic-scope array, while assessments that carry at least one scope serialise normally. " +
                "On the website the region for these renders as a bare ampersand with no text. Each affected taxon also has a valid scoped assessment, so there is no coverage gap. " +
                "The API has no usable payload for these records, so the species name, taxonomy, and assessed category shown here come from the offline CSV export (matched on the taxon's SIS id), which still carries each taxon and its real scoped assessment. " +
                "Repairing or removing the empty-scope record would let the API and website serve it.",
            Columns = new List<AuditColumn> {
                new() {
                    Key = "scientificName", Header = "Scientific name", Type = AuditColumnType.Taxon,
                    Value = f => f.ScientificName, Href = f => f.Get("realUrl") ?? f.RedlistUrl,
                    Help = "Filled in from the offline CSV export, since the API cannot serve the record.",
                },
                AuditColumns.Custom("authority", "Authority", AuditColumnType.Code),
                AuditColumns.Class(),
                AuditColumns.Order(),
                AuditColumns.Family(),
                AuditColumns.Status("Assessed as"),
                AuditColumns.Custom("realScope", "Assessed scope", AuditColumnType.Text,
                    "The geographic scope of the taxon's real (servable) assessment in the CSV export."),
                AuditColumns.AssessmentId("Failed assessment"),
                AuditColumns.Custom("httpStatus", "HTTP status", AuditColumnType.Number),
                AuditColumns.Latest(),
                AuditColumns.Year(),
                AuditColumns.Custom("attemptCount", "Attempts", AuditColumnType.Number),
                AuditColumns.TaxonId(),
                AuditColumns.RedlistLink("Red List (fails)"),
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

    private sealed record RawFail(
        string? EntityId, long? AssessmentId, int? HttpStatus, int Attempts, string? LastError,
        string? LastAttemptAt, string? NextAttemptAfter, long? SisId, long? RootSisId, bool? Latest, string? Year);

    private static IReadOnlyList<RawFail> ReadRaw(SqliteConnection connection, AuditContext ctx) {
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

        var rows = new List<RawFail>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var entityId = reader.IsDBNull(0) ? null : reader.GetString(0);
            long? assessmentId = long.TryParse(entityId, NumberStyles.Integer, CultureInfo.InvariantCulture, out var aid) ? aid : null;
            rows.Add(new RawFail(
                entityId,
                assessmentId,
                reader.IsDBNull(1) ? null : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8) != 0,
                reader.IsDBNull(9) ? null : reader.GetValue(9)?.ToString()));
        }
        return rows;
    }

    private static AuditFinding Build(RawFail r, IReadOnlyDictionary<long, CsvTaxon> taxa, IReadOnlyDictionary<long, CsvAssessment> assess) {
        var is500 = r.HttpStatus == 500;
        CsvTaxon? taxon = r.SisId is { } sid && taxa.TryGetValue(sid, out var t) ? t : null;
        CsvAssessment? real = r.SisId is { } sid2 && assess.TryGetValue(sid2, out var a) ? a : null;

        var finding = new AuditFinding {
            ReportId = "failed-assessments",
            Key = r.EntityId,
            TaxonId = r.SisId,
            AssessmentId = r.AssessmentId,
            RedlistUrl = IucnUrls.Species(r.SisId, r.AssessmentId),
            ScientificName = AuditMapping.Decode(taxon?.ScientificName),
            Kingdom = taxon?.Kingdom,
            Phylum = taxon?.Phylum,
            Class = taxon?.Class,
            Order = taxon?.Order,
            Family = taxon?.Family,
            Genus = taxon?.Genus,
            Species = taxon?.Species,
            StatusCategory = real?.Category,
            StatusCode = AuditMapping.CodeFromCategory(real?.Category),
            YearPublished = r.Year,
            Latest = r.Latest,
            DataSource = "iucn-api",
            Field = "assessment download",
            CurrentValue = r.HttpStatus?.ToString(CultureInfo.InvariantCulture),
            IssueType = is500 ? "api-empty-scope-500" : "api-download-failure",
            SeverityTier = (r.Latest == true ? 10 : 0) + (is500 ? 5 : 3),
            Detail = is500
                ? "Empty geographic-scope array. /api/v4/assessment/{id} returns HTTP 500 and the website shows the region as a bare ampersand."
                : $"Assessment download returned HTTP {(r.HttpStatus?.ToString(CultureInfo.InvariantCulture) ?? "no response")}.",
        };
        finding.Extra["httpStatus"] = r.HttpStatus?.ToString(CultureInfo.InvariantCulture);
        finding.Extra["attemptCount"] = r.Attempts.ToString(CultureInfo.InvariantCulture);
        finding.Extra["rootSisId"] = AuditMapping.LongToString(r.RootSisId);
        finding.Extra["lastError"] = r.LastError;
        finding.Extra["lastAttemptAt"] = r.LastAttemptAt;
        finding.Extra["nextAttemptAfter"] = r.NextAttemptAfter;
        finding.Extra["authority"] = AuditMapping.Decode(taxon?.Authority);
        if (real is not null) {
            finding.Extra["realScope"] = real.Scope;
            finding.Extra["realUrl"] = IucnUrls.Species(r.SisId, real.AssessmentId);
        }
        if (is500) {
            finding.Notes.Add("Phantom scope-less duplicate of the taxon's real per-scope assessments, so no coverage gap.");
        }
        return finding;
    }

    private sealed record CsvTaxon(
        string? ScientificName, string? Authority, string? Kingdom, string? Phylum,
        string? Class, string? Order, string? Family, string? Genus, string? Species);

    private sealed record CsvAssessment(string? Category, string? Scope, long? AssessmentId);

    // Pull species, taxonomy, and the real (servable) assessment for the failed taxa from the CSV
    // export. The empty-scope assessment that 500s is not in the export, so we match on taxonId and
    // prefer the taxon's Global-scope assessment for the category and scope shown.
    private static (IReadOnlyDictionary<long, CsvTaxon> Taxa, IReadOnlyDictionary<long, CsvAssessment> Assess) LoadCsv(AuditContext ctx, IEnumerable<long> sisIds) {
        var taxa = new Dictionary<long, CsvTaxon>();
        var assess = new Dictionary<long, CsvAssessment>();
        var ids = sisIds.Distinct().ToList();

        var csv = ctx.IucnCsvOrNull();
        if (csv is null || ids.Count == 0 || !AuditContext.ObjectExists(csv, "taxonomy_html")) {
            return (taxa, assess);
        }
        var inClause = string.Join(",", ids.Select((_, i) => "@p" + i.ToString(CultureInfo.InvariantCulture)));

        using (var cmd = csv.CreateCommand()) {
            cmd.CommandText =
                "SELECT taxonId, scientificName, authority, kingdomName, phylumName, className, orderName, familyName, genusName, speciesName " +
                $"FROM taxonomy_html WHERE taxonId IN ({inClause})";
            BindIds(cmd, ids);
            using var rd = cmd.ExecuteReader();
            while (rd.Read()) {
                taxa[rd.GetInt64(0)] = new CsvTaxon(
                    Str(rd, 1), Str(rd, 2), Str(rd, 3), Str(rd, 4), Str(rd, 5), Str(rd, 6), Str(rd, 7), Str(rd, 8), Str(rd, 9));
            }
        }

        if (AuditContext.ObjectExists(csv, "assessments_html")) {
            using var cmd = csv.CreateCommand();
            cmd.CommandText =
                $"SELECT taxonId, assessmentId, redlistCategory, scopes FROM assessments_html WHERE taxonId IN ({inClause})";
            BindIds(cmd, ids);
            using var rd = cmd.ExecuteReader();
            while (rd.Read()) {
                var id = rd.GetInt64(0);
                var candidate = new CsvAssessment(Str(rd, 2), Str(rd, 3), rd.IsDBNull(1) ? null : rd.GetInt64(1));
                // Keep the first match per taxon, but let a Global-scope assessment win.
                if (!assess.TryGetValue(id, out var existing) || (IsGlobal(candidate.Scope) && !IsGlobal(existing.Scope))) {
                    assess[id] = candidate;
                }
            }
        }

        return (taxa, assess);
    }

    private static void BindIds(SqliteCommand cmd, IReadOnlyList<long> ids) {
        for (var i = 0; i < ids.Count; i++) {
            cmd.Parameters.AddWithValue("@p" + i.ToString(CultureInfo.InvariantCulture), ids[i]);
        }
    }

    private static bool IsGlobal(string? scope) => scope?.Contains("Global", StringComparison.OrdinalIgnoreCase) ?? false;

    private static string? Str(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
