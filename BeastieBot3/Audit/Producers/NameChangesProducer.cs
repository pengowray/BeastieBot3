using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// SIS taxon ids whose cached assessments carry more than one distinct taxon_scientific_name, which
// would indicate a recorded binomial changed across assessment versions. In current data this is
// typically empty, because amended assessments keep the present name and record the former one in the
// errata narrative. Documents the coverage of a field-based detection approach.

namespace BeastieBot3.Audit.Producers;

internal sealed class NameChangesProducer : IAuditReportProducer {
    public string Id => "name-changes";

    private sealed class Snapshot {
        public long? AssessmentId;
        public string? Name;
        public string? Year;
        public bool Latest;
        public string? Code;
        public string? Url;
    }

    private sealed class History {
        public long SisId;
        public readonly HashSet<string> DistinctNames = new(StringComparer.Ordinal);
        public readonly List<Snapshot> Snapshots = new();
        public string? Json;
    }

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var histories = Scan(conn, ctx);
        var findings = histories.Values
            .Where(h => h.DistinctNames.Count > 1)
            .OrderByDescending(h => h.DistinctNames.Count)
            .ThenBy(h => h.SisId)
            .Select(Build)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Scientific name changes across assessment versions",
            Tier = AuditReportTier.Methodology,
            Breakage = BreakageClass.Advisory,
            DataSourceLabel = "IUCN API cache (taxon assessment summaries)",
            Summary =
                "This compares the scientific name recorded on each of a taxon's assessment summaries, grouped by SIS id, and lists any taxon whose assessments carry more than one distinct name. " +
                "In current data this is usually empty: amended assessments keep the taxon's present name and record the former name in the errata text rather than in a dedicated field, so a name-field comparison finds little. " +
                "The report documents the coverage of this approach and gives a place to track future divergence.",
            Columns = new List<AuditColumn> {
                AuditColumns.TaxonId(),
                AuditColumns.ScientificName("Current name"),
                AuditColumns.Custom("distinctNames", "Names recorded", AuditColumnType.Text),
                AuditColumns.Custom("nameCount", "Number of names", AuditColumnType.Number),
                AuditColumns.Status(),
                AuditColumns.Year("Latest year"),
                AuditColumns.Detail("Assessment timeline"),
                AuditColumns.RedlistLink(),
            },
            Findings = findings,
            HeadlineCount = findings.Count,
        };
    }

    private static Dictionary<long, History> Scan(SqliteConnection conn, AuditContext ctx) {
        var sql = "SELECT root_sis_id, json FROM taxa ORDER BY root_sis_id";
        using var cmd = conn.CreateCommand();
        cmd.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
        cmd.CommandTimeout = 0;

        var histories = new Dictionary<long, History>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }
            var json = reader.GetString(1);
            JsonDocument doc;
            try { doc = JsonDocument.Parse(json); } catch (JsonException) { continue; }
            using (doc) {
                if (!doc.RootElement.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
                    continue;
                }
                foreach (var a in arr.EnumerateArray()) {
                    if (a.ValueKind != JsonValueKind.Object) {
                        continue;
                    }
                    var sisTaxonId = AsLong(a, "sis_taxon_id") ?? rootSisId;
                    var name = Str(a, "taxon_scientific_name")?.Trim();
                    var snap = new Snapshot {
                        AssessmentId = AsLong(a, "assessment_id"),
                        Name = name,
                        Year = Str(a, "year_published") ?? Num(a, "year_published"),
                        Latest = a.TryGetProperty("latest", out var l) && (l.ValueKind == JsonValueKind.True || (l.ValueKind == JsonValueKind.String && string.Equals(l.GetString(), "true", StringComparison.OrdinalIgnoreCase))),
                        Code = Str(a, "red_list_category_code"),
                        Url = Str(a, "url"),
                    };
                    if (snap.AssessmentId is null && string.IsNullOrEmpty(snap.Name) && string.IsNullOrEmpty(snap.Year)) {
                        continue;
                    }
                    if (!histories.TryGetValue(sisTaxonId, out var history)) {
                        history = new History { SisId = sisTaxonId };
                        histories[sisTaxonId] = history;
                    }
                    history.Snapshots.Add(snap);
                    history.Json = json;
                    if (!string.IsNullOrEmpty(name)) {
                        history.DistinctNames.Add(name);
                    }
                }
            }
        }
        return histories;
    }

    private static AuditFinding Build(History h) {
        var ordered = h.Snapshots.OrderBy(s => int.TryParse(s.Year, out var y) ? y : int.MaxValue).ThenBy(s => s.AssessmentId ?? long.MaxValue).ToList();
        var latest = ordered.FirstOrDefault(s => s.Latest) ?? ordered.LastOrDefault();
        var taxonomy = h.Json is null ? null : IucnTaxaTaxonomyExtractor.Extract(h.Json);
        var code = AuditMapping.CodeFromCode(latest?.Code);
        var timeline = string.Join(" → ", ordered.Where(s => !string.IsNullOrEmpty(s.Name)).Select(s => $"{s.Name} ({s.Year ?? "?"}{(s.Latest ? ", latest" : "")})"));

        var finding = new AuditFinding {
            ReportId = "name-changes",
            Key = $"{h.SisId}",
            TaxonId = h.SisId,
            AssessmentId = latest?.AssessmentId,
            RedlistUrl = !string.IsNullOrEmpty(latest?.Url) ? latest!.Url : IucnUrls.Species(h.SisId, latest?.AssessmentId),
            ScientificName = latest?.Name ?? taxonomy?.ScientificName ?? $"SIS {h.SisId}",
            CommonName = taxonomy?.CommonName,
            Kingdom = taxonomy?.KingdomName,
            Class = taxonomy?.ClassName,
            Order = taxonomy?.OrderName,
            Family = taxonomy?.FamilyName,
            Rank = "species",
            IsFullSpecies = true,
            StatusCode = code,
            StatusCategory = AuditMapping.CategoryText(latest?.Code),
            YearPublished = latest?.Year,
            DataSource = "iucn-api-cache",
            Field = "taxon_scientific_name",
            IssueType = "scientific-name-change",
            SeverityTier = h.DistinctNames.Count,
            Detail = $"{h.DistinctNames.Count} distinct names: {timeline}",
        };
        finding.Extra["distinctNames"] = string.Join("; ", h.DistinctNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
        finding.Extra["nameCount"] = h.DistinctNames.Count.ToString();
        return finding;
    }

    private static string? Str(JsonElement e, string name) =>
        e.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;

    private static string? Num(JsonElement e, string name) =>
        e.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.Number ? p.GetRawText() : null;

    private static long? AsLong(JsonElement e, string name) {
        if (!e.TryGetProperty(name, out var p)) {
            return null;
        }
        return p.ValueKind switch {
            JsonValueKind.Number => p.GetInt64(),
            JsonValueKind.String when long.TryParse(p.GetString(), out var n) => n,
            _ => null,
        };
    }
}
