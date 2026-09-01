using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;

// Assessments published with no geographic scope. Every other assessment carries at least one
// ("Global", "Europe", "Mediterranean", ...); these carry an empty scopes array in the API and an
// empty scopes column in the CSV export. Until 2026-08 the API answered HTTP 500 for exactly this
// set, which is why they used to appear under failed-assessments; the server error was fixed
// without the scope being filled in, so the data gap outlived the symptom.
//
// Primary source is the API cache (it holds historical assessments too, which the CSV export omits),
// with the CSV export cross-checked per row and used on its own when no API cache is configured.

namespace BeastieBot3.Audit.Producers;

internal sealed class EmptyScopeProducer : IAuditReportProducer {
    public string Id => "empty-scope";

    public AuditReport? Produce(AuditContext ctx) {
        var api = ctx.IucnApiCacheOrNull();
        var csv = ctx.IucnCsvOrNull();
        var haveApi = api is not null && AuditContext.ObjectExists(api, "assessments");
        var haveCsv = csv is not null && AuditContext.ObjectExists(csv, "assessments_html");
        if (!haveApi && !haveCsv) {
            return null;
        }

        var rows = new Dictionary<long, ScopelessRow>();
        if (haveApi) {
            foreach (var row in ScanApi(api!, ctx)) {
                rows[row.AssessmentId] = row;
            }
        }

        // The CSV export is the release of record, so anything blank-scoped there counts even when
        // the API cache has not downloaded it.
        var csvBlank = haveCsv ? ScanCsv(csv!, ctx) : new Dictionary<long, CsvRow>();
        foreach (var pair in csvBlank) {
            if (!rows.ContainsKey(pair.Key)) {
                rows[pair.Key] = ScopelessRow.FromCsv(pair.Key, pair.Value);
            }
        }

        if (rows.Count == 0) {
            return null;
        }

        var taxonScopes = haveCsv
            ? LoadTaxonScopes(csv!, rows.Values.Select(r => r.SisId).OfType<long>())
            : new Dictionary<long, List<string>>();

        var findings = rows.Values
            .Select(r => Build(r, csvBlank.ContainsKey(r.AssessmentId), taxonScopes, haveCsv))
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var byLatest = findings
            .GroupBy(f => f.Latest == true ? "Current assessment" : "Historical assessment")
            .OrderByDescending(g => g.Key == "Current assessment")
            .Select(g => new[] {
                g.Key,
                g.Count().ToString("N0"),
                g.Count(x => string.Equals(x.Get("inCsvExport"), "yes", StringComparison.OrdinalIgnoreCase)).ToString("N0"),
            } as IReadOnlyList<string>)
            .ToList();

        var byClass = findings
            .GroupBy(f => f.Class ?? "(unspecified)")
            .OrderByDescending(g => g.Count()).ThenBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Assessments with no geographic scope",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.Breaking,
            KindLabel = "Missing data",
            DataSourceLabel = haveApi && haveCsv
                ? "IUCN API and CSV export"
                : haveApi ? "IUCN API" : "IUCN CSV export",
            Blurb = "Assessments whose geographic scope is blank, so any scope filter, including the usual \"Global\" filter, silently drops them.",
            Summary =
                "Every assessment is expected to record at least one geographic scope, the region it covers: Global, Europe, Mediterranean, Persian Gulf, and so on. " +
                "The assessments listed below have no scope at all.\n\n" +
                "Some are the taxon's current assessment. Those also appear in the downloadable CSV export with an empty `scopes` column, so the gap is in the underlying data rather than in one delivery format; " +
                "every other assessment row in the export has a scope. The rest are historical assessments, which the export does not include, so they are visible only through the API.\n\n" +
                "Until shortly before this site release, the API returned HTTP 500 (a server error) for every one of these records, and they could not be read at all. " +
                "They now return normally, with the scope still blank. On the species page at iucnredlist.org, the region line for these assessments displays as a bare \"&\" with no region text.\n\n" +
                "A few of the affected taxa have a scientific name ending in \"_new\" (Balaenoptera edeni_new, for example), which suggests working or draft records were published by accident.\n\n" +
                "### Why it matters\n\n" +
                "Any data consumer that filters assessments by scope silently drops these records, and filtering to \"Global\" is the standard first step, including for anyone reproducing the Red List's own summary statistics. " +
                "For some taxa the blank-scope record is the only assessment, or the only current one, so the taxon has no usable scope anywhere.\n\n" +
                "### Suggestion\n\n" +
                "Add the correct scope to each record, and add a validation rule so an assessment cannot be published without one. " +
                "For the records whose scientific name ends in \"_new\", check whether they were meant to be published at all.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName(),
                AuditColumns.Custom("authority", "Authority", AuditColumnType.Code),
                AuditColumns.Class(),
                AuditColumns.Order(),
                AuditColumns.Family(),
                AuditColumns.Status("Assessed as"),
                AuditColumns.AssessmentId("Assessment"),
                AuditColumns.Latest("Current"),
                AuditColumns.Year(),
                AuditColumns.Custom("inCsvExport", "In CSV export", AuditColumnType.Text,
                    "Whether the downloadable CSV export also includes this assessment with a blank scope. Historical assessments are not in the export at all."),
                AuditColumns.Custom("otherScopes", "Other scopes for this taxon", AuditColumnType.Text,
                    "Scopes on the taxon's other assessments in the CSV export. Blank means the taxon has no scoped assessment anywhere."),
                AuditColumns.TaxonId(),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() {
                    Title = "By assessment version",
                    Note = "The CSV export includes current assessments only, so historical rows can only be seen through the API.",
                    Headers = new[] { "Assessment", "Rows", "Also blank in the CSV export" },
                    Rows = byLatest, NumericColumns = new[] { 1, 2 },
                },
                new() { Title = "By class", Headers = new[] { "Class", "Rows" }, Rows = byClass, NumericColumns = new[] { 1 } },
            },
        };
    }

    private sealed record ScopelessRow(
        long AssessmentId, long? SisId, bool? Latest, string? Year, string? StatusCode,
        string? ScientificName, string? Authority, string? Kingdom, string? Phylum, string? Class,
        string? Order, string? Family, string? Genus, string? Species, string? Url) {

        public static ScopelessRow FromCsv(long assessmentId, CsvRow c) => new(
            assessmentId, c.TaxonId, true, c.Year, AuditMapping.CodeFromCategory(c.Category),
            c.ScientificName, c.Authority, c.Kingdom, c.Phylum, c.Class, c.Order, c.Family, c.Genus, c.Species,
            IucnUrls.Species(c.TaxonId, assessmentId));
    }

    private sealed record CsvRow(
        long? TaxonId, string? Category, string? Year, string? ScientificName, string? Authority,
        string? Kingdom, string? Phylum, string? Class, string? Order, string? Family, string? Genus, string? Species);

    // Scan every cached assessment payload for an absent or empty scopes array. This reads each
    // JSON blob, so it costs about ten seconds over a full cache; it runs once per site build.
    private static IReadOnlyList<ScopelessRow> ScanApi(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT assessment_id, json FROM assessments
WHERE json_extract(json, '$.scopes') IS NULL OR json_array_length(json_extract(json, '$.scopes')) = 0
ORDER BY assessment_id";

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + "\nLIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var rows = new List<ScopelessRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            if (reader.IsDBNull(1)) {
                continue;
            }
            var parsed = ParseAssessment(reader.GetInt64(0), reader.GetString(1));
            if (parsed is not null) {
                rows.Add(parsed);
            }
        }
        return rows;
    }

    private static ScopelessRow? ParseAssessment(long assessmentId, string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var taxon = root.TryGetProperty("taxon", out var t) && t.ValueKind == JsonValueKind.Object ? t : (JsonElement?)null;
            var code = root.TryGetProperty("red_list_category", out var cat) && cat.ValueKind == JsonValueKind.Object
                ? Text(cat, "code") : null;
            return new ScopelessRow(
                assessmentId,
                Number(root, "sis_taxon_id") ?? (taxon is null ? null : Number(taxon.Value, "sis_id")),
                root.TryGetProperty("latest", out var latest) ? latest.ValueKind == JsonValueKind.True : null,
                Text(root, "year_published"),
                AuditMapping.CodeFromCode(code),
                AuditMapping.Decode(taxon is null ? null : Text(taxon.Value, "scientific_name")),
                AuditMapping.Decode(taxon is null ? null : Text(taxon.Value, "authority")),
                taxon is null ? null : Text(taxon.Value, "kingdom_name"),
                taxon is null ? null : Text(taxon.Value, "phylum_name"),
                taxon is null ? null : Text(taxon.Value, "class_name"),
                taxon is null ? null : Text(taxon.Value, "order_name"),
                taxon is null ? null : Text(taxon.Value, "family_name"),
                taxon is null ? null : Text(taxon.Value, "genus_name"),
                taxon is null ? null : Text(taxon.Value, "species_name"),
                Text(root, "url"));
        } catch (JsonException) {
            return null;
        }
    }

    private static string? Text(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value)
            ? value.ValueKind switch {
                JsonValueKind.String => value.GetString(),
                JsonValueKind.Number => value.GetRawText(),
                _ => null,
            }
            : null;

    private static long? Number(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.Number
            ? value.GetInt64() : null;

    private static Dictionary<long, CsvRow> ScanCsv(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT a.assessmentId, a.taxonId, a.redlistCategory, a.yearPublished,
       t.scientificName, t.authority, t.kingdomName, t.phylumName, t.className, t.orderName,
       t.familyName, t.genusName, t.speciesName
FROM assessments_html a
LEFT JOIN taxonomy_html t ON t.taxonId = a.taxonId
WHERE a.scopes IS NULL OR TRIM(a.scopes) = ''";

        var rows = new Dictionary<long, CsvRow>();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            if (reader.IsDBNull(0)) {
                continue;
            }
            rows[reader.GetInt64(0)] = new CsvRow(
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                Str(reader, 2), Str(reader, 3),
                AuditMapping.Decode(Str(reader, 4)), AuditMapping.Decode(Str(reader, 5)),
                Str(reader, 6), Str(reader, 7), Str(reader, 8), Str(reader, 9), Str(reader, 10),
                Str(reader, 11), Str(reader, 12));
        }
        return rows;
    }

    // Scopes carried by the taxon's other assessments, so a row can say whether the taxon has a
    // usable scope anywhere.
    private static Dictionary<long, List<string>> LoadTaxonScopes(SqliteConnection connection, IEnumerable<long> sisIds) {
        var ids = sisIds.Distinct().ToList();
        var scopes = new Dictionary<long, List<string>>();
        if (ids.Count == 0 || !AuditContext.ObjectExists(connection, "assessments_html")) {
            return scopes;
        }

        var inClause = string.Join(",", ids.Select((_, i) => "@p" + i.ToString(CultureInfo.InvariantCulture)));
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT taxonId, scopes FROM assessments_html WHERE taxonId IN ({inClause})";
        for (var i = 0; i < ids.Count; i++) {
            command.Parameters.AddWithValue("@p" + i.ToString(CultureInfo.InvariantCulture), ids[i]);
        }
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var value = Str(reader, 1);
            if (string.IsNullOrWhiteSpace(value)) {
                continue;
            }
            var id = reader.GetInt64(0);
            if (!scopes.TryGetValue(id, out var list)) {
                scopes[id] = list = new List<string>();
            }
            foreach (var part in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                if (!list.Contains(part, StringComparer.OrdinalIgnoreCase)) {
                    list.Add(part);
                }
            }
        }
        return scopes;
    }

    private static AuditFinding Build(ScopelessRow r, bool inCsv, IReadOnlyDictionary<long, List<string>> taxonScopes, bool haveCsv) {
        var others = r.SisId is { } sis && taxonScopes.TryGetValue(sis, out var list) ? list : new List<string>();
        var isolated = haveCsv && others.Count == 0;
        var current = r.Latest == true;

        var finding = new AuditFinding {
            ReportId = "empty-scope",
            Key = $"{r.AssessmentId}:empty-scope",
            TaxonId = r.SisId,
            AssessmentId = r.AssessmentId,
            RedlistUrl = !string.IsNullOrWhiteSpace(r.Url) ? r.Url : IucnUrls.Species(r.SisId, r.AssessmentId),
            ScientificName = r.ScientificName ?? (r.SisId is { } s ? $"SIS {s}" : null),
            Kingdom = r.Kingdom,
            Phylum = r.Phylum,
            Class = r.Class,
            Order = r.Order,
            Family = r.Family,
            Genus = r.Genus,
            Species = r.Species,
            StatusCode = r.StatusCode,
            StatusCategory = AuditMapping.CategoryText(r.StatusCode),
            YearPublished = r.Year,
            Latest = r.Latest,
            DataSource = inCsv ? "iucn-csv" : "iucn-api",
            Field = "scopes",
            CurrentValue = "(blank)",
            IssueType = "empty-scope",
            SeverityTier = (isolated ? 20 : 0) + (current ? 10 : 0) + (inCsv ? 5 : 0),
            Detail = current
                ? inCsv
                    ? "The taxon's current assessment has a blank scope, in both the API and the CSV export; scope filters exclude it."
                    : "The taxon's current assessment has a blank scope; scope filters exclude it."
                : "Historical assessment with a blank scope; visible only through the API.",
        };
        finding.Extra["authority"] = r.Authority;
        finding.Extra["inCsvExport"] = haveCsv ? (inCsv ? "yes" : "no") : null;
        finding.Extra["otherScopes"] = haveCsv ? string.Join(", ", others) : null;
        if (isolated) {
            finding.Notes.Add("The taxon has no other assessment with a scope.");
        }
        if (r.ScientificName?.EndsWith("_new", StringComparison.Ordinal) == true) {
            finding.Notes.Add("The scientific name ends in \"_new\", which reads like a working record.");
        }
        return finding;
    }

    private static string? Str(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
