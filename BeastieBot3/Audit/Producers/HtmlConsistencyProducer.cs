using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// For six narrative fields, compares the HTML serialisation (tags stripped) against the stored
// plain-text serialisation of the same field. Reuses the pure comparison helpers in
// IucnHtmlUtilities; the comparison engine in IucnHtmlConsistencyCommand is private and capped, so
// the loop is reproduced here uncapped.

namespace BeastieBot3.Audit.Producers;

internal sealed class HtmlConsistencyProducer : IAuditReportProducer {
    public string Id => "html-consistency";

    private static readonly string[] Fields = { "rationale", "habitat", "threats", "population", "range", "useTrade" };

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "view_assessments_html_taxonomy_html") || !AuditContext.ObjectExists(conn, "assessments")) {
            return null;
        }

        var findings = Scan(conn, ctx);

        var byField = Fields
            .Select(field => new[] { field, findings.Count(f => f.Field == field).ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "HTML and plain-text narrative fields that differ",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Summary =
                "For each assessment, the HTML version of six narrative fields (rationale, habitat, threats, population, range, use and trade) is reduced to plain text and compared against the stored plain-text version of the same field. " +
                "Rows appear where the two differ after normalisation, or where one version is present and the other is empty. The comparison is about text serialisation only and says nothing about the scientific content.",
            Columns = new List<AuditColumn> {
                AuditColumns.Field(),
                AuditColumns.ScientificName("Species"),
                AuditColumns.Status(),
                AuditColumns.IssueType(),
                AuditColumns.CurrentValue("Stored plain text", AuditColumnType.LongText),
                AuditColumns.SuggestedValue("Recreated from HTML", AuditColumnType.LongText),
                AuditColumns.Class(),
                AuditColumns.TaxonId("Taxon id"),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By field", Headers = new[] { "Field", "Mismatches" }, Rows = byField, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static IReadOnlyList<AuditFinding> Scan(SqliteConnection connection, AuditContext ctx) {
        var sb = new System.Text.StringBuilder();
        sb.Append("SELECT v.assessmentId, v.taxonId, v.scientificName, v.redlistCategory, v.yearPublished, v.possiblyExtinct, v.possiblyExtinctInTheWild, ");
        sb.Append("v.kingdomName, v.phylumName, v.className, v.orderName, v.familyName, v.genusName, v.speciesName, v.infraType, v.infraName, v.subpopulationName");
        foreach (var f in Fields) {
            sb.Append($", v.{f} AS {f}_html, p.{f} AS {f}_plain");
        }
        sb.Append(" FROM view_assessments_html_taxonomy_html v JOIN assessments p ON p.assessmentId = v.assessmentId ORDER BY v.assessmentId");
        var sql = sb.ToString();

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var findings = new List<AuditFinding>();
        using var reader = command.ExecuteReader();
        var ord = new Dictionary<string, int>();
        for (var i = 0; i < reader.FieldCount; i++) {
            ord[reader.GetName(i)] = i;
        }

        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var assessmentId = reader.GetInt64(ord["assessmentId"]);
            var taxonId = reader.GetInt64(ord["taxonId"]);
            var scientificName = S(reader, ord, "scientificName");
            var category = S(reader, ord, "redlistCategory");
            var pe = S(reader, ord, "possiblyExtinct");
            var pew = S(reader, ord, "possiblyExtinctInTheWild");
            var year = S(reader, ord, "yearPublished");
            var infraType = S(reader, ord, "infraType");
            var subpop = S(reader, ord, "subpopulationName");
            var code = AuditMapping.CodeFromCategory(category, pe, pew);
            var (rank, isFull) = AuditMapping.Rank(infraType, subpop);

            foreach (var field in Fields) {
                var htmlVal = S(reader, ord, field + "_html");
                var plainVal = S(reader, ord, field + "_plain");
                var htmlExact = IucnHtmlUtilities.ConvertHtmlToExactPlainText(htmlVal);
                var plainExact = IucnHtmlUtilities.NormalizePlainTextExact(plainVal);
                if (IucnHtmlUtilities.NormalizedEquals(htmlExact, plainExact)) {
                    continue;
                }

                string issueType;
                int severity;
                if (htmlVal is null) {
                    issueType = "html-null-plain-present"; severity = 4;
                } else if (plainVal is null) {
                    issueType = "plain-null-html-present"; severity = 4;
                } else if (StripWhitespace(htmlExact) == StripWhitespace(plainExact)) {
                    issueType = "whitespace-only-mismatch"; severity = 2;
                } else {
                    issueType = "html-plain-text-mismatch"; severity = 5;
                }

                findings.Add(new AuditFinding {
                    ReportId = "html-consistency",
                    Key = $"{assessmentId}:{field}",
                    TaxonId = taxonId,
                    AssessmentId = assessmentId,
                    RedlistUrl = IucnUrls.Species(taxonId, assessmentId),
                    ScientificName = AuditMapping.Decode(scientificName) ?? $"SIS {taxonId}",
                    Rank = rank,
                    IsFullSpecies = isFull,
                    InfraType = infraType,
                    InfraName = S(reader, ord, "infraName"),
                    SubpopulationName = subpop,
                    Kingdom = S(reader, ord, "kingdomName"),
                    Phylum = S(reader, ord, "phylumName"),
                    Class = S(reader, ord, "className"),
                    Order = S(reader, ord, "orderName"),
                    Family = S(reader, ord, "familyName"),
                    Genus = S(reader, ord, "genusName"),
                    Species = S(reader, ord, "speciesName"),
                    StatusCode = code,
                    StatusCategory = category,
                    YearPublished = year,
                    DataSource = "iucn-csv",
                    Field = field,
                    CurrentValue = IucnHtmlUtilities.ShortenForDisplay(plainVal),
                    SuggestedValue = IucnHtmlUtilities.ShortenForDisplay(IucnHtmlUtilities.ConvertHtmlToPlainTextNeater(htmlVal)),
                    IssueType = issueType,
                    SeverityTier = severity,
                    Detail = $"The {field} field differs between its HTML and plain-text versions.",
                });
            }
        }

        return findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.Field, StringComparer.Ordinal)
            .ThenBy(f => f.AssessmentId)
            .ToList();
    }

    private static string StripWhitespace(string? value) =>
        value is null ? "" : new string(value.Where(c => !char.IsWhiteSpace(c)).ToArray());

    private static string? S(SqliteDataReader reader, Dictionary<string, int> ord, string name) {
        if (!ord.TryGetValue(name, out var i)) {
            return null;
        }
        return reader.IsDBNull(i) ? null : reader.GetString(i);
    }
}
