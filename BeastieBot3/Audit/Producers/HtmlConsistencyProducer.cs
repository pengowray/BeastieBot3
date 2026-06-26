using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// For six narrative fields, compares the HTML serialisation (tags stripped) against the stored
// plain-text serialisation of the same field. Reuses the tag-stripping in IucnHtmlUtilities, then
// compares the canonical readable text of each side so cosmetic-only differences (whitespace,
// non-breaking spaces, entity encoding) are not reported. Surfaces genuine differences and, in
// particular, fields where heavy redundant markup leaves the plain-text version empty or truncated.

namespace BeastieBot3.Audit.Producers;

internal sealed class HtmlConsistencyProducer : IAuditReportProducer {
    public string Id => "html-consistency";

    private static readonly string[] Fields = { "rationale", "habitat", "threats", "population", "range", "useTrade" };

    // The HTML is treated as heavy redundant markup when it is at least this many times the size of
    // its own readable text and large in absolute terms (the badly-behaved rich-text-editor pattern).
    private const double RedundantRatio = 3.0;
    private const int RedundantMinHtmlChars = 1000;

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "view_assessments_html_taxonomy_html") || !AuditContext.ObjectExists(conn, "assessments")) {
            return null;
        }

        var findings = Scan(conn, ctx);

        var byField = Fields
            .Select(field => new[] { field, findings.Count(f => f.Field == field).ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        var byKind = findings
            .GroupBy(f => f.IssueType ?? "")
            .OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        var summary =
            "For each assessment, the HTML version of six narrative fields (rationale, habitat, threats, population, range, use and trade) is reduced to plain text and compared against the stored plain-text version of the same field. " +
            "Differences that are only whitespace, non-breaking spaces, or entity encoding are treated as a match and not listed, so a row here means the readable text genuinely differs. The comparison is about text serialisation only and says nothing about the scientific content.";

        // A common cause is heavy redundant markup: some fields carry a large amount of repeated empty
        // tags (for example long runs of nested empty spans from a rich-text editor), and the
        // plain-text version then comes out empty or truncated. Name a live example when present.
        var example = findings.FirstOrDefault(f => f.IssueType is "plain-text-empty-redundant-markup" or "plain-text-truncated-redundant-markup");
        if (example is not null) {
            var ratio = example.Get("markupRatio");
            summary += $" A recurring pattern is heavy redundant markup: the HTML carries long runs of repeated empty tags and the plain-text version then comes out empty or truncated. " +
                       $"For example {example.ScientificName} ({example.Field}) has HTML about {ratio} times the size of its readable text, and its plain-text version did not get past the markup. " +
                       $"These rows are marked redundant-markup and explained in the detail column.";
        }

        return new AuditReport {
            Id = Id,
            Title = "HTML and plain-text narrative fields that differ",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Summary = summary,
            Columns = new List<AuditColumn> {
                AuditColumns.Field(),
                AuditColumns.ScientificName("Species"),
                AuditColumns.Status(),
                AuditColumns.IssueType(),
                AuditColumns.CurrentValue("Stored plain text", AuditColumnType.LongText),
                AuditColumns.SuggestedValue("Recreated from HTML", AuditColumnType.LongText),
                AuditColumns.Detail(),
                AuditColumns.Class(),
                AuditColumns.TaxonId("Taxon id"),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By observation", Headers = new[] { "Observation", "Count" }, Rows = byKind, NumericColumns = new[] { 1 } },
                new() { Title = "By field", Headers = new[] { "Field", "Differences" }, Rows = byField, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static IReadOnlyList<AuditFinding> Scan(SqliteConnection connection, AuditContext ctx) {
        var sb = new StringBuilder();
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

                // Compare with the exact tag-stripping (which aligns with how the IUCN plain-text
                // field is produced), then canonicalise both sides so only genuine readable-text
                // differences remain. The friendly conversion is used only for the display column.
                var htmlText = Canonical(IucnHtmlUtilities.ConvertHtmlToExactPlainText(htmlVal));
                var plainText = Canonical(plainVal);

                // Identical readable text (cosmetic-only differences) is not a finding.
                if (string.Equals(htmlText, plainText, StringComparison.Ordinal)) {
                    continue;
                }

                var rawHtmlLen = htmlVal?.Length ?? 0;
                var redundant = rawHtmlLen >= RedundantMinHtmlChars && htmlText.Length > 0 && rawHtmlLen >= htmlText.Length * RedundantRatio;
                var ratio = htmlText.Length > 0 ? (double)rawHtmlLen / htmlText.Length : 0;

                string issueType;
                string detail;
                int severity;

                if (plainText.Length == 0 && htmlText.Length > 0) {
                    if (redundant) {
                        issueType = "plain-text-empty-redundant-markup"; severity = 5;
                        detail = $"The plain-text field is empty while the HTML carries text. The HTML is about {ratio:N0} times the size of its readable text, with a large amount of redundant markup that the plain-text version appears not to get past.";
                    } else {
                        issueType = "plain-text-empty"; severity = 3;
                        detail = "The plain-text field is empty while the HTML version carries text.";
                    }
                } else if (htmlText.Length == 0 && plainText.Length > 0) {
                    issueType = "html-text-empty"; severity = 3;
                    detail = "The HTML version is empty while the plain-text field has text.";
                } else if (htmlText.StartsWith(plainText, StringComparison.Ordinal) && plainText.Length < htmlText.Length) {
                    if (redundant) {
                        issueType = "plain-text-truncated-redundant-markup"; severity = 5;
                        detail = $"The plain-text field stops early. The HTML is about {ratio:N0} times the size of its readable text, with a large amount of redundant markup that the plain-text version appears not to get past.";
                    } else {
                        issueType = "plain-text-truncated"; severity = 3;
                        detail = "The plain-text field stops early relative to the HTML version.";
                    }
                } else {
                    issueType = "text-differs"; severity = 4;
                    detail = $"The {field} field differs between its HTML and plain-text versions.";
                }

                var finding = new AuditFinding {
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
                    Detail = detail,
                };
                if (redundant) {
                    finding.Extra["markupRatio"] = ratio.ToString("N0", CultureInfo.InvariantCulture);
                }
                findings.Add(finding);
            }
        }

        return findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.IssueType, StringComparer.Ordinal)
            .ThenBy(f => f.Field, StringComparer.Ordinal)
            .ThenBy(f => f.AssessmentId)
            .ToList();
    }

    // Reduces text to its readable form for comparison: decode entities, treat every whitespace and
    // non-breaking/zero-width space as a single space, collapse runs, and trim. Applied to both the
    // tag-stripped HTML and the stored plain text so only genuine text differences remain.
    private static string Canonical(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return "";
        }
        var decoded = WebUtility.HtmlDecode(value);
        var sb = new StringBuilder(decoded.Length);
        var prevSpace = false;
        foreach (var ch in decoded) {
            var isSpace = char.IsWhiteSpace(ch)
                || ch is '\u200B' or '\u200C' or '\u200D' or '\uFEFF' or '\u00AD';
            if (isSpace) {
                if (!prevSpace) {
                    sb.Append(' ');
                    prevSpace = true;
                }
            } else {
                sb.Append(ch);
                prevSpace = false;
            }
        }
        return sb.ToString().Trim();
    }

    private static string? S(SqliteDataReader reader, Dictionary<string, int> ord, string name) {
        if (!ord.TryGetValue(name, out var i)) {
            return null;
        }
        return reader.IsDBNull(i) ? null : reader.GetString(i);
    }
}
