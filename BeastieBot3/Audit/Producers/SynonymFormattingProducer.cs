using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// Synonym names in the cached IUCN taxa records that carry formatting irregularities (leading or
// trailing whitespace, repeated spaces, non-breaking or control whitespace, embedded markup, or a
// blank value), each with a whitespace-normalised suggestion. Replicates the analysis in
// IucnSynonymFormattingReportCommand (its analyzer is private) and reuses IucnTaxaTaxonomyExtractor
// for taxonomy context.

namespace BeastieBot3.Audit.Producers;

internal sealed class SynonymFormattingProducer : IAuditReportProducer {
    public string Id => "synonym-formatting";

    private enum Issue { HtmlMarkup, SpecialWhitespace, LeadingTrailingWhitespace, RepeatedSpaces, EmptyOrWhitespace }

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var findings = Scan(conn, ctx);

        return new AuditReport {
            Id = Id,
            Title = "Synonym names with formatting irregularities",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = "IUCN API cache (taxon synonyms)",
            Summary =
                "Each row is a synonym name whose stored text carries a formatting irregularity, together with a whitespace-normalised suggestion. " +
                "The current value shows otherwise-invisible characters as markers. The scientific name column is the accepted taxon the synonym belongs to. " +
                "Tidier synonym strings help name matching and search.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Accepted taxon"),
                AuditColumns.Field(),
                AuditColumns.CurrentValue("Synonym (current)", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Suggested", AuditColumnType.Code),
                AuditColumns.IssueType("Issue(s)"),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId(),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
            },
            Findings = findings,
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static IReadOnlyList<AuditFinding> Scan(SqliteConnection connection, AuditContext ctx) {
        var sql = "SELECT root_sis_id, json FROM taxa ORDER BY root_sis_id";
        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
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

            JsonElement root;
            JsonDocument document;
            try { document = JsonDocument.Parse(json); } catch (JsonException) { continue; }
            using (document) {
                root = document.RootElement;
                if (!root.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                    continue;
                }
                if (!taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                    continue;
                }

                var taxonName = FirstString(taxon, "taxon_name", "scientific_name", "taxon_scientific_name", "name");
                var (assessmentId, url, code, year) = PrimaryAssessment(root, rootSisId);
                var taxonomy = IucnTaxaTaxonomyExtractor.Extract(json);

                foreach (var synElement in synonyms.EnumerateArray()) {
                    var synonym = SynonymName(synElement);
                    if (synonym is null) {
                        continue;
                    }
                    var (issues, normalized) = Analyze(synonym);
                    if (issues.Count == 0) {
                        continue;
                    }
                    var suggested = string.Equals(synonym, normalized, StringComparison.Ordinal) ? null : (normalized.Length == 0 ? null : normalized);
                    var top = issues.Min();

                    var finding = new AuditFinding {
                        ReportId = "synonym-formatting",
                        Key = $"{rootSisId}:{synonym}",
                        TaxonId = rootSisId,
                        AssessmentId = assessmentId,
                        RedlistUrl = !string.IsNullOrEmpty(url) ? url : IucnUrls.Species(rootSisId, assessmentId),
                        ScientificName = taxonName ?? taxonomy?.ScientificName ?? $"SIS {rootSisId}",
                        CommonName = taxonomy?.CommonName,
                        Kingdom = taxonomy?.KingdomName,
                        Phylum = taxonomy?.PhylumName,
                        Class = taxonomy?.ClassName,
                        Order = taxonomy?.OrderName,
                        Family = taxonomy?.FamilyName,
                        StatusCode = AuditMapping.CodeFromCode(code),
                        StatusCategory = AuditMapping.CategoryText(code),
                        YearPublished = year,
                        DataSource = "iucn-api-cache",
                        Field = "synonym",
                        CurrentValue = synonym,
                        SuggestedValue = suggested,
                        IssueType = string.Join("; ", issues.OrderBy(i => i).Select(Label)),
                        SeverityTier = 10 - (int)top,
                        Detail = string.Join("; ", issues.OrderBy(i => i).Select(Label)),
                    };
                    if (issues.Contains(Issue.HtmlMarkup)) {
                        finding.Notes.Add("Contains markup, which can indicate a copy from a rendered page.");
                    }
                    if (issues.Contains(Issue.EmptyOrWhitespace)) {
                        finding.Notes.Add("Blank or whitespace-only synonym, so no normalised value can be suggested.");
                    }
                    findings.Add(finding);
                }
            }
        }

        return findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.TaxonId)
            .ThenBy(f => f.CurrentValue, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static (List<Issue> Issues, string Normalized) Analyze(string value) {
        var issues = new List<Issue>();
        if (string.IsNullOrWhiteSpace(value)) {
            issues.Add(Issue.EmptyOrWhitespace);
            return (issues, "");
        }
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal)) {
            issues.Add(Issue.LeadingTrailingWhitespace);
        }
        if (HasRepeatedAsciiSpace(value)) {
            issues.Add(Issue.RepeatedSpaces);
        }
        if (value.Any(c => c is ' ' or ' ' or ' ' or ' ' or '\t' or '\r' or '\n')) {
            issues.Add(Issue.SpecialWhitespace);
        }
        if (HasShortMarkup(value)) {
            issues.Add(Issue.HtmlMarkup);
        }
        return (issues, Normalize(value));
    }

    private static bool HasRepeatedAsciiSpace(string value) {
        for (var i = 1; i < value.Length; i++) {
            if (value[i] == ' ' && value[i - 1] == ' ') {
                return true;
            }
        }
        return false;
    }

    private static bool HasShortMarkup(string value) {
        var open = value.IndexOf('<');
        while (open >= 0) {
            var close = value.IndexOf('>', open + 1);
            if (close < 0) {
                return false;
            }
            var inner = close - open - 1;
            if (inner is >= 1 and <= 64) {
                return true;
            }
            open = value.IndexOf('<', close + 1);
        }
        return false;
    }

    private static string Normalize(string value) {
        var trimmed = value.Trim();
        var sb = new StringBuilder(trimmed.Length);
        var prevSpace = false;
        foreach (var ch in trimmed) {
            var c = ch is ' ' or ' ' or ' ' or ' ' or '\t' or '\r' or '\n' ? ' ' : ch;
            if (char.IsWhiteSpace(c)) {
                if (prevSpace) {
                    continue;
                }
                sb.Append(' ');
                prevSpace = true;
            } else {
                sb.Append(c);
                prevSpace = false;
            }
        }
        return sb.ToString();
    }

    private static string Label(Issue issue) => issue switch {
        Issue.HtmlMarkup => "contains markup",
        Issue.SpecialWhitespace => "non-breaking or control whitespace",
        Issue.LeadingTrailingWhitespace => "leading or trailing whitespace",
        Issue.RepeatedSpaces => "double spaces",
        Issue.EmptyOrWhitespace => "blank or whitespace only",
        _ => issue.ToString(),
    };

    private static string? SynonymName(JsonElement element) {
        if (element.ValueKind == JsonValueKind.String) {
            return element.GetString();
        }
        if (element.ValueKind == JsonValueKind.Object && element.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String) {
            return name.GetString();
        }
        return null;
    }

    private static string? FirstString(JsonElement element, params string[] names) {
        foreach (var n in names) {
            if (element.TryGetProperty(n, out var prop) && prop.ValueKind == JsonValueKind.String) {
                var v = prop.GetString();
                if (!string.IsNullOrWhiteSpace(v)) {
                    return v.Trim();
                }
            }
        }
        return null;
    }

    private static (long? AssessmentId, string? Url, string? Code, string? Year) PrimaryAssessment(JsonElement root, long rootSisId) {
        if (!root.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
            return (null, null, null, null);
        }
        JsonElement? first = null;
        foreach (var a in arr.EnumerateArray()) {
            if (a.ValueKind != JsonValueKind.Object) {
                continue;
            }
            first ??= a;
            if (a.TryGetProperty("latest", out var latest) && IsTrue(latest)) {
                return Read(a);
            }
        }
        return first is { } f ? Read(f) : (null, null, null, null);
    }

    private static (long?, string?, string?, string?) Read(JsonElement a) {
        long? id = a.TryGetProperty("assessment_id", out var idp) ? AsLong(idp) : null;
        var url = a.TryGetProperty("url", out var up) && up.ValueKind == JsonValueKind.String ? up.GetString() : null;
        var code = a.TryGetProperty("red_list_category_code", out var cp) && cp.ValueKind == JsonValueKind.String ? cp.GetString() : null;
        var year = a.TryGetProperty("year_published", out var yp) ? (yp.ValueKind == JsonValueKind.String ? yp.GetString() : yp.ValueKind == JsonValueKind.Number ? yp.GetRawText() : null) : null;
        return (id, url, code, year);
    }

    private static bool IsTrue(JsonElement e) =>
        e.ValueKind == JsonValueKind.True || (e.ValueKind == JsonValueKind.String && string.Equals(e.GetString(), "true", StringComparison.OrdinalIgnoreCase));

    private static long? AsLong(JsonElement e) => e.ValueKind switch {
        JsonValueKind.Number => e.GetInt64(),
        JsonValueKind.String when long.TryParse(e.GetString(), out var n) => n,
        _ => null,
    };
}
