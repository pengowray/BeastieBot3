using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// One pass over the cached IUCN taxa JSON that finds synonym names carrying formatting
// irregularities and classifies each into a fixed set of issues. The whitespace report and the
// markup/unusual-character report both read this same scan (memoised per connection so the JSON is
// parsed once), then each keeps the records relevant to its family of issues. Replaces the analysis
// that used to live inline in SynonymFormattingProducer.

namespace BeastieBot3.Audit.Producers;

// Every irregularity a synonym name can carry. The first group is whitespace; the second is
// markup / unusual characters. Order is also severity order (lower = more serious) for sorting.
internal enum SynonymIssue {
    HtmlMarkup,
    UnusualCharacter,
    CurlyQuotes,
    HtmlEntity,
    EmptyOrWhitespace,
    SpecialWhitespace,
    SpaceInsideParentheses,
    SpaceBeforeComma,
    LeadingWhitespace,
    TrailingWhitespace,
    RepeatedSpaces,
}

internal static class SynonymIssues {
    public static readonly IReadOnlyList<SynonymIssue> Whitespace = new[] {
        SynonymIssue.LeadingWhitespace,
        SynonymIssue.TrailingWhitespace,
        SynonymIssue.RepeatedSpaces,
        SynonymIssue.SpecialWhitespace,
        SynonymIssue.SpaceInsideParentheses,
        SynonymIssue.SpaceBeforeComma,
        SynonymIssue.EmptyOrWhitespace,
    };

    public static readonly IReadOnlyList<SynonymIssue> Other = new[] {
        SynonymIssue.HtmlMarkup,
        SynonymIssue.HtmlEntity,
        SynonymIssue.CurlyQuotes,
        SynonymIssue.UnusualCharacter,
    };

    public static bool IsWhitespace(SynonymIssue issue) => Whitespace.Contains(issue);

    public static string Label(SynonymIssue issue) => issue switch {
        SynonymIssue.LeadingWhitespace => "leading whitespace",
        SynonymIssue.TrailingWhitespace => "trailing whitespace",
        SynonymIssue.RepeatedSpaces => "double spaces",
        SynonymIssue.SpecialWhitespace => "non-breaking or control whitespace",
        SynonymIssue.SpaceInsideParentheses => "space inside parentheses",
        SynonymIssue.SpaceBeforeComma => "space before a comma",
        SynonymIssue.EmptyOrWhitespace => "blank or whitespace only",
        SynonymIssue.HtmlMarkup => "HTML markup",
        SynonymIssue.HtmlEntity => "HTML entity",
        SynonymIssue.CurlyQuotes => "curly or typographic quotes",
        SynonymIssue.UnusualCharacter => "unusual character or encoding",
        _ => issue.ToString(),
    };
}

// A flagged synonym with its taxonomy context, the issues it carries, and the cleaned suggestion.
internal sealed class SynonymRecord {
    public required long RootSisId { get; init; }
    public long? AssessmentId { get; init; }
    public string? Url { get; init; }
    public required string AcceptedName { get; init; }
    public string? CommonName { get; init; }
    public string? Kingdom { get; init; }
    public string? Phylum { get; init; }
    public string? Class { get; init; }
    public string? Order { get; init; }
    public string? Family { get; init; }
    public string? StatusCode { get; init; }
    public string? StatusCategory { get; init; }
    public string? Year { get; init; }
    public required string Synonym { get; init; }
    public string? Suggested { get; init; }
    public required IReadOnlyList<SynonymIssue> Issues { get; init; }
}

internal sealed class SynonymScanResult {
    public required IReadOnlyList<SynonymRecord> Records { get; init; }
    public long TotalSynonyms { get; init; }   // every synonym examined, flagged or not
}

internal static class SynonymFormattingScan {
    private static readonly ConditionalWeakTable<SqliteConnection, SynonymScanResult> Cache = new();

    public static SynonymScanResult Scan(SqliteConnection connection, AuditContext ctx) {
        if (Cache.TryGetValue(connection, out var cached)) {
            return cached;
        }
        var result = ScanCore(connection, ctx);
        Cache.AddOrUpdate(connection, result);
        return result;
    }

    private static SynonymScanResult ScanCore(SqliteConnection connection, AuditContext ctx) {
        var sql = "SELECT root_sis_id, json FROM taxa ORDER BY root_sis_id";
        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var records = new List<SynonymRecord>();
        long total = 0;

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }
            var json = reader.GetString(1);

            JsonDocument document;
            try { document = JsonDocument.Parse(json); } catch (JsonException) { continue; }
            using (document) {
                var root = document.RootElement;
                if (!root.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                    continue;
                }
                if (!taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                    continue;
                }

                var taxonName = FirstString(taxon, "taxon_name", "scientific_name", "taxon_scientific_name", "name");
                var (assessmentId, url, code, year) = PrimaryAssessment(root);
                var taxonomy = IucnTaxaTaxonomyExtractor.Extract(json);

                foreach (var synElement in synonyms.EnumerateArray()) {
                    var synonym = SynonymName(synElement);
                    if (synonym is null) {
                        continue;
                    }
                    total++;

                    var issues = Classify(synonym);
                    if (issues.Count == 0) {
                        continue;
                    }
                    var cleaned = TextIrregularities.Clean(synonym);
                    var suggested = cleaned.Length == 0 || string.Equals(synonym, cleaned, StringComparison.Ordinal) ? null : cleaned;

                    records.Add(new SynonymRecord {
                        RootSisId = rootSisId,
                        AssessmentId = assessmentId,
                        Url = url,
                        AcceptedName = taxonName ?? taxonomy?.ScientificName ?? $"SIS {rootSisId}",
                        CommonName = taxonomy?.CommonName,
                        Kingdom = taxonomy?.KingdomName,
                        Phylum = taxonomy?.PhylumName,
                        Class = taxonomy?.ClassName,
                        Order = taxonomy?.OrderName,
                        Family = taxonomy?.FamilyName,
                        StatusCode = AuditMapping.CodeFromCode(code),
                        StatusCategory = AuditMapping.CategoryText(code),
                        Year = year,
                        Synonym = synonym,
                        Suggested = suggested,
                        Issues = issues,
                    });
                }
            }
        }

        return new SynonymScanResult { Records = records, TotalSynonyms = total };
    }

    private static IReadOnlyList<SynonymIssue> Classify(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return new[] { SynonymIssue.EmptyOrWhitespace };
        }
        var issues = new List<SynonymIssue>();
        if (TextIrregularities.HasLeadingWhitespace(value)) issues.Add(SynonymIssue.LeadingWhitespace);
        if (TextIrregularities.HasTrailingWhitespace(value)) issues.Add(SynonymIssue.TrailingWhitespace);
        if (TextIrregularities.HasDoubleSpace(value)) issues.Add(SynonymIssue.RepeatedSpaces);
        if (TextIrregularities.HasSpecialWhitespace(value)) issues.Add(SynonymIssue.SpecialWhitespace);
        if (TextIrregularities.HasSpaceInsideParentheses(value)) issues.Add(SynonymIssue.SpaceInsideParentheses);
        if (TextIrregularities.HasSpaceBeforeComma(value)) issues.Add(SynonymIssue.SpaceBeforeComma);
        if (TextIrregularities.HasMarkup(value)) issues.Add(SynonymIssue.HtmlMarkup);
        if (TextIrregularities.HasHtmlEntity(value)) issues.Add(SynonymIssue.HtmlEntity);
        if (TextIrregularities.HasCurlyQuotes(value)) issues.Add(SynonymIssue.CurlyQuotes);
        if (TextIrregularities.HasUnusualCharacter(value)) issues.Add(SynonymIssue.UnusualCharacter);
        return issues;
    }

    // Build an AuditFinding for one record, scoped to the issues a given report lists.
    public static AuditFinding BuildFinding(SynonymRecord r, string reportId, IReadOnlyList<SynonymIssue> shownIssues, string? suggested) {
        var labels = string.Join("; ", shownIssues.Select(SynonymIssues.Label));
        var finding = new AuditFinding {
            ReportId = reportId,
            Key = $"{r.RootSisId}:{r.Synonym}",
            TaxonId = r.RootSisId,
            AssessmentId = r.AssessmentId,
            RedlistUrl = !string.IsNullOrEmpty(r.Url) ? r.Url : IucnUrls.Species(r.RootSisId, r.AssessmentId),
            ScientificName = r.AcceptedName,
            CommonName = r.CommonName,
            Kingdom = r.Kingdom,
            Phylum = r.Phylum,
            Class = r.Class,
            Order = r.Order,
            Family = r.Family,
            StatusCode = r.StatusCode,
            StatusCategory = r.StatusCategory,
            YearPublished = r.Year,
            DataSource = "iucn-api",
            Field = "synonym",
            CurrentValue = r.Synonym,
            SuggestedValue = suggested,
            IssueType = labels,
            SeverityTier = shownIssues.Count,
            Detail = labels,
        };
        if (shownIssues.Contains(SynonymIssue.HtmlMarkup) || shownIssues.Contains(SynonymIssue.HtmlEntity)) {
            finding.Notes.Add("Contains markup, which can indicate a copy from a rendered page.");
        }
        if (shownIssues.Contains(SynonymIssue.EmptyOrWhitespace)) {
            finding.Notes.Add("Blank or whitespace-only synonym, so no normalised value can be suggested.");
        }
        return finding;
    }

    // -- JSON helpers (moved from the former SynonymFormattingProducer) ---------------------

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

    private static (long? AssessmentId, string? Url, string? Code, string? Year) PrimaryAssessment(JsonElement root) {
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
