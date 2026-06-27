using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// English common names in the cached IUCN taxa records that carry a likely error or a formatting
// choice third parties should be aware of. This is a refresh, against the current release, of a
// hand-compiled 2016 list of Red List common-name oddities: species codes sitting in the name field,
// all-capitals names, stray whitespace, an acute accent or backtick used as an apostrophe, a leading
// "The", a "(FB)" FishBase marker, ampersand/slash separators, non-English-script characters, and the
// curated set of likely plurals. Checks that found nothing this release (comma inside parentheses, a
// literal question mark) are still listed at zero so it is clear they ran. Low-value 2016 checks are
// intentionally dropped: abbreviation dots (St., Mt.) are legitimate, spelling needs a dictionary, and
// the broad "possible plural" sweep was almost all false positives.

namespace BeastieBot3.Audit.Producers;

// Ordered most-serious first; that order is reused for sorting and for the summary rows.
internal enum CommonNameIssue {
    SpeciesCode,
    NonEnglishScript,
    ControlCharacter,
    QuestionMark,
    AllCaps,
    AcuteApostrophe,
    CommaInParentheses,
    Ampersand,
    Slash,
    FishbaseMarker,
    RedundantThe,
    LikelyPlural,
    ContainsNumber,
    LeadingWhitespace,
    TrailingWhitespace,
    DoubleSpace,
}

internal sealed class CommonNameIssuesProducer : IAuditReportProducer {
    public string Id => "common-name-issues";

    // The 2016 "Likely plurals" list: name-final words that read as plural and should usually be
    // singular to match the rest of the database. High precision, unlike a blanket "ends in s".
    private static readonly HashSet<string> PluralEndings = new(StringComparer.OrdinalIgnoreCase) {
        "anchovies", "badgers", "bats", "carps", "cats", "crabs", "fishes", "frogs", "herrings",
        "mullets", "rats", "razorback", "silversides", "snails", "snakes", "snappers", "tetras",
        "toads", "treefrogs", "wrasses",
    };

    private static readonly Regex NonEnglishScript = new(@"[Ͱ-ϿЀ-ӿ]", RegexOptions.Compiled);
    private static readonly Regex CommaInParens = new(@"\([^)]*,[^)]*\)", RegexOptions.Compiled);
    private static readonly Regex FishbaseTag = new(@"\(fb\)\s*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var findings = new List<AuditFinding>();
        var perName = new List<IReadOnlyList<CommonNameIssue>>();
        Scan(conn, ctx, findings, perName);

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "English common name issues",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = "IUCN API (English common names)",
            Summary =
                "Each row is an English common name that carries a likely error or a formatting choice worth knowing about, with a tidied suggestion where one is clear. " +
                "The current value shows otherwise-invisible characters as markers. The scientific name column is the taxon the name belongs to. " +
                "This refreshes a hand-compiled 2016 review against the current release. Several of the original checks now find nothing, because the move to a Unicode API removed the question-mark and broken-separator artefacts; those checks are still listed at zero. " +
                "The summary counts each kind separately, so they add up to more than the distinct total when a name has several.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Taxon"),
                AuditColumns.CurrentValue("Common name (English)", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Suggested", AuditColumnType.Code),
                AuditColumns.IssueType("Issue(s)"),
                AuditColumns.Custom("mainName", "Primary", AuditColumnType.Text),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId(),
                AuditColumns.RedlistLink(),
            },
            Findings = ordered,
            SummaryTables = new List<AuditSummaryTable> {
                BuildSummary(perName),
            },
        };
    }

    private static void Scan(SqliteConnection connection, AuditContext ctx,
        List<AuditFinding> findings, List<IReadOnlyList<CommonNameIssue>> perName) {

        var sql = "SELECT root_sis_id, json FROM taxa ORDER BY root_sis_id";
        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + " LIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

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
                if (!taxon.TryGetProperty("common_names", out var names) || names.ValueKind != JsonValueKind.Array) {
                    continue;
                }

                TaxaTaxonomyInfo? taxonomy = null;
                (long? AssessmentId, string? Url, string? Code, string? Year)? assessment = null;

                foreach (var entry in names.EnumerateArray()) {
                    if (entry.ValueKind != JsonValueKind.Object) {
                        continue;
                    }
                    if (!(entry.TryGetProperty("language", out var lang) && lang.ValueKind == JsonValueKind.String && lang.GetString() == "eng")) {
                        continue;
                    }
                    if (!(entry.TryGetProperty("name", out var nm) && nm.ValueKind == JsonValueKind.String)) {
                        continue;
                    }
                    var name = nm.GetString();
                    if (string.IsNullOrEmpty(name)) {
                        continue;
                    }
                    var issues = Classify(name);
                    if (issues.Count == 0) {
                        continue;
                    }

                    // Resolve the shared taxon context lazily, only when this taxon has a flagged name.
                    taxonomy ??= IucnTaxaTaxonomyExtractor.Extract(json);
                    assessment ??= PrimaryAssessment(root);
                    var isMain = entry.TryGetProperty("main", out var mainEl) && mainEl.ValueKind == JsonValueKind.True;
                    var suggested = CleanedSuggestion(name, issues);

                    var finding = new AuditFinding {
                        ReportId = "common-name-issues",
                        Key = $"{rootSisId}:{name}",
                        TaxonId = rootSisId,
                        AssessmentId = assessment.Value.AssessmentId,
                        RedlistUrl = !string.IsNullOrEmpty(assessment.Value.Url) ? assessment.Value.Url : IucnUrls.Species(rootSisId, assessment.Value.AssessmentId),
                        ScientificName = taxonomy?.ScientificName ?? $"SIS {rootSisId}",
                        Kingdom = taxonomy?.KingdomName,
                        Phylum = taxonomy?.PhylumName,
                        Class = taxonomy?.ClassName,
                        Order = taxonomy?.OrderName,
                        Family = taxonomy?.FamilyName,
                        StatusCode = AuditMapping.CodeFromCode(assessment.Value.Code),
                        StatusCategory = AuditMapping.CategoryText(assessment.Value.Code),
                        YearPublished = assessment.Value.Year,
                        DataSource = "iucn-api",
                        Field = "common name (eng)",
                        CurrentValue = name,
                        SuggestedValue = suggested,
                        IssueType = string.Join("; ", issues.Select(Label)),
                        SeverityTier = 100 - (int)issues.Min(),
                        Detail = string.Join("; ", issues.Select(Label)),
                    };
                    finding.Extra["mainName"] = isMain ? "yes" : "no";
                    findings.Add(finding);
                    perName.Add(issues);
                }
            }
        }
    }

    private static IReadOnlyList<CommonNameIssue> Classify(string name) {
        var issues = new List<CommonNameIssue>();
        var lower = name.ToLowerInvariant();

        if (lower.Contains("species code", StringComparison.Ordinal)) issues.Add(CommonNameIssue.SpeciesCode);
        if (NonEnglishScript.IsMatch(name)) issues.Add(CommonNameIssue.NonEnglishScript);
        if (TextIrregularities.HasUnusualCharacter(name)) issues.Add(CommonNameIssue.ControlCharacter);
        if (name.Contains('?')) issues.Add(CommonNameIssue.QuestionMark);
        if (IsAllCaps(name)) issues.Add(CommonNameIssue.AllCaps);
        if (name.Contains('´') || name.Contains('`')) issues.Add(CommonNameIssue.AcuteApostrophe);
        if (CommaInParens.IsMatch(name)) issues.Add(CommonNameIssue.CommaInParentheses);
        if (name.Contains('&')) issues.Add(CommonNameIssue.Ampersand);
        if (name.Contains('/')) issues.Add(CommonNameIssue.Slash);
        if (FishbaseTag.IsMatch(name)) issues.Add(CommonNameIssue.FishbaseMarker);
        if (lower.StartsWith("the ", StringComparison.Ordinal)) issues.Add(CommonNameIssue.RedundantThe);
        if (IsLikelyPlural(name)) issues.Add(CommonNameIssue.LikelyPlural);
        if (name.Any(char.IsDigit)) issues.Add(CommonNameIssue.ContainsNumber);
        if (TextIrregularities.HasLeadingWhitespace(name)) issues.Add(CommonNameIssue.LeadingWhitespace);
        if (TextIrregularities.HasTrailingWhitespace(name)) issues.Add(CommonNameIssue.TrailingWhitespace);
        if (TextIrregularities.HasDoubleSpace(name)) issues.Add(CommonNameIssue.DoubleSpace);

        issues.Sort();
        return issues;
    }

    // All cased letters are uppercase, with enough letters and a space, so multi-word shouting names
    // ("STEPPE VOLE") are caught but short codes are not.
    private static bool IsAllCaps(string name) {
        if (!name.Contains(' ') || name.Count(char.IsLetter) < 4) {
            return false;
        }
        var hasCased = false;
        foreach (var c in name) {
            if (char.IsLower(c)) {
                return false;
            }
            if (char.IsUpper(c)) {
                hasCased = true;
            }
        }
        return hasCased;
    }

    private static bool IsLikelyPlural(string name) {
        var lastSpace = name.LastIndexOf(' ');
        var last = (lastSpace >= 0 ? name[(lastSpace + 1)..] : name).Trim().Trim('.', ',', ')', '(');
        return PluralEndings.Contains(last);
    }

    private static string? CleanedSuggestion(string name, IReadOnlyList<CommonNameIssue> issues) {
        var s = name.Replace('´', '\'').Replace('`', '\'');
        s = StripControl(s);
        s = CollapseWhitespace(s);
        if (issues.Contains(CommonNameIssue.AllCaps)) {
            s = TitleCase(s);
        }
        return s.Length == 0 || string.Equals(s, name, StringComparison.Ordinal) ? null : s;
    }

    private static string StripControl(string s) {
        if (!s.Any(c => c is '­' or '​' or '‌' or '‍' or '﻿' or '�' || (char.IsControl(c) && c is not ('\t' or '\r' or '\n')))) {
            return s;
        }
        var sb = new StringBuilder(s.Length);
        foreach (var c in s) {
            if (c is '­' or '​' or '‌' or '‍' or '﻿' or '�') continue;
            if (char.IsControl(c) && c is not ('\t' or '\r' or '\n')) continue;
            sb.Append(c);
        }
        return sb.ToString();
    }

    private static string CollapseWhitespace(string s) {
        var sb = new StringBuilder(s.Length);
        var prevSpace = false;
        foreach (var ch in s) {
            if (char.IsWhiteSpace(ch)) {
                if (!prevSpace) { sb.Append(' '); prevSpace = true; }
            } else {
                sb.Append(ch); prevSpace = false;
            }
        }
        return sb.ToString().Trim();
    }

    private static string TitleCase(string s) {
        var parts = s.Split(' ');
        for (var i = 0; i < parts.Length; i++) {
            var w = parts[i];
            if (w.Length > 0) {
                parts[i] = char.ToUpperInvariant(w[0]) + w[1..].ToLowerInvariant();
            }
        }
        return string.Join(' ', parts);
    }

    private static AuditSummaryTable BuildSummary(IReadOnlyList<IReadOnlyList<CommonNameIssue>> perName) {
        var rows = Enum.GetValues<CommonNameIssue>()
            .Select(issue => new[] {
                Label(issue),
                perName.Count(list => list.Contains(issue)).ToString("N0", CultureInfo.InvariantCulture),
            } as IReadOnlyList<string>)
            .ToList();
        rows.Add(new[] { "Total (distinct names)", perName.Count.ToString("N0", CultureInfo.InvariantCulture) });
        return new AuditSummaryTable {
            Title = "Issues by kind",
            Note = "Each kind is counted once per name; because a name can carry several, the kinds add up to more than the distinct total. Kinds listed at 0 were checked and found nothing this release.",
            Headers = new[] { "Issue", "Names" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static string Label(CommonNameIssue issue) => issue switch {
        CommonNameIssue.SpeciesCode => "species code, not a name",
        CommonNameIssue.NonEnglishScript => "non-English-script characters",
        CommonNameIssue.ControlCharacter => "control character (e.g. soft hyphen)",
        CommonNameIssue.QuestionMark => "question mark or replacement character",
        CommonNameIssue.AllCaps => "all capitals",
        CommonNameIssue.AcuteApostrophe => "acute accent or backtick for an apostrophe",
        CommonNameIssue.CommaInParentheses => "comma inside parentheses",
        CommonNameIssue.Ampersand => "ampersand",
        CommonNameIssue.Slash => "slash separating alternatives",
        CommonNameIssue.FishbaseMarker => "FishBase \"(FB)\" marker",
        CommonNameIssue.RedundantThe => "begins with \"The\"",
        CommonNameIssue.LikelyPlural => "likely plural",
        CommonNameIssue.ContainsNumber => "contains a number",
        CommonNameIssue.LeadingWhitespace => "leading whitespace",
        CommonNameIssue.TrailingWhitespace => "trailing whitespace",
        CommonNameIssue.DoubleSpace => "double spaces",
        _ => issue.ToString(),
    };

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
            if (a.TryGetProperty("latest", out var latest) &&
                (latest.ValueKind == JsonValueKind.True || (latest.ValueKind == JsonValueKind.String && string.Equals(latest.GetString(), "true", StringComparison.OrdinalIgnoreCase)))) {
                return Read(a);
            }
        }
        return first is { } f ? Read(f) : (null, null, null, null);
    }

    private static (long?, string?, string?, string?) Read(JsonElement a) {
        long? id = a.TryGetProperty("assessment_id", out var idp)
            ? (idp.ValueKind == JsonValueKind.Number ? idp.GetInt64() : idp.ValueKind == JsonValueKind.String && long.TryParse(idp.GetString(), out var n) ? n : null)
            : null;
        var url = a.TryGetProperty("url", out var up) && up.ValueKind == JsonValueKind.String ? up.GetString() : null;
        var code = a.TryGetProperty("red_list_category_code", out var cp) && cp.ValueKind == JsonValueKind.String ? cp.GetString() : null;
        var year = a.TryGetProperty("year_published", out var yp) ? (yp.ValueKind == JsonValueKind.String ? yp.GetString() : yp.ValueKind == JsonValueKind.Number ? yp.GetRawText() : null) : null;
        return (id, url, code, year);
    }
}
