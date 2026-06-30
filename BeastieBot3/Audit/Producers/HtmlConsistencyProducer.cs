using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
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

    // The IUCN API stores the same six narrative fields under its documentation object; the keys
    // differ from the CSV column names for two of them. Used to cross-check the CSV HTML against the
    // API for each flagged row, so the report can say whether a difference is in the source data.
    private static readonly IReadOnlyDictionary<string, string> ApiDocKeys = new Dictionary<string, string> {
        ["rationale"] = "rationale", ["habitat"] = "habitats", ["threats"] = "threats",
        ["population"] = "population", ["range"] = "range", ["useTrade"] = "use_trade",
    };

    // Zero-width and soft-hyphen characters: invisible on screen but counted as text differences.
    // Named so the modal can say exactly which one accounts for an otherwise-invisible difference.
    private static readonly IReadOnlyDictionary<char, string> InvisibleNames = new Dictionary<char, string> {
        ['­'] = "soft hyphen",
        ['​'] = "zero-width space",
        ['‌'] = "zero-width non-joiner",
        ['‍'] = "zero-width joiner",
        ['‎'] = "left-to-right mark",
        ['‏'] = "right-to-left mark",
        ['⁠'] = "word joiner",
        ['﻿'] = "zero-width no-break space",
    };

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
            "Six narrative fields (rationale, habitat, threats, population, range, use and trade) appear in two of the IUCN CSV exports: assessments_with_html.csv carries them with HTML markup, and assessments.csv carries a plain-text version of the same field. " +
            "For each assessment the assessments_with_html.csv value is reduced to plain text and compared against the assessments.csv value. " +
            "Differences that are only whitespace, non-breaking spaces, or entity encoding are treated as a match and not listed, so a row here means the readable text genuinely differs. The comparison is about text serialisation only and says nothing about the scientific content.";

        // A common cause is heavy redundant markup: some fields carry a large amount of repeated empty
        // tags (for example long runs of nested empty spans from a rich-text editor), and the
        // plain-text version then comes out empty or truncated. Call out the worst case when present.
        var worst = findings
            .Where(f => f.IssueType is "plain-text-empty, redundant-markup" or "plain-text-truncated, redundant-markup")
            .OrderByDescending(f => double.TryParse((f.Get("markupRatio") ?? "").Replace(",", ""), NumberStyles.Any, CultureInfo.InvariantCulture, out var r) ? r : 0)
            .FirstOrDefault();
        if (worst is not null) {
            var ratio = worst.Get("markupRatio");
            var len = worst.Get("htmlLen");
            summary += "\n\n" +
                       $"A recurring pattern is heavy redundant markup: the HTML carries long runs of repeated empty tags, and the plain-text version then comes out empty or truncated. " +
                       $"The most extreme case here is {worst.ScientificName} ({worst.Field}), whose HTML runs to about {len} characters, roughly {ratio} times its readable text, and whose plain-text export does not get past the markup. " +
                       $"These rows are marked redundant-markup and explained in the detail column.";
        }

        summary += "\n\n" +
            "Use the Compare button on any row to open a side-by-side view of the assessments.csv plain text and the suggested plain text extracted from assessments_with_html.csv, with the HTML source shown colour-coded so the empty-tag runs are visible, plus a suggested cleaned-up HTML with the redundant markup removed. " +
            "Each view opens with a short note on what changed (for example, an invisible character that accounts for an otherwise hidden difference) and, where the IUCN API copy is cached, a note on whether the field matches the API.";

        summary +=
            "\n\n### Why it matters\n\n" +
            "When the HTML and plain-text versions of a field disagree, readers see different content depending on which export or view they use, and a field that comes out empty or truncated drops information the HTML still holds.\n\n" +
            "### Suggestion\n\n" +
            "Regenerate the plain-text export from the HTML so the two agree, and clean up the redundant empty markup that causes the empty and truncated cases.";

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
                // Bulky narrative values are kept in the CSV download but shown via the Compare modal
                // on screen (CsvOnly). Headers name the source CSV so the reader knows which file each
                // side comes from.
                new AuditColumn {
                    Key = "currentValue", Header = "assessments.csv text at difference",
                    Type = AuditColumnType.LongText, CsvOnly = true, Value = f => f.CurrentValue,
                },
                new AuditColumn {
                    Key = "suggestedValue", Header = "assessments_with_html.csv text at difference",
                    Type = AuditColumnType.LongText, CsvOnly = true, Value = f => f.SuggestedValue,
                },
                new AuditColumn {
                    Key = "view", Header = "Compare", Type = AuditColumnType.Viewer, HtmlOnly = true,
                    Value = _ => "Compare",
                    Help = "Open a side-by-side view of the assessments.csv plain text, the suggested plain text extracted from the HTML, the assessments_with_html.csv HTML source, and a suggested cleaned-up HTML, with a note on what changed and how it compares with the IUCN API.",
                    Data = new Dictionary<string, Func<AuditFinding, string?>> {
                        ["view-name"] = f => f.ScientificName,
                        ["view-field"] = f => f.Field,
                        ["view-issue"] = f => f.IssueType,
                        ["view-ratio"] = f => f.Get("viewRatio"),
                        ["view-htmllen"] = f => f.Get("htmlLen"),
                        ["view-change-note"] = f => f.Get("viewChangeNote"),
                        ["view-api"] = f => f.Get("viewApi"),
                        ["view-plain"] = f => f.Get("viewPlain"),
                        ["view-readable"] = f => f.Get("viewReadable"),
                        ["view-html"] = f => f.Get("viewHtml"),
                        ["view-clean"] = f => f.Get("viewClean"),
                        ["view-clean-verified"] = f => f.Get("viewCleanVerified"),
                    },
                },
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

        // The IUCN API cache (when present) lets each flagged field be compared against the API copy,
        // so the modal can say whether a difference is in the source data. Looked up lazily and
        // memoised on the last assessment, since a row's fields are processed together.
        var apiConn = ctx.IucnApiCacheOrNull();
        var hasApi = apiConn is not null && AuditContext.ObjectExists(apiConn, "assessments");
        var apiCacheId = -1L;
        IReadOnlyDictionary<string, string?>? apiCacheDoc = null;
        IReadOnlyDictionary<string, string?>? GetApiDoc(long id) {
            if (!hasApi) {
                return null;
            }
            if (id != apiCacheId) {
                apiCacheId = id;
                apiCacheDoc = FetchApiDoc(apiConn!, id);
            }
            return apiCacheDoc;
        }

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

                // The two sides usually agree at the start (the plain field is often a truncated
                // prefix), so show each column windowed around the first point of difference rather
                // than the identical opening characters.
                var diffAt = FirstDifference(plainText, htmlText);

                var rawHtmlLen = htmlVal?.Length ?? 0;
                var redundant = rawHtmlLen >= RedundantMinHtmlChars && htmlText.Length > 0 && rawHtmlLen >= htmlText.Length * RedundantRatio;
                var ratio = htmlText.Length > 0 ? (double)rawHtmlLen / htmlText.Length : 0;

                string issueType;
                string detail;
                int severity;

                if (plainText.Length == 0 && htmlText.Length > 0) {
                    if (redundant) {
                        issueType = "plain-text-empty, redundant-markup"; severity = 5;
                        detail = $"The assessments.csv field is empty while assessments_with_html.csv carries text. The HTML is about {ratio:N0} times the size of its readable text, with a large amount of redundant markup that the plain-text version appears not to get past.";
                    } else {
                        issueType = "plain-text-empty"; severity = 3;
                        detail = "The assessments.csv field is empty while the assessments_with_html.csv version carries text.";
                    }
                } else if (htmlText.Length == 0 && plainText.Length > 0) {
                    issueType = "html-text-empty"; severity = 3;
                    detail = "The assessments_with_html.csv version is empty while the assessments.csv field has text.";
                } else if (htmlText.StartsWith(plainText, StringComparison.Ordinal) && plainText.Length < htmlText.Length) {
                    if (redundant) {
                        issueType = "plain-text-truncated, redundant-markup"; severity = 5;
                        detail = $"The assessments.csv field stops early. The HTML is about {ratio:N0} times the size of its readable text, with a large amount of redundant markup that the plain-text version appears not to get past.";
                    } else {
                        issueType = "plain-text-truncated"; severity = 3;
                        detail = "The assessments.csv field stops early relative to the assessments_with_html.csv version.";
                    }
                } else {
                    issueType = "text-differs"; severity = 4;
                    detail = $"The {field} field differs between its assessments_with_html.csv and assessments.csv versions.";
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
                    CurrentValue = Window(plainText, diffAt),
                    SuggestedValue = Window(htmlText, diffAt),
                    IssueType = issueType,
                    SeverityTier = severity,
                    Detail = detail,
                };
                if (redundant) {
                    finding.Extra["markupRatio"] = ratio.ToString("N0", CultureInfo.InvariantCulture);
                }

                // Payload for the modal viewer: the normalised readable text of each side (shown in
                // full — the scroll boxes handle the length), the raw HTML source, and a suggested
                // cleaned-up HTML. The cleaned suggestion is only attached when it actually removed
                // something, and is flagged "yes"/"no" by whether its extracted text still matches the
                // original so the modal can say whether it has been verified identical. A short
                // "what changed" note and, when the API cache is present, an API-comparison note are
                // attached so the reader does not have to hunt through the panes.
                finding.Extra["viewPlain"] = plainText;
                finding.Extra["viewReadable"] = htmlText;
                finding.Extra["viewHtml"] = htmlVal ?? "";
                finding.Extra["htmlLen"] = rawHtmlLen.ToString("N0", CultureInfo.InvariantCulture);
                finding.Extra["viewRatio"] = ratio > 0 ? ratio.ToString("N0", CultureInfo.InvariantCulture) : "";
                finding.Extra["viewChangeNote"] = DescribeChange(plainVal, htmlVal, plainText, htmlText, redundant, ratio);
                if (hasApi) {
                    finding.Extra["viewApi"] = DescribeApiComparison(GetApiDoc(assessmentId), field, htmlVal, htmlText);
                }

                var cleaned = IucnHtmlUtilities.CleanRedundantMarkup(htmlVal) ?? "";
                if (cleaned.Length > 0 && !string.Equals(cleaned, htmlVal, StringComparison.Ordinal)) {
                    finding.Extra["viewClean"] = cleaned;
                    var cleanedText = Canonical(IucnHtmlUtilities.ConvertHtmlToExactPlainText(cleaned));
                    finding.Extra["viewCleanVerified"] = string.Equals(cleanedText, htmlText, StringComparison.Ordinal) ? "yes" : "no";
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

    // Index of the first character where two strings differ (or the shorter length when one is a
    // prefix of the other). Used to centre the display window on where the two versions diverge.
    private static int FirstDifference(string a, string b) {
        var n = Math.Min(a.Length, b.Length);
        var i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    // A one-line summary of how the two versions differ, shown above the panes so the reader does not
    // have to hunt for it. The most useful case is an invisible character: the panes look identical
    // except for a gap, so the note names the exact character and the word it sits in.
    private static string DescribeChange(string? rawPlain, string? rawHtml, string plainText, string htmlText, bool redundant, double ratio) {
        if (redundant) {
            return $"Heavy redundant markup: the HTML is about {ratio:N0} times the size of its readable text, and the assessments.csv plain text does not get past it. The suggested cleaned-up HTML below restores the readable text.";
        }
        if (plainText.Length == 0) {
            return "The assessments.csv field is empty; the assessments_with_html.csv version carries the text shown below.";
        }
        if (htmlText.Length == 0) {
            return "The assessments_with_html.csv version is empty; the assessments.csv field carries the text shown below.";
        }
        if (TryDescribeInvisibleDifference(rawPlain, rawHtml, htmlText, out var note)) {
            return note;
        }
        if (htmlText.StartsWith(plainText, StringComparison.Ordinal)) {
            return "The assessments.csv field stops early; the assessments_with_html.csv version continues. The point where it stops is highlighted below.";
        }
        return "The readable text differs between the two versions. The first difference is highlighted below.";
    }

    // When the two readable texts match once zero-width / soft-hyphen characters are removed, the only
    // difference is one of those invisible characters. Names it and the word it sits in, and notes
    // that it shows in the assessments.csv pane as a gap while the HTML drops it.
    private static bool TryDescribeInvisibleDifference(string? rawPlain, string? rawHtml, string htmlText, out string note) {
        note = "";
        // htmlText already has invisibles removed (ConvertHtmlToExactPlainText drops them); compare
        // against the plain side with the same characters removed.
        var plainNoInvisible = Canonical(DeleteInvisibles(WebUtility.HtmlDecode(rawPlain ?? "")));
        if (!string.Equals(plainNoInvisible, htmlText, StringComparison.Ordinal)) {
            return false;
        }
        var found = FirstInvisible(rawPlain) ?? FirstInvisible(StripTagsLoose(rawHtml));
        if (found is null) {
            return false;
        }
        var (ch, word) = found.Value;
        var name = InvisibleNames.TryGetValue(ch, out var n) ? n : "zero-width character";
        var inWord = string.IsNullOrEmpty(word) ? "" : $" inside “{word}”";
        note = $"The only difference is an invisible {name} (U+{(int)ch:X4}){inWord}: assessments.csv keeps it (it shows here as a gap), while the HTML drops it. Recommend deleting the character.";
        return true;
    }

    // Compares the field's CSV HTML against the same field in the IUCN API. doc is the API's
    // documentation object for this assessment, or null when the assessment is not cached.
    private static string DescribeApiComparison(IReadOnlyDictionary<string, string?>? doc, string field, string? rawHtml, string htmlText) {
        if (doc is null) {
            return "This assessment is not in the IUCN API cache, so no comparison against the API was made.";
        }
        if (!doc.TryGetValue(field, out var apiVal) || apiVal is null) {
            return "The IUCN API has no value for this field, so no comparison was made.";
        }
        if (string.Equals(rawHtml ?? "", apiVal, StringComparison.Ordinal)) {
            return "The assessments_with_html.csv HTML for this field is identical to the IUCN API, so it is present in the source data, not introduced by the CSV export.";
        }
        var apiText = Canonical(IucnHtmlUtilities.ConvertHtmlToExactPlainText(apiVal));
        if (string.Equals(apiText, htmlText, StringComparison.Ordinal)) {
            return "The assessments_with_html.csv HTML matches the IUCN API in readable text for this field (markup or encoding aside).";
        }
        return "The assessments_with_html.csv HTML differs from the IUCN API for this field, so the difference may have appeared after the API export.";
    }

    // Reads the IUCN API's documentation object for one assessment from the API cache and maps the six
    // narrative fields to their CSV column names. Returns null when the assessment is not cached.
    private static IReadOnlyDictionary<string, string?>? FetchApiDoc(SqliteConnection conn, long assessmentId) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT json FROM assessments WHERE assessment_id = @a ORDER BY id DESC LIMIT 1";
        cmd.Parameters.AddWithValue("@a", assessmentId);
        if (cmd.ExecuteScalar() is not string json || json.Length == 0) {
            return null;
        }
        try {
            using var doc = JsonDocument.Parse(json);
            var map = new Dictionary<string, string?>(StringComparer.Ordinal);
            if (doc.RootElement.TryGetProperty("documentation", out var d) && d.ValueKind == JsonValueKind.Object) {
                foreach (var kv in ApiDocKeys) {
                    map[kv.Key] = d.TryGetProperty(kv.Value, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
                }
            }
            return map;
        } catch (JsonException) {
            return null;
        }
    }

    private static string DeleteInvisibles(string s) {
        var hit = false;
        foreach (var c in s) {
            if (InvisibleNames.ContainsKey(c)) { hit = true; break; }
        }
        if (!hit) {
            return s;
        }
        var sb = new StringBuilder(s.Length);
        foreach (var c in s) {
            if (!InvisibleNames.ContainsKey(c)) {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    // The first invisible character in a string and the (cleaned) word it sits in, or null.
    private static (char Ch, string Word)? FirstInvisible(string? s) {
        if (string.IsNullOrEmpty(s)) {
            return null;
        }
        var decoded = WebUtility.HtmlDecode(s);
        for (var i = 0; i < decoded.Length; i++) {
            if (InvisibleNames.ContainsKey(decoded[i])) {
                return (decoded[i], WordAround(decoded, i));
            }
        }
        return null;
    }

    private static string WordAround(string s, int index) {
        var start = index;
        var end = index;
        while (start > 0 && !char.IsWhiteSpace(s[start - 1])) {
            start--;
        }
        while (end < s.Length - 1 && !char.IsWhiteSpace(s[end + 1])) {
            end++;
        }
        var word = DeleteInvisibles(s.Substring(start, end - start + 1)).Trim();
        return word.Length <= 40 ? word : word.Substring(0, 40) + "…";
    }

    private static readonly System.Text.RegularExpressions.Regex TagLoose =
        new("<[^>]+>", System.Text.RegularExpressions.RegexOptions.Compiled);
    private static string StripTagsLoose(string? html) => html is null ? "" : TagLoose.Replace(html, " ");

    // A readable window of text around a position, with leading/trailing ellipses when it is clipped.
    private static string Window(string value, int center, int before = 40, int length = 200) {
        if (string.IsNullOrEmpty(value)) {
            return "";
        }
        var start = Math.Max(0, center - before);
        var take = Math.Min(length, value.Length - start);
        if (take <= 0) {
            start = Math.Max(0, value.Length - length);
            take = value.Length - start;
        }
        var slice = value.Substring(start, take);
        var prefix = start > 0 ? "…" : "";
        var suffix = start + take < value.Length ? "…" : "";
        return prefix + slice + suffix;
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
