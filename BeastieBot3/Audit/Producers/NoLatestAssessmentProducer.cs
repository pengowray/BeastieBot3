using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// Cached taxa with no assessment flagged latest=true. These commonly correspond to records that
// were removed, merged, or reclassified, so only historical assessments remain. Mirrors the scan in
// IucnNoCurrentAssessmentReportCommand and reuses IucnTaxaTaxonomyExtractor for taxonomy.

namespace BeastieBot3.Audit.Producers;

internal sealed class NoLatestAssessmentProducer : IAuditReportProducer {
    public string Id => "no-latest";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa") || !AuditContext.ObjectExists(conn, "taxa_assessment_backlog")) {
            return null;
        }

        var (findings, oldTaxa) = Scan(conn, ctx);
        var csv = ctx.IucnCsvOrNull();
        var inCsv = csv is not null && AuditContext.ObjectExists(csv, "assessments_html")
            ? CountInCsv(csv, findings.Select(f => f.TaxonId).OfType<long>())
            : (int?)null;
        var matches = csv is not null && AuditContext.ObjectExists(csv, "taxonomy_html") && AuditContext.ObjectExists(csv, "assessments_html")
            ? FindMatches(IucnSameNameTaxa.Load(csv, conn), findings, oldTaxa)
            : null;

        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName(),
            AuditColumns.CommonName(),
            AuditColumns.Group(),
            AuditColumns.Class(csvOnly: true),
            AuditColumns.Order(csvOnly: true),
            AuditColumns.Family(csvOnly: true),
            AuditColumns.Status("Last status"),
            AuditColumns.Year("Last assessed"),
            AuditColumns.Custom("lastScope", "Scope of last assessment", AuditColumnType.Text,
                "Geographic scope of the most recent assessment. Regional-only taxa were never assessed globally."),
        };
        if (matches is not null) {
            columns.Add(MatchColumn(SameNameKey, SameNameHeader, SameNameHelp, matches, m => m.SameName));
            columns.Add(MatchColumn(ViaSynonymKey, ViaSynonymHeader, ViaSynonymHelp, matches, m => m.ViaSynonym));
        }
        columns.AddRange(new[] {
            AuditColumns.Custom("assessmentCount", "Assessments", AuditColumnType.Number,
                "How many historical assessments the taxon has."),
            AuditColumns.TaxonId(),
            AuditColumns.RedlistLink(),
            AuditColumns.Detail(),
        });

        var summaryTables = new List<AuditSummaryTable> {
            ByYearBucket(findings),
            ByScope(findings),
        };
        if (matches is not null) {
            summaryTables.Add(ByMatch(findings, matches));
        }

        return new AuditReport {
            Id = Id,
            SectionId = "records",
            Title = "Taxa with no current assessment",
            Action = ActionClass.Policy,
            DataSourceLabel = "IUCN API",
            Blurb = "Taxa whose assessments are all historical: none is flagged as current, usually because the taxon was removed, merged, or reclassified. Split by when the last assessment was published and by its scope.",
            Summary =
                "The table below lists IUCN-assessed species and other taxa from the IUCN API where none of the taxon's assessments is flagged as the current (latest) one. " +
                "This commonly happens when a taxon was removed from the Red List, merged into another taxon, or reclassified, so only historical assessments remain. " +
                "The most recent assessment is shown for context. The count is a minimum: it covers the taxa retrieved from the API, which may not be every taxon in the release.\n\n" +
                CsvSentence(inCsv, ctx.Release) + " " +
                MatchSentence(findings, matches, ctx.Release) + "\n\n" +
                "The list sorts most recently assessed first. A taxon last assessed in the past few years is most likely a taxonomic change made since, and is the easiest to confirm; one last assessed in 1996 or 1998 has stayed in this state through every release since. " +
                "The Scope column separates taxa whose only assessments were regional (Europe, the Mediterranean, Pan-Africa) from taxa that once had a global assessment.\n\n" +
                "### Why it matters\n\n" +
                "These pages are easy to mistake for current assessments on the Red List website. " +
                "Many do not come up through search on iucnredlist.org, yet each stays reachable through its direct species URL. " +
                "Some of the pages include a note such as \"(This concept is no longer recognised)\", but it is easy to overlook, and many have no such note at all.\n\n" +
                "### Suggestion\n\n" +
                "- For any taxon which was removed, merged, or reclassified: create a new Not Evaluated (NE) assessment for the taxon. The NE \"assessment\" becomes the current assessment. This will show it's no longer assessed by the IUCN, and do so in a way consistent with the rest of the Red List data and site.\n" +
                "- If the taxon is still valid, flag its most recent assessment as current.\n" +
                "- Update how old assessments display on the website so they cannot be mistaken for the current one. Have old assessment pages include a link to the current assessment or make it clear when there is none.",
            Columns = columns,
            Findings = findings,
            SummaryTables = summaryTables,
            ShowGroupCounts = true,
        };
    }

    private static (IReadOnlyList<AuditFinding> Findings, Dictionary<string, IucnOldTaxon> OldTaxa) Scan(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT t.root_sis_id, t.json FROM taxa t
WHERE NOT EXISTS (
    SELECT 1 FROM taxa_assessment_backlog b WHERE b.taxa_id = t.id AND b.latest = 1
)
ORDER BY t.root_sis_id";

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + "\nLIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var findings = new List<AuditFinding>();
        var oldTaxa = new Dictionary<string, IucnOldTaxon>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }
            var json = reader.GetString(1);

            if (JsonHasLatest(json)) {
                continue; // stale-backlog false positive
            }

            var taxonomy = IucnTaxaTaxonomyExtractor.Extract(json);
            var (assessmentId, year, code, url, scope, count) = MostRecent(json);
            var statusCode = AuditMapping.CodeFromCode(code);

            var finding = new AuditFinding {
                ReportId = "no-latest",
                Key = $"{rootSisId}",
                TaxonId = rootSisId,
                AssessmentId = assessmentId,
                RedlistUrl = !string.IsNullOrEmpty(url) ? url : IucnUrls.Species(rootSisId, assessmentId),
                ScientificName = taxonomy?.ScientificName ?? $"SIS {rootSisId}",
                CommonName = taxonomy?.CommonName,
                Kingdom = taxonomy?.KingdomName,
                Phylum = taxonomy?.PhylumName,
                Class = taxonomy?.ClassName,
                Order = taxonomy?.OrderName,
                Family = taxonomy?.FamilyName,
                StatusCode = statusCode,
                StatusCategory = AuditMapping.CategoryText(code),
                YearPublished = year,
                Latest = false,
                DataSource = "iucn-api",
                Field = "latest",
                CurrentValue = "no latest assessment",
                IssueType = "no-latest-assessment",
                Detail = $"No assessment is flagged current. Most recent assessment: {statusCode ?? "unknown"}{(string.IsNullOrEmpty(year) ? "" : $" ({year})")}.",
            };
            finding.Extra["lastScope"] = scope;
            finding.Extra["assessmentCount"] = count.ToString(CultureInfo.InvariantCulture);
            findings.Add(finding);
            if (IucnSameNameTaxa.OldTaxonFromJson(rootSisId, json) is { } old) {
                oldTaxa[finding.Key] = old;
            }
        }

        var sorted = findings
            .OrderByDescending(f => int.TryParse(f.YearPublished, out var y) ? y : 0)
            .ThenBy(f => f.Kingdom, StringComparer.OrdinalIgnoreCase)
            .ThenBy(TaxonGroups.SortKey, StringComparer.Ordinal)
            .ThenBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Family, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return (sorted, oldTaxa);
    }

    // ---- Current taxa with the same name, or listing the name as an IUCN synonym ----

    private const string SameNameKey = "sameNameCurrent";
    private const string ViaSynonymKey = "viaSynonymCurrent";

    private sealed record RowMatches(IucnOldTaxon Old, IReadOnlyList<IucnSameNameMatch> SameName, IReadOnlyList<IucnSameNameMatch> ViaSynonym);

    // Matches per finding key. The plain text goes into Extra, so the CSV and the filter box see it;
    // the links are built from the matches by the column's Parts.
    private static Dictionary<string, RowMatches> FindMatches(IucnSameNameTaxa index, IReadOnlyList<AuditFinding> findings,
        IReadOnlyDictionary<string, IucnOldTaxon> oldTaxa) {
        var result = new Dictionary<string, RowMatches>(StringComparer.Ordinal);
        foreach (var finding in findings) {
            if (finding.Key is null || !oldTaxa.TryGetValue(finding.Key, out var old)) {
                continue;
            }
            var row = new RowMatches(old, index.SameName(old), index.ViaSynonym(old));
            result[finding.Key] = row;
            finding.Extra[SameNameKey] = NullIfEmpty(IucnSameNameTaxa.DescribeAll(row.SameName, old));
            finding.Extra[ViaSynonymKey] = NullIfEmpty(IucnSameNameTaxa.DescribeAll(row.ViaSynonym, old));
        }
        return result;
    }

    private static string? NullIfEmpty(string value) => value.Length == 0 ? null : value;

    private static AuditColumn MatchColumn(string key, string header, string help,
        IReadOnlyDictionary<string, RowMatches> matches, Func<RowMatches, IReadOnlyList<IucnSameNameMatch>> pick) => new() {
        Key = key, Header = header, Type = AuditColumnType.Text, Help = help,
        Value = f => f.Get(key),
        Parts = f => f.Key is not null && matches.TryGetValue(f.Key, out var row) ? Parts(pick(row), row.Old) : null,
    };

    // "2 taxa: " (when there are several), then each match linked to its current assessment, "; " between.
    private static IReadOnlyList<AuditCellPart> Parts(IReadOnlyList<IucnSameNameMatch> list, IucnOldTaxon old) {
        var parts = new List<AuditCellPart>();
        if (list.Count > 1) {
            parts.Add(new AuditCellPart(IucnSameNameTaxa.SeveralPrefix(list.Count)));
        }
        for (var i = 0; i < list.Count; i++) {
            if (i > 0) {
                parts.Add(new AuditCellPart(IucnSameNameTaxa.Separator));
            }
            parts.Add(new AuditCellPart(IucnSameNameTaxa.Describe(list[i], old), list[i].Url));
        }
        return parts;
    }

    private static bool JsonHasLatest(string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
                return false;
            }
            foreach (var a in arr.EnumerateArray()) {
                if (a.ValueKind == JsonValueKind.Object && a.TryGetProperty("latest", out var latest) &&
                    (latest.ValueKind == JsonValueKind.True ||
                     (latest.ValueKind == JsonValueKind.String && string.Equals(latest.GetString(), "true", StringComparison.OrdinalIgnoreCase)))) {
                    return true;
                }
            }
        } catch (JsonException) {
            return false;
        }
        return false;
    }

    private static (long? AssessmentId, string? Year, string? Code, string? Url, string? Scope, int Count) MostRecent(string json) {
        try {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("assessments", out var arr) || arr.ValueKind != JsonValueKind.Array) {
                return (null, null, null, null, null, 0);
            }
            long? bestId = null;
            string? bestYear = null, bestCode = null, bestUrl = null, bestScope = null;
            var bestYearNum = int.MinValue;
            var count = 0;
            foreach (var a in arr.EnumerateArray()) {
                count++;
                if (a.ValueKind != JsonValueKind.Object || !a.TryGetProperty("assessment_id", out var idp)) {
                    continue;
                }
                long? id = idp.ValueKind switch {
                    JsonValueKind.Number => idp.GetInt64(),
                    JsonValueKind.String when long.TryParse(idp.GetString(), out var n) => n,
                    _ => (long?)null,
                };
                if (id is null) {
                    continue;
                }
                var year = a.TryGetProperty("year_published", out var yp)
                    ? (yp.ValueKind == JsonValueKind.String ? yp.GetString() : yp.ValueKind == JsonValueKind.Number ? yp.GetRawText() : null)
                    : null;
                var yearNum = int.TryParse(year, out var yn) ? yn : int.MinValue;
                if (bestId is null || yearNum > bestYearNum) {
                    bestYearNum = yearNum;
                    bestId = id;
                    bestYear = year;
                    bestCode = a.TryGetProperty("red_list_category_code", out var cp) && cp.ValueKind == JsonValueKind.String ? cp.GetString() : null;
                    bestUrl = a.TryGetProperty("url", out var up) && up.ValueKind == JsonValueKind.String ? up.GetString() : null;
                    bestScope = ScopeLabel(a);
                }
            }
            return (bestId, bestYear, bestCode, bestUrl, bestScope, count);
        } catch (JsonException) {
            return (null, null, null, null, null, 0);
        }
    }

    // "Global", "Europe", or "Global; Europe" from the assessment's scopes array.
    private static string? ScopeLabel(JsonElement assessment) {
        if (!assessment.TryGetProperty("scopes", out var scopes) || scopes.ValueKind != JsonValueKind.Array) {
            return null;
        }
        var names = new List<string>();
        foreach (var s in scopes.EnumerateArray()) {
            if (s.TryGetProperty("description", out var d) && d.TryGetProperty("en", out var en) && en.ValueKind == JsonValueKind.String) {
                var name = en.GetString();
                if (!string.IsNullOrWhiteSpace(name)) {
                    names.Add(name!);
                }
            }
        }
        return names.Count == 0 ? "(blank)" : string.Join("; ", names);
    }

    private static int CountInCsv(SqliteConnection csv, IEnumerable<long> taxonIds) {
        var ids = new HashSet<long>(taxonIds);
        if (ids.Count == 0) {
            return 0;
        }
        using var cmd = csv.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT taxonId FROM assessments_html";
        cmd.CommandTimeout = 0;
        var found = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            if (!reader.IsDBNull(0) && ids.Contains(reader.GetInt64(0))) {
                found++;
            }
        }
        return found;
    }

    private const string SameNameHeader = "Current taxon with same name";
    private const string SameNameHelp = "Taxa in the current release with the same scientific name, in the same kingdom, and with a current assessment in the same scope as this taxon's last assessment. Shows the SIS id, the name and authority if they differ from this taxon's, and the current category and year.";
    private const string ViaSynonymHeader = "Current taxon via IUCN synonym";
    private const string ViaSynonymHelp = "Taxa in the current release that list this name as an IUCN synonym, in the same kingdom and scope. Taxa already in the same-name column are left out. When the synonym entry has a different author or a note such as [in part], the entry is quoted.";

    private static string MatchSentence(IReadOnlyList<AuditFinding> findings, IReadOnlyDictionary<string, RowMatches>? matches, string release) {
        if (matches is null) {
            return "Matches to current taxa with the same name or an IUCN synonym were not checked, because the CSV export database was not available.";
        }
        var (sameName, viaSynonym, neither) = MatchCounts(findings, matches);
        return $"{sameName:N0} of these taxa have a taxon in the {release} CSV export with the same scientific name, in the same kingdom and with a current assessment in the same scope (Global, or the same region). " +
            $"Another {viaSynonym:N0} have no same-name match, but their name is listed as an IUCN synonym of a current taxon. " +
            $"The remaining {neither:N0} have no match in either column. " +
            $"The columns {SameNameHeader} and {ViaSynonymHeader} link to the current assessments.";
    }

    private static (int SameName, int ViaSynonymOnly, int Neither) MatchCounts(IReadOnlyList<AuditFinding> findings, IReadOnlyDictionary<string, RowMatches> matches) {
        int sameName = 0, viaSynonym = 0, neither = 0;
        foreach (var f in findings) {
            RowMatches? row = null;
            if (f.Key is not null) {
                matches.TryGetValue(f.Key, out row);
            }
            if (row is { SameName.Count: > 0 }) {
                sameName++;
            } else if (row is { ViaSynonym.Count: > 0 }) {
                viaSynonym++;
            } else {
                neither++;
            }
        }
        return (sameName, viaSynonym, neither);
    }

    private static AuditSummaryTable ByMatch(IReadOnlyList<AuditFinding> findings, IReadOnlyDictionary<string, RowMatches> matches) {
        var (sameName, viaSynonym, neither) = MatchCounts(findings, matches);
        var rows = new[] {
            new[] { "Same scientific name", sameName.ToString("N0") } as IReadOnlyList<string>,
            new[] { "IUCN synonym only", viaSynonym.ToString("N0") },
            new[] { "No match", neither.ToString("N0") },
        };
        return new AuditSummaryTable {
            Title = "Current taxon with the same name or an IUCN synonym",
            Note = "Each taxon is counted once. A taxon with matches in both columns is counted under the same scientific name.",
            Headers = new[] { "Match", "Taxa" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static string CsvSentence(int? inCsv, string release) => inCsv switch {
        null => "Whether these taxa appear in the CSV export was not checked.",
        0 => $"None of these taxa is in the {release} CSV export, so each is genuinely absent from the current release rather than a current taxon missing a flag.",
        var n => $"{n:N0} of these taxa are in the {release} CSV export, so for those the taxon is current and only the API flag is missing.",
    };

    private static AuditSummaryTable ByYearBucket(IReadOnlyList<AuditFinding> findings) {
        static string Bucket(string? year) => int.TryParse(year, out var y) ? y switch {
            >= 2020 => "2020 or later",
            >= 2010 => "2010 to 2019",
            >= 2000 => "2000 to 2009",
            _ => "before 2000",
        } : "(unknown)";
        var order = new[] { "2020 or later", "2010 to 2019", "2000 to 2009", "before 2000", "(unknown)" };
        var counts = findings.GroupBy(f => Bucket(f.YearPublished)).ToDictionary(g => g.Key, g => g.Count());
        var rows = order.Where(counts.ContainsKey).Select(k => new[] { k, counts[k].ToString("N0") } as IReadOnlyList<string>).ToList();
        return new AuditSummaryTable {
            Title = "By year of last assessment",
            Note = "Recent years are taxonomic changes made since that assessment; the oldest have stayed in this state through every release since.",
            Headers = new[] { "Last assessed", "Taxa" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static AuditSummaryTable ByScope(IReadOnlyList<AuditFinding> findings) {
        var rows = findings.GroupBy(f => f.Get("lastScope") ?? "(unknown)")
            .OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        return new AuditSummaryTable {
            Title = "By scope of last assessment",
            Note = "Taxa whose scope was only ever regional were never globally assessed, which is a different situation from a global assessment that was withdrawn.",
            Headers = new[] { "Scope", "Taxa" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }
}
