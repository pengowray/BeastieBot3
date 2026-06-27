using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;

// Synonym names whose stored text carries a whitespace irregularity: leading or trailing spaces,
// double spaces, non-breaking or control whitespace, a space inside parentheses or before a comma,
// or a blank value. Each whitespace problem is counted separately in the summary, with a distinct
// total (a synonym can carry several). The companion SynonymOtherFormattingProducer covers markup
// and unusual characters; a synonym with both kinds appears in both lists. Reads SynonymFormattingScan.

namespace BeastieBot3.Audit.Producers;

internal sealed class SynonymWhitespaceProducer : IAuditReportProducer {
    public string Id => "synonym-whitespace";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var scan = SynonymFormattingScan.Scan(conn, ctx);

        var findings = new List<AuditFinding>();
        var perSynonym = new List<IReadOnlyList<SynonymIssue>>();
        foreach (var r in scan.Records) {
            var shown = r.Issues.Where(SynonymIssues.IsWhitespace).ToList();
            if (shown.Count == 0) {
                continue;
            }
            findings.Add(SynonymFormattingScan.BuildFinding(r, Id, shown, r.Suggested));
            perSynonym.Add(shown);
        }

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.TaxonId)
            .ThenBy(f => f.CurrentValue, System.StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Synonym names with whitespace irregularities",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = "IUCN API (taxon synonyms)",
            Summary =
                "Each row is a synonym name whose stored text carries a whitespace irregularity, together with a whitespace-normalised suggestion. " +
                "The current value shows otherwise-invisible characters as markers, so the difference is visible. The scientific name column is the accepted taxon the synonym belongs to. " +
                "The summary below counts each kind of whitespace problem separately. Because one synonym can have several types of issue, the kinds add up to more than the distinct total. " +
                "Tidier synonym strings help name matching and search.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Accepted taxon"),
                AuditColumns.CurrentValue("Synonym (current)", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Suggested", AuditColumnType.Code),
                AuditColumns.IssueType("Whitespace issue(s)"),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId(),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
            },
            Findings = ordered,
            SummaryTables = new List<AuditSummaryTable> {
                SynonymSummary.ByIssueType("Whitespace issues by kind", SynonymIssues.Whitespace, perSynonym,
                    "Each whitespace problem is counted once per synonym, so the kinds add up to more than the distinct total. Checks that found nothing are listed as 0 so it is clear they ran."),
            },
        };
    }
}
