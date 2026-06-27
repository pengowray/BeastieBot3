using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;

// Synonym names whose stored text carries a non-whitespace irregularity: HTML markup, a stray HTML
// entity, curly/typographic quotes where straight quotes are the norm, or an unusual character or
// encoding artefact. Plain non-ASCII letters (accents, ñ, ü) are deliberately not flagged. The
// summary gives a percentage of all synonyms for each kind and makes the case for whether HTML use
// is consistent. The companion SynonymWhitespaceProducer covers whitespace; a synonym with both
// kinds appears in both lists. Reads SynonymFormattingScan.

namespace BeastieBot3.Audit.Producers;

internal sealed class SynonymOtherFormattingProducer : IAuditReportProducer {
    public string Id => "synonym-markup";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var scan = SynonymFormattingScan.Scan(conn, ctx);

        var findings = new List<AuditFinding>();
        var perSynonym = new List<IReadOnlyList<SynonymIssue>>();
        long htmlCount = 0;
        foreach (var r in scan.Records) {
            var shown = r.Issues.Where(i => !SynonymIssues.IsWhitespace(i)).ToList();
            if (shown.Count == 0) {
                continue;
            }
            findings.Add(SynonymFormattingScan.BuildFinding(r, Id, shown, r.Suggested));
            perSynonym.Add(shown);
            if (shown.Contains(SynonymIssue.HtmlMarkup) || shown.Contains(SynonymIssue.HtmlEntity)) {
                htmlCount++;
            }
        }

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.TaxonId)
            .ThenBy(f => f.CurrentValue, System.StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Synonym names with markup or unusual characters",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = "IUCN API (taxon synonyms)",
            Summary =
                "Each row is a synonym name whose stored text carries a non-whitespace formatting irregularity — embedded HTML markup, a stray HTML entity, curly or typographic quotes where straight quotes are the norm, or an unusual character or encoding artefact — together with a cleaned suggestion. " +
                "Ordinary accented letters are not treated as a problem; only characters that look like markup or an encoding mistake are flagged. The scientific name column is the accepted taxon the synonym belongs to. " +
                "The summary gives each kind as a share of all synonyms examined and sets out whether HTML use is consistent.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Accepted taxon"),
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
            Findings = ordered,
            SummaryTables = new List<AuditSummaryTable> {
                SynonymSummary.ByIssueTypeWithPercent("Issues by kind", SynonymIssues.Other, perSynonym, scan.TotalSynonyms,
                    "Each kind is counted once per synonym and shown as a share of every synonym examined. Because one synonym can carry several, the kinds add up to more than the distinct total."),
                SynonymSummary.HtmlPresence(scan.TotalSynonyms, htmlCount),
            },
        };
    }
}
