using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// Per-field whitespace and marker cleanup opportunities in the taxonomy fields. Reuses the
// existing IucnDataCleanupAnalyzer (called with an unbounded sample budget so every row is kept)
// and joins each sample back to its taxonomy row for context.

namespace BeastieBot3.Audit.Producers;

internal sealed class TaxonomyCleanupProducer : IAuditReportProducer {
    public string Id => "taxonomy-cleanup";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null) {
            return null;
        }
        var repo = new IucnTaxonomyRepository(conn);
        if (!repo.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            return null;
        }

        var rows = repo.ReadRows(ctx.Limit ?? 0, ctx.Ct).ToList();
        var byAssessment = new Dictionary<long, IucnTaxonomyRow>();
        foreach (var r in rows) {
            byAssessment[r.AssessmentId] = r;
        }

        var result = IucnDataCleanupAnalyzer.Analyze(rows, int.MaxValue);

        var findings = new List<AuditFinding>();
        foreach (var kind in System.Enum.GetValues<DataCleanupIssueKind>()) {
            foreach (var sample in result.GetSamples(kind)) {
                byAssessment.TryGetValue(sample.AssessmentId, out var row);
                foreach (var field in sample.Fields) {
                    findings.Add(Build(kind, sample, field, row));
                }
            }
        }

        var ordered = findings
            .OrderByDescending(f => Priority(f.IssueType))
            .ThenBy(f => f.Class, System.StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, System.StringComparer.OrdinalIgnoreCase)
            .ToList();

        // Count the rows actually listed (one per affected field) so the summary reconciles with
        // the full list, including the disagreement kind which contributes two fields per record.
        var summary = ordered
            .GroupBy(f => f.IssueType ?? "")
            .OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Whitespace and marker cleanup in taxonomy fields",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Summary =
                "Each row reports a taxonomy field whose stored text carries a whitespace irregularity (leading or trailing spaces, repeated spaces, non-breaking or tab characters) or an infrarank marker that belongs in the name fields, together with a suggested normalised value. " +
                "Current values show otherwise-invisible characters as markers so the difference is visible. These are low-risk, concrete tidy-ups.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName(),
                AuditColumns.Field(),
                AuditColumns.IssueType("Cleanup"),
                AuditColumns.CurrentValue("Current value", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Suggested value", AuditColumnType.Code),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId(),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
            },
            Findings = ordered,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By cleanup kind", Headers = new[] { "Kind", "Count" }, Rows = summary, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static AuditFinding Build(DataCleanupIssueKind kind, DataCleanupIssueSample sample, DataCleanupFieldSuggestion field, IucnTaxonomyRow? row) {
        var (rank, isFull) = AuditMapping.Rank(row?.InfraType, row?.SubpopulationName);
        var code = AuditMapping.CodeFromCategory(row?.RedlistCategory);
        var name = AuditMapping.Decode(row?.ScientificNameTaxonomy ?? row?.ScientificNameAssessments) ?? $"SIS {sample.TaxonId}";
        return new AuditFinding {
            ReportId = "taxonomy-cleanup",
            Key = $"{sample.TaxonId}:{kind}:{field.FieldName}",
            TaxonId = sample.TaxonId,
            AssessmentId = sample.AssessmentId,
            RedlistUrl = IucnUrls.Species(sample.TaxonId, sample.AssessmentId),
            ScientificName = name,
            Rank = rank,
            IsFullSpecies = isFull,
            InfraType = row?.InfraType,
            InfraName = row?.InfraName,
            SubpopulationName = row?.SubpopulationName,
            Kingdom = row?.KingdomName,
            Phylum = row?.PhylumName,
            Class = row?.ClassName,
            Order = row?.OrderName,
            Family = row?.FamilyName,
            Genus = row?.GenusName,
            Species = row?.SpeciesName,
            StatusCode = code,
            StatusCategory = row?.RedlistCategory,
            DataSource = "iucn-csv",
            Field = field.FieldName,
            CurrentValue = AuditMapping.Decode(field.CurrentValue),
            SuggestedValue = AuditMapping.Decode(field.SuggestedValue),
            IssueType = Label(kind),
            Detail = sample.Detail,
        };
    }

    private static int Priority(string? label) => label switch {
        "scientificName vs scientificName:1 disagreement" => 5,
        "infraName marker prefix" => 4,
        "scientificName whitespace" or "scientificName:1 whitespace" => 3,
        _ => 2,
    };

    private static string Label(DataCleanupIssueKind kind) => kind switch {
        DataCleanupIssueKind.ScientificNameWhitespace => "scientificName whitespace",
        DataCleanupIssueKind.TaxonomyScientificNameWhitespace => "scientificName:1 whitespace",
        DataCleanupIssueKind.ScientificNameDisagreement => "scientificName vs scientificName:1 disagreement",
        DataCleanupIssueKind.InfraNameWhitespace => "infraName whitespace",
        DataCleanupIssueKind.InfraNameMarkerPrefix => "infraName marker prefix",
        DataCleanupIssueKind.SubpopulationWhitespace => "subpopulationName whitespace",
        DataCleanupIssueKind.AuthorityWhitespace => "authority whitespace",
        DataCleanupIssueKind.InfraAuthorityWhitespace => "infraAuthority whitespace",
        _ => kind.ToString(),
    };
}
