using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;

// Rows where the recorded scientificName fields and the structured genus/species/infra/subpopulation
// components disagree. Reuses IucnScientificNameVerifier (called with an unbounded sample budget),
// joining each sample back to its taxonomy row for the rest of the context.

namespace BeastieBot3.Audit.Producers;

internal sealed class TaxonomyConsistencyProducer : IAuditReportProducer {
    public string Id => "taxonomy-consistency";

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

        var result = IucnScientificNameVerifier.Analyze(rows, int.MaxValue);

        var findings = new List<AuditFinding>();
        foreach (var kind in System.Enum.GetValues<ScientificNameMismatchKind>()) {
            foreach (var s in result.GetSamples(kind)) {
                byAssessment.TryGetValue(s.AssessmentId, out var row);
                findings.Add(Build(kind, s, row));
            }
        }

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(f => f.IssueType, System.StringComparer.Ordinal)
            .ThenBy(f => f.TaxonId)
            .ToList();

        var summary = new List<IReadOnlyList<string>> {
            new[] { "scientificName vs scientificName:1 disagreement", result.FieldMismatchCount.ToString("N0") },
            new[] { "name not rebuilt from components", result.ReconstructionMismatchCount.ToString("N0") },
            new[] { "genus token missing from name", result.GenusMismatchCount.ToString("N0") },
            new[] { "species token missing from name", result.SpeciesMismatchCount.ToString("N0") },
            new[] { "infra-name token missing from name", result.InfraNameMismatchCount.ToString("N0") },
            new[] { "subpopulation token mismatch", result.SubpopulationMismatchCount.ToString("N0") },
        };

        return new AuditReport {
            Id = Id,
            Title = "Scientific name fields versus taxonomy components",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Summary =
                "Each row rebuilds an assessment's scientific name from its genus, species, infra rank, and subpopulation fields and compares it against the recorded scientificName values. " +
                "Rows appear where the two scientificName fields disagree, where the name cannot be rebuilt from the components, or where an expected genus, species, infra-name, or subpopulation token is absent from the recorded name. " +
                "Comparisons are exact after whitespace normalisation, so some token-presence rows can be benign for hybrids or unusual formatting.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Recorded name"),
                AuditColumns.Rank(),
                AuditColumns.IssueType("Observation"),
                AuditColumns.Field(),
                AuditColumns.CurrentValue("Recorded", AuditColumnType.Code),
                AuditColumns.SuggestedValue("Expected", AuditColumnType.Code),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId("Taxon id"),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
            },
            Findings = ordered,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By observation", Note = $"Over {result.TotalRows:N0} rows scanned.", Headers = new[] { "Observation", "Count" }, Rows = summary, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static AuditFinding Build(ScientificNameMismatchKind kind, ScientificNameMismatchSample s, IucnTaxonomyRow? row) {
        var primary = s.NormalizedAssess ?? s.NormalizedTaxonomy;
        var (rank, isFull) = AuditMapping.Rank(row?.InfraType ?? s.InfraType, row?.SubpopulationName ?? s.SubpopulationName);
        var code = AuditMapping.CodeFromCategory(row?.RedlistCategory);

        var (field, current, suggested, issueType, severity) = kind switch {
            ScientificNameMismatchKind.FieldDisagreement =>
                ("scientificName", s.NormalizedAssess, s.NormalizedTaxonomy, "field-disagreement", 5),
            ScientificNameMismatchKind.ReconstructionFailure =>
                ("scientificName", primary, s.NormalizedReconstructed, "reconstruction-mismatch", 5),
            ScientificNameMismatchKind.GenusMismatch =>
                ("genusName", primary, row?.GenusName, "genus-mismatch", 3),
            ScientificNameMismatchKind.SpeciesMismatch =>
                ("speciesName", primary, row?.SpeciesName, "species-missing", 3),
            ScientificNameMismatchKind.InfraNameMismatch =>
                ("infraName", primary, row?.InfraName, "infra-name-missing", 3),
            ScientificNameMismatchKind.SubpopulationMismatch =>
                ("subpopulationName", primary, row?.SubpopulationName, "subpopulation-mismatch", 3),
            _ => ("scientificName", primary, (string?)null, kind.ToString(), 1),
        };

        return new AuditFinding {
            ReportId = "taxonomy-consistency",
            Key = $"{s.TaxonId}:{issueType}",
            TaxonId = s.TaxonId,
            AssessmentId = s.AssessmentId,
            RedlistUrl = IucnUrls.Species(s.TaxonId, s.AssessmentId),
            ScientificName = AuditMapping.Decode(s.ScientificNameAssessments ?? s.ScientificNameTaxonomy) ?? $"SIS {s.TaxonId}",
            Rank = rank,
            IsFullSpecies = isFull,
            InfraType = row?.InfraType ?? s.InfraType,
            InfraName = row?.InfraName,
            SubpopulationName = row?.SubpopulationName ?? s.SubpopulationName,
            Kingdom = row?.KingdomName ?? s.KingdomName,
            Phylum = row?.PhylumName,
            Class = row?.ClassName,
            Order = row?.OrderName,
            Family = row?.FamilyName,
            Genus = row?.GenusName,
            Species = row?.SpeciesName,
            StatusCode = code,
            StatusCategory = row?.RedlistCategory,
            DataSource = "iucn-csv",
            Field = field,
            CurrentValue = current,
            SuggestedValue = suggested,
            IssueType = issueType,
            SeverityTier = severity,
            Detail = s.Detail,
        };
    }
}
