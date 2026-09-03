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

        // Granular breakdown: separate leading from trailing whitespace, double spaces, non-breaking
        // or control whitespace, the marker prefix, and the name-field disagreement. One field row can
        // carry several whitespace problems, so the kinds add up to more than the distinct row total.
        var issueTypeSummary = BuildIssueTypeSummary(ordered);

        // Secondary view: which field each row belongs to (one per affected field, so it reconciles
        // with the full list, including the disagreement kind that contributes two fields per record).
        var byField = ordered
            .GroupBy(f => f.Field ?? "")
            .OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            SectionId = "text",
            Title = "Stray whitespace in taxonomy fields",
            Action = ActionClass.Mechanical,
            CsvIsPatch = true,
            TriageRank = 2,
            TriageReason = "Every row has a replacement value; the CSV can be applied as-is.",
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Blurb = "Taxonomy field values with stray whitespace (leading, trailing, doubled, or non-breaking), each with a suggested cleaned-up value.",
            Summary =
                "The table below lists taxonomy field values with a whitespace problem: leading or trailing spaces, repeated spaces, or non-breaking and tab characters. Every row includes a suggested normalised value. " +
                "The same scan also checks for infrarank markers written into the wrong field and for the scientificName column disagreeing between the assessments and taxonomy files of the CSV export; the summary below shows what each check found, including checks that found nothing. " +
                "Otherwise-invisible characters in the current value are shown as visible markers, so the difference can be seen.\n\n" +
                "### Why it matters\n\n" +
                "Stray whitespace breaks exact-match comparisons, sort order, and search, and can appear as visible gaps on the website. Each case is small, but they add up across the release.\n\n" +
                "### Suggestion\n\n" +
                "Apply the suggested normalised value. These are low-risk, concrete tidy-ups.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName(),
                AuditColumns.Field(),
                AuditColumns.IssueType("Problem"),
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
                issueTypeSummary,
                new() { Title = "By field", Note = "The taxonomy field each listed value is in.", Headers = new[] { "Field", "Rows" }, Rows = byField, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    // Candidate issue types in display order. Whitespace kinds are classified from the stored value;
    // the marker prefix and name-field disagreement are identified from the row's own issue label.
    private static readonly string MarkerPrefix = "infrarank marker prefix";
    private static readonly string Disagreement = "scientificName differs between the assessments and taxonomy files";
    private static readonly string[] IssueTypeOrder = {
        "leading whitespace", "trailing whitespace", "double spaces", "non-breaking or control whitespace",
        "infrarank marker prefix", "scientificName differs between the assessments and taxonomy files",
    };

    private static AuditSummaryTable BuildIssueTypeSummary(IReadOnlyList<AuditFinding> findings) {
        var counts = IssueTypeOrder.ToDictionary(k => k, _ => 0, System.StringComparer.Ordinal);
        foreach (var f in findings) {
            foreach (var category in Categories(f)) {
                counts.TryGetValue(category, out var c);
                counts[category] = c + 1;
            }
        }
        var rows = IssueTypeOrder
            .Select(k => new[] { k, counts[k].ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        rows.Add(new[] { "Total (distinct rows)", findings.Count.ToString("N0") });
        return new AuditSummaryTable {
            Title = "By kind of problem",
            Note = "Each kind is counted once per field row; because one field can have several, the kinds add up to more than the distinct total.",
            Headers = new[] { "Problem", "Rows" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static IEnumerable<string> Categories(AuditFinding f) {
        var label = f.IssueType ?? "";
        if (label.Contains("marker prefix", System.StringComparison.Ordinal)) {
            return new[] { MarkerPrefix };
        }
        if (label.Contains("disagreement", System.StringComparison.Ordinal)) {
            return new[] { Disagreement };
        }
        var v = f.CurrentValue ?? "";
        var cats = new List<string>();
        if (TextIrregularities.HasLeadingWhitespace(v)) cats.Add("leading whitespace");
        if (TextIrregularities.HasTrailingWhitespace(v)) cats.Add("trailing whitespace");
        if (TextIrregularities.HasDoubleSpace(v)) cats.Add("double spaces");
        if (TextIrregularities.HasSpecialWhitespace(v)) cats.Add("non-breaking or control whitespace");
        return cats;
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
            Field = PrettyField(field.FieldName),
            CurrentValue = AuditMapping.Decode(field.CurrentValue),
            SuggestedValue = AuditMapping.Decode(field.SuggestedValue),
            IssueType = Label(kind),
            Detail = sample.Detail,
        };
    }

    private static int Priority(string? label) => label switch {
        "scientificName differs between the assessments and taxonomy files" => 5,
        "infraName marker prefix" => 4,
        "scientificName whitespace (assessments file)" or "scientificName whitespace (taxonomy file)" => 3,
        _ => 2,
    };

    // The CSV export carries scientificName in both the assessments and taxonomy files; the
    // duplicate is shown by file rather than by the ":1" suffix the import gives the second column.
    private static string PrettyField(string fieldName) => fieldName switch {
        "scientificName" => "scientificName (assessments)",
        "scientificName:1" => "scientificName (taxonomy)",
        _ => fieldName,
    };

    private static string Label(DataCleanupIssueKind kind) => kind switch {
        DataCleanupIssueKind.ScientificNameWhitespace => "scientificName whitespace (assessments file)",
        DataCleanupIssueKind.TaxonomyScientificNameWhitespace => "scientificName whitespace (taxonomy file)",
        DataCleanupIssueKind.ScientificNameDisagreement => "scientificName differs between the assessments and taxonomy files",
        DataCleanupIssueKind.InfraNameWhitespace => "infraName whitespace",
        DataCleanupIssueKind.InfraNameMarkerPrefix => "infraName marker prefix",
        DataCleanupIssueKind.SubpopulationWhitespace => "subpopulationName whitespace",
        DataCleanupIssueKind.AuthorityWhitespace => "authority whitespace",
        DataCleanupIssueKind.InfraAuthorityWhitespace => "infraAuthority whitespace",
        _ => kind.ToString(),
    };
}
