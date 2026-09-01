using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;

// Assessed subspecies and varieties whose parent species has no species-level assessment.
// Mirrors the query in IucnOrphanInfraranksReportCommand, extended to also read the year and
// the possibly-extinct flags so the status badge is exact.

namespace BeastieBot3.Audit.Producers;

internal sealed class OrphanInfraranksProducer : IAuditReportProducer {
    public string Id => "orphan-infraranks";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnCsvOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "view_assessments_html_taxonomy_html")) {
            return null;
        }

        var findings = Query(conn, ctx);

        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName("Subspecies or variety"),
            AuditColumns.Rank(),
            AuditColumns.Status(),
            AuditColumns.Custom("parentSpecies", "Parent species (unassessed)", AuditColumnType.Text),
            AuditColumns.Class(),
            AuditColumns.Order(),
            AuditColumns.Family(),
            AuditColumns.Kingdom(),
            AuditColumns.Year(),
            AuditColumns.TaxonId(),
            AuditColumns.AssessmentId(),
            AuditColumns.RedlistLink(),
            AuditColumns.Detail(),
        };

        var byInfra = findings.GroupBy(f => f.Rank ?? "")
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        var byClass = findings.GroupBy(f => f.Class ?? "(unspecified)").OrderByDescending(g => g.Count())
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        return new AuditReport {
            Id = Id,
            Title = "Subspecies and varieties with no assessed parent species",
            Breakage = BreakageClass.Breaking,
            DataSourceLabel = $"IUCN Red List {ctx.Release} (CSV export)",
            Blurb = "Assessed subspecies and varieties whose parent species has no assessment of its own, leaving them reachable only by their SIS id.",
            Summary =
                "The table below lists assessed subspecies and varieties whose parent species has no species-level assessment in the current release. " +
                "The IUCN API lists subspecies and varieties under their parent species, so when the parent has no assessment, the only way to reach these taxa through the API is by already knowing their SIS id. Each row links to the taxon's Red List page.\n\n" +
                "### Why it matters\n\n" +
                "Tools and users browsing through the API never encounter these taxa, even though their assessments exist and are published.\n\n" +
                "### Suggestion\n\n" +
                "Add a species-level assessment for the parent (e.g., 'Not Evaluated') so the infraspecific taxa become reachable, or provide another discovery route to them.",
            Columns = columns,
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                new() { Title = "By rank", Headers = new[] { "Rank", "Count" }, Rows = byInfra, NumericColumns = new[] { 1 } },
                new() { Title = "By class", Headers = new[] { "Class", "Count" }, Rows = byClass, NumericColumns = new[] { 1 } },
            },
            GroupLevels = AuditGroups.ByClass,
        };
    }

    private static IReadOnlyList<AuditFinding> Query(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT i.taxonId, i.assessmentId, i.scientificName, i.infraType, i.infraName, i.subpopulationName,
       i.redlistCategory, i.possiblyExtinct, i.possiblyExtinctInTheWild, i.yearPublished,
       i.kingdomName, i.phylumName, i.className, i.orderName, i.familyName, i.genusName, i.speciesName
FROM view_assessments_html_taxonomy_html i
WHERE i.infraType IS NOT NULL AND TRIM(i.infraType) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM view_assessments_html_taxonomy_html p
    WHERE p.genusName = i.genusName
      AND p.speciesName = i.speciesName
      AND (p.infraType IS NULL OR TRIM(p.infraType) = '')
      AND (p.subpopulationName IS NULL OR TRIM(p.subpopulationName) = '')
  )
ORDER BY i.kingdomName, i.className, i.orderName, i.familyName, i.scientificName, i.taxonId";

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + "\nLIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var rows = new List<AuditFinding>();
        var seen = new HashSet<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ctx.Ct.ThrowIfCancellationRequested();
            var taxonId = reader.GetInt64(0);
            if (!seen.Add(taxonId)) {
                continue;
            }
            var assessmentId = reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1);
            var scientificName = Str(reader, 2);
            var infraType = Str(reader, 3);
            var infraName = Str(reader, 4);
            var subpop = Str(reader, 5);
            var category = Str(reader, 6);
            var pe = Str(reader, 7);
            var pew = Str(reader, 8);
            var year = Str(reader, 9);
            var kingdom = Str(reader, 10);
            var phylum = Str(reader, 11);
            var className = Str(reader, 12);
            var order = Str(reader, 13);
            var family = Str(reader, 14);
            var genus = Str(reader, 15);
            var species = Str(reader, 16);

            var (rank, isFull) = AuditMapping.Rank(infraType, subpop);
            var parent = $"{genus} {species}".Trim();
            var code = AuditMapping.CodeFromCategory(category, pe, pew);

            var finding = new AuditFinding {
                ReportId = "orphan-infraranks",
                Key = $"{taxonId}",
                TaxonId = taxonId,
                AssessmentId = assessmentId,
                RedlistUrl = IucnUrls.Species(taxonId, assessmentId),
                ScientificName = AuditMapping.Decode(scientificName) ?? $"SIS {taxonId}",
                InfraType = infraType,
                InfraName = infraName,
                SubpopulationName = subpop,
                Rank = rank,
                IsFullSpecies = isFull,
                Kingdom = kingdom,
                Phylum = phylum,
                Class = className,
                Order = order,
                Family = family,
                Genus = genus,
                Species = species,
                StatusCode = code,
                StatusCategory = category,
                YearPublished = year,
                DataSource = "iucn-csv",
                Field = "parentSpecies",
                CurrentValue = parent,
                IssueType = "orphan-infrarank",
                Detail = $"Parent species {parent} has no species-level assessment, so this taxon is reachable only by its own SIS id.",
            };
            finding.Extra["parentSpecies"] = parent;
            finding.Notes.Add("Recoverable only by SIS id (the API cannot discover it through an assessed parent species).");
            rows.Add(finding);
        }

        return rows
            .OrderBy(f => AuditMapping.StatusSortKey(f.StatusCode))
            .ThenBy(f => f.Class, System.StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Order, System.StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Family, System.StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, System.StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string? Str(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
