using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;
using BeastieBot3.Col;
using BeastieBot3.Iucn;

// Publishes the IUCN vs Catalogue of Life crosscheck as several focused observation pages instead
// of one mixed list. ColCrosscheckEngine does the matching once; this class turns each bucket into
// an AuditReport:
//
//   col-close-match     no exact CoL match, but a near CoL name (likely spelling/encoding)
//   col-synonym         a species/subspecies/variety CoL treats as a synonym of another name
//   col-synonym-higher  a genus/family/order/class name CoL records only as a synonym
//   col-classification  a higher-rank placement difference that looks like a spelling variant
//   col-reorg           a higher-rank placement under a genuinely different parent
//   col-authority       a naming authority that differs like a typo (not just spacing)
//   col-not-found       no exact CoL match and no near candidate
//
// Pages are ordered most-actionable first; the noisier "not found" list comes last.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal sealed class ColCrosscheckProducer : IAuditReportSetProducer {
    public const string NotFoundId = "col-not-found";
    public const string CloseMatchId = "col-close-match";
    public const string SynonymId = "col-synonym";
    public const string SynonymHigherId = "col-synonym-higher";
    public const string ClassificationId = "col-classification";
    public const string ReorgId = "col-reorg";
    public const string AuthorityId = "col-authority";

    public string Id => "col-crosscheck";

    public IReadOnlyList<AuditReport> Produce(AuditContext ctx) {
        var iucn = ctx.IucnCsvOrNull();
        var col = ctx.ColOrNull();
        if (iucn is null || col is null) {
            return Array.Empty<AuditReport>();
        }
        var iucnRepo = new IucnTaxonomyRepository(iucn);
        if (!iucnRepo.ObjectExists("view_assessments_html_taxonomy_html", "view") || !AuditContext.ObjectExists(col, "nameusage")) {
            return Array.Empty<AuditReport>();
        }
        var colRepo = new ColTaxonRepository(col);

        var rows = iucnRepo.ReadRows(0, ctx.Ct)
            .Where(r => string.IsNullOrWhiteSpace(r.SubpopulationName))
            .ToList();
        if (ctx.Limit is > 0 && rows.Count > ctx.Limit.Value) {
            rows = rows.Take((int)ctx.Limit.Value).ToList();
        }

        var data = new ColCrosscheckEngine(colRepo).Run(rows, ctx.Ct);
        var source = $"IUCN Red List {ctx.Release} vs Catalogue of Life";
        var assessed = data.AssessedCompared;
        var higher = data.HigherTaxaCompared;

        return new[] {
            CloseMatch(source, assessed, data.CloseMatch),
            Synonym(source, assessed, data.Synonym),
            SynonymHigher(source, higher, data.SynonymHigher),
            Classification(source, higher, data.Classification),
            Reorg(source, higher, data.Reorg),
            Authority(source, assessed, data.Authority),
            NotFound(source, assessed, data.NotFound),
        };
    }

    // --- species / subspecies reports -------------------------------------------------------

    private static AuditReport NotFound(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = NotFoundId,
        Title = "Names not found in the Catalogue of Life",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is an IUCN name with no exact Catalogue of Life match and no close candidate from a fuzzy search over the same genus and epithet.\n\n" +
            "### Why it matters\n\n" +
            "A name absent from the Catalogue of Life cannot be cross-referenced there. This may point to a very recent name, a name from a source CoL does not yet cover, or a spelling that has drifted far from the CoL form.\n\n" +
            "### Suggestion\n\n" +
            "Use this as a list to spot-check against current literature. Many entries are expected to be legitimately newer than the Catalogue of Life snapshot.",
        Columns = SpeciesColumns(includeColName: false, colNameHeader: null, includeAuthority: false),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport CloseMatch(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = CloseMatchId,
        Title = "Names with a close Catalogue of Life match",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is an IUCN name with no exact Catalogue of Life match, paired with the closest Catalogue of Life name found by a fuzzy search over the same genus and epithet. The detail column names how they differ (punctuation, diacritics, Unicode encoding, or a short spelling variant).\n\n" +
            "### Why it matters\n\n" +
            "When the two catalogues spell a name slightly differently, an exact join between them fails even though the same taxon is almost certainly meant.\n\n" +
            "### Suggestion\n\n" +
            "Check each pair. Where it is the same name spelled differently, aligning the spelling lets the two catalogues match.",
        Columns = SpeciesColumns(includeColName: true, colNameHeader: "Closest CoL name", includeAuthority: false),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Synonym(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = SynonymId,
        Title = "Species and subspecies the Catalogue of Life treats as a synonym",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is an IUCN species, subspecies, or variety whose name the Catalogue of Life records as a synonym of a different accepted name, shown in the CoL accepted name column.\n\n" +
            "### Why it matters\n\n" +
            "Where CoL has moved a name into synonymy, the IUCN name may be an earlier combination or a lumped taxon. The accepted name is what a CoL-based system will use.\n\n" +
            "### Suggestion\n\n" +
            "Compare each IUCN name with the CoL accepted name and confirm which reflects current taxonomy.",
        Columns = SpeciesColumns(includeColName: true, colNameHeader: "CoL accepted name", includeAuthority: false),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings), ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Authority(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AuthorityId,
        Title = "Naming authority differences that look like typos",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is an IUCN name that matches the Catalogue of Life exactly but whose naming authority differs in a way that looks like a typo or an encoding difference (a diacritic, a Unicode-encoding difference, punctuation, or a short spelling difference). Authorities that differ only in spacing are left out, as are genuinely different authorities.\n\n" +
            "### Why it matters\n\n" +
            "When the same name carries slightly different authority strings, an author-aware comparison between the catalogues fails, and the difference is usually a small data slip.\n\n" +
            "### Suggestion\n\n" +
            "Check each authority pair against the original publication and align the spelling or encoding.",
        Columns = SpeciesColumns(includeColName: false, colNameHeader: null, includeAuthority: true),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    // --- higher-rank reports ----------------------------------------------------------------

    private static AuditReport SynonymHigher(string source, int higher, List<AuditFinding> findings) => new() {
        Id = SynonymHigherId,
        Title = "Higher-rank names the Catalogue of Life treats as a synonym",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is a higher-rank name (genus, family, order, or class) used in the IUCN classification that the Catalogue of Life records only as a synonym, never as an accepted name. The accepted name CoL uses is shown alongside, and the IUCN taxa column counts how many assessed taxa sit under the name.\n\n" +
            "### Why it matters\n\n" +
            "A genus or family name that CoL treats entirely as a synonym is often an older spelling or a superseded name, and it affects every assessed taxon placed under it.\n\n" +
            "### Suggestion\n\n" +
            "Check the accepted CoL name. Where it is a corrected spelling or an accepted replacement, updating the higher-rank name aligns the whole group.",
        Columns = HigherColumns(includePlacement: false),
        Findings = OrderHigher(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings) },
    };

    private static AuditReport Classification(string source, int higher, List<AuditFinding> findings) => new() {
        Id = ClassificationId,
        Title = "Higher-rank placement differences that look like spelling variants",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is a higher-rank name whose parent placement differs between IUCN and the Catalogue of Life in a way that looks like a spelling variant (a typo, a diacritic, a Unicode-encoding difference, or a short spelling difference). Only names within the same phylum are compared.\n\n" +
            "### Why it matters\n\n" +
            "A near-identical parent name usually means the same placement recorded two ways. Aligning the spelling makes the two classifications join cleanly.\n\n" +
            "### Suggestion\n\n" +
            "Confirm the intended spelling of the parent name in each row.",
        Columns = HigherColumns(includePlacement: true),
        Findings = OrderHigher(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings) },
    };

    private static AuditReport Reorg(string source, int higher, List<AuditFinding> findings) => new() {
        Id = ReorgId,
        Title = "Higher-rank names placed differently in the Catalogue of Life",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Summary =
            "Each row is a higher-rank name that the Catalogue of Life places under a genuinely different parent (a different order, class, or family), rather than a spelling variant. Only names within the same phylum are compared.\n\n" +
            "### Why it matters\n\n" +
            "A different parent placement reflects a different higher classification between the two catalogues. Neither is necessarily wrong, but anything grouped by higher rank will differ between them.\n\n" +
            "### Suggestion\n\n" +
            "Where the placement matters for grouping, note which classification each downstream use should follow.",
        Columns = HigherColumns(includePlacement: true),
        Findings = OrderHigher(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings) },
    };

    // --- column sets ------------------------------------------------------------------------

    private static IReadOnlyList<AuditColumn> SpeciesColumns(bool includeColName, string? colNameHeader, bool includeAuthority) {
        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName("IUCN name"),
            AuditColumns.Rank(),
        };
        if (includeAuthority) {
            columns.Add(AuditColumns.CurrentValue("IUCN authority", AuditColumnType.Text));
            columns.Add(AuditColumns.SuggestedValue("CoL authority", AuditColumnType.Text));
        }
        if (includeColName) {
            columns.Add(AuditColumns.SuggestedValue(colNameHeader!, AuditColumnType.Text));
        }
        if (includeColName || includeAuthority) {
            columns.Add(AuditColumns.ColLink());
        }
        columns.Add(AuditColumns.Status("IUCN status"));
        columns.Add(AuditColumns.Class());
        columns.Add(AuditColumns.Family());
        columns.Add(AuditColumns.TaxonId("Taxon id"));
        columns.Add(AuditColumns.RedlistLink());
        columns.Add(AuditColumns.Detail());
        return columns;
    }

    private static IReadOnlyList<AuditColumn> HigherColumns(bool includePlacement) {
        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName("IUCN name"),
            AuditColumns.Rank(),
        };
        if (includePlacement) {
            columns.Add(AuditColumns.Field("Rank compared"));
            columns.Add(AuditColumns.CurrentValue("IUCN placement", AuditColumnType.Text));
            columns.Add(AuditColumns.SuggestedValue("CoL placement", AuditColumnType.Text));
        } else {
            columns.Add(AuditColumns.SuggestedValue("CoL accepted name", AuditColumnType.Text));
        }
        columns.Add(AuditColumns.ColLink());
        columns.Add(AuditColumns.Kingdom());
        columns.Add(AuditColumns.Phylum());
        columns.Add(AuditColumns.Custom("iucnSpecies", "IUCN taxa", AuditColumnType.Number,
            "Number of assessed IUCN taxa placed under this name."));
        columns.Add(AuditColumns.Detail());
        return columns;
    }

    // --- ordering + summaries ---------------------------------------------------------------

    private static IReadOnlyList<AuditFinding> OrderSpecies(List<AuditFinding> findings) => findings
        .OrderByDescending(f => f.SeverityTier)
        .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.Family, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
        .ToList();

    private static IReadOnlyList<AuditFinding> OrderHigher(List<AuditFinding> findings) => findings
        .OrderBy(f => RankOrder(f.Rank))
        .ThenByDescending(f => f.SeverityTier)
        .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
        .ToList();

    private static int RankOrder(string? rank) => rank switch {
        "class" => 0,
        "order" => 1,
        "family" => 2,
        "genus" => 3,
        _ => 4,
    };

    private static AuditSummaryTable ByClassSummary(string title, IReadOnlyList<AuditFinding> findings, int compared) {
        var rows = findings
            .GroupBy(f => f.Class ?? "(unspecified)")
            .OrderByDescending(g => g.Count())
            .Take(15)
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        if (rows.Count == 0) {
            rows.Add(new[] { "(none)", "0" });
        }
        return new AuditSummaryTable {
            Title = title,
            Note = $"Over {compared:N0} assessments compared. Top classes shown; every row is in the CSV download.",
            Headers = new[] { "Class", "Count" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static AuditSummaryTable ByRankSummary(string title, IReadOnlyList<AuditFinding> findings) {
        var order = new[] { "class", "order", "family", "genus", "species", "subspecies", "variety" };
        var counts = findings.GroupBy(f => f.Rank ?? "(unspecified)").ToDictionary(g => g.Key, g => g.Count());
        var rows = order
            .Where(counts.ContainsKey)
            .Select(r => new[] { r, counts[r].ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        foreach (var kv in counts.Where(kv => !order.Contains(kv.Key))) {
            rows.Add(new[] { kv.Key, kv.Value.ToString("N0") });
        }
        if (rows.Count == 0) {
            rows.Add(new[] { "(none)", "0" });
        }
        return new AuditSummaryTable {
            Title = title,
            Note = "Every row is in the CSV download.",
            Headers = new[] { "Rank", "Count" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }
}
