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

        // IUCN carries synonyms only in the API cache; when it is present, index it so the synonym
        // report can flag CoL accepted names that IUCN already records as synonyms.
        var apiCache = ctx.IucnApiCacheOrNull();
        var iucnSynonyms = apiCache is null ? null : IucnSynonymIndex.Build(apiCache, ctx.Limit, ctx.Ct);

        var data = new ColCrosscheckEngine(colRepo, iucnSynonyms).Run(rows, ctx.Ct);
        var colRelease = ctx.ColReleaseLabel();
        var source = colRelease is null
            ? $"IUCN Red List {ctx.Release} vs Catalogue of Life"
            : $"IUCN Red List {ctx.Release} vs Catalogue of Life {colRelease}";
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
        Blurb = "IUCN names with no Catalogue of Life match, exact or approximate.",
        Summary =
            "The table below lists IUCN scientific names that the Catalogue of Life does not contain. For each name there is no exact match in CoL, and a search for similar spellings within the same genus and species epithet found no close candidate either.\n\n" +
            "### Why it matters\n\n" +
            "A name absent from the Catalogue of Life cannot be cross-referenced there. The likely reasons vary: the name may be newer than the Catalogue of Life release compared against, may come from a source CoL does not yet cover, or may be spelled differently enough that no match is found.\n\n" +
            "### Suggestion\n\n" +
            "Use this as a list to spot-check against current literature. Many entries are expected to be legitimately newer than the Catalogue of Life release compared against (its version is given above under Source).",
        Columns = NotFoundColumns(),
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
        Blurb = "IUCN names with no exact Catalogue of Life match, each paired with the most similar CoL name in the same genus, and a note on how the two spellings differ.",
        Summary =
            "The table below lists IUCN scientific names with no exact match in the Catalogue of Life, each paired with the most similar CoL name found within the same genus and species epithet. The detail column says how the two spellings differ (punctuation, diacritics, Unicode encoding, or a short spelling variant).\n\n" +
            "### Why it matters\n\n" +
            "When the two catalogues spell a name slightly differently, an exact join between them fails even though the same taxon is almost certainly meant.\n\n" +
            "### Suggestion\n\n" +
            "Check each pair. Where it is the same name spelled differently, aligning the spelling lets the two catalogues match.",
        Columns = CloseMatchColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Synonym(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = SynonymId,
        Title = "Species and subspecies treated as synonyms in the Catalogue of Life",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Blurb = "Assessed IUCN taxa whose name the Catalogue of Life records as a synonym of a different accepted name.",
        Summary =
            "The table below lists IUCN species, subspecies, and varieties whose scientific name the Catalogue of Life records as a synonym of a different accepted name. That accepted name is shown in the CoL accepted name column.\n\n" +
            "### Why it matters\n\n" +
            "Where CoL has moved a name into synonymy, the IUCN name may be an earlier combination of the same species, or a taxon that has since been merged into another. Databases that follow the Catalogue of Life file the taxon under the accepted name, not the IUCN one.\n\n" +
            "### Suggestion\n\n" +
            "Compare each IUCN name with the CoL accepted name and confirm which reflects current taxonomy.",
        Columns = SynonymColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings), ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Authority(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AuthorityId,
        Title = "Minor naming authority differences",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Blurb = "Names that match the Catalogue of Life exactly but whose author attribution is spelled slightly differently in the two catalogues.",
        Summary =
            "The table below lists IUCN names that match the Catalogue of Life exactly, except that the two catalogues spell the naming authority slightly differently (a diacritic, a Unicode-encoding difference, or a short spelling difference in the author name). Only differences in the letters of the author name are listed; differences in spacing, punctuation, or the year are ignored, as are authorities that are different outright.\n\n" +
            "### Why it matters\n\n" +
            "When the two catalogues spell the author name slightly differently, a comparison that includes the author fails to match the records, and the difference is usually a small transcription error on one side.\n\n" +
            "### Suggestion\n\n" +
            "Check each authority pair against the original publication and align the spelling or encoding.",
        Columns = AuthorityColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    // --- higher-rank reports ----------------------------------------------------------------

    private static AuditReport SynonymHigher(string source, int higher, List<AuditFinding> findings) => new() {
        Id = SynonymHigherId,
        Title = "Higher-rank names treated as synonyms in the Catalogue of Life",
        Tier = AuditReportTier.IucnCore,
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        Blurb = "Genus, family, order, and class names used in the IUCN classification that the Catalogue of Life records only as synonyms.",
        Summary =
            "The table below lists higher-rank names (genus, family, order, or class) used in the IUCN classification that the Catalogue of Life records only as a synonym, never as an accepted name. The name CoL accepts instead is shown alongside, and the IUCN taxa column counts how many assessed taxa sit under the name.\n\n" +
            "### Why it matters\n\n" +
            "A genus or family name that CoL treats entirely as a synonym is often an older spelling or a superseded name, and it affects every assessed taxon placed under it.\n\n" +
            "### Suggestion\n\n" +
            "Check the accepted CoL name. Where it is a corrected spelling or an accepted replacement, one change to the higher-rank name updates the whole group at once.",
        Columns = SynonymHigherColumns(),
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
        Blurb = "Names whose parent taxon is spelled slightly differently in IUCN and the Catalogue of Life, suggesting the same placement written two ways.",
        Summary =
            "The table below lists higher-rank names whose parent taxon is spelled slightly differently in IUCN and the Catalogue of Life (a typo, a diacritic, a Unicode-encoding difference, or a short spelling difference), which suggests the same placement written two ways. Only names within the same phylum are compared.\n\n" +
            "### Why it matters\n\n" +
            "A near-identical parent name usually means the same placement recorded two ways. Aligning the spelling lets the two classifications match up.\n\n" +
            "### Suggestion\n\n" +
            "Confirm the intended spelling of the parent name in each row.",
        Columns = PlacementColumns(),
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
        Blurb = "Names the Catalogue of Life places under a genuinely different parent taxon (not just a different spelling of the same one).",
        Summary =
            "The table below lists higher-rank names that the Catalogue of Life places under a genuinely different parent (a different order, class, or family), not just a different spelling of the same one. Only names within the same phylum are compared.\n\n" +
            "### Why it matters\n\n" +
            "A different parent placement reflects a different higher classification between the two catalogues. Neither is necessarily wrong, but any grouping by order, class, or family will come out differently depending on which catalogue is followed.\n\n" +
            "### Suggestion\n\n" +
            "Treat these as classification differences to be aware of rather than errors. Where a placement looks out of date, the CoL entry linked in each row shows the classification it currently uses.",
        Columns = PlacementColumns(),
        Findings = OrderHigher(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings) },
    };

    // --- column sets ------------------------------------------------------------------------
    //
    // Column order across the species-level reports: the IUCN side first (name, rank, status, year),
    // then the CoL side (matched value, authority/year, link, cross-checks), then taxonomy context
    // and the detail. The higher-rank reports drop the per-assessment status/year.

    private static AuditColumn ColYearColumn() => AuditColumns.Custom("colYear", "CoL year", AuditColumnType.Number,
        "Year of the Catalogue of Life name: its name-published year, or the year in its authority.");

    private static AuditColumn ColAuthorityColumn() => AuditColumns.Custom("colAuthority", "CoL authority", AuditColumnType.Text,
        "Authorship (with year) of the Catalogue of Life accepted name, indicating when that name was established.");

    private static IEnumerable<AuditColumn> IucnHead() => new[] {
        AuditColumns.ScientificName("IUCN name"),
        AuditColumns.Rank(),
        AuditColumns.Status("IUCN status"),
        AuditColumns.Year("IUCN year"),
    };

    private static IEnumerable<AuditColumn> SpeciesTail() => new[] {
        AuditColumns.Class(),
        AuditColumns.Family(),
        AuditColumns.TaxonId("Taxon id"),
        AuditColumns.RedlistLink(),
        AuditColumns.Detail(),
    };

    private static IReadOnlyList<AuditColumn> NotFoundColumns() =>
        IucnHead().Concat(SpeciesTail()).ToList();

    private static IReadOnlyList<AuditColumn> CloseMatchColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.SuggestedValue("Closest CoL name", AuditColumnType.Text),
            ColYearColumn(),
            AuditColumns.ColLink(),
        }).Concat(SpeciesTail()).ToList();

    // Synonym report: the CoL accepted name, its authority/year (when it was established), the link,
    // and whether IUCN already records the CoL accepted name as a synonym.
    private static IReadOnlyList<AuditColumn> SynonymColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.SuggestedValue("CoL accepted name", AuditColumnType.Text),
            ColAuthorityColumn(),
            ColYearColumn(),
            AuditColumns.ColLink(),
            AuditColumns.Custom("iucnSynonym", "CoL name in IUCN synonyms", AuditColumnType.Text,
                "Whether IUCN already records the CoL accepted name as a synonym. \"of same taxon\" means the two catalogues disagree on which name is accepted. Blank when IUCN synonym data from the IUCN API is unavailable."),
        }).Concat(SpeciesTail()).ToList();

    private static IReadOnlyList<AuditColumn> AuthorityColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.CurrentValue("IUCN authority", AuditColumnType.Text),
            AuditColumns.SuggestedValue("CoL authority", AuditColumnType.Text),
            ColYearColumn(),
            AuditColumns.ColLink(),
        }).Concat(SpeciesTail()).ToList();

    private static IEnumerable<AuditColumn> HigherTail() => new[] {
        AuditColumns.Kingdom(),
        AuditColumns.Phylum(),
        AuditColumns.Custom("iucnSpecies", "IUCN taxa", AuditColumnType.Number,
            "Number of assessed IUCN taxa placed under this name."),
        AuditColumns.Detail(),
    };

    private static IReadOnlyList<AuditColumn> SynonymHigherColumns() => new[] {
        AuditColumns.ScientificName("IUCN name"),
        AuditColumns.Rank(),
        AuditColumns.SuggestedValue("CoL accepted name", AuditColumnType.Text),
        ColAuthorityColumn(),
        ColYearColumn(),
        AuditColumns.ColLink(),
    }.Concat(HigherTail()).ToList();

    private static IReadOnlyList<AuditColumn> PlacementColumns() => new[] {
        AuditColumns.ScientificName("IUCN name"),
        AuditColumns.Rank(),
        AuditColumns.Field("Rank compared"),
        AuditColumns.CurrentValue("IUCN placement", AuditColumnType.Text),
        AuditColumns.SuggestedValue("CoL placement", AuditColumnType.Text),
        AuditColumns.ColLink(),
    }.Concat(HigherTail()).ToList();

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
            Note = $"{compared:N0} assessments compared. Top classes shown; every row is in the CSV download.",
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
