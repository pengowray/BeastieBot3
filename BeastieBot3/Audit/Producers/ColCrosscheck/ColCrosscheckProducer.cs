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
    public const string SynonymLeadId = "col-other-name";

    // Shared by every page below, so each one can show where it sits in the crosscheck.
    private const string ColFamily = "col";
    public const string CloseMatchId = "col-close-match";
    public const string SynonymId = "col-synonym";
    public const string AcceptedDiffersId = "col-accepted-differs";
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
            AcceptedDiffers(source, assessed, data.AcceptedDiffers),
            SynonymHigher(source, higher, data.SynonymHigher),
            Classification(source, higher, data.Classification),
            Reorg(source, higher, data.Reorg),
            Authority(source, assessed, data.Authority),
            SynonymLead(source, assessed, data.SynonymLead),
            NotFound(source, assessed, data.NotFound),
        };
    }

    // --- species / subspecies reports -------------------------------------------------------

    private static AuditReport NotFound(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = NotFoundId,
        Title = "Names not found in the Catalogue of Life",
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 6,
        FamilyScope = "The name is absent from CoL: no near spelling, and no other name for the taxon either.",
        Blurb = "IUCN names that appear nowhere in the Catalogue of Life, with no close spelling match and no other name for the taxon there either.",
        Summary =
            "The IUCN names below appear nowhere in the Catalogue of Life, neither as accepted names nor as synonyms, and a search for similar spellings within the same genus and species epithet found no close candidate either. A further check looked up each taxon's other IUCN-listed names; those are absent from CoL too. Taxa where such a name was found have their own page: [Names absent from the Catalogue of Life where another name for the taxon is present](col-other-name.html).\n\n" +
            "### Why it matters\n\n" +
            "There is no route from these taxa into the Catalogue of Life: not the name, not a near spelling, not another name for the taxon. The likely reasons vary: the name may be newer than the CoL release compared against, may come from a source CoL does not yet cover, or may be spelled differently enough that no match is found.\n\n" +
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
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 4,
        FamilyScope = "The name is absent from CoL; a near spelling exists in the same genus or epithet.",
        Blurb = "IUCN names that appear nowhere in the Catalogue of Life, each paired with the most similar CoL spelling and a note on how the two differ.",
        Summary =
            "The IUCN names below appear nowhere in the Catalogue of Life, neither as accepted names nor as synonyms. Each is paired with the most similar CoL spelling in the same genus or with the same species epithet. The detail column says how the two spellings differ (punctuation, diacritics, Unicode encoding, or a small spelling change).\n\n" +
            "Two checks run on each suggested match, shown as columns: whether CoL treats it as an accepted name or as a synonym, and whether IUCN already lists it as a synonym of the taxon. Rows where IUCN already lists the close CoL spelling as a synonym of the same taxon are on [Name pairs where IUCN and the Catalogue of Life differ on which name is accepted](col-accepted-differs.html) instead.\n\n" +
            "### Why it matters\n\n" +
            "A name spelled slightly differently in the two catalogues will not cross reference, even when both records describe the same taxon. Automated matching fails and manual searches come up empty.\n\n" +
            "### Suggestion\n\n" +
            "Where the paired spelling is a variant of the same name, recording it as a synonym or aligning the spelling would let the two catalogues link. Where the closest CoL name is itself a CoL synonym, the CoL accepted name is the better one to compare against. The rows marked \"of other taxon\" pair the name with a similar spelling IUCN assigns to a different taxon; the resemblance there may be coincidence.",
        Columns = CloseMatchColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Synonym(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = SynonymId,
        Title = "Species and subspecies treated as synonyms in the Catalogue of Life",
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 3,
        FamilyScope = "CoL treats the name as a synonym of an accepted name IUCN does not list.",
        Blurb = "Assessed IUCN taxa whose name the Catalogue of Life records as a synonym of a different accepted name.",
        Summary =
            "The table below lists IUCN species, subspecies, and varieties whose scientific name the Catalogue of Life records as a synonym of a different accepted name. That accepted name is shown in the CoL accepted name column. Rows where IUCN in turn lists the CoL accepted name as its own synonym are on [Name pairs where IUCN and the Catalogue of Life differ on which name is accepted](col-accepted-differs.html) instead.\n\n" +
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

    // The reversed pairs pulled out of CloseMatch and Synonym: both catalogues hold both names and
    // each points at the other, so the records can be joined and nothing here is a broken link.
    private static AuditReport AcceptedDiffers(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AcceptedDiffersId,
        Title = "Name pairs where IUCN and the Catalogue of Life differ on which name is accepted",
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 2,
        FamilyScope = "Both catalogues hold both names and disagree only on which is accepted.",
        Blurb = "The two catalogues accept different names for the same taxon; IUCN's synonym list already links the pair.",
        Summary =
            "IUCN and the Catalogue of Life accept different names for each taxon on this page, and the IUCN record already lists the name CoL accepts among its synonyms. Some rows are mutual: CoL likewise treats the IUCN name as a synonym of its accepted name. In the others the name IUCN accepts appears nowhere in CoL, and the pairing rests on the IUCN synonym list alone. Either way the two records can be cross referenced, so nothing on this page is a missing name or a broken link.\n\n" +
            "### Why it matters\n\n" +
            "Not much is at stake: nothing on either side is wrong, and nothing here needs fixing. Two details are still worth knowing. The link between the pairs lives in the IUCN synonym list, which is published through the Red List API but not in the CSV export, so a match run against the export alone misses it. And a small number of rows give the same author two different publication years, one per catalogue; that can be a real bibliographic difference, as with a work issued in parts, or a transcription error, and those rows sort to the top of the table.\n\n" +
            "### Suggestion\n\n" +
            "No change is suggested. The table works as a concordance between the two datasets, as the list of taxa that would change name if the Catalogue of Life treatment were adopted, and, where the Authority years column is filled, as a short checklist of publication dates the two catalogues disagree on.",
        Columns = AcceptedDiffersColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByRankSummary("By rank", findings), ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    // Split out of NotFound: the IUCN name is absent from CoL, but another name IUCN records for the
    // taxon is there as a synonym. A lead to follow, which is a different job from the plain
    // not-found list, so it is a different page.
    private static AuditReport SynonymLead(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = SynonymLeadId,
        Title = "Names absent from the Catalogue of Life where another name for the taxon is present",
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 5,
        FamilyScope = "The name is absent from CoL; another IUCN-listed name for the taxon is there as a synonym.",
        Blurb = "The IUCN name is not in CoL, but one of the taxon's other IUCN-listed names is there as a synonym; a lead to check, not a confirmed match.",
        Summary =
            "The IUCN names below are absent from the Catalogue of Life: not accepted, not a synonym, and no near spelling either. But each taxon has another IUCN-listed name that does appear in CoL as a synonym. That name is shown per row, with the accepted name CoL gives for it where that link resolves. The chain can end at a name that is not the same taxon, so each row is a lead to check rather than a match.\n\n" +
            "### Why it matters\n\n" +
            "Searched by accepted name alone, these taxa look absent from the Catalogue of Life; searched through their synonyms, each one surfaces. Where the CoL record resolves to an accepted name, that name may be the same taxon under a different treatment, or the synonymy chain may have crossed to a different taxon; the table cannot tell the two apart.\n\n" +
            "### Suggestion\n\n" +
            "Check each lead: look up the CoL accepted name where one is shown, or the bare synonym record where none is, and judge whether it is the same taxon. Where it is, listing that accepted name among the assessment's synonyms would let the two catalogues link directly.",
        Columns = SynonymLeadColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static IReadOnlyList<AuditColumn> SynonymLeadColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.Custom("iucnSynonymInCol", "IUCN synonym in CoL", AuditColumnType.Text,
                "An IUCN-listed synonym of this taxon that appears in the Catalogue of Life."),
            AuditColumns.Custom("colAcceptedForSynonym", "CoL accepted name", AuditColumnType.Text,
                "The accepted name CoL gives for that synonym. Blank when CoL does not link it to one."),
            AuditColumns.ColLink(),
        }).Concat(SpeciesTail()).ToList();

    private static AuditReport Authority(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AuthorityId,
        Title = "Minor naming authority differences",
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 1,
        FamilyScope = "Exact name match; the author is spelled differently.",
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
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 7,
        FamilyScope = "A genus or higher name CoL records only as a synonym.",
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
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 8,
        FamilyScope = "A higher taxon whose CoL parent name looks like a spelling variant of IUCN's.",
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
        Breakage = BreakageClass.Advisory,
        DataSourceLabel = source,
        FamilyId = ColFamily,
        FamilyRank = 9,
        FamilyScope = "A higher taxon placed under a genuinely different CoL parent.",
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
            AuditColumns.Custom("colStatus", "CoL status", AuditColumnType.Text,
                "What the Catalogue of Life calls the closest name: an accepted name, or a synonym of some other accepted name."),
            AuditColumns.Custom("iucnSynonym", "Closest name in IUCN synonyms", AuditColumnType.Text,
                "Whether IUCN already records the closest CoL name as a synonym. Blank when IUCN synonym data from the IUCN API is unavailable."),
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
                "Whether IUCN already records the CoL accepted name as a synonym. Blank when IUCN synonym data from the IUCN API is unavailable."),
        }).Concat(SpeciesTail()).ToList();

    // Accepted-name-differs report: the two names side by side with their authorities, the year
    // discrepancy flag, then the shared tail. Rank and assessment year are dropped; neither is acted
    // on here, and the trinomials show the rank anyway.
    private static IReadOnlyList<AuditColumn> AcceptedDiffersColumns() => new[] {
        AuditColumns.ScientificName("IUCN name"),
        AuditColumns.Custom("iucnAuthority", "IUCN authority", AuditColumnType.Text,
            "Naming authority recorded by IUCN for its accepted name."),
        AuditColumns.Status("IUCN status"),
        AuditColumns.SuggestedValue("CoL name", AuditColumnType.Text),
        ColAuthorityColumn(),
        AuditColumns.Custom("authorityYears", "Authority years", AuditColumnType.Text,
            "Filled only where both catalogues credit the same author but give different years for the name."),
        AuditColumns.ColLink(),
    }.Concat(SpeciesTail()).ToList();

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
