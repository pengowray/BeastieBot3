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
//   col-via-wiki        no CoL match for the IUCN name, but Wikidata/Wikipedia record a name CoL has
//   col-not-found       no exact CoL match and no near candidate
//
// Pages are ordered most-actionable first; the noisier "not found" list comes last.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal sealed class ColCrosscheckProducer : IAuditReportSetProducer {
    public const string NotFoundId = "col-not-found";
    public const string SynonymLeadId = "col-other-name";
    public const string ViaWikiId = "col-via-wiki";

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

        // Wikidata and Wikipedia sit outside both catalogues, so they can say whether a name CoL
        // has never heard of is in use anywhere else, and occasionally supply the name that is in
        // CoL. Only the not-found page uses them, and only for its own few hundred rows.
        var otherSources = OtherSourceIndex.Build(ctx.WikidataCacheOrNull(), ctx.WikipediaCacheOrNull());

        var data = new ColCrosscheckEngine(colRepo, iucnSynonyms, otherSources).Run(rows, ctx.Ct);
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
            ViaWiki(source, assessed, data),
            NotFound(source, assessed, data),
        };
    }

    // --- species / subspecies reports -------------------------------------------------------

    private static AuditReport NotFound(string source, int assessed, ColCrosscheckData data) {
        var findings = data.NotFound;
        return new AuditReport {
            Id = NotFoundId,
            Title = "Names not in the Catalogue of Life under any known name",
            Action = ActionClass.Informational,
            IsAppendix = true,
            DataSourceLabel = source,
            SectionId = ColFamily,
            FamilyId = ColFamily,
            FamilyRank = 10,
            FamilyScope = "Not in CoL under this name, a near spelling, or any other name known for the taxon.",
            Blurb = "IUCN names that are not in the Catalogue of Life under any name known for the taxon: not this name, not a near spelling, not an IUCN synonym, and not a name from Wikidata or Wikipedia.",
            Summary =
                "The IUCN names below are not in the Catalogue of Life, either as accepted names or as synonyms. A search for similar spellings in the same genus, and with the same species epithet, found nothing. The taxon's other IUCN-listed names are not in CoL either. " +
                "Taxa where an IUCN synonym is in CoL are on [Names not in the Catalogue of Life, but an IUCN synonym is](col-other-name.html), and taxa where Wikidata or Wikipedia record a name CoL has are on [Names not in the Catalogue of Life, but a Wikidata or Wikipedia name is](col-via-wiki.html).\n\n" +
                OtherSourceSummary(data) + "\n\n" +
                "### Why it matters\n\n" +
                "Nothing links these taxa to a Catalogue of Life record. The usual reasons: the name is newer than the CoL release compared against, it comes from a source CoL does not yet cover, or it is spelled differently enough that no match is found.\n\n" +
                "### Suggestion\n\n" +
                "No change is suggested. The list is for spot-checking against current literature; many entries are legitimately newer than the Catalogue of Life release compared against (its version is given above under Source).",
            Columns = NotFoundColumns(data.OtherSourcesChecked),
            Findings = OrderSpecies(findings),
            HeadlineCount = findings.Count,
            SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
            GroupLevels = AuditGroups.ByClassOrderFamily,
        };
    }

    // Split out of NotFound once Wikidata and Wikipedia have been consulted: the IUCN name is absent
    // from CoL, but one of those sources records another name for the taxon that CoL does hold.
    // Usually a genus transfer that CoL has made and the Red List has not.
    private static AuditReport ViaWiki(string source, int assessed, ColCrosscheckData data) {
        var findings = data.ViaWiki;
        return new AuditReport {
            Id = ViaWikiId,
            Title = "Names not in the Catalogue of Life, but a Wikidata or Wikipedia name is",
            Action = ActionClass.ByHand,
            IsAppendix = true,
            DataSourceLabel = source,
            SectionId = ColFamily,
            FamilyId = ColFamily,
            FamilyRank = 3,
            FamilyScope = "Not in CoL under this name or an IUCN synonym; Wikidata or Wikipedia give a name CoL has.",
            Blurb = "IUCN names that are not in the Catalogue of Life, where Wikidata or English Wikipedia record another name for the same taxon that is in CoL. No name currently links the two records.",
            Summary =
                "The IUCN names below are not in the Catalogue of Life: not accepted, not a synonym, no near spelling, and no IUCN-listed synonym is there either. " +
                "For each, Wikidata or English Wikipedia records another name for the same taxon, and that name is in CoL. The Name in CoL column shows it, with whether CoL treats it as an accepted name or a synonym, and links to the CoL record. " +
                "Most are genus transfers that the Catalogue of Life has adopted and the Red List has not.\n\n" +
                "### Why it matters\n\n" +
                "No name currently appears in both catalogues for these taxa, so nothing joins the two records. Anyone matching the Red List to the Catalogue of Life by name, in either direction, misses every one of them. Wikidata and Wikipedia are the only places the two names are recorded together, and those links are unverified.\n\n" +
                "### Suggestion\n\n" +
                "Check whether the Name in CoL is a valid synonym of the IUCN taxon. Where it is, adding it to the assessment's synonym list would give the two catalogues a shared name to join on.",
            Columns = ViaWikiColumns(),
            Findings = OrderSpecies(findings),
            HeadlineCount = findings.Count,
            SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
            GroupLevels = AuditGroups.ByClassOrderFamily,
        };
    }

    private static AuditReport CloseMatch(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = CloseMatchId,
        Title = "Names with a close Catalogue of Life match",
        Action = ActionClass.ByHand,
        TriageRank = 5,
        TriageReason = "Spelling differences which prevent cross referencing.",
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 1,
        FamilyScope = "Not in CoL under this name; a near spelling is, in the same genus or with the same epithet.",
        Blurb = "IUCN names that appear nowhere in the Catalogue of Life, each paired with the most similar CoL spelling and a note on how the two differ.",
        Summary =
            "The IUCN names below appear nowhere in the Catalogue of Life, neither as accepted names nor as synonyms. Each is paired with the most similar CoL spelling in the same genus or with the same species epithet. The Detail column says how the two spellings differ (punctuation, diacritics, Unicode encoding, or a small spelling change).\n\n" +
            "Two checks run on each suggested match, shown as columns: whether CoL treats it as an accepted name or as a synonym, and whether IUCN already lists it as a synonym of the taxon. Rows where IUCN already lists the close CoL spelling as a synonym of the same taxon are on [Name pairs where IUCN and the Catalogue of Life differ on which name is accepted](col-accepted-differs.html) instead.\n\n" +
            "### Why it matters\n\n" +
            "A name spelled slightly differently in the two catalogues will not cross reference, even when both records describe the same taxon. Automated matching fails and manual searches come up empty.\n\n" +
            "### Suggestion\n\n" +
            "Where the paired spelling is a variant of the same name, recording it as a synonym or aligning the spelling would let the two catalogues link. Where the closest CoL name is itself a CoL synonym, the CoL accepted name is the better one to compare against. Rows marked \"of other taxon\" pair the name with a similar spelling that IUCN assigns to a different taxon; those may be coincidence.",
        Columns = CloseMatchColumns(),
        Findings = OrderSpecies(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static AuditReport Synonym(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = SynonymId,
        Title = "Species and subspecies treated as synonyms in the Catalogue of Life",
        Action = ActionClass.Policy,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 4,
        FamilyScope = "CoL treats the name as a synonym of an accepted name that IUCN does not list.",
        Blurb = "Assessed IUCN taxa whose name the Catalogue of Life records as a synonym of a different accepted name.",
        Summary =
            "The table below lists IUCN species, subspecies, and varieties whose scientific name the Catalogue of Life records as a synonym of a different accepted name. That accepted name is shown in the CoL accepted name column. Rows where IUCN in turn lists the CoL accepted name as its own synonym are on [Name pairs where IUCN and the Catalogue of Life differ on which name is accepted](col-accepted-differs.html) instead.\n\n" +
            NameInUseIntro + "\n\n" +
            "### Why it matters\n\n" +
            "Where CoL has moved a name into synonymy, the IUCN name may be an earlier combination of the same species, or a taxon that has since been merged into another. Databases that follow the Catalogue of Life file the taxon under the accepted name, not the IUCN one.\n\n" +
            "### Suggestion\n\n" +
            "Compare each IUCN name with the CoL accepted name and confirm which reflects current taxonomy. Rows where Wikidata and Wikipedia already use the CoL name sort first; those are the likeliest to need a change.",
        Columns = SynonymColumns(),
        Findings = OrderByNameInUse(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { NameInUseSummary(findings), ByRankSummary("By rank", findings), ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    // The reversed pairs pulled out of CloseMatch and Synonym: both catalogues hold both names and
    // each points at the other, so the records can be joined and nothing here is a broken link.
    private static AuditReport AcceptedDiffers(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AcceptedDiffersId,
        Title = "Name pairs where IUCN and the Catalogue of Life differ on which name is accepted",
        Action = ActionClass.Informational,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 8,
        FamilyScope = "Both catalogues hold both names and disagree only on which is accepted.",
        Blurb = "The two catalogues accept different names for the same taxon; IUCN's synonym list already links the pair.",
        Summary =
            "IUCN and the Catalogue of Life accept different names for each taxon on this page, and the IUCN record already lists the name CoL accepts among its synonyms. Some rows are mutual: CoL likewise treats the IUCN name as a synonym of its accepted name. In the others the name IUCN accepts appears nowhere in CoL, and the pairing rests on the IUCN synonym list alone. Either way the two records can be cross referenced, so nothing on this page is a missing name or a broken link.\n\n" +
            "### Why it matters\n\n" +
            "Nothing on either side is wrong, and nothing here needs fixing. Two details are worth knowing. The link between each pair is the IUCN synonym list, which the Red List API publishes but the CSV export does not, so a match run against the export alone misses it. And a few rows give the same author two different publication years, one per catalogue; that can be a real bibliographic difference, as with a work issued in parts, or a transcription error. Those rows sort to the top of the table.\n\n" +
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
        Title = "Names not in the Catalogue of Life, but an IUCN synonym is",
        Action = ActionClass.ByHand,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 2,
        FamilyScope = "Not in CoL under this name; one of the taxon's IUCN synonyms is there as a synonym.",
        Blurb = "The IUCN name is not in CoL, but one of the taxon's IUCN-listed synonyms is there as a synonym. Each row needs checking; the synonym may point at a different taxon.",
        Summary =
            "The IUCN names below are not in the Catalogue of Life: not accepted, not a synonym, and no near spelling either. But each taxon has an IUCN-listed synonym that is in CoL as a synonym. That synonym is shown per row, with the accepted name CoL gives for it where CoL records one. A synonym can belong to a different taxon in CoL, so each row needs checking.\n\n" +
            NameInUseIntro + "\n\n" +
            "### Why it matters\n\n" +
            "Searched by accepted name alone, these taxa look absent from the Catalogue of Life; searched by synonym, each one is found. Where CoL gives an accepted name for the synonym, that name may be the same taxon under a different treatment, or a different taxon that shares the synonym; the table cannot tell the two apart.\n\n" +
            "### Suggestion\n\n" +
            "Check each row: look up the CoL accepted name where one is shown, or the synonym record where none is, and judge whether it is the same taxon. Where it is, adding that accepted name to the assessment's synonyms would let the two catalogues link directly.",
        Columns = SynonymLeadColumns(),
        Findings = OrderByNameInUse(findings),
        HeadlineCount = findings.Count,
        SummaryTables = new[] { NameInUseSummary(findings), ByClassSummary("By class", findings, assessed) },
        GroupLevels = AuditGroups.ByClassOrderFamily,
    };

    private static IReadOnlyList<AuditColumn> SynonymLeadColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.Custom("iucnSynonymInCol", "IUCN synonym in CoL", AuditColumnType.Text,
                "An IUCN-listed synonym of this taxon that is in the Catalogue of Life."),
            AuditColumns.Custom("colAcceptedForSynonym", "CoL accepted name", AuditColumnType.Text,
                "The accepted name CoL gives for that synonym. Blank when CoL does not link it to one."),
            NameInUseColumn(),
            AuditColumns.ColLink(),
        }).Concat(SpeciesTail()).ToList();

    private static AuditReport Authority(string source, int assessed, List<AuditFinding> findings) => new() {
        Id = AuthorityId,
        Title = "Author names spelled differently",
        Action = ActionClass.ByHand,
        IsAppendix = true,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 7,
        FamilyScope = "Same name in both; the author is spelled slightly differently.",
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
        Action = ActionClass.ByHand,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 5,
        FamilyScope = "A genus, family, order, or class name that CoL records only as a synonym.",
        Blurb = "Genus, family, order, and class names used in the IUCN classification that the Catalogue of Life records only as synonyms.",
        Summary =
            "The table below lists higher-rank names (genus, family, order, or class) used in the IUCN classification that the Catalogue of Life records only as a synonym, never as an accepted name. The CoL accepted name column shows the name CoL uses instead, and the IUCN taxa column counts the assessed taxa placed under the name.\n\n" +
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
        Title = "Parent taxon spelled differently in the Catalogue of Life",
        Action = ActionClass.ByHand,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 6,
        FamilyScope = "Same parent taxon in both, spelled slightly differently.",
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
        Title = "Higher-rank names placed under a different parent in the Catalogue of Life",
        Action = ActionClass.Informational,
        DataSourceLabel = source,
        SectionId = ColFamily,
        FamilyId = ColFamily,
        FamilyRank = 9,
        FamilyScope = "CoL places the name under a different order, class, or family.",
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

    private static IReadOnlyList<AuditColumn> NotFoundColumns(bool otherSourcesChecked) {
        var columns = IucnHead().ToList();
        if (otherSourcesChecked) {
            columns.AddRange(OtherSourceColumns());
        }
        columns.AddRange(SpeciesTail());
        return columns;
    }

    // Shown only when the Wikidata/Wikipedia caches were actually read. Leaving them out entirely
    // beats three empty columns, which read as "checked, found nothing".
    private static IEnumerable<AuditColumn> OtherSourceColumns() => new[] {
        new AuditColumn {
            Key = "wikidataId", Header = "Wikidata", Type = AuditColumnType.Url,
            Help = "The Wikidata item linked to this taxon's IUCN id. Blank: no item found.",
            Value = f => f.Get("wikidataId"), Href = f => f.Get("wikidataUrl"),
        },
        new AuditColumn {
            Key = "wikipediaTitle", Header = "Wikipedia", Type = AuditColumnType.Url,
            Help = "The English Wikipedia article matched to this taxon. Blank: no article found.",
            Value = f => f.Get("wikipediaTitle"), Href = f => f.Get("wikipediaUrl"),
        },
    };

    private static IReadOnlyList<AuditColumn> ViaWikiColumns() =>
        IucnHead().Concat(OtherSourceColumns()).Concat(new[] {
            new AuditColumn {
                Key = "otherName", Header = "Name in CoL", Type = AuditColumnType.Url,
                Help = "A name for this taxon recorded on Wikidata or Wikipedia that is in the Catalogue of Life. Links to the CoL record.",
                Value = f => f.Get("otherName"), Href = f => f.Get("colUrl"),
            },
            AuditColumns.Custom("otherNameColStatus", "CoL status", AuditColumnType.Text,
                "Whether the Catalogue of Life treats that name as an accepted name or as a synonym."),
            AuditColumns.ColLink(),
        }).Concat(SpeciesTail()).ToList();

    // The counts belong in the introduction, because the columns are blank for most rows and a
    // reader needs to know whether that is a finding or a check that never ran.
    private static string OtherSourceSummary(ColCrosscheckData data) {
        if (!data.OtherSourcesChecked) {
            return "The Wikidata and Wikipedia check did not run for this report, so the Wikidata and Wikipedia columns are blank on every row. Blank here does not mean nothing was found.";
        }
        return $"Each of these names was also looked up on Wikidata and English Wikipedia. Of the {data.NotFound.Count:N0} taxa, " +
               $"{data.OtherSourcesWithWikidata:N0} have a Wikidata item and {data.OtherSourcesWithWikipedia:N0} have an English Wikipedia article. " +
               "Neither source gives a name for them that is in the Catalogue of Life. The rest have no Wikidata item and no Wikipedia article, so the name is not in use in any source this check covers.";
    }

    private static IReadOnlyList<AuditColumn> CloseMatchColumns() =>
        IucnHead().Concat(new[] {
            AuditColumns.SuggestedValue("Closest CoL name", AuditColumnType.Text),
            AuditColumns.Custom("colStatus", "CoL status", AuditColumnType.Text,
                "What the Catalogue of Life calls the closest name: an accepted name, or a synonym of some other accepted name."),
            AuditColumns.Custom("iucnSynonym", "Closest name listed as IUCN synonym", AuditColumnType.Text,
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
            AuditColumns.Custom("iucnSynonym", "CoL name listed as IUCN synonym", AuditColumnType.Text,
                "Whether IUCN already records the CoL accepted name as a synonym. Blank when IUCN synonym data from the IUCN API is unavailable."),
            NameInUseColumn(),
        }).Concat(SpeciesTail()).ToList();

    private const string NameInUseIntro =
        "The Name used on Wikidata and Wikipedia column says which of the two names those sources use for the taxon: the CoL name, the IUCN name, both (usually one redirecting to the other), or neither. Neither source is an authority, but each is a third party that chose one of the two names. Blank means the taxon has no Wikidata item and no English Wikipedia article, or the check did not run.";

    private static AuditColumn NameInUseColumn() => AuditColumns.Custom("nameInUse", "Name used on Wikidata and Wikipedia", AuditColumnType.Text,
        "Which of the two names Wikidata and English Wikipedia use for this taxon: CoL name, IUCN name, both, or neither. Blank when the taxon is in neither source.");

    private static int NameInUseOrder(string? value) => value switch {
        "CoL name" => 0,
        "both" => 1,
        "neither" => 2,
        "IUCN name" => 3,
        _ => 4,
    };

    private static IReadOnlyList<AuditFinding> OrderByNameInUse(List<AuditFinding> findings) => findings
        .OrderBy(f => NameInUseOrder(f.Get("nameInUse")))
        .ThenByDescending(f => f.SeverityTier)
        .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.Family, StringComparer.OrdinalIgnoreCase)
        .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
        .ToList();

    private static AuditSummaryTable NameInUseSummary(IReadOnlyList<AuditFinding> findings) {
        var order = new[] { "CoL name", "both", "neither", "IUCN name" };
        var counts = findings.GroupBy(f => f.Get("nameInUse") ?? "").ToDictionary(g => g.Key, g => g.Count());
        var rows = order.Select(k => new[] { k, (counts.TryGetValue(k, out var n) ? n : 0).ToString("N0") } as IReadOnlyList<string>).ToList();
        rows.Add(new[] { "Not on Wikidata or Wikipedia", (counts.TryGetValue("", out var blank) ? blank : 0).ToString("N0") });
        return new AuditSummaryTable {
            Title = "Name used on Wikidata and Wikipedia",
            Note = "Which of the two names each third-party source uses for the taxon. \"Both\" is usually one name redirecting to the other.",
            Headers = new[] { "Name in use", "Taxa" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

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
