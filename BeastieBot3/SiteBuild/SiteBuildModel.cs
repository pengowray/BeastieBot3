// The rows `site build-db` collects before writing them to the site database, and the counts its
// summary reports. Columns follow BeastieBot3.Shared/SiteData/SiteDbSchema.cs.

namespace BeastieBot3.SiteBuild;

/// Where each source database is. IucnDatabase and ApiCache are required; a missing optional
/// source leaves its columns empty and the build says so.
internal sealed record SiteBuildInputs {
    public required string IucnDatabase { get; init; }
    public required string ApiCache { get; init; }
    public string? CommonNames { get; init; }
    public string? WikidataCache { get; init; }
    public string? WikipediaCache { get; init; }
    /// "<COL_sqlite>.placement.sqlite", built by `col build-placement`.
    public string? ColPlacement { get; init; }
    /// The folder with the release's ColDP zip (Datasets:COL_dir), for its citation and DOI.
    public string? ColDir { get; init; }
    /// The CoL database itself; only its file name is read, for the release when there is no placement file.
    public string? ColDatabase { get; init; }
    public string? SpratDatabase { get; init; }
    /// `iucn resolve-dois`'s cache (Datastore:IUCN_doi_cache_sqlite): DOIs found in Crossref's list
    /// of IUCN DOIs or at doi.org.
    public string? DoiCache { get; init; }
    /// rules-list.txt, whose "Scientific name = common name" lines override the best English name,
    /// as they do in the Wikipedia lists.
    public string? RulesList { get; init; }
    /// GBIF's copy of the IUCN checklist (a Darwin Core Archive zip).
    public string? GbifChecklist { get; init; }
    /// The Wikidata assessment item model (rules/wikidata/iucn-status.yml, assessment_item) stored
    /// in meta for the site's QuickStatements batches, and the file it was read from (null: the
    /// defaults, when no file was found).
    public Shared.Wikitext.WikidataItemModel WikidataItemModel { get; init; } = new();
    public string? WikidataItemModelSource { get; init; }
    /// The site database to replace.
    public required string Output { get; init; }
    /// Only the first N taxa by taxon id.
    public int? Limit { get; init; }
}

/// One taxon row, filled in phase by phase. The name lists are only kept until the names are written.
internal sealed class SiteTaxon {
    public required long TaxonId { get; init; }
    public required string ScientificName { get; init; }
    public required string Kind { get; init; }
    public string? Kingdom { get; init; }
    public string? Phylum { get; init; }
    public string? ClassName { get; init; }
    public string? OrderName { get; init; }
    public string? Family { get; init; }
    public string? Genus { get; init; }
    public string? SpeciesEpithet { get; init; }
    public string? InfraRank { get; init; }
    public string? InfraName { get; init; }
    public string? SubpopulationName { get; init; }
    public string? Authority { get; init; }

    public long? ParentTaxonId { get; set; }
    public string? CommonNameEn { get; set; }
    public string? EnwikiTitle { get; set; }
    public string? WikidataQid { get; set; }
    public string? WikidataQidSource { get; set; }
    /// For an item that states the taxon's IUCN taxon id: its P141 statements as JSON
    /// (WikidataStatusStatement), and the day the Wikidata cache downloaded it. Null when the
    /// cache has not downloaded the item.
    public string? WikidataP141 { get; set; }
    public string? WikidataItemDownloaded { get; set; }
    /// The item states the taxon's IUCN taxon id only at deprecated rank.
    public bool WikidataP627Deprecated { get; set; }
    /// The other items that state the taxon's IUCN taxon id, as JSON (WikidataOtherTaxonItem);
    /// null when no other item does.
    public string? WikidataOtherItems { get; set; }
    public string? ColId { get; set; }
    public long? LatestGlobalAssessmentId { get; set; }

    /// False for a taxon that is only in the IUCN API cache, not in the release's CSV export (an old
    /// or merged id, or a taxon IUCN no longer assesses). None of its assessments is latest.
    public bool InRelease { get; init; } = true;

    /// For a taxon not in the release: the taxon in the release with the same scientific name.
    public long? CurrentTaxonId { get; set; }

    /// SPRAT profiles and EPBC Act listings (epbc_listing rows).
    public List<EpbcListing> EpbcListings { get; } = new();

    /// The API's taxon record lists the species of an infraspecific taxon (species_taxa); used for
    /// the parent when the CSV has no species of that name.
    public long? ApiSpeciesId { get; set; }

    public List<IucnCommonName> IucnCommonNames { get; set; } = new();
    public List<string> IucnSynonyms { get; set; } = new();
    public List<(string Name, string Source, bool IsPreferred)> EnglishNames { get; } = new();
    public List<string> ColSynonyms { get; } = new();
}

internal sealed record IucnCommonName(string Name, string? Language, bool IsMain);

internal static class EpbcAppliesTo {
    public const string Taxon = "taxon";
    public const string Population = "population";
}

/// One SPRAT profile of a taxon. Status: the EPBC Act category code, null when the profile is not
/// listed. Population: for a profile named after the taxon with a population in brackets, the text
/// in the brackets ("combined populations of Qld, NSW and the ACT").
internal sealed record EpbcListing(long SpratTaxonId, string ListedName, string? Status, string AppliesTo, string? Population);

/// One assessment row to write. The CSV gives the latest assessments; the API headers add the
/// earlier ones, whose trend and criteria version come from the payload.
internal sealed class SiteAssessment {
    public required long AssessmentId { get; init; }
    public required long TaxonId { get; init; }
    public required string Scope { get; init; }
    public required bool IsLatest { get; set; }
    public required string Category { get; init; }
    public bool PossiblyExtinct { get; init; }
    public bool PossiblyExtinctInTheWild { get; init; }
    public string? Criteria { get; init; }
    public string? CriteriaVersion { get; set; }
    public int? YearPublished { get; init; }
    public string? AssessmentDate { get; init; }
    public string? PopulationTrend { get; set; }
    public string? CitationJson { get; set; }
    /// The Wikidata item for the assessment as a publication, and the properties it has
    /// (SiteWikidataItems).
    public string? WikidataItemQid { get; set; }
    public string? WikidataItemProperties { get; set; }
    /// That item's title statements as JSON (WikidataTitle; null when not recorded) and English label.
    public string? WikidataItemTitles { get; set; }
    public string? WikidataItemLabelEn { get; set; }
    /// The assessment that item is for. An errata version that shares the item of the assessment
    /// it corrects has that assessment's id here.
    public long? WikidataItemAssessmentId { get; set; }
    /// True when the row came from the CSV, whose values win over the API's.
    public required bool FromCsv { get; init; }
}

/// What the summary reports. Plain counters: one build runs on one thread.
internal sealed class SiteBuildStats {
    public string? IucnRelease;
    public readonly Dictionary<string, int> TaxaByKind = new(StringComparer.Ordinal);
    public int ParentsByName;
    public int ParentsFromApi;
    public int ParentsMissing;

    public int CsvAssessments;
    public int CsvAssessmentsNoScope;
    public int CsvAssessmentsUnknownCategory;
    public int ApiTaxaRowsRead;
    public int ApiTaxaRowsUnreadable;
    public int ApiHeadersOtherTaxon;
    public int ApiHeadersNoScope;
    public int ApiHeadersUnpublished;
    public int ApiHeadersNoCategory;
    public int ApiLatestCoveredByCsv;
    public int ApiLatestNotInCsv;
    public int CsvCategoryDiffersFromApi;

    public int AssessmentsGlobalLatest;
    public int AssessmentsRegionalLatest;
    public int AssessmentsHistory;

    public int PayloadsRead;
    public int PayloadsUnreadable;
    public int CitationsParsed;
    public int CitationsNotCached;
    public readonly Dictionary<CitationParseFailure, int> CitationFailures = new();
    public readonly Dictionary<Shared.Wikitext.DoiSource, int> DoisBySource = new();
    public DateTime? DownloadedFrom;
    public DateTime? DownloadedTo;
    /// Author names with a letter lost to an encoding error: repaired (from, to), and not repaired.
    public readonly Dictionary<(string From, string To), int> AuthorNameRepairs = new();
    public readonly Dictionary<string, int> AuthorNamesNotRepaired = new(StringComparer.Ordinal);

    /// replaced_by_assessment_id values written, by the kind of version that replaced the assessment.
    public int ReplacedByErrata;
    public int ReplacedByAmended;
    /// Errata versions whose replaced assessment PredecessorIds missed and was found from the DOI
    /// that `iucn resolve-dois` gave them (included in ReplacedByErrata unless claimed twice).
    public int ReplacedFoundFromDoi;
    /// Errata and amended versions whose replaced assessment was not found, or not one alone.
    public int ReplacedNoCandidate;
    public int ReplacedSeveralCandidates;
    /// Earlier assessments that two newer versions both name; left unlinked.
    public int ReplacedClaimedTwice;

    public readonly Dictionary<string, int> NamesByType = new(StringComparer.Ordinal);
    public int CommonNamesEnglish;
    /// Common names in any language that CommonNameQuality found to be junk (left out of the name
    /// table), and that it repaired (stored repaired). Counted per taxon by SiteNameSet.
    public int CommonNamesJunk;
    public int CommonNamesRepaired;
    public int CommonNameEn;
    public int CommonNameEnUnusable;
    public int CommonNameEnFromRules;
    public int SynonymsBuiltFromFullName;

    public int EnwikiTitles;
    /// Wikidata items for assessments (wikidata_iucn_assessment_items): which were kept, and the
    /// assessments that got one, their own or (an errata version) the one its DOI names.
    public SiteWikidataItems WikidataItems = new();
    public int AssessmentsWithOwnItem;
    public int AssessmentsWithItemThroughDoi;
    public string? WikidataItemModelSource;
    public int QidsFromP627;
    public int QidsFromNameMatch;
    public int QidsNameMatchOtherKingdom;
    public int QidTieBreaks;
    /// Taxa linked through P627 whose item the Wikidata cache has downloaded, so their P141
    /// statements are known; and of those, items with no P141 statement.
    public int QidsWithP141Known;
    public int QidsWithNoP141;
    /// Taxa linked through P627 whose chosen item states the id only at deprecated rank. Null: the
    /// cache had no table of deprecated ids.
    public int? QidsP627Deprecated;
    /// Edition items of the Red List read from the cache; null when the cache has no such table.
    public int? RedListEditions;
    /// Items whose JSON was read for P141 references the index does not record, and the P141
    /// statements found to cite IUCN that way: by a reference URL on iucnredlist.org, or by a
    /// stated in after a reference's first.
    public int P141ItemsReadAsJson;
    public int P141CitesIucnByUrl;
    public int P141CitesIucnByLaterStatedIn;
    /// Citations whose DOI has a title registered with Crossref, and of those, the ones whose name
    /// differs from IUCN's citation name (WikidataCitation.SameName).
    public int RegisteredNames;
    public int RegisteredNamesDiffer;
    public int ColIdsFromPlacement;
    public int ColIdsFromCrossReference;
    public int SpratMatched;
    public int EpbcStatuses;
    public int SpratPopulationProfiles;
    public int EpbcPopulationListings;
    /// SPRAT rows named after a taxon with something in brackets that is not a population: a
    /// voucher or a sense ("sensu lato").
    public int SpratBracketsNotPopulation;

    /// Taxa only in the API cache (not in the CSV export), by kind; and their API headers flagged
    /// latest, which are stored as earlier assessments.
    public readonly Dictionary<string, int> NotInReleaseByKind = new(StringComparer.Ordinal);
    public int NotInReleaseRecordsUnusable;
    public int NotInReleaseWithCurrentTaxon;
    public int NotInReleaseLatestHeaders;

    /// `iucn resolve-dois`'s cache: assessments it checked, of those with a DOI, and its newest check.
    public int DoiChecksRead;
    public int DoiChecksWithDoi;
    public DateTime? DoiCheckedTo;

    public string? GbifVersion;
    public string? GbifPublished;
    public string? GbifCitation;
    public string? GbifDoi;
    public string? ColRelease;
    public string? ColCitation;
    public string? ColDoi;
    public string? SpratReport;
    public readonly List<string> MissingSources = new();
    public readonly List<string> Warnings = new();

    public long FileBytes;
    public readonly List<(string Phase, TimeSpan Elapsed)> Phases = new();

    public void Count<TKey>(Dictionary<TKey, int> counts, TKey key) where TKey : notnull =>
        counts[key] = counts.GetValueOrDefault(key) + 1;
}
