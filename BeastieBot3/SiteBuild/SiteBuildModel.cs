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
    /// The Wikispecies cache (`wikispecies fetch`), for the comparison of ranks.
    public string? WikispeciesCache { get; init; }
    /// "<COL_sqlite>.placement.sqlite", built by `col build-placement`.
    public string? ColPlacement { get; init; }
    /// The folder with the release's ColDP zip (Datasets:COL_dir), for its citation and DOI.
    public string? ColDir { get; init; }
    /// The CoL database itself; only its file name is read, for the release when there is no placement file.
    public string? ColDatabase { get; init; }
    public string? SpratDatabase { get; init; }
    /// The status lists store (`statuses natureserve-fetch`, `statuses ecos-import`).
    public string? StatusListsDatabase { get; init; }
    /// The rows of IUCN's summary tables 7 and 9 (`iucn summary-tables`): reasons for category
    /// changes, and species listed as Possibly Extinct.
    public string? SummaryTables { get; init; }
    /// `iucn resolve-dois`'s cache (Datastore:IUCN_doi_cache_sqlite): DOIs found in Crossref's list
    /// of IUCN DOIs or at doi.org.
    public string? DoiCache { get; init; }
    /// rules/iucn-not-assigned.yml: orders and families for taxa that IUCN gives as "NOT ASSIGNED",
    /// for the tree of groups, as in the Wikipedia lists.
    public Iucn.IucnNotAssignedRules NotAssignedRules { get; init; } = Iucn.IucnNotAssignedRules.None;
    /// taxon-rules.yml, whose common names for groups the list headings use.
    public string? TaxonRules { get; init; }
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
    /// The checklists store (`checklists import`), for the Mammal Diversity Database's and AmphibiaWeb's
    /// names and the Mammal Diversity Database's and the Reptile Database's subspecies.
    public string? Checklists { get; init; }
    /// rules/wikidata-taxon-ranks.csv: the names of Wikidata's rank items, for ladder_node.
    public string? WikidataRanks { get; init; }
    /// The site database to replace.
    public required string Output { get; init; }
    /// Only the first N taxa by taxon id.
    public int? Limit { get; init; }
    /// Species from the Catalogue of Life and Wikidata that are not IUCN taxa (extra_species).
    public ExtraSpecies.ExtraPlacement ExtraSpecies { get; init; } = SiteBuild.ExtraSpecies.ExtraPlacement.Family;
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
    /// The {{IUCN status}} code of the latest global assessment ("CR(PE)", "LR/nt"); null when none.
    public string? LatestGlobalStatusCode { get; set; }

    /// The tree of groups (SiteTaxonTree): the lowest group the taxon is in, and its place in the tree.
    public int? NodeId { get; set; }
    public int? TreePos { get; set; }
    /// What a Wikipedia list line links (SpeciesLineFormatter): the taxon's article, and for a
    /// subspecies or variety its species' article.
    public string? ListArticleTitle { get; set; }
    public string? ListParentArticleTitle { get; set; }

    /// False for a taxon that is only in the IUCN API cache, not in the release's CSV export (an old
    /// or merged id, or a taxon IUCN no longer assesses). None of its assessments is latest.
    public bool InRelease { get; init; } = true;

    /// For a taxon not in the release: the taxon in the release with the same scientific name
    /// (SiteTaxonLinks).
    public long? CurrentTaxonId { get; set; }

    /// SPRAT profiles and EPBC Act listings (epbc_listing rows).
    public List<EpbcListing> EpbcListings { get; } = new();

    /// Statuses in lists other than the IUCN Red List (other_status rows).
    public List<OtherStatus> OtherStatuses { get; } = new();

    /// The API's taxon record lists the species of an infraspecific taxon (species_taxa); used for
    /// the parent when the CSV has no species of that name.
    public long? ApiSpeciesId { get; set; }

    public List<IucnCommonName> IucnCommonNames { get; set; } = new();
    public List<SiteSynonym> IucnSynonyms { get; set; } = new();
    public List<(string Name, string Source, bool IsPreferred)> EnglishNames { get; } = new();
    public List<SiteSynonym> ColSynonyms { get; } = new();
    public List<SiteSynonym> WikidataSynonyms { get; } = new();
    /// English names and synonyms from the Mammal Diversity Database and AmphibiaWeb: name, type, authority, source.
    public List<(string Name, string Type, string? Authority, string Source)> ChecklistNames { get; } = new();
    /// The scientific name and the synonyms in the taxobox of the taxon's English Wikipedia article.
    public List<SiteSynonym> WikipediaSynonyms { get; } = new();
    /// Common names in languages other than English from the Catalogue of Life, Wikidata and
    /// Wikipedia (SiteOtherLanguageNames).
    public List<SiteOtherName> OtherLanguageNames { get; } = new();
}

/// A synonym and its authority as its source gives it; Authority is null when the source gives none.
internal sealed record SiteSynonym(string Name, string? Authority = null);

internal sealed record IucnCommonName(string Name, string? Language, bool IsMain);

internal static class EpbcAppliesTo {
    public const string Taxon = "taxon";
    public const string Population = "population";
}

/// One SPRAT profile of a taxon. Status: the EPBC Act category code, null when the profile is not
/// listed. Population: for a profile named after the taxon with a population in brackets, the text
/// in the brackets ("combined populations of Qld, NSW and the ACT").
internal sealed record EpbcListing(long SpratTaxonId, string ListedName, string? Status, string AppliesTo, string? Population);

/// One status of a taxon in a list other than the IUCN Red List (other_status). System: an
/// OtherStatusSystems key. StatusCode: NatureServe's rounded rank. Population: the population or
/// area the listing applies to, null for the whole taxon. ListedOn: yyyy-MM-dd. Report: the
/// publication an NZTCS status comes from. Country, ListKey and Qualifier: other_status's columns of the
/// same names.
internal sealed record OtherStatus(string System, string Status, string? StatusCode, string? ListedName, string? Population,
    string Source, string SourceId, string? Url, string? ListedOn, string? Report = null, string? Country = null,
    string? ListKey = null, string? Qualifier = null, string? ListedUnder = null);

/// One row of other_status_list.
internal sealed record OtherStatusList(string ListKey, string System, string? Country, string? Region, string Name, string? Title,
    int SortOrder, string? Publisher, string? Licence, string? LicenceUrl, string? Citation, string? Url, string? Version, string? Fetched);

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
    /// The number of mature individuals as IUCN publishes it ('1000-1200', '500000-999999,800000', 'U'); null when not given.
    public string? PopulationSize { get; set; }
    public string? CitationJson { get; set; }
    /// The credits as StoredCredits JSON (ids into credit_name); null when there is no payload or it has no credits.
    public string? CreditsJson { get; set; }
    /// Whether the cached payload's taxonomic notes have text; null when there is no payload.
    public bool? HasTaxonomicNotes { get; set; }

    /// The IUCN API answered 404 when this assessment was requested (failed_requests).
    public bool ApiNotFound { get; set; }
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
    public ExtraSpecies.ExtraSpeciesStats? ExtraSpecies;
    /// What linking IUCN's summary tables found; null when the build did not read them.
    public SiteSummaryTablesResult? SummaryTables;
    public readonly Dictionary<string, int> TaxaByKind = new(StringComparer.Ordinal);
    public int ParentsByName;
    public int ParentsFromApi;
    public int ParentsMissing;
    public int TreeNodes;
    public int TreeColGroups;
    public int TreeRuleGroups;
    public int TreeTaxaUnderRuleOrder;
    public int TreeTaxaUnderRuleFamily;
    public int TreeTaxaWithUnassignedRank;
    public int TreeTaxaWithoutKingdom;
    public string? ColPlacementState;
    public int GroupCommonNames;
    public int GroupArticles;
    public int GroupColIds;
    public int GroupsWithColNames;
    public int GroupWikipediaArticles;
    public int GroupWikipediaArticlesWithoutRedirects;
    public int GroupsWithWikipediaNames;
    public int GroupWikipediaNames;
    public int GroupsGainingEnglishNameCandidate;
    public int ListArticleTitles;

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
    /// Payloads whose documentation.taxonomic_notes has text.
    public int PayloadsWithTaxonomicNotes;
    public int ColLadderNodes;
    public int ChecklistTaxa;
    public int ChecklistCommonNames;
    public int ChecklistSynonyms;
    public string? MddVersion;
    public string? AmphibiaWebVersion;
    /// The Reptile Database's version in the checklists store ("ChecklistBank 1008 (2026-06)"), for the About page's credit of its subspecies.
    public string? ReptileDbVersion;
    public int WikidataLadderNodes;
    public int TaxaWithAreas;
    public int TaxonAreaRows;
    public int Areas;
    public int AssessmentsWithCredits;
    public long CreditEntries;
    public int CreditNames;
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
    /// Common names in languages other than English read from the Catalogue of Life, from Wikidata
    /// (P1843, labels and aliases) and from Wikipedia sitelink titles, and what happened to them.
    public readonly OtherNameCounts ColOtherNames = new();
    public readonly OtherNameCounts WikidataOtherNames = new();
    public readonly OtherNameCounts WikipediaOtherNames = new();
    /// The subspecies and varieties of the species in the release (infraspecific_name, SiteSubspecies).
    public readonly SubspeciesCounts Subspecies = new();
    /// name rows written for common names in languages other than English (or none), by source.
    public readonly Dictionary<string, int> OtherLanguageNameRows = new(StringComparer.Ordinal);
    /// Those names after merging the sources' rows: one per taxon, language and name (case and
    /// Unicode normalisation ignored, SiteNameKey.CaseFold), as the species page shows them.
    public int OtherLanguageNamesMerged;
    public readonly HashSet<string> OtherLanguages = new(StringComparer.Ordinal);
    /// Common names in any language that CommonNameQuality found to be junk (left out of the name
    /// table), and that it repaired (stored repaired). Counted per taxon by SiteNameSet.
    public int CommonNamesJunk;
    public int CommonNamesRepaired;
    public int CommonNameEn;
    public int CommonNameEnUnusable;
    public int CommonNameEnFromRules;
    public int SynonymsBuiltFromFullName;
    /// Synonym items named by the taxa's Wikidata items (P1420), and how many of them the cache has a name for.
    public int WikidataSynonymItems;
    public int WikidataSynonymsNamed;
    /// Names from Wikipedia taxoboxes added as synonyms (before the ones equal to a scientific name or
    /// another source's are dropped), and pages matched to several taxa none of which has the taxobox's name.
    public int WikipediaTaxoboxSynonyms;
    public int WikipediaTaxoboxPagesShared;
    /// CoL synonyms given their authorship from the CoL database.
    public int ColSynonymAuthorities;
    /// Assessments the IUCN API answered 404 for.
    public int AssessmentsApiNotFound;

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
    /// Australian state and territory statuses written to other_status (from the SPRAT profiles above).
    public int StateStatuses;
    /// The status lists store: NatureServe records, and the taxa they were matched to by their
    /// scientific name, by one of NatureServe's synonyms, or by one of IUCN's synonyms.
    public int NatureServeRecords;
    public int NatureServeByName;
    public int NatureServeBySynonym;
    public int NatureServeByIucnSynonym;
    /// other_status rows from NatureServe: global ranks (unranked and not applicable left out), COSEWIC and SARA.
    public int NatureServeRanks;
    public int NatureServeNationalRanks;
    public int JnccDesignations;
    public int FranceRows;
    public int FranceByTaxrefLink;
    public int FranceByName;
    public int FranceSiteRows;
    public string? FranceFetched;
    public int JapanRows;
    public int JapanMatched;
    public int JapanSiteRows;
    public int JapanNames;
    public string? JapanFetched;
    public int RedListStatuses;
    public int RedListMatched;
    public int RedListRows;
    public int CitesTaxa;
    public int CitesMatched;
    public int CitesByHigherTaxon;
    public int CitesRows;
    public string? CitesFetched;
    public string? CitesCitation;
    public string? JnccFetched;
    public string? JnccFileDate;
    public string? JnccAttribution;
    public int JnccMatched;
    public int JnccRows;
    public int NatureServeSubnationalRanks;
    public int CosewicStatuses;
    public int SaraStatuses;
    /// ECOS listings, and those matched to a taxon.
    public int EcosListings;
    public int EcosMatched;
    /// Current NZTCS assessments with a scientific name and a status, and those matched to a taxon.
    public int NztcsAssessments;
    public int NztcsMatched;
    public string? NztcsFetched;
    /// Taxa with a Green Status assessment in the API cache, and those written (taxa on the site).
    public int GreenStatusRecords;
    public int GreenStatusTaxa;
    public string? GreenStatusFetched;
    /// Current SALVE assessments, and those matched to a taxon.
    public int SalveAssessments;
    public int SalveMatched;
    public string? SalveFetched;
    public string? NatureServeFetched;
    public string? EcosFetched;

    /// Taxa only in the API cache (not in the CSV export), by kind; and their API headers flagged
    /// latest, which are stored as earlier assessments.
    public readonly Dictionary<string, int> NotInReleaseByKind = new(StringComparer.Ordinal);
    public int NotInReleaseRecordsUnusable;
    public int NotInReleaseWithCurrentTaxon;
    /// Links from old ids to the one taxon in the release whose IUCN synonyms list their name, and
    /// old ids whose name two or more taxa in the release list (left unlinked).
    public int NotInReleaseSynonymLinks;
    public int NotInReleaseSynonymOfSeveral;
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
