namespace BeastieBot3.Site.Data;

// Rows of the site database (BeastieBot3.Shared.SiteData.SiteDbSchema) as the pages use them.

public sealed record TaxonRow(
    long TaxonId,
    string ScientificName,
    string Kind,
    string? Kingdom,
    string? Phylum,
    string? ClassName,
    string? OrderName,
    string? Family,
    string? Genus,
    string? SubpopulationName,
    string? Authority,
    long? ParentTaxonId,
    string? CommonNameEn,
    string? EnwikiTitle,
    string? WikidataQid,
    string? ColId,
    long? LatestGlobalAssessmentId,
    bool InRelease = true,
    long? CurrentTaxonId = null,
    string? WikidataQidSource = null,
    string? WikidataP141 = null,
    string? WikidataItemDownloaded = null,
    bool WikidataP627Deprecated = false,
    string? WikidataOtherItems = null,
    int? NodeId = null,
    string? SpeciesEpithet = null) {
    /// The taxon's item states this taxon's IUCN taxon id (P627), rather than being matched by name.
    public bool WikidataItemStatesTaxonId => WikidataQidSource == "p627";
}

public sealed record AssessmentRow(
    long AssessmentId,
    long TaxonId,
    string Scope,
    bool IsLatest,
    string Category,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild,
    string? Criteria,
    string? CriteriaVersion,
    int? YearPublished,
    string? AssessmentDate,
    string? PopulationTrend,
    string? CitationJson,
    long? ReplacedByAssessmentId = null,
    string? WikidataItemQid = null,
    string? WikidataItemProperties = null,
    string? WikidataItemTitles = null,
    string? WikidataItemLabelEn = null,
    long? WikidataItemAssessmentId = null,
    string? PopulationSize = null,
    bool ApiNotFound = false) {
    public bool IsGlobal => string.Equals(Scope.Trim(), "Global", StringComparison.OrdinalIgnoreCase);

    /// IUCN published the assessment with no geographic scope. It is not global, and is listed
    /// with the regional assessments.
    public bool HasNoScope => Scope.Trim().Length == 0;
}

public sealed record NameRow(
    long NameId,
    string Name,
    string NameType,
    string? Language,
    string Source,
    bool IsPreferred,
    string? Authority = null);

/// A taxon with its category, as listed in search results, name lookups and the children of a taxon.
/// InRelease: false for a taxon that is not in the release (no current assessment).
public sealed record TaxonSummary(
    long TaxonId,
    string ScientificName,
    string? SubpopulationName,
    string Kind,
    string? CommonNameEn,
    string? Category,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild,
    bool InRelease = true);

/// A taxon in the species / subspecies table of a taxon page, with the criteria, year and id of its
/// latest global assessment (all null when it has none).
public sealed record RelatedTaxonRow(TaxonSummary Taxon, string? Criteria, int? YearPublished, long? LatestGlobalAssessmentId);

/// A taxon linked to the page's taxon in taxon_link. Kind: TaxonLinkKinds.SameName or IucnSynonym.
public sealed record TaxonLinkRow(TaxonRow Taxon, string Kind) {
    public bool IsSynonym => Kind == TaxonLinkKinds.IucnSynonym;
}

public static class TaxonLinkKinds {
    /// The two taxa have the same scientific name.
    public const string SameName = "same-name";
    /// IUCN lists the old taxon's scientific name as a synonym of the taxon in the release.
    public const string IucnSynonym = "iucn-synonym";
}

/// One SPRAT profile of a taxon (epbc_listing). Status: the EPBC Act category code, null when the
/// profile is not listed. Population: for a profile of one population, the population's name.
public sealed record EpbcListingRow(long SpratTaxonId, string ListedName, string? Status, string AppliesTo, string? Population) {
    public bool IsPopulation => AppliesTo == "population";
}

/// One search result: the taxon and the name that matched best.
/// IsStrongExactMatch: an exact match on the taxon's scientific name, a synonym, its English name
/// for display (common_name_en) or the title of its English Wikipedia article. An exact match on any
/// other common name (a Catalogue of Life vernacular, say) is exact but not strong.
public sealed record SearchHit(
    TaxonSummary Taxon,
    string MatchedName,
    string MatchedNameType,
    string? MatchedLanguage,
    bool IsExactMatch,
    bool IsStrongExactMatch = false);
/// A taxon found by an IUCN id in the search text. AssessmentId is set when the id was an
/// assessment's; then Scope and YearPublished are that assessment's, and IsDefault says whether it
/// is the one the taxon's page shows first (its latest global assessment).
public sealed record IdHit(TaxonSummary Taxon, long? AssessmentId, string? Scope, int? YearPublished, bool IsDefault) {
    public bool IsAssessment => AssessmentId is not null;
}

public sealed record SearchResult(IReadOnlyList<SearchHit> Hits, long TotalTaxa) {
    public static readonly SearchResult Empty = new([], 0);
}

public static class NameTypes {
    public const string Scientific = "scientific";
    public const string Common = "common";
    public const string Synonym = "synonym";
}

public static class TaxonKinds {
    public const string Species = "species";
    public const string Subspecies = "subspecies";
    public const string Variety = "variety";
    public const string Subpopulation = "subpopulation";
}

/// The IUCN status in the taxobox of the taxon's English Wikipedia article (enwiki_taxobox_status).
public sealed record EnwikiTaxoboxStatusRow(string? Status, string? StatusSystem, long? RefAssessmentId, long? RevisionId, string Downloaded);
