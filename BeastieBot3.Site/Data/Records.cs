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
    string? WikidataOtherItems = null) {
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
    long? WikidataItemAssessmentId = null) {
    public bool IsGlobal => string.Equals(Scope.Trim(), "Global", StringComparison.OrdinalIgnoreCase);
}

public sealed record NameRow(
    long NameId,
    string Name,
    string NameType,
    string? Language,
    string Source,
    bool IsPreferred);

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
public sealed record SearchHit(
    TaxonSummary Taxon,
    string MatchedName,
    string MatchedNameType,
    string? MatchedLanguage,
    bool IsExactMatch);

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
