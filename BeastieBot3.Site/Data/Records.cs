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
    long? SpratTaxonId,
    string? EpbcStatus,
    long? LatestGlobalAssessmentId);

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
    string? CitationJson) {
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
public sealed record TaxonSummary(
    long TaxonId,
    string ScientificName,
    string? SubpopulationName,
    string Kind,
    string? CommonNameEn,
    string? Category,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild);

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
