namespace BeastieBot3.Site.Data;

// Rows of the tree of groups (higher_taxon and the tables beside it in SiteDbSchema).

/// One group: an IUCN rank (kingdom to genus) or a Catalogue of Life group between two of them.
public sealed record GroupRow(
    int NodeId,
    int? ParentNodeId,
    int Depth,
    string Rank,
    string Name,
    string Source,
    bool ShowRank,
    string Kingdom,
    string? ColId,
    string? CommonNameEn,
    string? CommonNameSource,
    string? EnwikiTitle,
    int FirstPos,
    int LastPos,
    int SpeciesCount,
    int InfraCount,
    int SubpopulationCount,
    string? LinkQuery = null) {
    /// A Catalogue of Life group, not one of IUCN's ranks.
    public bool IsCol => Source == GroupSources.Col;
    /// An order or family IUCN gives as "NOT ASSIGNED", taken from the site's rules.
    public bool IsFromRule => Source == GroupSources.IucnRule;
    public bool IsGenus => Rank == "genus";
}

/// A rank found inside a group: how deep it first appears, and whether only Catalogue of Life groups have it.
public sealed record GroupRank(string Rank, int MinDepth, bool OnlyFromCol);

public static class GroupSources {
    public const string Iucn = "iucn";
    public const string IucnRule = "iucn-rule";
    public const string Col = "col";
}

/// How many taxa in a group have a category ({{IUCN status}} code) in their latest global assessment.
public sealed record GroupCategoryCount(string Category, int Species, int Infra, int Subpopulations);

/// One taxon for a list: what a Wikipedia list line needs, with its place in the tree.
public sealed record ListTaxonRow(
    long TaxonId,
    string ScientificName,
    string Kind,
    string? Kingdom,
    string? Genus,
    string? SpeciesEpithet,
    string? InfraRank,
    string? InfraName,
    string? SubpopulationName,
    string? CommonNameEn,
    string? ListArticleTitle,
    string? ListParentArticleTitle,
    long? ParentTaxonId,
    int NodeId,
    int TreePos,
    long AssessmentId,
    string Category,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild,
    int? YearPublished);
