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
    /// The CoL database itself; only its file name is read, for the release when there is no placement file.
    public string? ColDatabase { get; init; }
    public string? SpratDatabase { get; init; }
    /// GBIF's copy of the IUCN checklist (a Darwin Core Archive zip).
    public string? GbifChecklist { get; init; }
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
    public string? ColId { get; set; }
    public long? SpratTaxonId { get; set; }
    public string? EpbcStatus { get; set; }
    public long? LatestGlobalAssessmentId { get; set; }

    /// The API's taxon record lists the species of an infraspecific taxon (species_taxa); used for
    /// the parent when the CSV has no species of that name.
    public long? ApiSpeciesId { get; set; }

    public List<IucnCommonName> IucnCommonNames { get; set; } = new();
    public List<string> IucnSynonyms { get; set; } = new();
    public List<(string Name, string Source, bool IsPreferred)> EnglishNames { get; } = new();
    public List<string> ColSynonyms { get; } = new();
}

internal sealed record IucnCommonName(string Name, string? Language, bool IsMain);

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

    public readonly Dictionary<string, int> NamesByType = new(StringComparer.Ordinal);
    public int CommonNamesEnglish;
    public int CommonNameEn;
    public int SynonymsBuiltFromFullName;

    public int EnwikiTitles;
    public int QidsFromP627;
    public int QidsFromNameMatch;
    public int QidTieBreaks;
    public int ColIdsFromPlacement;
    public int ColIdsFromCrossReference;
    public int SpratMatched;
    public int EpbcStatuses;

    public string? GbifVersion;
    public string? GbifPublished;
    public string? ColRelease;
    public string? SpratReport;
    public readonly List<string> MissingSources = new();
    public readonly List<string> Warnings = new();

    public long FileBytes;
    public readonly List<(string Phase, TimeSpan Elapsed)> Phases = new();

    public void Count<TKey>(Dictionary<TKey, int> counts, TKey key) where TKey : notnull =>
        counts[key] = counts.GetValueOrDefault(key) + 1;
}
