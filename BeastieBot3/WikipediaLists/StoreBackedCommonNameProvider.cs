using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3;
using BeastieBot3.CommonNames;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;

// CommonNameStore-backed provider for Wikipedia list generation: finds a taxon's names in the
// store and has CommonNameChooser pick and capitalise the best one (source priority, ambiguous
// names skipped, caps.txt rules). Also gives Wikipedia article titles from the store and redirect
// targets from the Wikipedia cache.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Common name provider backed by the CommonNameStore.
/// Uses pre-aggregated common names with source priority and ambiguity detection.
/// </summary>
internal sealed class StoreBackedCommonNameProvider : IDisposable {
    private readonly CommonNameStore _store;
    private readonly bool _ownsStore;
    private readonly WikipediaCacheStore? _wikiCache;
    private readonly bool _ownsWikiCache;
    private readonly bool _allowAmbiguous;

    // Per-run memoization. Generation resolves the same taxon's id/name/article several times per
    // record (Style B/C sort + line formatting + parent-species link) and again for the same taxon
    // across multiple lists; caching makes each unique lookup happen once. The provider is created
    // once per generate-lists invocation, so this spans the whole run. Single-threaded — no locking.
    private readonly Dictionary<long, long?> _storeTaxonIdCache = new();
    private readonly Dictionary<long, string?> _commonNameCache = new();
    private readonly Dictionary<long, string?> _wikiArticleCache = new();
    private readonly Dictionary<string, string?> _articleByScientificCache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Creates a provider that owns and will dispose the store, which it opens read-only.
    /// </summary>
    public StoreBackedCommonNameProvider(string commonNameDbPath, string? wikipediaCachePath = null, bool allowAmbiguous = false) {
        _store = CommonNameStore.OpenReadOnly(commonNameDbPath);
        _ownsStore = true;
        _allowAmbiguous = allowAmbiguous;
        Chooser = CommonNameChooser.ForStore(_store, allowAmbiguous: allowAmbiguous);

        if (!string.IsNullOrWhiteSpace(wikipediaCachePath) && File.Exists(wikipediaCachePath)) {
            _wikiCache = WikipediaCacheStore.Open(wikipediaCachePath);
            _ownsWikiCache = true;
        } else {
            _wikiCache = null;
            _ownsWikiCache = false;
        }
    }

    /// <summary>
    /// Creates a provider using an existing store (caller retains ownership).
    /// </summary>
    public StoreBackedCommonNameProvider(CommonNameStore store, WikipediaCacheStore? wikiCache = null, bool allowAmbiguous = false) {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _ownsStore = false;
        _allowAmbiguous = allowAmbiguous;
        Chooser = CommonNameChooser.ForStore(_store, allowAmbiguous: allowAmbiguous);
        _wikiCache = wikiCache;
        _ownsWikiCache = false;
    }

    /// <summary>
    /// How many English common names the store gives to more than one taxon; each is skipped for
    /// every taxon but the one that keeps it (<see cref="AmbiguousNames"/>). 0 when the provider was
    /// created with allowAmbiguous. The store computes the verdicts on first use and caches them,
    /// so reading this before generation costs nothing extra.
    /// </summary>
    public int AmbiguousNameCount => _allowAmbiguous ? 0 : _store.GetAmbiguousNames("en").Count;

    /// <summary>
    /// The chooser this provider picks store names with; it has no rules-list.txt
    /// (<see cref="CommonNameChooser.WithRules"/> adds one).
    /// </summary>
    public CommonNameChooser Chooser { get; }

    /// <summary>
    /// Get the best common name for a species record.
    /// Returns null if no suitable name found.
    /// </summary>
    public string? GetBestCommonName(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }
        if (_commonNameCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }

        // Look up by taxon_id (which maps to IUCN sis_id as primary_source_id)
        var taxonId = FindTaxonId(record);
        var binomial = !string.IsNullOrWhiteSpace(record.GenusName) && !string.IsNullOrWhiteSpace(record.SpeciesName)
            ? $"{record.GenusName} {record.SpeciesName}"
            : record.ScientificNameTaxonomy;
        var resolved = taxonId.HasValue ? BestStoreName(taxonId.Value, binomial) : null;

        _commonNameCache[record.TaxonId] = resolved;
        return resolved;
    }

    /// <summary>
    /// Get the best common name for a taxon by scientific name (supports higher taxa).
    /// </summary>
    public string? GetBestCommonNameByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        return taxonId.HasValue ? BestStoreName(taxonId.Value, scientificName) : null;
    }

    // The store's best English name for a store taxon, repaired and capitalised; null when it has
    // none that is usable and not ambiguous for it. scientificName lets the chooser skip a name
    // that is the scientific name with a subgenus.
    private string? BestStoreName(long storeTaxonId, string? scientificName) =>
        Chooser.FromStore(storeTaxonId, CommonNameStore.ToCandidates(_store.GetCommonNamesForTaxon(storeTaxonId, "en")),
            scientificName)?.DisplayName;

    /// <summary>
    /// Get the Wikipedia article title for a species record: the page its wikipedia_title or
    /// wikipedia_taxobox name came from (<see cref="CommonNameStore.GetWikipediaArticleTitle"/>).
    /// </summary>
    public string? GetWikipediaArticleTitle(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }
        if (_wikiArticleCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }

        var taxonId = FindTaxonId(record);
        var resolved = taxonId.HasValue ? _store.GetWikipediaArticleTitle(taxonId.Value, "en") : null;
        _wikiArticleCache[record.TaxonId] = resolved;
        return resolved;
    }

    /// <summary>
    /// Get the Wikipedia article title for a taxon by scientific name (supports higher taxa).
    /// </summary>
    public string? GetWikipediaArticleTitleByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }
        var cacheKey = $"{scientificName}{kingdom}";
        if (_articleByScientificCache.TryGetValue(cacheKey, out var cached)) {
            return cached;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        var resolved = taxonId.HasValue ? _store.GetWikipediaArticleTitle(taxonId.Value, "en") : null;
        _articleByScientificCache[cacheKey] = resolved;
        return resolved;
    }

    /// <summary>
    /// Get the redirect target title for a scientific name using the Wikipedia cache.
    /// Useful for higher taxa where the scientific name redirects to a common-name article.
    /// </summary>
    public string? GetWikipediaRedirectTitleByScientificName(string scientificName) {
        if (_wikiCache is null || string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var normalized = WikipediaTitleHelper.Normalize(scientificName);
        if (string.IsNullOrWhiteSpace(normalized)) {
            return null;
        }

        var summary = _wikiCache.GetPageByNormalizedTitle(normalized);
        if (summary is null) {
            return null;
        }

        if (!summary.IsRedirect || string.IsNullOrWhiteSpace(summary.RedirectTarget)) {
            return null;
        }

        return summary.RedirectTarget;
    }

    private long? FindTaxonId(IucnSpeciesRecord record) {
        if (_storeTaxonIdCache.TryGetValue(record.TaxonId, out var cached)) {
            return cached;
        }
        var resolved = FindTaxonIdUncached(record);
        _storeTaxonIdCache[record.TaxonId] = resolved;
        return resolved;
    }

    private long? FindTaxonIdUncached(IucnSpeciesRecord record) {
        // Try by IUCN taxon_id first (most reliable)
        var taxonId = _store.FindTaxonBySourceId("iucn", record.TaxonId.ToString());
        if (taxonId.HasValue) {
            return taxonId;
        }

        // Fall back to scientific name lookup
        var scientificName = record.ScientificNameTaxonomy 
            ?? record.ScientificNameAssessments 
            ?? ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);
        
        if (!string.IsNullOrWhiteSpace(scientificName)) {
            return _store.FindTaxonByScientificName(scientificName);
        }

        return null;
    }

    private long? FindTaxonIdByScientificName(string scientificName, string? kingdom) {
        // Prefer kingdom-filtered lookup when provided
        var taxonId = _store.FindTaxonByScientificName(scientificName, kingdom);
        if (taxonId.HasValue) {
            return taxonId;
        }

        // Fall back to kingdom-agnostic lookup
        return _store.FindTaxonByScientificName(scientificName);
    }

    public void Dispose() {
        if (_ownsWikiCache) {
            _wikiCache?.Dispose();
        }
        if (_ownsStore) {
            _store.Dispose();
        }
    }
}
