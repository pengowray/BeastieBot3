using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3;

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
    private readonly Dictionary<string, string> _capsRules;
    private readonly bool _allowAmbiguous;

    /// <summary>
    /// Creates a provider that owns and will dispose the store.
    /// </summary>
    public StoreBackedCommonNameProvider(string commonNameDbPath, string? wikipediaCachePath = null, bool allowAmbiguous = false) {
        _store = CommonNameStore.Open(commonNameDbPath);
        _ownsStore = true;
        _capsRules = _store.GetAllCapsRules();
        _allowAmbiguous = allowAmbiguous;

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
        _capsRules = _store.GetAllCapsRules();
        _allowAmbiguous = allowAmbiguous;
        _wikiCache = wikiCache;
        _ownsWikiCache = false;
    }

    /// <summary>
    /// Get the best common name for a species record.
    /// Returns null if no suitable name found.
    /// </summary>
    public string? GetBestCommonName(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }

        // Look up by taxon_id (which maps to IUCN sis_id as primary_source_id)
        var taxonId = FindTaxonId(record);
        if (!taxonId.HasValue) {
            return null;
        }

        var result = _store.GetBestCommonNameForTaxon(taxonId.Value, "en", _allowAmbiguous);
        if (result is null) {
            return null;
        }

        // Apply capitalization rules and return
        return ApplyCapitalization(result.DisplayName);
    }

    /// <summary>
    /// Get the best common name for a taxon by scientific name (supports higher taxa).
    /// </summary>
    public string? GetBestCommonNameByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        if (!taxonId.HasValue) {
            return null;
        }

        var result = _store.GetBestCommonNameForTaxon(taxonId.Value, "en", _allowAmbiguous);
        if (result is null) {
            return null;
        }

        return ApplyCapitalization(result.DisplayName);
    }

    /// <summary>
    /// Get the best common name for a taxon by its IUCN taxon ID.
    /// </summary>
    public string? GetBestCommonNameByTaxonId(long iucnTaxonId) {
        // The taxa table uses primary_source_id = taxon_id for IUCN taxa
        var taxonId = _store.FindTaxonBySourceId("iucn", iucnTaxonId.ToString());
        if (!taxonId.HasValue) {
            return null;
        }

        var result = _store.GetBestCommonNameForTaxon(taxonId.Value, "en", _allowAmbiguous);
        if (result is null) {
            return null;
        }

        return ApplyCapitalization(result.DisplayName);
    }

    /// <summary>
    /// Get the full result including source and ambiguity info.
    /// </summary>
    public CommonNameResult? GetBestCommonNameResult(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }

        var taxonId = FindTaxonId(record);
        if (!taxonId.HasValue) {
            return null;
        }

        var result = _store.GetBestCommonNameForTaxon(taxonId.Value, "en", _allowAmbiguous);
        if (result is null) {
            return null;
        }

        // Apply capitalization and return updated result
        var displayName = ApplyCapitalization(result.DisplayName);
        return result with { DisplayName = displayName };
    }

    /// <summary>
    /// Get the Wikipedia article title for a species record.
    /// Returns the article title from wikipedia_title or wikipedia_taxobox sources.
    /// </summary>
    public string? GetWikipediaArticleTitle(IucnSpeciesRecord record) {
        if (record is null) {
            return null;
        }

        var taxonId = FindTaxonId(record);
        if (!taxonId.HasValue) {
            return null;
        }

        return _store.GetWikipediaArticleTitle(taxonId.Value, "en");
    }

    /// <summary>
    /// Get the Wikipedia article title for a taxon by scientific name (supports higher taxa).
    /// </summary>
    public string? GetWikipediaArticleTitleByScientificName(string scientificName, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        var taxonId = FindTaxonIdByScientificName(scientificName, kingdom);
        if (!taxonId.HasValue) {
            return null;
        }

        return _store.GetWikipediaArticleTitle(taxonId.Value, "en");
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

    /// <summary>
    /// Batch lookup for multiple records.
    /// </summary>
    public Dictionary<long, string> GetBestCommonNames(IEnumerable<IucnSpeciesRecord> records) {
        var results = new Dictionary<long, string>();
        var taxonIdMap = new Dictionary<long, long>(); // iucnTaxonId -> store taxonId

        foreach (var record in records) {
            var taxonId = FindTaxonId(record);
            if (taxonId.HasValue) {
                taxonIdMap[record.TaxonId] = taxonId.Value;
            }
        }

        if (taxonIdMap.Count == 0) {
            return results;
        }

        var storeResults = _store.GetBestCommonNamesForTaxa(taxonIdMap.Values, "en", _allowAmbiguous);

        // Map back to IUCN taxon IDs
        foreach (var (iucnTaxonId, storeTaxonId) in taxonIdMap) {
            if (storeResults.TryGetValue(storeTaxonId, out var result)) {
                results[iucnTaxonId] = ApplyCapitalization(result.DisplayName);
            }
        }

        return results;
    }

    private long? FindTaxonId(IucnSpeciesRecord record) {
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

    private string ApplyCapitalization(string name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return name;
        }

        // Split into words and apply rules
        var words = name.Split(' ');
        for (int i = 0; i < words.Length; i++) {
            var word = words[i];
            var lower = word.ToLowerInvariant();
            
            if (_capsRules.TryGetValue(lower, out var correctForm)) {
                // Use the caps rule if we have one
                words[i] = correctForm;
            } else if (IsAllCaps(word) && word.Length > 1) {
                // If word is ALL CAPS (more than 1 letter) and not in caps rules, apply default casing:
                // - First word: Title Case
                // - Other words: lowercase
                words[i] = i == 0 ? ToTitleCase(word) : lower;
            }
            // Otherwise leave as-is (single letters or mixed case words are likely already correct)
        }

        // Ensure first letter is capitalized
        var result = string.Join(' ', words);
        if (result.Length > 0 && char.IsLower(result[0])) {
            result = char.ToUpperInvariant(result[0]) + result[1..];
        }

        return result;
    }

    /// <summary>
    /// Check if a word is entirely uppercase letters (ignoring non-letters).
    /// </summary>
    private static bool IsAllCaps(string word) {
        if (string.IsNullOrEmpty(word)) return false;
        
        bool hasLetter = false;
        foreach (var c in word) {
            if (char.IsLetter(c)) {
                hasLetter = true;
                if (!char.IsUpper(c)) return false;
            }
        }
        return hasLetter;
    }

    /// <summary>
    /// Convert to title case (first letter upper, rest lower).
    /// </summary>
    private static string ToTitleCase(string word) {
        if (string.IsNullOrEmpty(word)) return word;
        if (word.Length == 1) return word.ToUpperInvariant();
        return char.ToUpperInvariant(word[0]) + word[1..].ToLowerInvariant();
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
