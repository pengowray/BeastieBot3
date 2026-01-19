using System;
using System.Collections.Generic;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Common name provider backed by the CommonNameStore.
/// Uses pre-aggregated common names with source priority and ambiguity detection.
/// </summary>
internal sealed class StoreBackedCommonNameProvider : IDisposable {
    private readonly CommonNameStore _store;
    private readonly bool _ownsStore;
    private readonly Dictionary<string, string> _capsRules;
    private readonly bool _allowAmbiguous;

    /// <summary>
    /// Creates a provider that owns and will dispose the store.
    /// </summary>
    public StoreBackedCommonNameProvider(string commonNameDbPath, bool allowAmbiguous = false) {
        _store = CommonNameStore.Open(commonNameDbPath);
        _ownsStore = true;
        _capsRules = _store.GetAllCapsRules();
        _allowAmbiguous = allowAmbiguous;
    }

    /// <summary>
    /// Creates a provider using an existing store (caller retains ownership).
    /// </summary>
    public StoreBackedCommonNameProvider(CommonNameStore store, bool allowAmbiguous = false) {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _ownsStore = false;
        _capsRules = _store.GetAllCapsRules();
        _allowAmbiguous = allowAmbiguous;
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

    private string ApplyCapitalization(string name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return name;
        }

        // Split into words and apply rules
        var words = name.Split(' ');
        for (int i = 0; i < words.Length; i++) {
            var lower = words[i].ToLowerInvariant();
            if (_capsRules.TryGetValue(lower, out var correctForm)) {
                words[i] = correctForm;
            }
        }

        // Ensure first letter is capitalized
        var result = string.Join(' ', words);
        if (result.Length > 0 && char.IsLower(result[0])) {
            result = char.ToUpperInvariant(result[0]) + result[1..];
        }

        return result;
    }

    public void Dispose() {
        if (_ownsStore) {
            _store.Dispose();
        }
    }
}
