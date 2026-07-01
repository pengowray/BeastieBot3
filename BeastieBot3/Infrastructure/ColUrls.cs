// Single place that builds public Catalogue of Life URLs. A name usage (accepted taxon or
// synonym) has a stable short id (e.g. "6DBT"); the CoL data portal renders it at
// https://www.catalogueoflife.org/data/taxon/{id}. Used by the audit crosscheck reports to link
// each matched IUCN name to its Catalogue of Life entry for easy cross-reference. The counterpart
// to IucnUrls.Species for the CoL side.

namespace BeastieBot3.Infrastructure;

internal static class ColUrls {
    public const string TaxonBase = "https://www.catalogueoflife.org/data/taxon";

    /// <summary>
    /// The Catalogue of Life data portal page for a name usage id, or null when there is no id.
    /// </summary>
    public static string? Taxon(string? id) =>
        string.IsNullOrWhiteSpace(id) ? null : $"{TaxonBase}/{id.Trim()}";
}
