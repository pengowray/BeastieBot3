using System.Globalization;

// Single place that builds public IUCN Red List URLs. The species/assessment page is
// https://www.iucnredlist.org/species/{taxonId}/{assessmentId}. Before this helper the
// literal was hand-written in ~7 report commands with an inconsistent first segment
// (some passed taxonId, some the SIS/root id, which are the same value). Standardize on
// taxonId. Where a cached assessment JSON already carries its own url, prefer that at the
// call site and fall back to Species(...).

namespace BeastieBot3.Infrastructure;

internal static class IucnUrls {
    public const string SpeciesBase = "https://www.iucnredlist.org/species";

    /// <summary>
    /// The canonical Red List page for an assessment. Returns null when there is no taxon id
    /// to build from. When the assessment id is missing the species-only form is returned;
    /// the site deep-links best with both ids present, so pass both whenever available.
    /// </summary>
    public static string? Species(long? taxonId, long? assessmentId) {
        if (taxonId is null) {
            return null;
        }

        var taxon = taxonId.Value.ToString(CultureInfo.InvariantCulture);
        return assessmentId is null
            ? $"{SpeciesBase}/{taxon}"
            : $"{SpeciesBase}/{taxon}/{assessmentId.Value.ToString(CultureInfo.InvariantCulture)}";
    }
}
