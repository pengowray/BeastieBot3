// Links each taxon that is not in the release (an old IUCN id) to the taxa in the release that the
// site's pages show beside it (taxon_link, and taxon.current_taxon_id). Two kinds of link:
//
//   same-name     The taxon in the release with the same scientific name: one in the same kingdom
//                 first, then one of the same kind, then the lowest id. This is also current_taxon_id.
//   iucn-synonym  The taxon in the release whose IUCN synonyms (from its API record) include the old
//                 taxon's scientific name, exactly as written, when it is the only such taxon in the
//                 same kingdom, other than the same-name taxon. Both kingdoms must be known. Two or more
//                 such taxa (a name IUCN lists under several taxa) give no link.
//
// An old id can have both kinds, to two different taxa: when IUCN split a taxon, the old id's name
// stays with one part and is a synonym of the other (Platanista gangetica, old id 41758, and
// Platanista minor). In release 2026-1, 1,723 old ids have a same-name link and 1,298 a synonym
// link, 52 of them both; 21 old ids have a name that two or more taxa list as a synonym.
//
// Synonyms are compared with ordinal equality, so "ssp." and "subsp." spellings do not match; folding
// case and accents would add 6 links in release 2026-1.

namespace BeastieBot3.SiteBuild;

internal static class SiteLinkKind {
    public const string SameName = "same-name";
    public const string IucnSynonym = "iucn-synonym";
}

internal sealed record SiteTaxonLink(long TaxonId, long CurrentTaxonId, string Kind);

internal static class SiteTaxonLinks {
    /// Sets CurrentTaxonId on each taxon not in the release that has a same-name link, and returns
    /// every link, ordered by old id and then kind (same-name first). Call it while the taxa still
    /// have their IUCN synonyms (before the names are written).
    public static List<SiteTaxonLink> Find(IReadOnlyCollection<SiteTaxon> taxa, SiteBuildStats stats) {
        var byName = taxa.Where(t => t.InRelease)
            .GroupBy(t => t.ScientificName, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
        var bySynonym = new Dictionary<string, List<SiteTaxon>>(StringComparer.Ordinal);
        foreach (var taxon in taxa.Where(t => t.InRelease)) {
            foreach (var synonym in taxon.IucnSynonyms.Distinct(StringComparer.Ordinal)) {
                if (!bySynonym.TryGetValue(synonym, out var list)) {
                    bySynonym[synonym] = list = new List<SiteTaxon>();
                }
                list.Add(taxon);
            }
        }

        var links = new List<SiteTaxonLink>();
        foreach (var taxon in taxa.Where(t => !t.InRelease).OrderBy(t => t.TaxonId)) {
            if (byName.TryGetValue(taxon.ScientificName, out var sameName)) {
                taxon.CurrentTaxonId = sameName
                    .OrderBy(c => string.Equals(c.Kingdom, taxon.Kingdom, StringComparison.OrdinalIgnoreCase) ? 0 : 1)
                    .ThenBy(c => c.Kind == taxon.Kind ? 0 : 1)
                    .ThenBy(c => c.TaxonId)
                    .First().TaxonId;
                links.Add(new SiteTaxonLink(taxon.TaxonId, taxon.CurrentTaxonId.Value, SiteLinkKind.SameName));
                stats.NotInReleaseWithCurrentTaxon++;
            }

            if (string.IsNullOrWhiteSpace(taxon.Kingdom) || !bySynonym.TryGetValue(taxon.ScientificName, out var listers)) {
                continue;
            }
            var candidates = listers
                .Where(c => c.TaxonId != taxon.CurrentTaxonId
                    && !string.IsNullOrWhiteSpace(c.Kingdom)
                    && string.Equals(c.Kingdom, taxon.Kingdom, StringComparison.OrdinalIgnoreCase))
                .Select(c => c.TaxonId)
                .Distinct()
                .ToList();
            if (candidates.Count == 1) {
                links.Add(new SiteTaxonLink(taxon.TaxonId, candidates[0], SiteLinkKind.IucnSynonym));
                stats.NotInReleaseSynonymLinks++;
            } else if (candidates.Count > 1) {
                stats.NotInReleaseSynonymOfSeveral++;
            }
        }
        return links;
    }
}
