using BeastieBot3.Shared.SiteData;
using BeastieBot3.Taxonomy;

// Links each taxon that is not in the release (an old IUCN id) to the taxa in the release that the
// site's pages show beside it (taxon_link, and taxon.current_taxon_id), and links two kinds of pairs
// of taxa that are both in the release. The links from old ids:
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
//
// The links between two taxa in the release:
//
//   working-name      A taxon whose name ends in "_new" ("Balaenoptera edeni_new") and the one taxon
//                     of the same kingdom and kind named without it ("Balaenoptera edeni"). IUCN made
//                     these records for national Red List assessments (the UAE National Red List
//                     Workshops of 2018 and 2019, Greece in 2023, South Africa in 2013) and published
//                     them with no geographic scope. Four in release 2026-1, three with a taxon to link.
//   provisional-name  A taxon with a provisional name whose quoted tag is a would-be epithet
//                     ("Notogomphus sp. nov. 'gorilla'", ProvisionalNames.Read) and the one taxon of the
//                     same kingdom named with that epithet ("Notogomphus gorilla"), which may be the same
//                     species described since. Not linked when IUCN's synonyms of the provisional taxon
//                     name a described species and the built name is not among them. One in 2026-1.

namespace BeastieBot3.SiteBuild;

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
            foreach (var synonym in taxon.IucnSynonyms.Select(s => s.Name).Distinct(StringComparer.Ordinal)) {
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
                links.Add(new SiteTaxonLink(taxon.TaxonId, taxon.CurrentTaxonId.Value, TaxonLinkKinds.SameName));
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
                links.Add(new SiteTaxonLink(taxon.TaxonId, candidates[0], TaxonLinkKinds.IucnSynonym));
                stats.NotInReleaseSynonymLinks++;
            } else if (candidates.Count > 1) {
                stats.NotInReleaseSynonymOfSeveral++;
            }
        }
        links.AddRange(FindInRelease(taxa, stats));
        return links;
    }

    /// The working-name and provisional-name links between taxa in the release, ordered by the
    /// first taxon's id.
    internal static List<SiteTaxonLink> FindInRelease(IReadOnlyCollection<SiteTaxon> taxa, SiteBuildStats stats) {
        var inRelease = taxa.Where(t => t.InRelease && !string.IsNullOrWhiteSpace(t.Kingdom)).ToList();
        var byName = inRelease
            .GroupBy(t => (Kingdom(t), t.Kind, Name: t.ScientificName))
            .ToDictionary(g => g.Key, g => g.Select(t => t.TaxonId).ToList());
        // Infraspecific taxa also by their name without the rank marker, which is how
        // ProvisionalNames.Read builds a trinomial ("Heideella andreae boulanouari").
        var byBareName = inRelease
            .Where(t => !ProvisionalNames.IsProvisional(t.ScientificName))
            .GroupBy(t => (Kingdom(t), BareName(t)))
            .Where(g => g.Key.Item2 is not null)
            .ToDictionary(g => g.Key, g => g.Select(t => t.TaxonId).ToList());

        var links = new List<SiteTaxonLink>();
        foreach (var taxon in inRelease.OrderBy(t => t.TaxonId)) {
            if (WorkingNameBase(taxon.ScientificName) is { } baseName) {
                if (byName.TryGetValue((Kingdom(taxon), taxon.Kind, baseName), out var named) && named.Count == 1) {
                    links.Add(new SiteTaxonLink(taxon.TaxonId, named[0], TaxonLinkKinds.WorkingName));
                    stats.WorkingNameLinks++;
                }
                continue;
            }
            var infraspecific = taxon.Kind != SiteTaxonKind.Species;
            var provisional = ProvisionalNames.Read(taxon.ScientificName, taxon.Genus, taxon.SpeciesEpithet, infraspecific);
            if (provisional.CandidateName is not { } candidate) {
                continue;
            }
            var described = taxon.IucnSynonyms.Select(s => s.Name).Where(n => !ProvisionalNames.IsProvisional(n)).ToList();
            if (described.Count > 0 && !described.Contains(candidate, StringComparer.Ordinal)) {
                continue;
            }
            if (byBareName.TryGetValue((Kingdom(taxon), candidate), out var found) && found.Count == 1 && found[0] != taxon.TaxonId) {
                links.Add(new SiteTaxonLink(taxon.TaxonId, found[0], TaxonLinkKinds.ProvisionalName));
                stats.ProvisionalNameLinks++;
            }
        }
        return links;
    }

    /// The name before "_new" ("Balaenoptera edeni" for "Balaenoptera edeni_new"); null for any
    /// other name.
    internal static string? WorkingNameBase(string scientificName) =>
        scientificName.EndsWith("_new", StringComparison.Ordinal) && scientificName.Length > 4
            ? scientificName[..^4]
            : null;

    private static string Kingdom(SiteTaxon taxon) => taxon.Kingdom!.Trim().ToUpperInvariant();

    // "Genus epithet" for a species, "Genus epithet infraname" for a subspecies or variety; null for
    // a subpopulation or a taxon without the parts.
    private static string? BareName(SiteTaxon taxon) => taxon.Kind switch {
        SiteTaxonKind.Species => taxon.ScientificName,
        SiteTaxonKind.Subspecies or SiteTaxonKind.Variety
            when taxon.Genus is { } g && taxon.SpeciesEpithet is { } e && taxon.InfraName is { } i => $"{g} {e} {i}",
        _ => null,
    };
}
