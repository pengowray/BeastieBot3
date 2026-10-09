using BeastieBot3.Checklists;
using BeastieBot3.Shared.SiteData;

// The subspecies of the Mammal Diversity Database (the subspecies column of its species file) and the
// Reptile Database (accepted subspecies in its ColDP export on ChecklistBank), from the checklists
// store (`checklists import`). Each IUCN species in the release of the source's class takes the
// subspecies of the source's species that SiteChecklistNames.Match gives it: the species of its own
// name, else the one species the source's synonyms lead its name to, unless another IUCN species also
// leads there. A row's source_id is the source's record of the species, whose page lists its
// subspecies: the MDD id ("1000002") or the query of the Reptile Database's species page
// ("genus=Tachyglossus&species=aculeatus"). MDD's fossil subspecies are left out, as the Wikidata
// reader leaves out fossil taxa; its recently extinct and Holocene subspecies are kept.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteSubspecies {
    public const string Mdd = SiteNameSource.Mdd;
    public const string ReptileDb = "reptiledb";

    private static readonly (string Source, string ClassName)[] Checklists = [
        (Mdd, "MAMMALIA"),
        (ReptileDb, "REPTILIA"),
    ];

    public static List<SiteInfraspecificName> ReadChecklists(string storePath, IEnumerable<SiteTaxon> taxa, SubspeciesCounts counts,
        CancellationToken ct) {
        var rows = new List<SiteInfraspecificName>();
        using var store = ChecklistStore.OpenReadOnly(storePath);
        if (store is null) {
            return rows;
        }
        var taxonList = taxa as IReadOnlyCollection<SiteTaxon> ?? taxa.ToList();
        foreach (var (source, className) in Checklists) {
            ct.ThrowIfCancellationRequested();
            var sourceCounts = source == Mdd ? counts.Mdd : counts.ReptileDb;
            var subspecies = store.Infraspecific(source)
                .GroupBy(i => SiteNameKey.Fold(i.SpeciesName))
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
            if (subspecies.Count == 0) {
                continue;
            }
            var recordIds = store.Species(source)
                .GroupBy(s => SiteNameKey.Fold(s.ScientificName))
                .ToDictionary(g => g.Key, g => g.First().RecordId, StringComparer.Ordinal);
            sourceCounts.SourceSpecies = subspecies.Count;
            var sourceSpecies = recordIds.Keys.Concat(subspecies.Keys).ToHashSet(StringComparer.Ordinal);
            foreach (var (taxon, key) in SiteChecklistNames.Match(store, source, className, sourceSpecies, taxonList)) {
                if (!subspecies.TryGetValue(key, out var list)) {
                    continue;
                }
                sourceCounts.SourceSpeciesMatched++;
                var recordId = recordIds.GetValueOrDefault(key);
                foreach (var s in list) {
                    if (s.Note == MddSubspecies.Fossil) {
                        sourceCounts.LeftOutFossil++;
                        continue;
                    }
                    var name = s.Name.Trim();
                    if (recordId is null || InfraspecificNames.Split(name) is null) {
                        sourceCounts.Unreadable++;
                        continue;
                    }
                    rows.Add(new SiteInfraspecificName(taxon.TaxonId, source, recordId, s.Rank, name, SiteBuildRules.NullIfBlank(s.Authority)));
                    sourceCounts.Rows++;
                    sourceCounts.Species.Add(taxon.TaxonId);
                }
            }
        }
        return rows;
    }
}
