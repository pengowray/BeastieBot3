using BeastieBot3.Checklists;
using BeastieBot3.Shared.SiteData;

// English names and synonyms from the Mammal Diversity Database and AmphibiaWeb (`checklists import`)
// for the site's name table, with their own sources. An IUCN species of the source's class takes the
// names of the source's species of the same name, else of the species the source gives the IUCN name
// for (AmphibiaWeb's gaa_name), unless another IUCN species also leads there (the source lumps them).
// They do not change how the site chooses a taxon's English name (CommonNameChooser).
// SiteSubspecies.ReadChecklists matches species the same way (Match).

namespace BeastieBot3.SiteBuild;

internal static class SiteChecklistNames {
    private static readonly (string Source, string ClassName, string SiteSource)[] Sources = [
        ("mdd", "MAMMALIA", SiteNameSource.Mdd),
        ("amphibiaweb", "AMPHIBIA", SiteNameSource.AmphibiaWeb),
    ];

    public static void Read(string storePath, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats) {
        using var store = ChecklistStore.OpenReadOnly(storePath);
        if (store is null) {
            return;
        }
        var info = store.Sources().ToDictionary(s => s.Source, StringComparer.Ordinal);
        stats.MddVersion = info.GetValueOrDefault("mdd")?.Version;
        stats.AmphibiaWebVersion = info.GetValueOrDefault("amphibiaweb")?.Version;
        stats.ReptileDbVersion = info.GetValueOrDefault("reptiledb")?.Version;
        var matched = new HashSet<long>();
        foreach (var (source, className, siteSource) in Sources) {
            if (!info.ContainsKey(source)) {
                continue;
            }
            var names = store.Names(source).GroupBy(n => SiteNameKey.Fold(n.ScientificName))
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
            foreach (var (taxon, key) in Match(store, source, className, names.Keys.ToHashSet(StringComparer.Ordinal), taxa.Values)) {
                matched.Add(taxon.TaxonId);
                foreach (var n in names[key]) {
                    taxon.ChecklistNames.Add((n.Name, n.NameType, n.Authority, siteSource));
                    if (n.NameType == ChecklistNameTypes.Common) {
                        stats.ChecklistCommonNames++;
                    } else {
                        stats.ChecklistSynonyms++;
                    }
                }
            }
        }
        stats.ChecklistTaxa = matched.Count;
    }

    /// The IUCN species in the release of the class (className, "MAMMALIA") that take the data of a
    /// source's species, each with the key (SiteNameKey.Fold) of that species' name, which is one of
    /// sourceSpecies. A species takes the source's species of its own name when sourceSpecies has it;
    /// else the one species that the source's synonyms (checklist_synonym) lead its name to, unless
    /// another IUCN species also leads there (the source lumps them).
    public static List<(SiteTaxon Taxon, string Key)> Match(ChecklistStore store, string source, string className,
        IReadOnlySet<string> sourceSpecies, IEnumerable<SiteTaxon> taxa) {
        var iucnToSource = store.Synonyms(source)
            .Where(p => p.Value.Count == 1)
            .ToDictionary(p => SiteNameKey.Fold(p.Key), p => SiteNameKey.Fold(p.Value[0]), StringComparer.Ordinal);
        var species = taxa.Where(t => t.InRelease && t.Kind == SiteTaxonKind.Species && t.ClassName == className).ToList();
        var keyOf = species.ToDictionary(t => t, t => {
            var own = SiteNameKey.Fold(t.ScientificName);
            return sourceSpecies.Contains(own) ? own : iucnToSource.GetValueOrDefault(own);
        });
        var taxaPerKey = keyOf.Values.OfType<string>().GroupBy(k => k).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var matches = new List<(SiteTaxon, string)>();
        foreach (var taxon in species) {
            if (keyOf[taxon] is not { } key || !sourceSpecies.Contains(key)
                || (key != SiteNameKey.Fold(taxon.ScientificName) && taxaPerKey[key] > 1)) {
                continue;
            }
            matches.Add((taxon, key));
        }
        return matches;
    }
}
