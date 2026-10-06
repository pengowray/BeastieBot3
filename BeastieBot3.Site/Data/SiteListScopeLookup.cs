using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Data;

/// ListScope's reads over the site database. Group paths are kept, since the taxa of one list share
/// most of their groups.
/// statuses: for leaving out the species of the Catalogue of Life whose name is an IUCN taxon's
/// scientific name or synonym ("Acer hybridum", a synonym of Acer pseudoplatanus).
public sealed class SiteListScopeLookup(SiteQueries queries, SpeciesTableQueries tables, IStatusLookup statuses) : IListScopeLookup {
    private readonly Dictionary<int, IReadOnlyList<GroupRow>> _paths = [];

    public IReadOnlyList<GroupRow> PathOf(int nodeId) =>
        _paths.TryGetValue(nodeId, out var path) ? path : _paths[nodeId] = queries.GetGroupPath(nodeId);

    public IReadOnlyList<GroupCategoryCount> CountsOf(int nodeId) => queries.GetGroupCounts(nodeId);

    public IReadOnlyList<ListTaxonRow> TaxaIn(GroupRow group, IReadOnlyCollection<string> kinds) => queries.GetListTaxa(group, kinds);

    public IReadOnlyDictionary<long, TableTaxonExtra> ExtrasOf(GroupRow group) => tables.GetExtras(group);

    public IReadOnlyList<ExtraSpeciesRow> ExtraSpeciesIn(GroupRow group) {
        if (queries.GetExtraSpeciesCounts(group.NodeId) is not { } counts) {
            return [];
        }
        var likely = queries.GetExtraOverlaps(group, counts.LastNodeId).Where(o => o.Likely && o.TaxonId is not null)
            .Select(o => o.ExtraId).ToHashSet();
        // Only species the Catalogue of Life has: those only on Wikidata are mostly fossil species
        // (123 of the 161 extra species of six cat genera in October 2026). A name IUCN has for a
        // taxon, as its name or a synonym, is that taxon, which the comparison already covers.
        return [.. queries.GetExtraSpecies(group.NodeId, counts.LastNodeId).Where(e => e.InCol && !likely.Contains(e.ExtraId)
            && statuses.InReleaseTaxaWithName(e.ScientificName, StatusNameKind.Scientific).Count == 0
            && statuses.InReleaseTaxaWithName(e.ScientificName, StatusNameKind.Synonym).Count == 0)];
    }
}
