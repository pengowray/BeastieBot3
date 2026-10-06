using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Data;

/// ListScope's reads over the site database. Group paths are kept, since the taxa of one list share
/// most of their groups.
public sealed class SiteListScopeLookup(SiteQueries queries) : IListScopeLookup {
    private readonly Dictionary<int, IReadOnlyList<GroupRow>> _paths = [];

    public IReadOnlyList<GroupRow> PathOf(int nodeId) =>
        _paths.TryGetValue(nodeId, out var path) ? path : _paths[nodeId] = queries.GetGroupPath(nodeId);

    public IReadOnlyList<GroupCategoryCount> CountsOf(int nodeId) => queries.GetGroupCounts(nodeId);

    public IReadOnlyList<ListTaxonRow> TaxaIn(GroupRow group, IReadOnlyCollection<string> kinds) => queries.GetListTaxa(group, kinds);
}
