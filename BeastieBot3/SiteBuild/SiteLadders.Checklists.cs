using BeastieBot3.Checklists;
using BeastieBot3.Shared.SiteData;

// The classification of the Mammal Diversity Database, the Reptile Database and AmphibiaWeb, from the
// checklists store (checklist_rank, `checklists import`), for the IUCN species of the source's class
// that SiteChecklistNames.Match matches to one of the source's species. The species' node has the
// source's id of its record as its id (the MDD id, the query of the Reptile Database's species page,
// AmphibiaWeb's species number) and the name the source gives it; taxon_ladder links the IUCN taxon to
// it. A group's node is "<rank>:<name>", or "<rank>:<name>#2" when another species puts a group of
// that rank and name under another parent. MDD and AmphibiaWeb list only their own class and give no
// class column, so that class is the top node above their groups.

namespace BeastieBot3.SiteBuild;

internal sealed record TaxonLadderStart(long TaxonId, string Source, string NodeId);

internal static partial class SiteLadders {
    public const string Mdd = "mdd";
    public const string ReptileDb = "reptiledb";
    public const string AmphibiaWeb = "amphibiaweb";

    private static readonly (string Source, string ClassName, string? ScopeClass)[] Checklists = [
        (Mdd, "MAMMALIA", "Mammalia"),
        (ReptileDb, "REPTILIA", null),
        (AmphibiaWeb, "AMPHIBIA", "Amphibia"),
    ];

    public static (List<LadderNode> Nodes, List<TaxonLadderStart> Starts) ReadChecklists(string storePath, IEnumerable<SiteTaxon> taxa,
        CancellationToken ct) {
        var nodes = new List<LadderNode>();
        var starts = new List<TaxonLadderStart>();
        using var store = ChecklistStore.OpenReadOnly(storePath);
        if (store is null) {
            return (nodes, starts);
        }
        var taxonList = taxa as IReadOnlyCollection<SiteTaxon> ?? taxa.ToList();
        foreach (var (source, className, scopeClass) in Checklists) {
            ct.ThrowIfCancellationRequested();
            var ranks = store.Ranks(source);
            if (ranks.Count == 0) {
                continue;
            }
            var species = store.Species(source).Where(s => s.RecordId is not null && ranks.ContainsKey(s.ScientificName))
                .GroupBy(s => SiteNameKey.Fold(s.ScientificName))
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
            var parents = new Dictionary<string, string?>(StringComparer.Ordinal);
            string GroupId(string? rank, string name, string? parent) {
                var baseId = $"{rank ?? "unranked"}:{name}";
                for (var i = 1; ; i++) {
                    var id = i == 1 ? baseId : $"{baseId}#{i}";
                    if (!parents.TryGetValue(id, out var known)) {
                        parents[id] = parent;
                        nodes.Add(new LadderNode(source, id, parent, rank, name));
                        return id;
                    }
                    if (known == parent) {
                        return id;
                    }
                }
            }
            foreach (var (taxon, key) in SiteChecklistNames.Match(store, source, className, species.Keys.ToHashSet(StringComparer.Ordinal), taxonList)) {
                var record = species[key];
                var groups = ranks[record.ScientificName];
                string? parent = null;
                if (scopeClass is not null && !groups.Any(g => g.Rank == "class")) {
                    parent = GroupId("class", scopeClass, null);
                }
                foreach (var (rank, name) in groups) {
                    parent = GroupId(rank == "unranked" ? null : rank, name, parent);
                }
                if (!parents.ContainsKey(record.RecordId!)) {
                    parents[record.RecordId!] = parent;
                    nodes.Add(new LadderNode(source, record.RecordId!, parent, "species", record.ScientificName));
                }
                starts.Add(new TaxonLadderStart(taxon.TaxonId, source, record.RecordId!));
            }
        }
        return (nodes, starts);
    }
}
