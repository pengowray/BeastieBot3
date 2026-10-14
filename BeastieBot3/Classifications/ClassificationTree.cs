// Turns a source's accepted taxa (each with its parent) and synonyms into what the classifications
// store keeps: every accepted species and the groups above it (classification_node), and the names
// that lead to each species (classification_name). Taxa below species, and groups with no species
// under them, are left out.
//
// Some sources link a species only to its genus or family and name the higher groups in columns of
// the species' row (Plants of the World Online: kingdom, phylum, class, order). AddColumnGroups puts
// those groups above the highest group the parents reach, with ids '<rank>:<name>', when that group
// has no parent of its own.

namespace BeastieBot3.Classifications;

internal sealed class ClassificationTree {
    private readonly Dictionary<string, (string? Parent, string Rank, string Name)> _taxa = new(StringComparer.Ordinal);
    private readonly List<(string Name, string AcceptedId)> _synonyms = [];
    private readonly Dictionary<string, IReadOnlyList<(string Rank, string Name)>> _columnGroups = new(StringComparer.Ordinal);

    // The most steps up from a species: deeper chains are loops in the data.
    private const int MaxDepth = 80;

    public int TaxonCount => _taxa.Count;

    /// An accepted taxon. Rank is lower-cased; an empty parent or one of "0" means none.
    public void AddTaxon(string id, string? parentId, string? rank, string name) {
        var parent = string.IsNullOrWhiteSpace(parentId) || parentId == "0" || parentId == id ? null : parentId;
        _taxa.TryAdd(id, (parent, NormaliseRank(rank), name.Trim()));
    }

    public void AddSynonym(string name, string acceptedId) => _synonyms.Add((name.Trim(), acceptedId));

    /// The groups a species' row names in columns, highest first.
    public void AddColumnGroups(string speciesId, IReadOnlyList<(string Rank, string Name)> groups) => _columnGroups.TryAdd(speciesId, groups);

    /// Lower case, without a bracketed second name (WoRMS's "Phylum (Division)" is "phylum").
    public static string NormaliseRank(string? rank) {
        var r = rank?.Trim().ToLowerInvariant() ?? "";
        if (r.IndexOf(" (", StringComparison.Ordinal) is var bracket and > 0) {
            r = r[..bracket];
        }
        return r is "" or "unranked" or "no rank" ? "unranked" : r;
    }

    public (List<ClassificationNode> Nodes, List<ClassificationName> Names) Build() {
        var nodes = new Dictionary<string, ClassificationNode>(StringComparer.Ordinal);
        var names = new List<ClassificationName>();
        foreach (var (id, taxon) in _taxa) {
            if (taxon.Rank != "species") {
                continue;
            }
            names.Add(new ClassificationName(taxon.Name, id, false));
            var top = id;
            for (var (at, depth) = ((string?)id, 0); at is not null && depth < MaxDepth && _taxa.TryGetValue(at, out var t); depth++) {
                if (!nodes.TryAdd(at, new ClassificationNode(at, t.Parent is not null && _taxa.ContainsKey(t.Parent) ? t.Parent : null, t.Rank, t.Name))) {
                    top = null; // the rest of the chain is there already
                    break;
                }
                top = at;
                at = t.Parent;
            }
            if (top is not null && nodes[top].ParentId is null && _columnGroups.TryGetValue(id, out var groups)) {
                AddAbove(nodes, top, groups);
            }
        }
        foreach (var (name, acceptedId) in _synonyms) {
            if (nodes.TryGetValue(acceptedId, out var node) && node.Rank == "species" && name.Length > 0) {
                names.Add(new ClassificationName(name, acceptedId, true));
            }
        }
        return ([.. nodes.Values], names);
    }

    // The column groups above `top` whose rank is above top's rank, highest first, each the parent of
    // the next. Nothing goes above a top whose rank is not one of the column ranks (WoRMS's root,
    // "Biota", is a superdomain): the archive's own tree reaches its root there.
    private static void AddAbove(Dictionary<string, ClassificationNode> nodes, string top, IReadOnlyList<(string Rank, string Name)> groups) {
        var topRank = Array.IndexOf(ColumnRanks, nodes[top].Rank);
        if (topRank <= 0) {
            return;
        }
        var above = groups.Where(g => g.Name.Length > 0 && Array.IndexOf(ColumnRanks, g.Rank) is var i && i >= 0 && i < topRank).ToList();
        string? parent = null;
        foreach (var (rank, name) in above) {
            var id = $"{rank}:{name}";
            nodes.TryAdd(id, new ClassificationNode(id, parent, rank, name));
            parent = id;
        }
        if (parent is not null) {
            nodes[top] = nodes[top] with { ParentId = parent };
        }
    }

    /// The ranks a row's columns can name, highest first.
    public static readonly string[] ColumnRanks = ["kingdom", "phylum", "class", "subclass", "order", "family", "genus"];
}
