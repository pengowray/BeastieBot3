using BeastieBot3.Classifications;
using BeastieBot3.Shared.SiteData;

// The classifications of the classifications store (`classifications import`): AviList and the IOC
// World Bird List for birds, ITIS, Plants of the World Online for plants, and WoRMS. Each IUCN species
// in the release that is in the source's scope is matched to one of the source's species:
//   1. by id: AviList's BirdLife id (the IUCN taxon id), or the id the taxon's Wikidata item gives
//      for the source (ITIS TSN P815, POWO P5037, WoRMS AphiaID P850);
//   2. else by its own name: the one species of the source with that name;
//   3. else by a synonym: the one species the source's synonyms lead the name to, unless another IUCN
//      species also leads there (the source lumps them).
// A source with a kingdom in its classification must put the species in IUCN's kingdom, so a name
// shared by an animal and a plant is not matched across kingdoms. The species' node and every node
// above it are written with the source's ids (taxon_ladder links the IUCN taxon to the species'
// node); a bird list's highest group gets class Aves above it.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLadders {
    /// Scope: the IUCN species the source covers. IdProperty: the Wikidata property whose value is
    /// the source's id, and the prefix that value has that the store's ids do not.
    private sealed record ClassificationSpec(string Source, Func<SiteTaxon, bool> Scope, string? TopClass, string? IdProperty = null,
        string IdPrefix = "", bool IucnLinks = false);

    private static readonly ClassificationSpec[] ClassificationSpecs = [
        new(ClassificationSources.Ioc, t => t.ClassName == "AVES", "Aves"),
        new(ClassificationSources.AviList, t => t.ClassName == "AVES", "Aves", IucnLinks: true),
        new(ClassificationSources.Powo, t => t.Kingdom == "PLANTAE", null, "P5037", "urn:lsid:ipni.org:names:"),
        new(ClassificationSources.Worms, _ => true, null, "P850"),
        new(ClassificationSources.Itis, _ => true, null, "P815"),
    ];

    public static (List<LadderNode> Nodes, List<TaxonLadderStart> Starts, List<string> Summary) ReadClassifications(string storePath,
        IEnumerable<SiteTaxon> taxa, IReadOnlyList<SiteExternalId> externalIds, CancellationToken ct) {
        var nodes = new List<LadderNode>();
        var starts = new List<TaxonLadderStart>();
        var summary = new List<string>();
        using var store = ClassificationStore.OpenReadOnly(storePath);
        if (store is null) {
            return (nodes, starts, summary);
        }
        var imported = store.Sources().Select(s => s.Source).ToHashSet(StringComparer.Ordinal);
        var species = taxa.Where(t => t.InRelease && t.Kind == SiteTaxonKind.Species).ToList();
        foreach (var spec in ClassificationSpecs) {
            ct.ThrowIfCancellationRequested();
            if (!imported.Contains(spec.Source)) {
                continue;
            }
            var matches = MatchClassification(store, spec, species.Where(spec.Scope), externalIds, ct);
            var added = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (taxon, nodeId, _) in matches) {
                AddChain(store, spec, nodeId, nodes, added);
                starts.Add(new TaxonLadderStart(taxon.TaxonId, spec.Source, nodeId));
            }
            summary.Add($"{spec.Source} {matches.Count:N0} taxa ({string.Join(", ", matches.GroupBy(m => m.Method).OrderBy(g => g.Key).Select(g => $"{g.Count():N0} by {g.Key}"))})");
        }
        return (nodes, starts, summary);
    }

    private static List<(SiteTaxon Taxon, string NodeId, string Method)> MatchClassification(ClassificationStore store, ClassificationSpec spec,
        IEnumerable<SiteTaxon> taxa, IReadOnlyList<SiteExternalId> externalIds, CancellationToken ct) {
        var kingdoms = new Dictionary<string, string?>(StringComparer.Ordinal);
        // The kingdom the source puts a node in, or null when its classification names none.
        string? KingdomOf(string nodeId) {
            var path = new List<string>();
            string? found = null;
            for (var (at, depth) = ((string?)nodeId, 0); at is not null && depth < MaxDepth; depth++) {
                if (kingdoms.TryGetValue(at, out var known)) {
                    found = known;
                    break;
                }
                path.Add(at);
                if (store.Node(spec.Source, at) is not { } node) {
                    break;
                }
                if (node.Rank == "kingdom") {
                    found = node.Name;
                    break;
                }
                at = node.ParentId;
            }
            foreach (var p in path) {
                kingdoms[p] = found;
            }
            return found;
        }
        bool InKingdom(SiteTaxon taxon, string nodeId) =>
            KingdomOf(nodeId) is not { } kingdom || taxon.Kingdom is null || string.Equals(kingdom, taxon.Kingdom, StringComparison.OrdinalIgnoreCase);
        bool IsSpecies(string nodeId) => store.Node(spec.Source, nodeId) is { Rank: "species" };

        var iucnLinks = spec.IucnLinks ? store.Links(spec.Source, AviListClassification.IucnScheme) : null;
        var idsByTaxon = spec.IdProperty is { } property
            ? externalIds.Where(e => e.Property == property).GroupBy(e => e.TaxonId)
                .ToDictionary(g => g.Key, g => g.Select(e => e.Value.StartsWith(spec.IdPrefix, StringComparison.Ordinal) ? e.Value[spec.IdPrefix.Length..] : e.Value).ToList())
            : [];

        var candidates = new List<(SiteTaxon Taxon, string NodeId, string Method)>();
        foreach (var taxon in taxa) {
            ct.ThrowIfCancellationRequested();
            List<string> byId = [.. (iucnLinks?.GetValueOrDefault(taxon.TaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture)) ?? [])
                .Concat(idsByTaxon.GetValueOrDefault(taxon.TaxonId) ?? [])
                .Distinct(StringComparer.Ordinal).Where(id => IsSpecies(id) && InKingdom(taxon, id))];
            if (byId.Count == 1) {
                candidates.Add((taxon, byId[0], "id"));
                continue;
            }
            var key = SiteNameKey.Fold(taxon.ScientificName);
            var own = store.NodesNamed(spec.Source, key, synonym: false).Where(id => InKingdom(taxon, id)).Distinct(StringComparer.Ordinal).ToList();
            if (own.Count == 1) {
                candidates.Add((taxon, own[0], "name"));
                continue;
            }
            if (own.Count > 1) {
                continue;
            }
            var viaSynonym = store.NodesNamed(spec.Source, key, synonym: true).Where(id => InKingdom(taxon, id)).Distinct(StringComparer.Ordinal).ToList();
            if (viaSynonym.Count == 1) {
                candidates.Add((taxon, viaSynonym[0], "synonym"));
            }
        }
        // A species that two IUCN species lead to by name or synonym is a lump: only one matched by
        // its own name keeps it.
        var claims = candidates.Where(c => c.Method != "id").GroupBy(c => c.NodeId).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        return [.. candidates.Where(c => c.Method != "synonym" || claims[c.NodeId] == 1)];
    }

    // The species' node and the nodes above it, each once.
    private static void AddChain(ClassificationStore store, ClassificationSpec spec, string nodeId, List<LadderNode> nodes, HashSet<string> added) {
        for (var (at, depth) = ((string?)nodeId, 0); at is not null && depth < MaxDepth && added.Add(at); depth++) {
            if (store.Node(spec.Source, at) is not { } node) {
                break;
            }
            var parent = node.ParentId;
            if (parent is null && spec.TopClass is { } topClass && node.Rank != "class") {
                parent = "class:" + topClass;
                if (added.Add(parent)) {
                    nodes.Add(new LadderNode(spec.Source, parent, null, "class", topClass));
                }
            }
            nodes.Add(new LadderNode(spec.Source, at, parent, node.Rank == "unranked" ? null : node.Rank, node.Name));
            at = node.ParentId;
        }
    }
}
