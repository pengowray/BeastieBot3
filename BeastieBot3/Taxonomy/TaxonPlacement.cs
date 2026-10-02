using System;
using System.Collections.Generic;
using System.Linq;

// Where an IUCN taxon sits in Catalogue of Life's finer hierarchy. IUCN classifies only by kingdom,
// phylum, class, order, family and genus; CoL adds nodes between those ranks (suborder Serpentes
// between order Squamata and the snake families, infraclass Batoidea between class Chondrichthyes and
// the ray orders, subfamilies between a family and its genera). A placement lists those CoL nodes for
// each IUCN class/order, order/family and family/genus pair, decided once per release by majority vote
// over every species under that IUCN taxon, so a family lands under the same CoL node on every page.
// IUCN stays the backbone: CoL nodes are only inserted between IUCN ranks, never used to move a taxon.
// Built by TaxonPlacementBuilder, stored by TaxonPlacementStore, read by the list tree builder.

namespace BeastieBot3.Taxonomy;

/// <summary>
/// One Catalogue of Life node used as a grouping between two IUCN ranks.
/// </summary>
/// <param name="Name">The CoL name, in CoL's case (e.g. "Serpentes").</param>
/// <param name="ColRank">The CoL rank, lower case (e.g. "suborder", "infraclass", "subfamily", "unranked").</param>
/// <param name="ShowRank">True when the CoL rank lies strictly between the two IUCN ranks the node sits
/// between (a suborder between order and family), so a heading can say "Suborder Serpentes". False when
/// the node has the same rank as the IUCN taxon above it or no usable rank: CoL's order Cetacea inside
/// IUCN's order Artiodactyla is shown as "Cetacea", not "Order Cetacea" under "Order Artiodactyla".</param>
internal sealed record PlacementNode(string Name, string ColRank, bool ShowRank);

/// <summary>Which pair of IUCN ranks a placement path sits between.</summary>
internal enum PlacementSpan {
    ClassToOrder,
    OrderToFamily,
    FamilyToGenus,
}

/// <summary>
/// Looks up the CoL nodes between two IUCN ranks for a taxon. Arguments are IUCN values as stored
/// (higher ranks upper case, genus mixed case); lookups ignore case. Every method returns an empty list
/// when CoL adds nothing there or the taxon is unknown, never null. Paths run broad to narrow.
/// </summary>
internal interface ITaxonPlacement {
    IReadOnlyList<PlacementNode> BetweenClassAndOrder(string? kingdom, string? className, string? orderName);

    IReadOnlyList<PlacementNode> BetweenOrderAndFamily(string? kingdom, string? className, string? orderName, string? familyName);

    IReadOnlyList<PlacementNode> BetweenFamilyAndGenus(string? kingdom, string? familyName, string? genusName);
}

/// <summary>One stored placement path: the nodes between two IUCN ranks for one IUCN taxon key.</summary>
internal sealed record PlacementPath(PlacementSpan Span, string Key, IReadOnlyList<PlacementNode> Nodes);

/// <summary>
/// In-memory <see cref="ITaxonPlacement"/> over three dictionaries keyed by <see cref="KeyFor"/>.
/// </summary>
internal sealed class TaxonPlacementIndex : ITaxonPlacement {
    public static readonly TaxonPlacementIndex Empty = new(Array.Empty<PlacementPath>());

    private readonly Dictionary<string, IReadOnlyList<PlacementNode>> _classToOrder = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<PlacementNode>> _orderToFamily = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<PlacementNode>> _familyToGenus = new(StringComparer.Ordinal);

    public TaxonPlacementIndex(IEnumerable<PlacementPath> paths) {
        foreach (var path in paths) {
            if (path.Nodes.Count == 0) {
                continue;
            }
            MapFor(path.Span)[path.Key] = path.Nodes;
        }
    }

    public int Count => _classToOrder.Count + _orderToFamily.Count + _familyToGenus.Count;

    public IEnumerable<PlacementPath> Paths =>
        _classToOrder.Select(kv => new PlacementPath(PlacementSpan.ClassToOrder, kv.Key, kv.Value))
            .Concat(_orderToFamily.Select(kv => new PlacementPath(PlacementSpan.OrderToFamily, kv.Key, kv.Value)))
            .Concat(_familyToGenus.Select(kv => new PlacementPath(PlacementSpan.FamilyToGenus, kv.Key, kv.Value)));

    public IReadOnlyList<PlacementNode> BetweenClassAndOrder(string? kingdom, string? className, string? orderName) =>
        Lookup(_classToOrder, OrderKey(kingdom, className, orderName));

    public IReadOnlyList<PlacementNode> BetweenOrderAndFamily(string? kingdom, string? className, string? orderName, string? familyName) =>
        Lookup(_orderToFamily, FamilyKey(kingdom, className, orderName, familyName));

    public IReadOnlyList<PlacementNode> BetweenFamilyAndGenus(string? kingdom, string? familyName, string? genusName) =>
        Lookup(_familyToGenus, GenusKey(kingdom, familyName, genusName));

    /// <summary>Key of the IUCN order a class-to-order path belongs to.</summary>
    public static string OrderKey(string? kingdom, string? className, string? orderName) =>
        Join(kingdom, className, orderName);

    /// <summary>Key of the IUCN family an order-to-family path belongs to.</summary>
    public static string FamilyKey(string? kingdom, string? className, string? orderName, string? familyName) =>
        Join(kingdom, className, orderName, familyName);

    /// <summary>Key of the IUCN genus a family-to-genus path belongs to.</summary>
    public static string GenusKey(string? kingdom, string? familyName, string? genusName) =>
        Join(kingdom, familyName, genusName);

    public static string KeyFor(PlacementSpan span, string? kingdom, string? className, string? orderName, string? familyName, string? genusName) =>
        span switch {
            PlacementSpan.ClassToOrder => OrderKey(kingdom, className, orderName),
            PlacementSpan.OrderToFamily => FamilyKey(kingdom, className, orderName, familyName),
            PlacementSpan.FamilyToGenus => GenusKey(kingdom, familyName, genusName),
            _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
        };

    private Dictionary<string, IReadOnlyList<PlacementNode>> MapFor(PlacementSpan span) => span switch {
        PlacementSpan.ClassToOrder => _classToOrder,
        PlacementSpan.OrderToFamily => _orderToFamily,
        PlacementSpan.FamilyToGenus => _familyToGenus,
        _ => throw new ArgumentOutOfRangeException(nameof(span), span, null),
    };

    private static IReadOnlyList<PlacementNode> Lookup(Dictionary<string, IReadOnlyList<PlacementNode>> map, string key) =>
        map.TryGetValue(key, out var nodes) ? nodes : Array.Empty<PlacementNode>();

    private static string Join(params string?[] parts) =>
        string.Join("|", parts.Select(p => (p ?? string.Empty).Trim().ToUpperInvariant()));
}
