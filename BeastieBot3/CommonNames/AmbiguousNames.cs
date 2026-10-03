using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3.CommonNames;

/// <summary>
/// The one ambiguity rule for common names, built by <see cref="CommonNameStore.GetAmbiguousNames"/>
/// and read by list generation (<see cref="CommonNameStore.ChooseBest"/>), `site build-db` and
/// `common-names report --report ambiguous`.
///
/// A shared name is a normalized name that two or more valid, non-fossil taxa with different
/// scientific names have. Store taxa with the same scientific name and kingdom count as one taxon:
/// the store keeps a taxon for an old IUCN id beside the one for its current id (Arthroleptella
/// bicolor is IUCN 58057 and 121376651), and both are given the same Wikipedia title. A taxon's
/// priority for a name is the best <see cref="CommonNameStore.GetSourcePriority"/> of the sources
/// it has the name from. One taxon keeps a shared name when every other taxon has the name at a
/// lower priority, or at the same priority and is one of its own subspecies, varieties or
/// subpopulations. A shared name is ambiguous for every other taxon that has it, and for all of
/// them when two unrelated taxa tie for the best priority.
///
/// Examples from 2026: Panthera leo keeps "Lion" (its Wikipedia title) over Panthera leo ssp. leo
/// (an IUCN name that is not IUCN's main one); Panthera tigris keeps "Tiger" over Plectropomus
/// oligacanthus (a Catalogue of Life name).
/// </summary>
internal sealed class AmbiguousNames {
    public static AmbiguousNames None { get; } = new(Array.Empty<string>(), new Dictionary<string, long[]>());

    // For each shared name, the store taxa that keep it (one scientific name; empty when no taxon
    // keeps it), in store id order.
    private readonly Dictionary<string, long[]> _keepersByName;

    private AmbiguousNames(IReadOnlyList<string> names, Dictionary<string, long[]> keepersByName) {
        Names = names;
        _keepersByName = keepersByName;
    }

    /// <summary>Every shared name, the names with the most taxa first.</summary>
    public IReadOnlyList<string> Names { get; }

    public int Count => Names.Count;

    /// <summary>Whether two or more taxa with different scientific names have the name.</summary>
    public bool IsShared(string normalizedName) => _keepersByName.ContainsKey(normalizedName);

    /// <summary>
    /// The taxon that keeps a shared name; null when the name is not shared or no taxon keeps it.
    /// When several store taxa have the keeper's scientific name (<see cref="Keeps"/> is true for
    /// each), the one with the lowest store id.
    /// </summary>
    public long? KeptBy(string normalizedName) =>
        _keepersByName.TryGetValue(normalizedName, out var keepers) && keepers.Length > 0 ? keepers[0] : null;

    /// <summary>Whether the name is shared and <paramref name="taxonId"/> is one of the taxa that keep it.</summary>
    public bool Keeps(long taxonId, string normalizedName) =>
        _keepersByName.TryGetValue(normalizedName, out var keepers) && Array.IndexOf(keepers, taxonId) >= 0;

    /// <summary>Whether the name is shared and <paramref name="taxonId"/> does not keep it.</summary>
    public bool IsAmbiguousFor(long taxonId, string normalizedName) =>
        _keepersByName.TryGetValue(normalizedName, out var keepers) && Array.IndexOf(keepers, taxonId) < 0;

    /// <summary>Applies the rule to every (name, taxon, source) row of the taxa that count.</summary>
    internal static AmbiguousNames Build(IEnumerable<NameHolding> holdings) {
        // For each name, one holder per scientific name and kingdom.
        var byName = new Dictionary<string, Dictionary<(string CanonicalName, string? Kingdom), Holder>>(StringComparer.OrdinalIgnoreCase);
        foreach (var holding in holdings) {
            if (!byName.TryGetValue(holding.NormalizedName, out var holders)) {
                byName[holding.NormalizedName] = holders = new Dictionary<(string, string?), Holder>();
            }
            var key = (holding.CanonicalName, holding.Kingdom);
            if (!holders.TryGetValue(key, out var holder)) {
                holders[key] = holder = new Holder(holding.CanonicalName);
            }
            holder.Add(holding.TaxonId, holding.Priority);
        }

        var shared = byName
            .Where(kv => kv.Value.Count > 1)
            .OrderByDescending(kv => kv.Value.Count)
            .ThenBy(kv => kv.Key, StringComparer.Ordinal)
            .ToList();
        var keepersByName = new Dictionary<string, long[]>(shared.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, holders) in shared) {
            keepersByName[name] = FindKeeper(holders.Values.ToList())?.TaxonIds.Order().ToArray() ?? Array.Empty<long>();
        }
        return new AmbiguousNames(shared.Select(kv => kv.Key).ToList(), keepersByName);
    }

    // The store taxa with one scientific name (and kingdom) that have a name, and their best
    // priority for it.
    private sealed class Holder(string canonicalName) {
        public string CanonicalName { get; } = canonicalName;
        public int Priority { get; private set; } = int.MaxValue;
        public List<long> TaxonIds { get; } = new();

        public void Add(long taxonId, int priority) {
            if (!TaxonIds.Contains(taxonId)) {
                TaxonIds.Add(taxonId);
            }
            Priority = Math.Min(Priority, priority);
        }
    }

    // At most one holder can keep a name: of two holders at the best priority, at most one is the
    // species of the other.
    private static Holder? FindKeeper(IReadOnlyList<Holder> holders) {
        var best = holders.Min(h => h.Priority);
        var atBest = holders.Where(h => h.Priority == best).ToList();
        if (atBest.Count == 1) {
            return atBest[0];
        }
        foreach (var candidate in atBest) {
            if (atBest.All(other => ReferenceEquals(other, candidate)
                                    || IsSpeciesOf(candidate.CanonicalName, other.CanonicalName))) {
                return candidate;
            }
        }
        return null;
    }

    /// <summary>
    /// Whether <paramref name="species"/> is a two-word species name and <paramref name="other"/>
    /// begins with it: a subspecies or variety ("panthera leo ssp. leo") or a subpopulation
    /// ("megaptera novaeangliae arabian sea subpopulation", which the store ranks as a species).
    /// Canonical names are lower case with single spaces.
    /// </summary>
    internal static bool IsSpeciesOf(string species, string other) =>
        species.Count(c => c == ' ') == 1
        && other.Length > species.Length + 1
        && other[species.Length] == ' '
        && other.StartsWith(species, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// One common name of one taxon from one source, as <see cref="AmbiguousNames.Build"/> reads it.
/// <paramref name="Kingdom"/> is the taxon's kingdom as the store has it, or null.
/// </summary>
internal readonly record struct NameHolding(string NormalizedName, long TaxonId, string CanonicalName, int Priority,
    string? Kingdom = null);
