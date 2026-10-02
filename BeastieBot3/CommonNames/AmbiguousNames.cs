using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3.CommonNames;

/// <summary>
/// The one ambiguity rule for common names, built by <see cref="CommonNameStore.GetAmbiguousNames"/>
/// and read by list generation (<see cref="CommonNameStore.ChooseBest"/>), `site build-db` and
/// `common-names report --report ambiguous`.
///
/// A shared name is a normalized name that two or more valid, non-fossil taxa have. A taxon's
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
    public static AmbiguousNames None { get; } = new(Array.Empty<string>(), new Dictionary<string, long?>());

    private readonly Dictionary<string, long?> _keeperByName;

    private AmbiguousNames(IReadOnlyList<string> names, Dictionary<string, long?> keeperByName) {
        Names = names;
        _keeperByName = keeperByName;
    }

    /// <summary>Every shared name, the names with the most taxa first.</summary>
    public IReadOnlyList<string> Names { get; }

    public int Count => Names.Count;

    /// <summary>Whether two or more taxa have the name.</summary>
    public bool IsShared(string normalizedName) => _keeperByName.ContainsKey(normalizedName);

    /// <summary>The taxon that keeps a shared name; null when the name is not shared or no taxon keeps it.</summary>
    public long? KeptBy(string normalizedName) =>
        _keeperByName.TryGetValue(normalizedName, out var keeper) ? keeper : null;

    /// <summary>Whether the name is shared and <paramref name="taxonId"/> does not keep it.</summary>
    public bool IsAmbiguousFor(long taxonId, string normalizedName) =>
        _keeperByName.TryGetValue(normalizedName, out var keeper) && keeper != taxonId;

    /// <summary>Applies the rule to every (name, taxon, source) row of the taxa that count.</summary>
    internal static AmbiguousNames Build(IEnumerable<NameHolding> holdings) {
        var byName = new Dictionary<string, Dictionary<long, (string CanonicalName, int Priority)>>(StringComparer.OrdinalIgnoreCase);
        foreach (var holding in holdings) {
            if (!byName.TryGetValue(holding.NormalizedName, out var taxa)) {
                byName[holding.NormalizedName] = taxa = new Dictionary<long, (string, int)>();
            }
            if (!taxa.TryGetValue(holding.TaxonId, out var current) || holding.Priority < current.Priority) {
                taxa[holding.TaxonId] = (holding.CanonicalName, holding.Priority);
            }
        }

        var shared = byName
            .Where(kv => kv.Value.Count > 1)
            .OrderByDescending(kv => kv.Value.Count)
            .ThenBy(kv => kv.Key, StringComparer.Ordinal)
            .ToList();
        var keeperByName = new Dictionary<string, long?>(shared.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, taxa) in shared) {
            keeperByName[name] = FindKeeper(taxa);
        }
        return new AmbiguousNames(shared.Select(kv => kv.Key).ToList(), keeperByName);
    }

    // At most one taxon can keep a name: of two taxa at the best priority, at most one is the
    // species of the other.
    private static long? FindKeeper(Dictionary<long, (string CanonicalName, int Priority)> taxa) {
        var best = taxa.Values.Min(t => t.Priority);
        var atBest = taxa.Where(kv => kv.Value.Priority == best).ToList();
        if (atBest.Count == 1) {
            return atBest[0].Key;
        }
        foreach (var candidate in atBest) {
            if (atBest.All(other => other.Key == candidate.Key
                                    || IsSpeciesOf(candidate.Value.CanonicalName, other.Value.CanonicalName))) {
                return candidate.Key;
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

/// <summary>One common name of one taxon from one source, as <see cref="AmbiguousNames.Build"/> reads it.</summary>
internal readonly record struct NameHolding(string NormalizedName, long TaxonId, string CanonicalName, int Priority);
