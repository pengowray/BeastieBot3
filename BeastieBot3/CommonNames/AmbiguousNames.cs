using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3.CommonNames;

/// <summary>
/// The one ambiguity rule for common names, built by <see cref="CommonNameStore.GetAmbiguousNames"/>
/// and read by list generation (<see cref="CommonNameChooser.ChooseBest"/>), `site build-db` and
/// `common-names report --report ambiguous`.
///
/// A shared name is a normalized name that two or more valid, non-fossil taxa with different
/// scientific names have. Store taxa with the same scientific name and kingdom count as one taxon:
/// the store keeps a taxon for an old IUCN id beside the one for its current id (Arthroleptella
/// bicolor is IUCN 58057 and 121376651), and both are given the same Wikipedia title. A taxon's
/// priority for a name is the best <see cref="KeeperPriority"/> of the sources it has the name
/// from. One taxon keeps a shared name when every other taxon has the name at a lower priority, or
/// at the same priority and is one of its own subspecies, varieties or subpopulations. When two
/// or more taxa have the name as IUCN's main English name and none has it from a Wikipedia title,
/// a taxon that also has it from a Wikipedia taxobox beats the others, and failing that, a taxon
/// that also has it as a Wikidata label (<see cref="IucnMainTieBreak"/>). A shared name is
/// ambiguous for every other taxon that has it, and for all of them when two unrelated taxa tie
/// for the best priority.
///
/// A Wikipedia title does not decide between the taxa that one article covers. When the taxa
/// with the best priority have the name as the title of the same page, the other taxa that have
/// the name and are matched to that page (a Wikipedia cross-reference, usually `other_taxon_page`:
/// a species IUCN has split while Wikipedia keeps one article) are compared with them. Of those
/// taxa, a species keeps the name over its own subspecies, varieties and subpopulations; failing
/// that, the one taxon with the name as IUCN's main English name keeps it (with the same
/// tie-break as above); failing that, the title decides as before.
///
/// Examples from 2026: Panthera leo keeps "Lion" (its Wikipedia title) over Panthera leo ssp. leo
/// (an IUCN name that is not IUCN's main one); Panthera tigris keeps "Tiger" over Plectropomus
/// oligacanthus (a Catalogue of Life name); Lithobates sylvaticus keeps "Wood frog" (its Wikipedia
/// title) over Papurana daemeli (IUCN's main name); Quercus alba keeps "White oak" (IUCN's main
/// name and its taxobox name) over Grevillea baileyana (IUCN's main name only); Anisognathus
/// lunulatus keeps "Scarlet-bellied mountain tanager" (IUCN's main name) over Anisognathus
/// igniventris, whose article has that title and covers both species.
/// </summary>
internal sealed class AmbiguousNames {
    public static AmbiguousNames None { get; } = new(Array.Empty<string>(), new Dictionary<string, long[]>());

    private const int WikipediaTitlePriority = 1;
    private const int IucnMainPriority = 2;

    /// <summary>
    /// A source's priority when deciding which taxon keeps a shared name; lower numbers win:
    /// a Wikipedia article title, IUCN's main English name, a Wikipedia taxobox name, a Wikidata
    /// label, IUCN's other English names, Wikidata's other names, the Catalogue of Life.
    /// This is not the order in which the chooser tries one taxon's own names
    /// (<see cref="CommonNameStore.GetSourcePriority"/>), where a taxobox name and a Wikidata label
    /// still come before IUCN's main name. Until October 2026 the keeper used that same order;
    /// moving IUCN's main name above the taxobox name also moved it above a Wikidata label, which
    /// ranked below the taxobox name before and still does.
    /// </summary>
    internal static int KeeperPriority(string source, bool isPreferred) => source.ToLowerInvariant() switch {
        "wikipedia_title" => WikipediaTitlePriority,
        "iucn" => isPreferred ? IucnMainPriority : 5,
        "wikipedia_taxobox" => 3,
        "wikidata_label" => 4,
        "wikidata" => 6,
        "col" => 7,
        _ => 99,
    };

    /// <summary>
    /// Between taxa that all have a name as IUCN's main English name, lower wins: a taxon that
    /// also has it from a Wikipedia taxobox, then one that also has it as a Wikidata label. This
    /// keeps the order the keeper used before October 2026 among those taxa, so a taxobox name
    /// still decides between two IUCN main names ("White oak": Quercus alba over Grevillea
    /// baileyana) instead of leaving the name to neither.
    /// </summary>
    internal static int IucnMainTieBreak(string source) => source.ToLowerInvariant() switch {
        "wikipedia_taxobox" => 1,
        "wikidata_label" => 2,
        _ => 3,
    };

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

    /// <summary>
    /// Applies the rule to every (name, taxon, source) row of the taxa that count.
    /// <paramref name="taxaByPage"/> gives, for a Wikipedia page title, the store taxa matched to
    /// that page (their Wikipedia cross-references); without it a title always decides.
    /// </summary>
    internal static AmbiguousNames Build(IEnumerable<NameHolding> holdings,
        IReadOnlyDictionary<string, HashSet<long>>? taxaByPage = null) {
        taxaByPage ??= new Dictionary<string, HashSet<long>>();
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
            holder.Add(holding);
        }

        var shared = byName
            .Where(kv => kv.Value.Count > 1)
            .OrderByDescending(kv => kv.Value.Count)
            .ThenBy(kv => kv.Key, StringComparer.Ordinal)
            .ToList();
        var keepersByName = new Dictionary<string, long[]>(shared.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, holders) in shared) {
            keepersByName[name] = FindKeeper(holders.Values.ToList(), taxaByPage)?.TaxonIds.Order().ToArray() ?? Array.Empty<long>();
        }
        return new AmbiguousNames(shared.Select(kv => kv.Key).ToList(), keepersByName);
    }

    // The store taxa with one scientific name (and kingdom) that have a name, their best priority
    // for it, their best tie-break among IUCN main names, whether they have it as IUCN's main name,
    // and the pages they have it from as a Wikipedia title.
    private sealed class Holder(string canonicalName) {
        public string CanonicalName { get; } = canonicalName;
        public int Priority { get; private set; } = int.MaxValue;
        public int TieBreak { get; private set; } = int.MaxValue;
        public bool HasIucnMain { get; private set; }
        public bool HasTitleWithoutPage { get; private set; }
        public HashSet<string> TitlePages { get; } = new(StringComparer.Ordinal);
        public List<long> TaxonIds { get; } = new();

        public void Add(NameHolding holding) {
            if (!TaxonIds.Contains(holding.TaxonId)) {
                TaxonIds.Add(holding.TaxonId);
            }
            var priority = KeeperPriority(holding.Source, holding.IsPreferred);
            Priority = Math.Min(Priority, priority);
            TieBreak = Math.Min(TieBreak, IucnMainTieBreak(holding.Source));
            HasIucnMain |= priority == IucnMainPriority;
            if (priority == WikipediaTitlePriority) {
                if (string.IsNullOrEmpty(holding.TitlePage)) {
                    HasTitleWithoutPage = true;
                } else {
                    TitlePages.Add(holding.TitlePage);
                }
            }
        }
    }

    // At most one holder can keep a name: of two holders at the best priority (and tie-break), at
    // most one is the species of the other.
    private static Holder? FindKeeper(IReadOnlyList<Holder> holders, IReadOnlyDictionary<string, HashSet<long>> taxaByPage) {
        var best = holders.Min(h => h.Priority);
        var atBest = holders.Where(h => h.Priority == best).ToList();
        if (best == WikipediaTitlePriority && TaxaOfOneArticle(atBest, holders, taxaByPage) is { } article) {
            if ((SpeciesOfAll(article) ?? ByIucnMainName(article)) is { } keeper) {
                return keeper;
            }
        }
        if (best == IucnMainPriority && atBest.Count > 1) {
            var tieBreak = atBest.Min(h => h.TieBreak);
            atBest = atBest.Where(h => h.TieBreak == tieBreak).ToList();
        }
        return atBest.Count == 1 ? atBest[0] : SpeciesOfAll(atBest);
    }

    // The holders that one Wikipedia article covers: the title holders, when all of them have the
    // name as the title of the same page, and every other holder matched to that page. Null when
    // the title holders' pages differ or are not known, or the article covers only one holder.
    private static List<Holder>? TaxaOfOneArticle(List<Holder> titleHolders, IReadOnlyList<Holder> holders,
        IReadOnlyDictionary<string, HashSet<long>> taxaByPage) {
        if (titleHolders.Any(h => h.HasTitleWithoutPage)) {
            return null;
        }
        var pages = titleHolders.SelectMany(h => h.TitlePages).Distinct(StringComparer.Ordinal).ToList();
        if (pages.Count != 1) {
            return null;
        }
        var linked = taxaByPage.GetValueOrDefault(pages[0]);
        var article = titleHolders
            .Concat(holders.Where(h => !titleHolders.Contains(h) && linked is not null && h.TaxonIds.Any(linked.Contains)))
            .ToList();
        return article.Count > 1 ? article : null;
    }

    // The one holder with the name as IUCN's main name, after the tie-break; null when none or
    // several.
    private static Holder? ByIucnMainName(List<Holder> holders) {
        var main = holders.Where(h => h.HasIucnMain).ToList();
        if (main.Count == 0) {
            return null;
        }
        var tieBreak = main.Min(h => h.TieBreak);
        var best = main.Where(h => h.TieBreak == tieBreak).ToList();
        return best.Count == 1 ? best[0] : null;
    }

    // The holder that is the species of every other holder; null when there is none.
    private static Holder? SpeciesOfAll(List<Holder> holders) {
        foreach (var candidate in holders) {
            if (holders.All(other => ReferenceEquals(other, candidate)
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
/// <paramref name="Source"/> and <paramref name="IsPreferred"/> are the store's; <paramref name="Kingdom"/>
/// is the taxon's kingdom as the store has it, or null. <paramref name="TitlePage"/> is the page a
/// <c>wikipedia_title</c> name came from (its source_identifier), or null.
/// </summary>
internal readonly record struct NameHolding(string NormalizedName, long TaxonId, string CanonicalName, string Source,
    bool IsPreferred, string? Kingdom = null, string? TitlePage = null);
