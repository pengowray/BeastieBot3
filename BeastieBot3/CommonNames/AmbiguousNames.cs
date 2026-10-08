using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3.CommonNames;

/// <summary>
/// The one ambiguity rule for common names, built by <see cref="CommonNameStore.GetAmbiguousNames"/>
/// and read by list generation (<see cref="CommonNameChooser.ChooseBest"/>), `site build-db` and
/// `common-names report --report ambiguous`.
///
/// A shared name is a normalized name that two or more valid, non-fossil taxa of the same kingdom
/// with different scientific names have. Taxa of different kingdoms are never compared: "Chestnut"
/// is used for the moth Conistra vaccinii and for the tree Pochota fendleri, "Bluebonnet" for the
/// parrot Northiella haematogaster and for the plant Ageratum conyzoides, and when two plants and one
/// moth have a name, the rule below decides between the two plants and the moth uses the name.
/// Everything below applies within one kingdom. A taxon with no kingdom is compared with the taxa
/// of every kingdom, because it cannot be shown to be in another kingdom; it uses a shared name
/// only when it keeps it in each of them. Store taxa with the same scientific name and kingdom count as one taxon:
/// the store keeps a taxon for an old IUCN id beside the one for its current id (Arthroleptella
/// bicolor is IUCN 58057 and 121376651), and both are given the same Wikipedia title. A taxon's
/// priority for a name is the best <see cref="KeeperPriority"/> of the sources it has the name
/// from. When the only taxa with a shared name are a species and its own subspecies, varieties or
/// subpopulations, the order a taxon uses for its own names decides instead
/// (<see cref="CommonNameStore.GetSourcePriority"/>: title, taxobox, Wikidata label, IUCN's main
/// name ...), and the species keeps the name when it ties for the best. Otherwise one taxon keeps a
/// shared name when every other taxon has the name at a lower priority, or at the same priority and
/// is one of its own subspecies, varieties or subpopulations. When two
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
/// (an IUCN name that is not IUCN's main one); Chrysoritis pyramus keeps "Pyramus opal" (its
/// taxobox name) over its nominate subspecies (IUCN's main name); Panthera tigris keeps "Tiger" over Plectropomus
/// oligacanthus (a Catalogue of Life name); Lithobates sylvaticus keeps "Wood frog" (its Wikipedia
/// title) over Papurana daemeli (IUCN's main name); Quercus alba keeps "White oak" (IUCN's main
/// name and its taxobox name) over Grevillea baileyana (IUCN's main name only); Anisognathus
/// lunulatus keeps "Scarlet-bellied mountain tanager" (IUCN's main name) over Anisognathus
/// igniventris, whose article has that title and covers both species. Until 8 October 2026 taxa
/// of different kingdoms were compared too, and the moth kept "Chestnut" (IUCN's main name) over
/// the tree, which has it as one of IUCN's other names.
/// </summary>
internal sealed class AmbiguousNames {
    public static AmbiguousNames None { get; } = new(Array.Empty<SharedName>(),
        new Dictionary<string, Dictionary<long, bool>>(), new Dictionary<(string Name, string? Kingdom), long[]>());

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

    // For each shared name, every counted taxon that has it and whether it uses it: true for the
    // keepers of each kingdom where the name is shared and for a taxon that is the only one in its
    // kingdom with the name, false for the others.
    private readonly Dictionary<string, Dictionary<long, bool>> _usesByName;

    // For each name shared within a kingdom, the store taxa that keep it there (one scientific
    // name; empty when no taxon keeps it), in store id order.
    private readonly Dictionary<(string Name, string? Kingdom), long[]> _keepers;

    private AmbiguousNames(IReadOnlyList<SharedName> shared, Dictionary<string, Dictionary<long, bool>> usesByName,
        Dictionary<(string Name, string? Kingdom), long[]> keepers) {
        Shared = shared;
        Names = shared.Select(s => s.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        _usesByName = usesByName;
        _keepers = keepers;
    }

    /// <summary>
    /// Every name shared within a kingdom, once for each kingdom it is shared in, the entries with
    /// the most taxa first. <see cref="SharedName.Kingdom"/> is null for a name shared only by taxa
    /// with no kingdom.
    /// </summary>
    public IReadOnlyList<SharedName> Shared { get; }

    /// <summary>Every shared name once, the names with the most taxa first.</summary>
    public IReadOnlyList<string> Names { get; }

    /// <summary>The number of entries in <see cref="Shared"/>: a name shared within two kingdoms counts twice.</summary>
    public int Count => Shared.Count;

    /// <summary>Whether two or more taxa of one kingdom with different scientific names have the name.</summary>
    public bool IsShared(string normalizedName) => _usesByName.ContainsKey(normalizedName);

    /// <summary>
    /// The entries of <see cref="Shared"/> for one kingdom (compared ignoring case), or all of them
    /// when <paramref name="kingdom"/> is null or blank.
    /// </summary>
    public IReadOnlyList<SharedName> SharedIn(string? kingdom) =>
        NormalizeKingdom(kingdom) is { } k ? Shared.Where(s => s.Kingdom == k).ToList() : Shared;

    /// <summary>
    /// Whether a taxon of <paramref name="taxonKingdom"/> is compared in the group of the shared
    /// entry <paramref name="entry"/>: it is of that kingdom, or it has no kingdom.
    /// </summary>
    public static bool IsInGroupOf(SharedName entry, string? taxonKingdom) =>
        NormalizeKingdom(taxonKingdom) is not { } k || k == entry.Kingdom;

    /// <summary>
    /// The taxon that keeps a name shared within <paramref name="kingdom"/>; null when the name is
    /// not shared there or no taxon keeps it. When several store taxa have the keeper's scientific
    /// name (<see cref="Keeps"/> is true for each), the one with the lowest store id.
    /// </summary>
    public long? KeptBy(string normalizedName, string? kingdom) =>
        _keepers.TryGetValue((normalizedName, NormalizeKingdom(kingdom)), out var keepers) && keepers.Length > 0 ? keepers[0] : null;

    /// <summary>
    /// The taxon that keeps a name shared within one kingdom only; null when the name is not
    /// shared or no taxon keeps it. Throws when the name is shared within two or more kingdoms,
    /// because each of them has its own keeper (use <see cref="KeptBy(string, string?)"/>).
    /// </summary>
    public long? KeptBy(string normalizedName) {
        var entries = Shared.Where(s => string.Equals(s.Name, normalizedName, StringComparison.OrdinalIgnoreCase)).ToList();
        return entries.Count switch {
            0 => null,
            1 => KeptBy(normalizedName, entries[0].Kingdom),
            _ => throw new InvalidOperationException(
                $"'{normalizedName}' is shared within {entries.Count} kingdoms. Give the kingdom."),
        };
    }

    /// <summary>
    /// Whether the name is shared and <paramref name="taxonId"/> uses it: the taxon keeps it in its
    /// kingdom, or no other taxon of its kingdom has it.
    /// </summary>
    public bool Keeps(long taxonId, string normalizedName) =>
        _usesByName.TryGetValue(normalizedName, out var uses) && uses.TryGetValue(taxonId, out var keeps) && keeps;

    /// <summary>
    /// Whether the name is shared and <paramref name="taxonId"/> does not use it: another taxon of
    /// its kingdom keeps it, or two or more taxa of its kingdom tie for it. A taxon that the rule
    /// does not count (not valid, or a fossil) uses no shared name.
    /// </summary>
    public bool IsAmbiguousFor(long taxonId, string normalizedName) =>
        _usesByName.TryGetValue(normalizedName, out var uses) && !(uses.TryGetValue(taxonId, out var keeps) && keeps);

    /// <summary>A kingdom as the rule compares it: trimmed and upper case, null when blank.</summary>
    internal static string? NormalizeKingdom(string? kingdom) =>
        string.IsNullOrWhiteSpace(kingdom) ? null : kingdom.Trim().ToUpperInvariant();

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
            var kingdom = NormalizeKingdom(holding.Kingdom);
            var key = (holding.CanonicalName, kingdom);
            if (!holders.TryGetValue(key, out var holder)) {
                holders[key] = holder = new Holder(holding.CanonicalName, kingdom);
            }
            holder.Add(holding);
        }

        var shared = new List<(SharedName Entry, long[] Keepers)>();
        var usesByName = new Dictionary<string, Dictionary<long, bool>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, holders) in byName) {
            if (holders.Count < 2) {
                continue;
            }
            var groups = KingdomGroups(holders.Values.ToList());
            if (groups.All(g => g.Holders.Count < 2)) {
                continue;
            }
            // A taxon uses the name when it keeps it in every group it is in (a taxon with no
            // kingdom is in the group of each kingdom).
            var uses = new Dictionary<long, bool>();
            foreach (var (kingdom, group) in groups) {
                var keeper = group.Count > 1 ? FindKeeper(group, taxaByPage) : group[0];
                foreach (var holder in group) {
                    foreach (var taxonId in holder.TaxonIds) {
                        uses[taxonId] = uses.GetValueOrDefault(taxonId, true) && ReferenceEquals(holder, keeper);
                    }
                }
                if (group.Count > 1) {
                    shared.Add((new SharedName(name, kingdom, group.Count),
                        keeper?.TaxonIds.Order().ToArray() ?? Array.Empty<long>()));
                }
            }
            usesByName[name] = uses;
        }

        var ordered = shared
            .OrderByDescending(s => s.Entry.TaxonCount)
            .ThenBy(s => s.Entry.Name, StringComparer.Ordinal)
            .ThenBy(s => s.Entry.Kingdom, StringComparer.Ordinal)
            .ToList();
        var keepers = ordered.ToDictionary(s => (s.Entry.Name, s.Entry.Kingdom), s => s.Keepers);
        return new AmbiguousNames(ordered.Select(s => s.Entry).ToList(), usesByName, keepers);
    }

    // The holders of one name, by kingdom: taxa of different kingdoms are not compared, so a name
    // that one plant and one animal have is used for both. A taxon with no kingdom cannot be shown
    // to be in another kingdom than any other taxon, so it is compared with the taxa of every
    // kingdom, and the taxa with no kingdom are a group of their own only when no taxon with the
    // name has a kingdom.
    private static List<(string? Kingdom, List<Holder> Holders)> KingdomGroups(List<Holder> holders) {
        var withoutKingdom = holders.Where(h => h.Kingdom is null).ToList();
        var groups = holders
            .Where(h => h.Kingdom is not null)
            .GroupBy(h => h.Kingdom, StringComparer.Ordinal)
            .Select(g => (g.Key, g.Concat(withoutKingdom).ToList()))
            .ToList();
        if (groups.Count == 0) {
            groups.Add((null, withoutKingdom));
        }
        return groups;
    }

    // The store taxa with one scientific name (and kingdom) that have a name, their best priority
    // for it, their best tie-break among IUCN main names, whether they have it as IUCN's main name,
    // and the pages they have it from as a Wikipedia title.
    private sealed class Holder(string canonicalName, string? kingdom) {
        public string CanonicalName { get; } = canonicalName;
        public string? Kingdom { get; } = kingdom;
        public int Priority { get; private set; } = int.MaxValue;
        // The best priority by the order a taxon uses for its own names (CommonNameStore.GetSourcePriority).
        public int OwnNamePriority { get; private set; } = int.MaxValue;
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
            OwnNamePriority = Math.Min(OwnNamePriority, CommonNameStore.GetSourcePriority(holding.Source, holding.IsPreferred));
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
        // A name held only by a species and its own subspecies, varieties or subpopulations is
        // decided by the order a taxon uses for its own names (title, taxobox, Wikidata label, IUCN
        // main ...), with the species winning ties. The keeper order ranks IUCN's main name above a
        // taxobox name to settle names between unrelated taxa; between a species and its nominate
        // subspecies it would hand the species' taxobox name ("Pyramus opal") to the subspecies.
        if (SpeciesOfAll(holders.ToList()) is not null) {
            var ownBest = holders.Min(h => h.OwnNamePriority);
            var ownAtBest = holders.Where(h => h.OwnNamePriority == ownBest).ToList();
            return ownAtBest.Count == 1 ? ownAtBest[0] : SpeciesOfAll(ownAtBest);
        }
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

/// <summary>
/// A name shared within one kingdom (<see cref="AmbiguousNames.Shared"/>): the normalized name, the
/// kingdom in upper case (null when no taxon with the name has a kingdom), and how many taxa of
/// that kingdom have the name, counting taxa with the same scientific name once.
/// </summary>
internal readonly record struct SharedName(string Name, string? Kingdom, int TaxonCount);
