using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

// Canonical order of Catalogue of Life rank strings, broad to narrow, so code can ask whether one
// CoL node sits above or below another ("is this node between order and family?"). CoL ranks are
// free-text strings such as "suborder", "section zoology" or "infraspecific name"; each known one
// gets an ordinal, a bigger number meaning a narrower rank. Ranks that say nothing about position
// ("unranked", "clade", "other", blank) get no ordinal. Used by TaxonPlacementBuilder and
// ColLineageMatcher.

namespace BeastieBot3.Taxonomy;

internal static class ColRankOrder {
    // Broad to narrow. Names on one line share an ordinal (aliases).
    private static readonly string[][] Ladder = {
        new[] { "superdomain" },
        new[] { "domain" },
        new[] { "realm" },
        new[] { "subrealm" },
        new[] { "superkingdom" },
        new[] { "kingdom" },
        new[] { "subkingdom" },
        new[] { "infrakingdom" },
        new[] { "superphylum", "superdivision" },
        new[] { "phylum", "division" },
        new[] { "subphylum", "subdivision" },
        new[] { "infraphylum", "infradivision" },
        new[] { "parvphylum" },
        new[] { "gigaclass" },
        new[] { "megaclass" },
        new[] { "superclass" },
        new[] { "class" },
        new[] { "subclass" },
        new[] { "infraclass" },
        new[] { "subterclass" },
        new[] { "parvclass" },
        new[] { "megacohort" },
        new[] { "supercohort" },
        new[] { "cohort" },
        new[] { "subcohort" },
        new[] { "infracohort" },
        new[] { "magnorder" },
        new[] { "superorder" },
        new[] { "grandorder" },
        new[] { "mirorder" },
        new[] { "order" },
        new[] { "suborder" },
        new[] { "infraorder" },
        new[] { "parvorder" },
        new[] { "nanorder" },
        new[] { "section zoology" },
        new[] { "subsection zoology" },
        new[] { "series zoology" },
        new[] { "superfamily" },
        new[] { "epifamily" },
        new[] { "family" },
        new[] { "subfamily" },
        new[] { "infrafamily" },
        new[] { "supertribe" },
        new[] { "tribe" },
        new[] { "subtribe" },
        new[] { "infratribe" },
        new[] { "genus" },
        new[] { "subgenus" },
        new[] { "infragenus" },
        new[] { "infrageneric name" },
        new[] { "section botany" },
        new[] { "subsection botany" },
        new[] { "series botany" },
        new[] { "subseries botany" },
        new[] { "superspecies" },
        new[] { "species aggregate" },
        new[] { "species" },
        new[] { "subspecies", "infraspecific name" },
        new[] { "proles" },
        new[] { "natio" },
        new[] { "variety" },
        new[] { "subvariety" },
        new[] { "form", "forma" },
        new[] { "subform" },
        new[] { "forma specialis" },
        new[] { "morph" },
        new[] { "lusus" },
        new[] { "aberration" },
        new[] { "mutatio" },
        new[] { "infrasubspecific name" },
        new[] { "cultivar group" },
        new[] { "cultivar" },
        new[] { "grex" },
        new[] { "strain" },
    };

    private static readonly Dictionary<string, int> Ordinals = BuildOrdinals();

    // Raw rank string to ordinal, so the hot path (CoL ranks are already lower case) does no
    // string work. CoL has about 60 distinct rank strings, so this stays small.
    private static readonly ConcurrentDictionary<string, int?> Seen = new(StringComparer.Ordinal);

    public static readonly int Kingdom = Required("kingdom");
    public static readonly int Phylum = Required("phylum");
    public static readonly int Class = Required("class");
    public static readonly int Order = Required("order");
    public static readonly int Family = Required("family");
    public static readonly int Genus = Required("genus");
    public static readonly int Species = Required("species");

    /// <summary>
    /// The ordinal of a CoL rank string, bigger meaning narrower. Case, surrounding spaces and
    /// underscores are ignored. Returns null for blank, "unranked", "clade", "other" and any rank
    /// not in the ladder.
    /// </summary>
    public static int? Of(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return null;
        }
        if (Ordinals.TryGetValue(rank, out var direct)) {
            return direct;
        }
        return Seen.GetOrAdd(rank, r => Ordinals.TryGetValue(Normalize(r), out var ordinal) ? ordinal : null);
    }

    /// <summary>True for "species" and every rank below it (subspecies, variety, form and so on).</summary>
    public static bool IsSpeciesOrBelow(string? rank) => Of(rank) is { } ordinal && ordinal >= Species;

    /// <summary>The rank string lower-cased and trimmed, or "unranked" when blank. Used for display and storage.</summary>
    public static string Clean(string? rank) =>
        string.IsNullOrWhiteSpace(rank) ? "unranked" : Normalize(rank);

    private static string Normalize(string rank) =>
        string.Join(' ', rank.Trim().Replace('_', ' ').ToLowerInvariant()
            .Split(' ', StringSplitOptions.RemoveEmptyEntries));

    private static Dictionary<string, int> BuildOrdinals() {
        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < Ladder.Length; i++) {
            foreach (var name in Ladder[i]) {
                map[name] = (i + 1) * 10;
            }
        }
        return map;
    }

    private static int Required(string rank) => Ordinals[rank];
}
