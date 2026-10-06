using BeastieBot3.Site.Data;
using Microsoft.Extensions.Primitives;

// Which sources a group page's list takes its species from, and which source it prefers when two
// may list the same species. Read from and written to the query string with the other list options
// (GroupListQuery):
//
//   src     iucn | col | wd    a source to include; repeated; IUCN only when absent or none is given
//   prefer  icw | ciw | cwi | wci | iwc | wic    the order of preference (i IUCN, c CoL, w Wikidata)
//   genera  0: leave out the species placed under a family because IUCN does not have their genus
//
// Species from the Catalogue of Life or Wikidata that IUCN does not have come from the extra_species
// table. They have no IUCN assessment, so they go in the NE section, which is included whenever CoL
// or Wikidata is.

namespace BeastieBot3.Site.Lists;

public enum ListSource { Iucn, Col, Wikidata }

public sealed record ListSourceOptions {
    public static readonly IReadOnlyList<ListSource> DefaultOrder = [ListSource.Iucn, ListSource.Col, ListSource.Wikidata];

    public static readonly ListSourceOptions Default = new();

    /// Every order the page offers, by query key.
    public static readonly IReadOnlyList<(string Key, IReadOnlyList<ListSource> Order)> Orders = [
        ("icw", [ListSource.Iucn, ListSource.Col, ListSource.Wikidata]),
        ("iwc", [ListSource.Iucn, ListSource.Wikidata, ListSource.Col]),
        ("ciw", [ListSource.Col, ListSource.Iucn, ListSource.Wikidata]),
        ("cwi", [ListSource.Col, ListSource.Wikidata, ListSource.Iucn]),
        ("wic", [ListSource.Wikidata, ListSource.Iucn, ListSource.Col]),
        ("wci", [ListSource.Wikidata, ListSource.Col, ListSource.Iucn]),
    ];

    public IReadOnlySet<ListSource> Enabled { get; init; } = new HashSet<ListSource> { ListSource.Iucn };
    public IReadOnlyList<ListSource> Order { get; init; } = DefaultOrder;
    /// Whether to list the species that site build-db placed under a family, because IUCN does not
    /// have their genus.
    public bool OtherGenera { get; init; } = true;

    /// Whether the list has any species beyond IUCN's.
    public bool HasOtherSources => Enabled.Contains(ListSource.Col) || Enabled.Contains(ListSource.Wikidata);

    public string OrderKey => Orders.FirstOrDefault(o => o.Order.SequenceEqual(Order)).Key ?? "icw";

    /// The place of a source in the order: 0 for the most preferred.
    public int Rank(ListSource source) {
        for (var i = 0; i < Order.Count; i++) {
            if (Order[i] == source) {
                return i;
            }
        }
        return Order.Count;
    }

    public static string Key(ListSource source) => source switch {
        ListSource.Col => "col",
        ListSource.Wikidata => "wd",
        _ => "iucn",
    };

    public static ListSourceOptions Read(IQueryCollection query) {
        var options = Default;
        if (query.TryGetValue("src", out var values)) {
            var picked = Values(values).Select(v => v switch {
                "iucn" => (ListSource?)ListSource.Iucn,
                "col" => ListSource.Col,
                "wd" => ListSource.Wikidata,
                _ => null,
            }).Where(s => s is not null).Select(s => s!.Value).ToHashSet();
            if (picked.Count > 0) {
                options = options with { Enabled = picked };
            }
        }
        if (query.TryGetValue("prefer", out var prefer) && prefer.Count > 0
            && Orders.FirstOrDefault(o => o.Key == prefer[^1]?.Trim()) is { Key: not null } order) {
            options = options with { Order = order.Order };
        }
        if (query.TryGetValue("genera", out var genera) && genera.Count > 0) {
            options = options with { OtherGenera = genera[^1]?.Trim() != "0" };
        }
        return options;
    }

    /// The query string parts that differ from the defaults.
    public IEnumerable<string> Write() {
        if (!Enabled.SetEquals(Default.Enabled)) {
            foreach (var source in DefaultOrder.Where(Enabled.Contains)) {
                yield return "src=" + Key(source);
            }
        }
        if (!Order.SequenceEqual(DefaultOrder)) {
            yield return "prefer=" + OrderKey;
        }
        if (!OtherGenera) {
            yield return "genera=0";
        }
    }

    private static IEnumerable<string> Values(StringValues values) =>
        values.Where(v => !string.IsNullOrWhiteSpace(v)).Select(v => v!.Trim());
}

/// One species from the Catalogue of Life or Wikidata that IUCN does not have (an extra_species row).
public sealed record ExtraSpeciesRow(
    int ExtraId,
    bool InCol,
    bool InWikidata,
    string ScientificName,
    string? WikidataName,
    string Genus,
    string Epithet,
    string Kingdom,
    string? ColId,
    string? WikidataQid,
    string? CommonNameEn,
    string? EnwikiTitle,
    int NodeId,
    int SortPos,
    // CoL's authorship of ScientificName; null for a species only on Wikidata.
    string? Authority = null) {
    /// The id the list uses for its row: negative, so it never equals an IUCN taxon id.
    public long RowId => -ExtraId;
}

/// What other sources say about an IUCN taxon in a list: its CoL usage and Wikidata item, and their
/// names for it when they differ from IUCN's.
public sealed record IucnSourceInfo(long TaxonId, string? ColId, string? WikidataQid, string? ColName, string? WikidataName);

/// An extra_overlap row.
public sealed record ExtraOverlapRow(int ExtraId, long? TaxonId, int? OtherExtraId, string Reason, bool Likely);

/// An IUCN taxon named in an overlap but not in the list's group.
public sealed record OverlapTaxon(long TaxonId, string ScientificName, string? ColId, string? WikidataQid);
