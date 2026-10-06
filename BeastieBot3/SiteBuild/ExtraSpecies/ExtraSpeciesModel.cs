// The rows `site build-db` collects for species that are in the Catalogue of Life or Wikidata but
// are not IUCN taxa (extra_species and the tables beside it in SiteDbSchema).

namespace BeastieBot3.SiteBuild.ExtraSpecies;

/// How far a species from CoL or Wikidata may be placed in the IUCN tree of groups.
internal enum ExtraPlacement {
    /// No species from CoL or Wikidata.
    None,
    /// Only species whose genus is an IUCN genus (same name, same kingdom).
    Genus,
    /// Also species whose family is an IUCN family, under that family.
    Family,
}

/// One accepted species from the Catalogue of Life.
internal sealed record ColSpeciesRow(string Id, string Genus, string Epithet, string? Authority, string? Family, string Kingdom);

/// One species item from the Wikidata sweep, with what the build worked out about it.
internal sealed record WikidataSpeciesRow(
    long Qid,
    string Genus,
    string Epithet,
    IReadOnlyList<string> ColIds,
    IReadOnlyList<long> IucnTaxonIds,
    string? EnwikiTitle,
    string? LabelEn,
    string? Kingdom,
    string? Family);

/// A species that will be an extra_species row: from CoL, Wikidata or both.
internal sealed class ExtraEntry {
    public string? ColId { get; set; }
    public long? Qid { get; set; }
    public required string Genus { get; init; }
    public required string Epithet { get; init; }
    public string Name => Genus + " " + Epithet;
    /// Wikidata's taxon name, when the entry is in both sources and Wikidata spells it differently.
    public string? WikidataName { get; set; }
    public string? Authority { get; init; }
    /// IUCN's spelling: upper case.
    public required string Kingdom { get; init; }
    public string? CommonNameEn { get; set; }
    public string? EnwikiTitle { get; set; }
    public required SiteTreeNode Node { get; init; }
    /// The family group the entry is under (its genus's family, or the group it is placed in).
    public SiteTreeNode? FamilyNode { get; init; }
    /// Sorts after the taxon with this tree_pos.
    public int SortPos { get; set; }
    public int ExtraId { get; set; }

    public string Sources => ColId is not null && Qid is not null ? "col wikidata" : ColId is not null ? "col" : "wikidata";
}

/// A possible match between an extra entry and an IUCN taxon or another extra entry.
internal sealed record ExtraOverlap(ExtraEntry Entry, long? TaxonId, ExtraEntry? Other, string Reason) {
    public bool Likely => OverlapReason.IsLikely(Reason);
}

/// The name another source gives an IUCN taxon, when it differs from IUCN's.
internal sealed record TaxonSourceName(long TaxonId, string Source, string ScientificName);

/// What the summary reports about the extra species.
internal sealed class ExtraSpeciesStats {
    public ExtraPlacement Placement;
    public int ColRead;
    public int ColSameAsIucn;
    public int ColExtinct;
    public int WikidataRead;
    public int WikidataLeftOutByInstance;
    public int WikidataNotBinomial;
    public int WikidataSameAsIucn;
    public int WikidataMergedWithCol;
    public int WikidataSameNameRepeat;
    public int WikidataUnplaced;
    public int WikidataAmbiguousKingdom;
    public int ColEntries;
    public int WikidataEntries;
    public int BothEntries;
    public int PlacedUnderGenus;
    public int PlacedUnderFamily;
    public int CommonNames;
    public int Articles;
    public readonly Dictionary<string, int> OverlapsByReason = new(StringComparer.Ordinal);
    public int TaxonSourceNames;
    public int ColSynonymsRead;
    public long TableBytes;
    public string? WikidataSweepFinished;
}
