namespace BeastieBot3.Shared.SiteData;

// Values of the site database's text columns that `site build-db` writes and the site reads, so the
// two sides use the same strings. SiteDbSchema's DDL comments describe each value.

/// The values of taxon_link.link_kind.
public static class TaxonLinkKinds {
    /// The two taxa have the same scientific name.
    public const string SameName = "same-name";
    /// IUCN lists the old taxon's scientific name as a synonym of the taxon in the release.
    public const string IucnSynonym = "iucn-synonym";
}

/// The values of higher_taxon.source. Col is also the source of a Catalogue of Life name in
/// higher_taxon_name.
public static class GroupSources {
    public const string Iucn = "iucn";
    /// IUCN gives "NOT ASSIGNED" and rules/iucn-not-assigned.yml gives the order or family.
    public const string IucnRule = "iucn-rule";
    /// A Catalogue of Life group.
    public const string Col = "col";
}
