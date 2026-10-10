namespace BeastieBot3.Shared.SiteData;

// Values of the site database's text columns that `site build-db` writes and the site reads, so the
// two sides use the same strings. SiteDbSchema's DDL comments describe each value.

/// The values of taxon_link.link_kind.
public static class TaxonLinkKinds {
    /// The two taxa have the same scientific name.
    public const string SameName = "same-name";
    /// IUCN lists the old taxon's scientific name as a synonym of the taxon in the release.
    public const string IucnSynonym = "iucn-synonym";
    /// Both taxa are in the release, and the first one's name is the other's name with "_new" after it
    /// ("Balaenoptera edeni_new"): a second IUCN record of the same taxon, made for a national assessment.
    public const string WorkingName = "working-name";
    /// Both taxa are in the release: the first one has a provisional name ("Notogomphus sp. nov.
    /// 'gorilla'") and the other one's name is the name built from its quoted epithet ("Notogomphus
    /// gorilla"). It may be the same species, described since.
    public const string ProvisionalName = "provisional-name";

    /// The kinds that link an old IUCN id (not in the release) to a taxon in the release, whose global
    /// assessments a page shows in one history table.
    public static bool IsEarlierId(string kind) => kind is SameName or IucnSynonym;
}

/// Values of extra_overlap.reason that the site reads by name (the others it only shows).
public static class ExtraOverlapReasons {
    /// The IUCN taxon has a provisional name and the extra species is named with its quoted epithet.
    public const string ProvisionalName = "provisional-name";
    /// The IUCN taxon's name is the extra species' name with "_new" after it.
    public const string WorkingName = "working-name";
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
