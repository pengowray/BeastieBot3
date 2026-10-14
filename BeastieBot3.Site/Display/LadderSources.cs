namespace BeastieBot3.Site.Display;

/// A classification that taxon_ladder links a taxon to (`site build-db`): its key in ladder_node, the
/// column's title and hover help, the address of a node (null when the source has no page for it),
/// and the address the title links when the species' node has none.
public sealed record LadderSource(string Key, string Title, Func<string, bool, string?> NodeUrl, string? HomeUrl = null, string? Help = null);

/// The classifications of taxon_ladder, in the order of their columns after the Catalogue of Life:
/// the checklists of one class, the bird lists, then the sources of every group.
public static class LadderSources {
    // isSpecies: the node is the taxon's own; the checklists have pages only for species.
    public static readonly IReadOnlyList<LadderSource> All = [
        new("mdd", SiteText.SourceLabel("mdd"), (id, isSpecies) => isSpecies ? SiteText.MddUrl + "taxon/" + Uri.EscapeDataString(id) + "/" : null),
        new("reptiledb", SiteText.SourceLabel("reptiledb"), (id, isSpecies) => isSpecies ? "https://reptile-database.reptarium.cz/species?" + id : null),
        new("amphibiaweb", SiteText.SourceLabel("amphibiaweb"), (id, isSpecies) => isSpecies ? SiteText.AmphibiaWebUrl + "species/" + Uri.EscapeDataString(id) : null),
        new("ioc", SiteText.SourceLabel("ioc"), (_, _) => null, SiteText.IocUrl, SiteText.RanksIocHelp),
        new("avilist", SiteText.SourceLabel("avilist"), (_, _) => null, SiteText.AviListUrl),
        // A group POWO names only in a column has an id "<rank>:<name>" and no page.
        new("powo", SiteText.SourceLabel("powo"), (id, _) => id.Contains(':') ? null : "https://powo.science.kew.org/taxon/urn:lsid:ipni.org:names:" + id),
        new("worms", SiteText.SourceLabel("worms"), (id, _) => "https://www.marinespecies.org/aphia.php?p=taxdetails&id=" + Uri.EscapeDataString(id),
            Help: SiteText.RanksWormsHelp),
        new("itis", SiteText.SourceLabel("itis"), (id, _) => "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=" + Uri.EscapeDataString(id),
            Help: SiteText.RanksItisHelp),
    ];

    public static LadderSource? Find(string key) => All.FirstOrDefault(s => s.Key == key);
}
