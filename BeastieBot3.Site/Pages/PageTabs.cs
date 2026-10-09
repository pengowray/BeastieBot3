using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Web;

namespace BeastieBot3.Site.Pages;

/// One tab of _PageTabs: its text, address, and whether it is the page shown.
public sealed record PageTab(string Text, string Href, bool Current);

/// The row of tabs under a taxon's or group's name: its reference page and its tool page. Label is
/// the row's accessible name.
public sealed record PageTabs(string Label, IReadOnlyList<PageTab> Tabs) {
    /// "Taxon page | Citations". citations: the citations page (Wikipedia or Wikidata) is shown.
    /// assessmentId: the assessment the citations page shows, when it is not the default one.
    public static PageTabs ForTaxon(long taxonId, bool citations, long? assessmentId = null) => new(SiteText.TaxonTabsLabel, [
        new PageTab(SiteText.TabTaxonPage, "/species/" + taxonId, !citations),
        new PageTab(SiteText.TabCitations, AssessmentToolModel.WikipediaPath(taxonId, assessmentId is { } id ? "?assessment=" + id : ""), citations),
    ]);

    /// "Group page | Species list". list: the list page is shown.
    public static PageTabs ForGroup(GroupRow group, bool list) => new(GroupText.GroupTabsLabel, [
        new PageTab(GroupText.TabGroupPage, SiteUrls.Group(group), !list),
        new PageTab(GroupText.TabList, SiteUrls.GroupList(group), list),
    ]);
}
