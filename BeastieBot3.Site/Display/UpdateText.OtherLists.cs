using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The status update page's counts by category of the updated wikitext, and its choice of another list
// whose statuses are shown beside IUCN's (OtherStatusChoices).

public static partial class UpdateText {
    // Counts by category (IucnStatusesSummary.Counts), under the result's summary.
    public const string CountsLabel = "IUCN statuses in the updated wikitext:";
    public static string CountsTotal(int items) => $"Total {SiteFormat.Number(items)}";
    public static string CountsPossiblyExtinct(int n) => $"({SiteFormat.Number(n)} possibly extinct)";
    public static string CountsHelp(bool fromRows, int uncounted) =>
        (fromRows ? "Counted from the {{Species table/row}} rows." : "Counted from the {{IUCN status}} templates.")
        + (uncounted > 0 ? $" {SiteFormat.Number(uncounted)} not counted (NA, RE or other codes)." : "");
    public const string CountsBoxLabel = "As an {{IUCN statuses}} box:";

    // The choice of another list.
    public const string OtherListLabel = "Show statuses from another list";
    public const string OtherListNoneOption = "None";
    public static string OtherListOption(string name, int taxa) =>
        $"{name} ({SiteFormat.Number(taxa)} {(taxa == 1 ? "taxon" : "taxa")})";
    public const string OtherListHelp = "Adds a column to the report with each taxon's status in the chosen list. The wikitext is not changed.";

    public static string MissingOtherList(int listed, int missing, string heading) {
        if (listed == 0) {
            return $"None of the taxa missing from the page has a status in {heading}.";
        }
        var of = missing == 1 ? "the 1 taxon missing from the page" : $"the {SiteFormat.Number(missing)} taxa missing from the page";
        return $"{SiteFormat.Number(listed)} of {of} {(listed == 1 ? "has" : "have")} a status in {heading}:";
    }
}
