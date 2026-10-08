using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The status update page's EPBC Act listings, shown beside IUCN statuses for Australian lists.

public static partial class UpdateText {
    public const string OptionEpbc = "Show Australia's EPBC Act listing beside each IUCN status in the report";
    public const string ColumnEpbc = "EPBC Act";

    /// "EN", or "VU (Victorian population)" for a listing of a population; listings joined by "; ".
    /// Empty when the taxon has no listing.
    public static string Epbc(IReadOnlyList<EpbcListingRow>? listings) => listings is null
        ? string.Empty
        : string.Join("; ", listings.Where(l => l.Status is not null)
            .Select(l => l.AppliesTo == "population" && l.Population is { Length: > 0 } p ? $"{l.Status} ({p})" : l.Status));

    public static string MissingEpbc(int listed, int missing) {
        if (listed == 0) {
            return "None of the taxa missing from the page is listed under the EPBC Act.";
        }
        var of = missing == 1 ? "the 1 taxon missing from the page" : $"the {SiteFormat.Number(missing)} taxa missing from the page";
        return $"{SiteFormat.Number(listed)} of {of} {(listed == 1 ? "is" : "are")} listed under the EPBC Act:";
    }
}
