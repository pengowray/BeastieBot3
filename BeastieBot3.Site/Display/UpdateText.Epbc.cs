using System.Globalization;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The status update page's EPBC Act listings, shown beside IUCN statuses for Australian lists.

public static partial class UpdateText {
    public const string OptionEpbc = "Show the EPBC Act status (Australia) beside each IUCN status";
    public const string ColumnEpbc = "EPBC Act";

    /// "EN", or "VU (Victorian population)" for a listing of a population; listings joined by "; ".
    /// Empty when the taxon has no listing.
    public static string Epbc(IReadOnlyList<EpbcListingRow>? listings) => listings is null
        ? string.Empty
        : string.Join("; ", listings.Where(l => l.Status is not null)
            .Select(l => l.AppliesTo == "population" && l.Population is { Length: > 0 } p ? $"{l.Status} ({p})" : l.Status));

    public static string MissingEpbc(int listed, int missing) => listed == 0
        ? "None of the missing taxa is listed under the EPBC Act."
        : $"{listed.ToString("N0", CultureInfo.InvariantCulture)} of the {missing.ToString("N0", CultureInfo.InvariantCulture)} missing taxa are listed under the EPBC Act:";
}
