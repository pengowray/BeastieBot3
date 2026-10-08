using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The species page's table of statuses in lists other than the IUCN Red List (other_status).
public static partial class SiteText {
    public const string HeadingOtherStatuses = "Other conservation statuses";
    public const string OtherStatusesIntro =
        "Each list has its own categories. A status in this table is not an IUCN Red List category, even when it has the same name, such as \"Endangered\".";

    public const string ColOtherList = "List";
    public const string ColOtherStatus = "Status";
    public const string ColOtherAppliesTo = "Applies to";
    public const string ColOtherListedName = "Name in list";
    public const string ColOtherListedNameTitle = "Shown when the list uses a different scientific name from this page.";
    public const string ColOtherListedOn = "In effect from";
    public const string ColOtherSource = "Source";

    /// In the "In effect from" column, for a listing whose source gives no date.
    public const string OtherStatusNoDate = "not given";

    /// The heading over the rows of one group: a country, or NatureServe.
    public static string OtherStatusGroup(string group) => group switch {
        "AU" => "Australia",
        "CA" => "Canada",
        "US" => "United States",
        OtherStatusSystems.NatureServeGroup => "NatureServe",
        _ => group,
    };

    /// The row label of a list, and the full name for its hover title when the label is an abbreviation.
    public static (string Label, string? Title) OtherStatusList(string system) => system switch {
        OtherStatusSystems.Epbc => (EpbcAbbr, EpbcFullName),
        OtherStatusSystems.AustralianCapitalTerritory => ("Australian Capital Territory", null),
        OtherStatusSystems.NewSouthWales => ("New South Wales", null),
        OtherStatusSystems.NorthernTerritory => ("Northern Territory", null),
        OtherStatusSystems.Queensland => ("Queensland", null),
        OtherStatusSystems.SouthAustralia => ("South Australia", null),
        OtherStatusSystems.Tasmania => ("Tasmania", null),
        OtherStatusSystems.Victoria => ("Victoria", null),
        OtherStatusSystems.WesternAustralia => ("Western Australia", null),
        OtherStatusSystems.Cosewic => ("COSEWIC", "Committee on the Status of Endangered Wildlife in Canada"),
        OtherStatusSystems.Sara => ("SARA", "Species at Risk Act"),
        OtherStatusSystems.Esa => ("ESA", "Endangered Species Act"),
        OtherStatusSystems.NatureServeGlobal => ("Global rank", null),
        _ => (system, null),
    };

    /// The status cell: the status as the list writes it; for a NatureServe rank, with what its rounded rank means.
    public static string OtherStatusText(OtherStatusRow row) =>
        row.System == OtherStatusSystems.NatureServeGlobal && OtherStatusSystems.NatureServeRankMeaning(row.StatusCode) is { } meaning
            ? $"{row.Status} ({meaning})"
            : row.Status;

    /// In the "Applies to" column, for a listing of the whole taxon.
    public static string OtherStatusWholeTaxon(string kind) => kind switch {
        TaxonKinds.Subspecies => "whole subspecies",
        TaxonKinds.Variety => "whole variety",
        TaxonKinds.Subpopulation => "whole subpopulation",
        _ => "whole species",
    };

    /// The link to the record a status comes from.
    public static string OtherStatusSourceLink(string source) => source switch {
        OtherStatusSources.Sprat => LinkSprat,
        OtherStatusSources.Ecos => "ECOS profile",
        OtherStatusSources.NatureServe => "NatureServe Explorer",
        _ => source,
    };

    /// Under the tables when they have rows from SPRAT. reportDate: "25 June 2026", or null when unknown.
    public static string OtherStatusSpratNote(string? reportDate) =>
        (reportDate is null
            ? "All Australian statuses are from SPRAT, the Australian Government's Species Profile and Threats Database. "
            : $"All Australian statuses are from SPRAT, the Australian Government's Species Profile and Threats Database, downloaded on {reportDate}. ")
        + "SPRAT's state and territory statuses may differ from current state and territory lists.";

    /// Under the tables when they have rows from ECOS.
    public static string OtherStatusEcosNote(string? date) =>
        "Endangered Species Act listings are from ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System"
        + (date is null ? "." : $", downloaded on {date}.");

    /// Under the tables when they have rows from NatureServe.
    public static string OtherStatusNatureServeNote(string? date) =>
        "NatureServe ranks and the COSEWIC and SARA statuses are from NatureServe Explorer (CC BY 4.0)"
        + (date is null ? ". " : $", downloaded on {date}. ")
        + "The COSEWIC and SARA statuses are NatureServe's copy and may differ from Canada's Species at Risk Public Registry.";
}
