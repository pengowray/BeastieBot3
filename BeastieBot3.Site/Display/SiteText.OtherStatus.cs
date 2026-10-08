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

    /// The heading over the rows of one country.
    public static string OtherStatusCountry(string country) => country switch {
        "AU" => "Australia",
        _ => country,
    };

    /// The row label of a list. The EPBC Act's is shown as an abbreviation (EpbcAbbr, EpbcFullName).
    public static string OtherStatusList(string system) => system switch {
        OtherStatusSystems.Epbc => EpbcAbbr,
        OtherStatusSystems.AustralianCapitalTerritory => "Australian Capital Territory",
        OtherStatusSystems.NewSouthWales => "New South Wales",
        OtherStatusSystems.NorthernTerritory => "Northern Territory",
        OtherStatusSystems.Queensland => "Queensland",
        OtherStatusSystems.SouthAustralia => "South Australia",
        OtherStatusSystems.Tasmania => "Tasmania",
        OtherStatusSystems.Victoria => "Victoria",
        OtherStatusSystems.WesternAustralia => "Western Australia",
        _ => system,
    };

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
        _ => source,
    };

    /// Under the table when it has rows from SPRAT. reportDate: "25 June 2026", or null when unknown.
    public static string OtherStatusSpratNote(string? reportDate) =>
        (reportDate is null
            ? "All Australian statuses are from SPRAT, the Australian Government's Species Profile and Threats Database. "
            : $"All Australian statuses are from SPRAT, the Australian Government's Species Profile and Threats Database, downloaded on {reportDate}. ")
        + "SPRAT's state and territory statuses may differ from current state and territory lists.";
}
