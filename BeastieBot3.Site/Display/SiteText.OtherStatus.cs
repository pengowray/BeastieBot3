using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The species page's table of statuses in lists other than the IUCN Red List (other_status).
public static partial class SiteText {
    public const string HeadingOtherStatuses = "Other conservation statuses";
    public const string OtherStatusesIntro =
        "Each list has its own categories. A status in this section is not an IUCN Red List category, even when it has the same name, such as \"Endangered\" or \"Vulnerable\".";

    public const string ColOtherList = "List";
    public const string ColOtherStatus = "Status";
    public const string ColOtherAppliesTo = "Applies to";
    public const string ColOtherListedName = "Name in list";
    public const string ColOtherListedNameTitle = "Shown when the list uses a different scientific name from this page.";
    public const string ColOtherListedOn = "In effect from";
    public const string ColOtherSource = "Source";

    /// In the "In effect from" column, for a listing whose source gives no date.
    public const string OtherStatusNoDate = "not given";

    /// The heading over the rows of one group: a country, or NatureServe's global ranks.
    public static string OtherStatusGroup(string group) => group switch {
        "AU" => "Australia",
        "CA" => "Canada",
        "US" => "United States",
        OtherStatusSystems.NatureServeGroup => "Global",
        _ => group,
    };

    /// The row label of a list, and a hover title: the full name when the label is an abbreviation
    /// (shown with abbr), else a note on what the list is, or null.
    public static (string Label, string? Title, bool IsAbbreviation) OtherStatusList(string system) => system switch {
        OtherStatusSystems.Epbc => (EpbcAbbr, EpbcFullName, true),
        OtherStatusSystems.AustralianCapitalTerritory => ("Australian Capital Territory", null, false),
        OtherStatusSystems.NewSouthWales => ("New South Wales", null, false),
        OtherStatusSystems.NorthernTerritory => ("Northern Territory", null, false),
        OtherStatusSystems.Queensland => ("Queensland", null, false),
        OtherStatusSystems.SouthAustralia => ("South Australia", null, false),
        OtherStatusSystems.Tasmania => ("Tasmania", null, false),
        OtherStatusSystems.Victoria => ("Victoria", null, false),
        OtherStatusSystems.WesternAustralia => ("Western Australia", null, false),
        OtherStatusSystems.Cosewic => ("COSEWIC", "Committee on the Status of Endangered Wildlife in Canada, an independent committee that assesses species", true),
        OtherStatusSystems.Sara => ("Species at Risk Act", "Canada's Species at Risk Act, the federal law", false),
        OtherStatusSystems.Esa => ("Endangered Species Act", null, false),
        OtherStatusSystems.NatureServeGlobal => ("NatureServe", "NatureServe's global conservation status rank", false),
        _ => (system, null, false),
    };

    /// The second line of a NatureServe rank's status cell: what its rounded rank means, and which
    /// part of the rank that is when the rank is a range or has a T rank ("Vulnerable (rounded rank
    /// G3)" for G3G4, "Imperiled (subspecies rank T2)" for G5T2). Null for a row that is not a rank.
    public static string? NatureServeRankMeaning(OtherStatusRow row, string kind) {
        if (row.System != OtherStatusSystems.NatureServeGlobal || OtherStatusSystems.NatureServeRankMeaning(row.StatusCode) is not { } meaning) {
            return null;
        }
        var rounded = row.StatusCode!.Trim().ToUpperInvariant();
        if (string.Equals(row.Status.Trim(), rounded, StringComparison.OrdinalIgnoreCase)) {
            return meaning;
        }
        if (rounded[0] == 'T') {
            var rank = kind == TaxonKinds.Variety ? "variety rank" : "subspecies rank";
            return row.Status.Trim().EndsWith(rounded, StringComparison.OrdinalIgnoreCase)
                ? $"{meaning} ({rank} {rounded})"
                : $"{meaning} (rounded {rank} {rounded})";
        }
        return $"{meaning} (rounded rank {rounded})";
    }

    /// The hover title of a NatureServe rank with a "?" or a "Q"; null for any other.
    public static string? NatureServeRankTitle(OtherStatusRow row) {
        if (row.System != OtherStatusSystems.NatureServeGlobal) {
            return null;
        }
        var parts = new List<string>();
        if (row.Status.Contains('?')) {
            parts.Add("The question mark means the rank is uncertain.");
        }
        if (row.Status.Contains('Q')) {
            parts.Add("Q means the taxonomy is questionable.");
        }
        return parts.Count == 0 ? null : string.Join(' ', parts);
    }

    /// In the "Applies to" column, for a listing of the whole taxon.
    public static string OtherStatusWholeTaxon(string kind) => kind switch {
        TaxonKinds.Subspecies => "whole subspecies",
        TaxonKinds.Variety => "whole variety",
        TaxonKinds.Subpopulation => "whole subpopulation",
        _ => "whole species",
    };

    /// True when the source says nothing about which populations a status applies to: the COSEWIC
    /// and SARA statuses that NatureServe records (COSEWIC can assess populations separately).
    public static bool OtherStatusAppliesToUnknown(OtherStatusRow row) =>
        row.Population is null && row.System is OtherStatusSystems.Cosewic or OtherStatusSystems.Sara;

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
        "United States statuses are from ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System"
        + (date is null ? "." : $", downloaded on {date}.");

    /// The note under the tables when they have rows from NatureServe, in three parts around the
    /// links to NatureServe Explorer and the licence: what the rows are, the date, and, when there are
    /// Canadian rows, that they are NatureServe's copy.
    public static string OtherStatusNatureServeSubject(bool canadian, bool global) => (canadian, global) switch {
        (true, true) => "Canadian statuses and NatureServe global ranks are from ",
        (true, false) => "Canadian statuses are from ",
        _ => "NatureServe global ranks are from ",
    };
    public static string OtherStatusNatureServeDate(string? date) => date is null ? "." : $", downloaded on {date}.";
    public const string OtherStatusNatureServeCanadaCopy =
        "NatureServe's copy of the Canadian statuses may differ from Canada's Species at Risk Public Registry.";
}
