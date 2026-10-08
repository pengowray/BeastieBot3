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
    /// In place of ColOtherListedOn in a table with ECOS rows: ECOS gives the date of the first listing.
    public const string ColOtherFirstListed = "First listed";
    /// In place of ColOtherListedOn in a table whose dates are all SALVE's: the end of the assessment.
    public const string ColOtherAssessed = "Assessed";
    public const string ColOtherFirstListedTitle = "ECOS gives the date the species or population was first listed. The status may have changed since then.";
    public const string ColOtherSource = "Source";
    /// The column of the publication an NZTCS status comes from.
    public const string ColOtherReport = "Published in";

    /// In the "In effect from" column, for a listing whose source gives no date.
    public const string OtherStatusNoDate = "not given";

    /// The heading over the rows of one group: a country, or NatureServe's global ranks.
    public static string OtherStatusGroup(string group) => group switch {
        "AU" => "Australia",
        "BR" => "Brazil",
        "CA" => "Canada",
        "GB" => "United Kingdom",
        OtherStatusSystems.InternationalGroup => "International",
        "NZ" => "New Zealand",
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
        OtherStatusSystems.Salve => ("ICMBio", "Chico Mendes Institute for Biodiversity Conservation, which assesses the extinction risk of Brazil's fauna", true),
        OtherStatusSystems.Cosewic => ("COSEWIC", "Committee on the Status of Endangered Wildlife in Canada, an independent committee that assesses species", true),
        OtherStatusSystems.Sara => ("Species at Risk Act", "Canada's Species at Risk Act, the federal law", false),
        OtherStatusSystems.Nztcs => ("NZTCS", "New Zealand Threat Classification System", true),
        OtherStatusSystems.Esa => ("Endangered Species Act", null, false),
        OtherStatusSystems.Cites => ("CITES", "Convention on International Trade in Endangered Species of Wild Fauna and Flora", true),
        OtherStatusSystems.NatureServeGlobal => ("NatureServe", "NatureServe's global conservation status rank", false),
        OtherStatusSystems.NatureServeNational => ("NatureServe", "NatureServe's national conservation status rank", false),
        _ => (system, null, false),
    };

    /// The second line of a NatureServe rank's status cell: what its rounded rank means, and which
    /// part of the rank that is when the rank is a range or has a T rank ("Vulnerable (rounded rank
    /// G3)" for G3G4, "Imperiled (subspecies rank T2)" for G5T2). A T rank is a variety's when
    /// NatureServe's name has "var.", or, when NatureServe uses the page's name, when the page's taxon
    /// is a variety. Null for a row that is not a rank.
    public static string? NatureServeRankMeaning(OtherStatusRow row, string kind) {
        if (row.System == OtherStatusSystems.NatureServeNational) {
            return NatureServeLocalRankMeaning(row.Status, row.Qualifier);
        }
        if (row.System != OtherStatusSystems.NatureServeGlobal || OtherStatusSystems.NatureServeRankMeaning(row.StatusCode) is not { } meaning) {
            return null;
        }
        var rounded = row.StatusCode!.Trim().ToUpperInvariant();
        if (string.Equals(row.Status.Trim(), rounded, StringComparison.OrdinalIgnoreCase)) {
            return meaning;
        }
        if (rounded[0] == 'T') {
            // The kind of NatureServe's taxon: its name, when the listing names another taxon, else the page's.
            var variety = row.ListedName is { } listed ? listed.Contains(" var. ", StringComparison.Ordinal) : kind == TaxonKinds.Variety;
            var rank = variety ? "variety rank" : "subspecies rank";
            return row.Status.Trim().EndsWith(rounded, StringComparison.OrdinalIgnoreCase)
                ? $"{meaning} ({rank} {rounded})"
                : $"{meaning} (rounded {rank} {rounded})";
        }
        return $"{meaning} (rounded rank {rounded})";
    }

    /// The line under a NatureServe national or subnational rank: what each part means, with the
    /// season it applies to ("Apparently Secure when breeding; Secure when not breeding"), and
    /// "exotic" when NatureServe says the taxon is exotic there. Null when no part has a meaning.
    public static string? NatureServeLocalRankMeaning(string rank, string? qualifier) {
        var parts = OtherStatusSystems.NatureServeRankParts(rank)
            .Select(p => OtherStatusSystems.NatureServeLocalRankMeaning(p.Code) is { } meaning ? meaning + Season(p.Season) : null)
            .OfType<string>()
            .ToList();
        if (qualifier == "exotic") {
            parts.Add("exotic");
        }
        return parts.Count == 0 ? null : string.Join("; ", parts);

        static string Season(NatureServeSeason season) => season switch {
            NatureServeSeason.Breeding => " when breeding",
            NatureServeSeason.Nonbreeding => " when not breeding",
            NatureServeSeason.Migrant => " on migration",
            _ => "",
        };
    }

    /// The text that opens the collapsed table of NatureServe's ranks in a country's states, provinces
    /// or territories: how many places have a rank, and how many of them rank the taxon S1, S2, SH or SX.
    public static string PlaceRanksSummary(string group, int places, int imperiled) {
        var noun = group == "CA" ? (places == 1 ? "province or territory" : "provinces and territories") : (places == 1 ? "state" : "states");
        var count = places.ToString("N0", System.Globalization.CultureInfo.InvariantCulture);
        var rest = imperiled == 0 ? "" : $", imperiled or worse in {imperiled.ToString("N0", System.Globalization.CultureInfo.InvariantCulture)}";
        return $"NatureServe ranks in {count} {noun}{rest}";
    }

    /// The first column's heading in the table of NatureServe's ranks in a country's states, provinces or territories.
    public static string PlaceRanksHeading(string group) => group == "CA" ? "Province or territory" : "State";
    public const string ColPlaceRank = "Rank";
    public const string ColPlaceRankMeaning = "Meaning";

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

    /// The text of a row's link to the record its status comes from (OtherStatusSourceInfo.LinkText;
    /// a SPRAT row's is LinkSprat). NatureServeExplorer is also the text of the link in NatureServe's note.
    public const string OtherStatusEcosRecordLink = "ECOS profile";
    public const string NatureServeExplorer = "NatureServe Explorer";
    public const string OtherStatusNztcsRecordLink = "NZTCS assessment";
    public const string OtherStatusSalveRecordLink = "SALVE assessment";

    /// The text of a link to LicenceCcBy.
    public const string LicenceCcByName = "CC BY 4.0";

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
    /// local: the page has national or state, province or territory ranks.
    public static string OtherStatusNatureServeSubject(bool canadian, bool global, bool local = false) => (canadian, global || local, local) switch {
        (true, true, true) => "Canadian statuses and NatureServe ranks are from ",
        (true, true, false) => "Canadian statuses and NatureServe global ranks are from ",
        (true, false, _) => "Canadian statuses are from ",
        (false, _, true) => "NatureServe ranks are from ",
        _ => "NatureServe global ranks are from ",
    };
    public static string OtherStatusNatureServeDate(string? date) => date is null ? "." : $", downloaded on {date}.";
    /// Between the links to NatureServe Explorer and the licence.
    public const string OtherStatusNatureServeCopyright = " (© NatureServe, ";
    /// The note under the tables when they have NZTCS rows, in two parts around the links to the
    /// NZTCS database and the licence.
    public const string OtherStatusNztcsSubject = "New Zealand statuses are from the ";
    public const string OtherStatusNztcsLink = "New Zealand Threat Classification System database";
    /// Between the links to the NZTCS database and the licence.
    public const string OtherStatusNztcsPublisher = " (Department of Conservation, ";
    public static string OtherStatusNztcsDate(string? date) => date is null ? "." : $", downloaded on {date}.";

    /// The note under the tables when they have SALVE rows, in two parts around the link to SALVE.
    public const string OtherStatusSalveSubject = "Brazilian statuses are ICMBio's national assessments of Brazil's fauna, from ";
    public const string OtherStatusSalveLink = "SALVE";
    /// After the link to SALVE: SALVE's full name.
    public const string OtherStatusSalveName = " (Sistema de Avaliação do Risco de Extinção da Biodiversidade)";
    public static string OtherStatusSalveRest(string? date) =>
        (date is null ? "." : $", downloaded on {date}.")
        + " Brazil's official list of threatened species (Portaria MMA 148/2022) can differ.";

    /// The line under a CITES appendix when the listing covers the taxon as part of a higher taxon
    /// ("family Trochilidae").
    public static string OtherStatusListedUnder(string higherTaxon) => $"as part of {higherTaxon}";

    /// The text of a CITES row's link to the taxon's page on Species+.
    public const string OtherStatusCitesRecordLink = "Species+";
    /// The note under the tables when they have CITES rows, in parts around the link to the Checklist,
    /// with the citation the Checklist asks for. accessed: the date this site downloaded it.
    public const string OtherStatusCitesSubject = "CITES listings are from the ";
    public const string OtherStatusCitesLink = "Checklist of CITES Species";
    public const string CitesChecklistUrl = "https://checklist.cites.org/";
    public static string OtherStatusCitesRest(DateOnly? accessed) =>
        accessed is { } date
            ? $", compiled by UNEP-WCMC, downloaded on {SiteFormat.Date(date)}. Citation: UNEP-WCMC (Comps.) {date.Year}. The Checklist of CITES Species Website. CITES Secretariat, Geneva, Switzerland. Compiled by UNEP-WCMC, Cambridge, UK. Available at: http://checklist.cites.org. [Accessed {date.ToString("dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture)}]."
            : ", compiled by UNEP-WCMC.";

    /// The text of a JNCC row's link to JNCC's page of the designations.
    public const string OtherStatusJnccRecordLink = "JNCC";

    /// The note under the tables when they have JNCC rows, in parts around the links to JNCC's page and
    /// the licence. fileDate: the date of JNCC's workbook ("9 June 2026"); downloaded: when this site
    /// downloaded it; attribution: the line JNCC asks for.
    public const string OtherStatusJnccSubject = "United Kingdom statuses are from ";
    public const string OtherStatusJnccLink = "JNCC's Conservation Designations for UK Taxa";
    public static string OtherStatusJnccDates(string? fileDate, string? downloaded) =>
        (fileDate is null ? "" : $" (the file of {fileDate})") + (downloaded is null ? "." : $", downloaded on {downloaded}.");
    public static string OtherStatusJnccAttribution(string? attribution) => attribution is null ? " " : $" {attribution}, used under the ";
    public const string OtherStatusJnccLicence = "Open Government Licence v3.0";

    public const string OtherStatusNatureServeCanadaCopy =
        "NatureServe's copy of the Canadian statuses may differ from Canada's Species at Risk Public Registry.";
}
