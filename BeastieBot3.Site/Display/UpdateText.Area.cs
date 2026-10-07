using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's comparison of a list with one country or area: the choices, and the
// words for a taxon's record in that one area. No page lists a taxon's areas.

public static partial class UpdateText {
    public const string AreaChooseLabel = "Country or area";
    public const string AreaWholeGroupOption = "Any (the whole group)";
    public const string AreaCountriesGroup = "Countries";
    public const string AreaPartsGroup = "Parts of countries";
    public const string AreaModeLabel = "Include";

    /// "Hawaiian Is. (United States)".
    public static string AreaOption(AreaName area, AreaNames areas) =>
        area.Country is { } country && areas.ByCode(country) is { } c ? $"{area.Name} ({c.Name})" : area.Name;

    public static string AreaModeOption(AreaMode mode) => mode switch {
        AreaMode.Native => "Native species",
        AreaMode.Endemic => "Endemic species only",
        AreaMode.NativeAndIntroduced => "Native and introduced species",
        _ => "All records, vagrants included",
    };

    // "native to Brazil", for "... species that IUCN records as native to Brazil".
    private static string AreaPhrase(AreaMode mode, string area) => mode switch {
        AreaMode.Native => $"native to {area}",
        AreaMode.Endemic => $"endemic to {area}",
        AreaMode.NativeAndIntroduced => $"native or introduced in {area}",
        _ => $"occurring in {area}",
    };

    public static string NotInAreaHeading(int n, string area) => $"{Taxa(n)} that IUCN does not record in {area} as the list counts them";

    /// What one record says about a taxon in the area, beside it in a table: "Introduced",
    /// "Vagrant", "Native, possibly extinct here". Empty for a native, extant taxon.
    public static string AreaRecordText(AreaRecord? record, string area) {
        if (record is null) {
            return $"Not recorded in {area}";
        }
        var origin = record.Origin switch {
            AreaOrigin.Native => record.Endemic ? "Endemic" : "Native",
            AreaOrigin.Reintroduced => "Reintroduced",
            AreaOrigin.Introduced => "Introduced",
            AreaOrigin.AssistedColonisation => "Assisted colonisation",
            AreaOrigin.Vagrant => "Vagrant",
            _ => "Origin uncertain",
        };
        var presence = record.Presence switch {
            AreaPresence.PossiblyExtant => "possibly extant",
            AreaPresence.PresenceUncertain => "presence uncertain",
            AreaPresence.PossiblyExtinct => "possibly extinct here",
            AreaPresence.ExtinctPost1500 => "extinct here since 1500",
            _ => null,
        };
        return presence is null ? origin : $"{origin}, {presence}";
    }

    public static string MissingAreaNotesHeading(int n, string area) => $"Missing taxa with a note for {area} ({Count(n)})";
    public static string ColumnInArea(string area) => $"In {area}";
}
