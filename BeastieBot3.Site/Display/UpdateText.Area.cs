using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's comparison of a list with one country or area: the choices, and the
// words for a taxon's record in that one area. No page lists a taxon's areas.

public static partial class UpdateText {
    public const string AreaChooseLabel = "Country or area";
    public const string AreaWholeGroupOption = "None (whole group)";
    public const string AreaCountriesGroup = "Countries";
    public const string AreaPartsGroup = "Parts of countries";
    public const string AreaModeLabel = "Origin";

    /// "Hawaiian Islands (United States)".
    public static string AreaOption(AreaName area, AreaNames areas) =>
        area.Country is { } country && areas.ByCode(country) is { } c ? $"{area.DisplayName} ({c.DisplayName})" : area.DisplayName;

    /// Origin and assisted colonisation count as introduced; origin uncertain counts only under the last choice.
    public static string AreaModeOption(AreaMode mode) => mode switch {
        AreaMode.Native => "Native or reintroduced",
        AreaMode.Endemic => "Endemic",
        AreaMode.NativeAndIntroduced => "Native, reintroduced or introduced",
        _ => "Any origin, vagrants included",
    };

    // "IUCN records as native or reintroduced in Brazil", "... in Brazil, vagrants included".
    private static string AreaPhrase(AreaMode mode, string area) => mode switch {
        AreaMode.Native => $"as native or reintroduced in {area}",
        AreaMode.Endemic => $"as endemic to {area}",
        AreaMode.NativeAndIntroduced => $"as native, reintroduced or introduced in {area}",
        _ => $"in {area}, vagrants included",
    };

    public static string NotInAreaHeading(int n, AreaMode mode, string area) => mode == AreaMode.All
        ? $"Taxa in the wikitext with no IUCN record in {area} ({Count(n)})"
        : $"Taxa in the wikitext that IUCN does not record {AreaPhrase(mode, area)} ({Count(n)})";

    /// What the record for the area says, in a column headed with the area's name: "Introduced",
    /// "Vagrant", "Native, possibly extinct". Presence is extant unless stated.
    public static string AreaRecordText(AreaRecord? record) {
        if (record is null) {
            return "Not recorded";
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
            AreaPresence.PossiblyExtinct => "possibly extinct",
            AreaPresence.ExtinctPost1500 => "extinct post-1500",
            _ => null,
        };
        return presence is null ? origin : $"{origin}, {presence}";
    }

    public static string MissingAreaNotesHeading(int n) => $"Missing taxa to check before adding ({Count(n)})";

    /// "IUCN records these taxa in Brazil as introduced, vagrant or possibly extinct.", from the
    /// records in the table.
    public static string MissingAreaNotesIntro(IEnumerable<AreaRecord> records, string area) {
        var words = new List<string>();
        var list = records.ToList();
        void Word(bool any, string word) {
            if (any) {
                words.Add(word);
            }
        }
        Word(list.Any(r => r.Origin == AreaOrigin.Reintroduced), "reintroduced");
        Word(list.Any(r => r.Origin is AreaOrigin.Introduced or AreaOrigin.AssistedColonisation), "introduced");
        Word(list.Any(r => r.Origin == AreaOrigin.Vagrant), "vagrant");
        Word(list.Any(r => r.Origin == AreaOrigin.OriginUncertain || r.Presence is AreaPresence.PossiblyExtant or AreaPresence.PresenceUncertain), "of uncertain origin or presence");
        Word(list.Any(r => r.Presence == AreaPresence.PossiblyExtinct), "possibly extinct");
        Word(list.Any(r => r.Presence == AreaPresence.ExtinctPost1500), "extinct");
        var joined = words.Count <= 1 ? string.Concat(words) : $"{string.Join(", ", words.Take(words.Count - 1))} or {words[^1]}";
        return $"IUCN records these taxa in {area} as {joined}.";
    }

    public static string ColumnInArea(string area) => $"In {area}";
    public const string ColumnInAreaHelp = "Presence is extant unless stated.";
}
