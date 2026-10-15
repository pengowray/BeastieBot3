using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

/// A list the reader can choose, on the status update page and the species list maker, to see each
/// taxon's status in beside its IUCN status. Key: the value the form sends (the list key, the system
/// key, or the system key and the country for NatureServe's national ranks). Group: the code of the
/// heading it is listed under (a country code, OtherStatusSystems.InternationalGroup or
/// NatureServeGroup). Name: its name under that heading ("Victoria"); Heading: its name on its own,
/// for a column heading or a sentence ("Victoria (Australia)"); LineLabel: its name before a status
/// at the end of a list line, without brackets or a leading "The" ("Victoria, Australia", "Swedish
/// Red List 2025").
public sealed record OtherStatusChoice(string Key, OtherStatusListCount List, string Group, string Name, string Heading, string LineLabel) {
    public string System => List.System;

    /// The statuses of one taxon as one cell: "Endangered; Vulnerable (Victorian population)". A
    /// population or area that the list gives for nearly every status (List.Area: "Great Britain" on
    /// the Great Britain Red List) is left out. Empty when the taxon has no status in the list.
    public string Cell(IReadOnlyList<ChosenStatus>? statuses) => statuses is null
        ? string.Empty
        : string.Join("; ", statuses
            .Select(s => s.Population is { Length: > 0 } p && p != List.Area ? $"{s.Status} ({p})" : s.Status)
            .Distinct(StringComparer.Ordinal));
}

/// The lists of other_status that can be chosen, grouped and ordered as on the species page: CITES,
/// then each country by name, then NatureServe's global ranks.
public sealed partial class OtherStatusChoices {
    public static readonly OtherStatusChoices None = new([]);

    // The list a page of a country's taxa is compared with by default. A country not here gets the
    // one of its national red lists from GBIF that has the most of the page's taxa.
    private static readonly Dictionary<string, string> CountryLists = new(StringComparer.Ordinal) {
        ["AU"] = OtherStatusSystems.Epbc,
        ["BR"] = OtherStatusSystems.Salve,
        ["CA"] = OtherStatusSystems.Cosewic,
        ["FR"] = OtherStatusSystems.FranceRedList,
        ["GB"] = "jncc-redlist-gb",
        ["JP"] = OtherStatusSystems.JapanMoe,
        ["NZ"] = OtherStatusSystems.Nztcs,
        ["US"] = OtherStatusSystems.Esa,
    };

    // Systems whose name does not say which country's list it is ("Victoria", "National red list").
    private static readonly HashSet<string> NamedWithCountry = [
        OtherStatusSystems.AustralianCapitalTerritory, OtherStatusSystems.NewSouthWales, OtherStatusSystems.NorthernTerritory,
        OtherStatusSystems.Queensland, OtherStatusSystems.SouthAustralia, OtherStatusSystems.Tasmania, OtherStatusSystems.Victoria,
        OtherStatusSystems.WesternAustralia, OtherStatusSystems.Salve, OtherStatusSystems.Sara, OtherStatusSystems.FranceRedList,
        OtherStatusSystems.FranceProtection, OtherStatusSystems.JapanMoe, OtherStatusSystems.NatureServeNational,
    ];

    private readonly Dictionary<string, OtherStatusChoice> _byKey;

    public OtherStatusChoices(IReadOnlyList<OtherStatusListCount> lists) {
        var choices = lists.Select(Choose).ToList();
        Groups = choices
            .GroupBy(c => c.Group)
            .OrderBy(g => g.Key == OtherStatusSystems.InternationalGroup ? 0 : g.Key == OtherStatusSystems.NatureServeGroup ? 2 : 1)
            .ThenBy(g => SiteText.OtherStatusGroup(g.Key), StringComparer.Ordinal)
            .Select(g => (g.Key, (IReadOnlyList<OtherStatusChoice>)g
                .OrderBy(c => OtherStatusSystems.Order(c.System))
                .ThenBy(c => c.List.ListSortOrder)
                .ThenBy(c => c.Name, StringComparer.Ordinal)
                .ToList()))
            .ToList();
        _byKey = choices.ToDictionary(c => c.Key, StringComparer.Ordinal);
    }

    /// The choices under each heading, in order: the group code and its lists.
    public IReadOnlyList<(string Group, IReadOnlyList<OtherStatusChoice> Choices)> Groups { get; }

    public OtherStatusChoice? Find(string? key) =>
        key is { Length: > 0 } k && _byKey.TryGetValue(k.Trim(), out var choice) ? choice : null;

    public static string KeyOf(OtherStatusListCount list) =>
        list.ListKey ?? (list.Country is { } country ? $"{list.System}:{country}" : list.System);

    /// The list to show for a page about the country's taxa: the country's main list, or else the one
    /// of its national red lists with the most taxa. Null when the list has none of the page's taxa
    /// (taxaInText: how many of the page's taxa have a status in a list), or the country has no list.
    public OtherStatusChoice? DefaultFor(string? country, Func<OtherStatusChoice, int> taxaInText) {
        if (country is null) {
            return null;
        }
        var candidates = CountryLists.TryGetValue(country, out var key)
            ? Find(key) is { } main ? [main] : []
            : Groups.Where(g => g.Group == country).SelectMany(g => g.Choices).Where(c => c.System == OtherStatusSystems.NationalRedList).ToList();
        return candidates
            .Select(c => (Choice: c, Taxa: taxaInText(c)))
            .Where(c => c.Taxa > 0)
            .OrderByDescending(c => c.Taxa)
            .Select(c => c.Choice)
            .FirstOrDefault();
    }

    private static OtherStatusChoice Choose(OtherStatusListCount list) {
        var group = OtherStatusSystems.Find(list.System)?.Group ?? list.ListCountry ?? list.Country ?? string.Empty;
        var name = list.ListName ?? SiteText.OtherStatusList(list.System, list.Country).Label;
        var withCountry = NamedWithCountry.Contains(list.System);
        var heading = withCountry ? $"{name} ({SiteText.OtherStatusGroup(group)})" : name;
        // A name's closing bracket becomes a comma: "... of Switzerland (2016)" is "... of Switzerland, 2016".
        var label = withCountry ? $"{name}, {SiteText.OtherStatusGroup(group)}"
            : TrailingBracket().Replace(name.StartsWith("The ", StringComparison.Ordinal) ? name[4..] : name, ", $1");
        return new OtherStatusChoice(KeyOf(list), list, group, name, heading, label);
    }

    [System.Text.RegularExpressions.GeneratedRegex(@" \(([^()]+)\)$")]
    private static partial System.Text.RegularExpressions.Regex TrailingBracket();
}

/// The statuses in the chosen list of the taxa on a page, for its column in the page's tables.
public sealed record OtherStatusColumn(OtherStatusChoice Choice, IReadOnlyDictionary<long, IReadOnlyList<ChosenStatus>> Statuses) {
    public bool Has(long taxonId) => Statuses.ContainsKey(taxonId);
    public string Cell(long taxonId) => Choice.Cell(Statuses.GetValueOrDefault(taxonId));
}
