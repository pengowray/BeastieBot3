namespace BeastieBot3.Shared.SiteData;

/// The names of NatureServe's codes for the states, provinces and territories of its national and
/// subnational ranks. NatureServe's search gives only the codes. Under the United States: the states,
/// DC and NN (the Navajo Nation); under Canada: the provinces and territories, with NF (the island of
/// Newfoundland) and LB (Labrador) apart.
public static class NatureServePlaces {
    private static readonly Dictionary<string, string> UnitedStates = new(StringComparer.OrdinalIgnoreCase) {
        ["AL"] = "Alabama", ["AK"] = "Alaska", ["AZ"] = "Arizona", ["AR"] = "Arkansas", ["CA"] = "California",
        ["CO"] = "Colorado", ["CT"] = "Connecticut", ["DE"] = "Delaware", ["DC"] = "District of Columbia",
        ["FL"] = "Florida", ["GA"] = "Georgia", ["HI"] = "Hawaii", ["ID"] = "Idaho", ["IL"] = "Illinois",
        ["IN"] = "Indiana", ["IA"] = "Iowa", ["KS"] = "Kansas", ["KY"] = "Kentucky", ["LA"] = "Louisiana",
        ["ME"] = "Maine", ["MD"] = "Maryland", ["MA"] = "Massachusetts", ["MI"] = "Michigan", ["MN"] = "Minnesota",
        ["MS"] = "Mississippi", ["MO"] = "Missouri", ["MT"] = "Montana", ["NE"] = "Nebraska", ["NV"] = "Nevada",
        ["NH"] = "New Hampshire", ["NJ"] = "New Jersey", ["NM"] = "New Mexico", ["NY"] = "New York",
        ["NC"] = "North Carolina", ["ND"] = "North Dakota", ["OH"] = "Ohio", ["OK"] = "Oklahoma", ["OR"] = "Oregon",
        ["PA"] = "Pennsylvania", ["RI"] = "Rhode Island", ["SC"] = "South Carolina", ["SD"] = "South Dakota",
        ["TN"] = "Tennessee", ["TX"] = "Texas", ["UT"] = "Utah", ["VT"] = "Vermont", ["VA"] = "Virginia",
        ["WA"] = "Washington", ["WV"] = "West Virginia", ["WI"] = "Wisconsin", ["WY"] = "Wyoming",
        ["NN"] = "Navajo Nation", ["PR"] = "Puerto Rico", ["VI"] = "US Virgin Islands", ["GU"] = "Guam",
        ["AS"] = "American Samoa", ["MP"] = "Northern Mariana Islands",
    };

    private static readonly Dictionary<string, string> Canada = new(StringComparer.OrdinalIgnoreCase) {
        ["AB"] = "Alberta", ["BC"] = "British Columbia", ["MB"] = "Manitoba", ["NB"] = "New Brunswick",
        ["NF"] = "Newfoundland (island)", ["LB"] = "Labrador", ["NL"] = "Newfoundland and Labrador",
        ["NS"] = "Nova Scotia", ["NT"] = "Northwest Territories", ["NU"] = "Nunavut", ["ON"] = "Ontario",
        ["PE"] = "Prince Edward Island", ["QC"] = "Quebec", ["SK"] = "Saskatchewan", ["YT"] = "Yukon",
    };

    private static readonly HashSet<string> States = UnitedStates
        .Where(p => p.Key is not ("DC" or "NN" or "PR" or "VI" or "GU" or "AS" or "MP"))
        .Select(p => p.Value)
        .ToHashSet(StringComparer.Ordinal);

    /// Whether a name is one of the 50 US states (not DC, the Navajo Nation or a territory).
    public static bool IsUsState(string name) => States.Contains(name);

    /// The name of a state, province or territory, or the code itself when it is not known.
    public static string Name(string nationCode, string subnationCode) {
        var names = nationCode.ToUpperInvariant() switch {
            "US" => UnitedStates,
            "CA" => Canada,
            _ => null,
        };
        return names?.GetValueOrDefault(subnationCode.Trim()) ?? subnationCode.Trim();
    }
}
