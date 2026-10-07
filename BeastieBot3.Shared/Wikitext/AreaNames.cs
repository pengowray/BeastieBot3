using System.Globalization;
using System.Text;

namespace BeastieBot3.Shared.Wikitext;

/// A country or part of a country that IUCN codes in assessments (the site database's area table).
/// Country: for part of a country, the code of the country it is in.
public sealed record AreaName(string Code, string Name, string? Country) {
    public bool IsCountry => Country is null && Code.Length == 2;

    // IUCN's catalogue forms, as English Wikipedia names them.
    private static readonly Dictionary<string, string> Common = new(StringComparer.Ordinal) {
        ["Bolivia, Plurinational States of"] = "Bolivia",
        ["Bolivia, Plurinational State of"] = "Bolivia",
        ["Congo, The Democratic Republic of the"] = "Democratic Republic of the Congo",
        ["Iran, Islamic Republic of"] = "Iran",
        ["Korea, Democratic People's Republic of"] = "North Korea",
        ["Korea, Republic of"] = "South Korea",
        ["Lao People's Democratic Republic"] = "Laos",
        ["Micronesia, Federated States of"] = "Federated States of Micronesia",
        ["Moldova, Republic of"] = "Moldova",
        ["Palestine, State of"] = "Palestine",
        ["Russian Federation"] = "Russia",
        ["Syrian Arab Republic"] = "Syria",
        ["Taiwan, Province of China"] = "Taiwan",
        ["Tanzania, United Republic of"] = "Tanzania",
        ["Venezuela, Bolivarian Republic of"] = "Venezuela",
        ["Viet Nam"] = "Vietnam",
        ["Virgin Islands, British"] = "British Virgin Islands",
        ["Virgin Islands, U.S."] = "United States Virgin Islands",
        ["Brunei Darussalam"] = "Brunei",
        ["Falkland Islands (Malvinas)"] = "Falkland Islands",
        ["Holy See (Vatican City State)"] = "Vatican City",
    };

    // Names that take "the" in a sentence ("native to the Netherlands"), besides those starting
    // "United", "Republic" or "Democratic" and those ending "Islands".
    private static readonly HashSet<string> WithThe = new(StringComparer.Ordinal) {
        "Netherlands", "Philippines", "Bahamas", "Gambia", "Maldives", "Comoros", "Central African Republic",
        "Dominican Republic", "Czech Republic", "Seychelles", "Federated States of Micronesia",
    };

    /// The name as English Wikipedia writes it: "Tanzania" for "Tanzania, United Republic of",
    /// "Hawaiian Islands" for "Hawaiian Is.".
    public string DisplayName {
        get {
            if (Common.TryGetValue(Name, out var common)) {
                return common;
            }
            var name = Name.EndsWith(" Is.", StringComparison.Ordinal) ? Name[..^4] + " Islands"
                : Name.EndsWith(" I.", StringComparison.Ordinal) ? Name[..^3] + " Island"
                : Name;
            return name.Replace(" Is. ", " Islands ", StringComparison.Ordinal);
        }
    }

    /// The name in a sentence: "the United States", "the Hawaiian Islands", "Brazil".
    public string SentenceName {
        get {
            var name = DisplayName;
            return WithThe.Contains(name) || name.StartsWith("United ", StringComparison.Ordinal)
                || name.StartsWith("Republic ", StringComparison.Ordinal) || name.StartsWith("Democratic ", StringComparison.Ordinal)
                || name.EndsWith(" Islands", StringComparison.Ordinal)
                ? "the " + name
                : name;
        }
    }
}

/// Which of a taxon's records for an area put it on a list of that area.
public enum AreaMode {
    /// Native or reintroduced: what a list of an area's species usually means.
    Native,
    /// Endemic to the area ("List of endemic birds of Brazil").
    Endemic,
    /// Native, reintroduced, introduced or assisted colonisation.
    NativeAndIntroduced,
    /// Every record, vagrant and origin uncertain included.
    All,
}

/// Finds the area a list's title is of ("List of birds of Brazil", "List of mammals of the United
/// States", "List of birds of Hawaii") among IUCN's area names, with the usual English names for the
/// ones IUCN writes another way ("Viet Nam", "Tanzania, United Republic of", "Hawaiian Is.").
public sealed class AreaNames {
    // Names English Wikipedia uses for areas IUCN names otherwise, by area code.
    private static readonly (string Name, string Code)[] Aliases = [
        ("united states", "US"), ("usa", "US"), ("united states of america", "US"),
        ("united kingdom", "GB"), ("great britain", "GB"), ("britain", "GB"), ("uk", "GB"),
        ("vietnam", "VN"), ("russia", "RU"), ("ivory coast", "CI"), ("laos", "LA"), ("syria", "SY"), ("taiwan", "TW"),
        ("democratic republic of the congo", "CD"), ("dr congo", "CD"), ("republic of the congo", "CG"),
        ("cape verde", "CV"), ("czech republic", "CZ"), ("czechia", "CZ"), ("turkey", "TR"), ("swaziland", "SZ"), ("eswatini", "SZ"),
        ("burma", "MM"), ("myanmar", "MM"), ("east timor", "TL"), ("timor leste", "TL"), ("brunei", "BN"),
        ("south korea", "KR"), ("north korea", "KP"), ("korea", "KR"), ("north macedonia", "MK"), ("macedonia", "MK"),
        ("palestine", "PS"), ("vatican city", "VA"), ("micronesia", "FM"), ("federated states of micronesia", "FM"),
        ("falkland islands", "FK"), ("saint helena", "SH"), ("bolivia", "BO"), ("venezuela", "VE"), ("iran", "IR"),
        ("tanzania", "TZ"), ("moldova", "MD"), ("the bahamas", "BS"), ("bahamas", "BS"), ("the gambia", "GM"), ("gambia", "GM"),
    ];

    // Names of places larger than an area: a title of one names no area.
    private static readonly HashSet<string> Regions = [
        "the world", "world", "europe", "africa", "asia", "oceania", "north america", "south america", "central america",
        "the americas", "americas", "antarctica", "the caribbean", "caribbean", "the middle east", "middle east",
        "southeast asia", "south asia", "east asia", "west africa", "east africa", "southern africa", "central africa",
        "the arctic", "arctic", "the pacific", "the atlantic", "the indian ocean", "the mediterranean", "mediterranean",
    ];

    private readonly Dictionary<string, AreaName> _byKey = new(StringComparer.Ordinal);
    private readonly Dictionary<string, AreaName> _byCode = new(StringComparer.Ordinal);

    public AreaNames(IEnumerable<AreaName> areas) {
        var list = areas.ToList();
        foreach (var area in list) {
            _byCode[area.Code] = area;
        }
        // A name two areas share goes to the country ("Georgia"), else to neither.
        var ambiguous = new HashSet<string>(StringComparer.Ordinal);
        void Add(string name, AreaName area) {
            var key = Key(name);
            if (key.Length == 0 || ambiguous.Contains(key)) {
                return;
            }
            if (_byKey.TryGetValue(key, out var known) && known.Code != area.Code) {
                if (known.IsCountry == area.IsCountry) {
                    _byKey.Remove(key);
                    ambiguous.Add(key);
                } else if (area.IsCountry) {
                    _byKey[key] = area;
                }
                return;
            }
            _byKey[key] = area;
        }
        foreach (var area in list) {
            Add(area.Name, area);
            if (area.DisplayName != area.Name) {
                Add(area.DisplayName, area);
            }
        }
        // "Tanzania, United Republic of": the part before the comma, unless an area has that name
        // ("Congo" is the Republic of the Congo, not "Congo, The Democratic Republic of the").
        var exact = new HashSet<string>(_byKey.Keys.Concat(ambiguous), StringComparer.Ordinal);
        foreach (var area in list) {
            var comma = area.Name.IndexOf(',');
            if (comma > 0 && !exact.Contains(Key(area.Name[..comma]))) {
                Add(area.Name[..comma], area);
            }
        }
        foreach (var (name, code) in Aliases) {
            if (_byCode.TryGetValue(code, out var area)) {
                _byKey[Key(name)] = area;
            }
        }
        if (list.FirstOrDefault(a => Key(a.Name) == "hawaiian islands") is { } hawaii) {
            _byKey[Key("Hawaii")] = hawaii;
        }
    }

    public AreaName? ByCode(string? code) => code is not null && _byCode.TryGetValue(code, out var area) ? area : null;

    public IEnumerable<AreaName> All => _byCode.Values;

    /// The area a name names, or null.
    public AreaName? Find(string name) {
        var key = Key(name);
        if (Regions.Contains(key)) {
            return null;
        }
        if (_byKey.TryGetValue(key, out var area)) {
            return area;
        }
        return key.StartsWith("the ", StringComparison.Ordinal) && _byKey.TryGetValue(key[4..], out var withoutThe) ? withoutThe : null;
    }

    /// The area a list's title is of: the words after its last " of " or " in " ("List of birds of
    /// Brazil", "List of reptiles in Tasmania"), without a closing bracket; null when they name no area.
    public AreaName? FromTitle(string? title) {
        if (string.IsNullOrWhiteSpace(title)) {
            return null;
        }
        var t = title.Trim();
        var bracket = t.IndexOf(" (", StringComparison.Ordinal);
        if (bracket > 0) {
            t = t[..bracket];
        }
        // Try the longest ending first, so "List of birds of the Democratic Republic of the Congo" finds
        // the whole name before "the Congo", and "List of birds of the Isle of Man" finds "the Isle of Man".
        var starts = new List<int>();
        foreach (var word in new[] { " of ", " in " }) {
            for (var i = t.IndexOf(word, StringComparison.OrdinalIgnoreCase); i >= 0; i = t.IndexOf(word, i + 1, StringComparison.OrdinalIgnoreCase)) {
                starts.Add(i + word.Length);
            }
        }
        foreach (var start in starts.Order()) {
            if (Find(t[start..]) is { } area) {
                return area;
            }
        }
        // The first " of " is the "List of": the rest is the subject, not an area.
        return null;
    }

    /// "endemic" in the title: a list of the area's endemic taxa.
    public static AreaMode ModeFromTitle(string? title) =>
        title is not null && title.Contains("endemic", StringComparison.OrdinalIgnoreCase) ? AreaMode.Endemic : AreaMode.Native;

    /// Lower case, no accents or punctuation, "&" as "and", "Is." as "islands", "St." as "saint".
    public static string Key(string name) {
        var decomposed = name.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(decomposed.Length);
        foreach (var c in decomposed) {
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark) {
                continue;
            }
            sb.Append(char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : c == '&' ? '&' : ' ');
        }
        var words = sb.ToString().Replace("&", " and ").Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(w => w switch { "is" => "islands", "st" => "saint", "ste" => "sainte", _ => w });
        return string.Join(' ', words);
    }
}
