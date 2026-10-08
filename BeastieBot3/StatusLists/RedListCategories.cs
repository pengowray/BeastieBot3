using System.Text.RegularExpressions;

// The IUCN category of a status as a red list writes it in threatStatus. National lists that follow
// IUCN's regional guidelines write the codes (EX, EW, RE, CR, EN, VU, NT, LC, DD, NA, NE), the names
// ("Least Concern"), or both ("Least Concern (LC)"), sometimes with a mark of their own:
//   CR(PE), CR-PE, CR*          CR, possibly or presumed extinct (Switzerland, Ecuador, France)
//   VU° and the like            a category lowered for immigration from outside the country (Norway)
//   NAa, NAb                    France's two kinds of NA (introduced after 1500; occasional)
//   Lower Risk/near threatened  IUCN's categories before 2001: LR/nt and LR/cd are NT, LR/lc is LC
// Anything else is not an IUCN category and has no code: Germany's 0, 1, 2, 3, G, R, V, D, *, nb,
// Ukraine's categories, Luxembourg's R, the Netherlands' REW. Those keep their label from the
// dataset's categories in national-red-lists.yml (Label).

namespace BeastieBot3.StatusLists;

internal static partial class RedListCategories {
    /// The codes a status can have.
    public static readonly IReadOnlyList<string> Codes = ["EX", "EW", "RE", "CR", "EN", "VU", "NT", "LC", "DD", "NA", "NE"];

    private static readonly Dictionary<string, string> Names = new(StringComparer.OrdinalIgnoreCase) {
        ["Extinct"] = "EX",
        ["Extinct in the Wild"] = "EW",
        ["Regionally Extinct"] = "RE",
        ["Critically Endangered"] = "CR",
        ["Endangered"] = "EN",
        ["Vulnerable"] = "VU",
        ["Near Threatened"] = "NT",
        ["Least Concern"] = "LC",
        ["Data Deficient"] = "DD",
        ["Not Applicable"] = "NA",
        ["Not Evaluated"] = "NE",
        ["Lower Risk/near threatened"] = "NT",
        ["Lower Risk/conservation dependent"] = "NT",
        ["Lower Risk/least concern"] = "LC",
        ["LR/nt"] = "NT",
        ["LR/cd"] = "NT",
        ["LR/lc"] = "LC",
    };

    /// The IUCN code of a status as the archive writes it, or null when it is not an IUCN category.
    public static string? ToIucnCode(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return null;
        }
        var text = Collapse(status);
        if (Code(text) is { } code) {
            return code;
        }
        if (Names.TryGetValue(text, out var named)) {
            return named;
        }
        // "Least Concern (LC)": a name with its code in brackets.
        if (NameWithCode().Match(text) is { Success: true } both && Names.TryGetValue(both.Groups["name"].Value.Trim(), out var fromName)
            && Code(both.Groups["code"].Value) == fromName) {
            return fromName;
        }
        // CR(PE), CR (PEW), CR-PE, CR*, VU°, VUº, NAa, NAb.
        if (MarkedCode().Match(text) is { Success: true } marked) {
            var baseCode = Code(marked.Groups["code"].Value);
            var mark = marked.Groups["mark"].Value;
            if (baseCode == "CR" && PossiblyExtinctMark().IsMatch(mark)) {
                return "CR";
            }
            if (baseCode is not null && mark is "°" or "º" or "*" && baseCode != "NA" && baseCode != "NE") {
                return baseCode;
            }
            if (baseCode == "NA" && mark is "a" or "b" or "c" or "d") {
                return "NA";
            }
        }
        return null;
    }

    /// The label the dataset's categories give a status, matched without regard to case or to the
    /// kind of space ("зниклий в<U+00A0>природі" matches "зниклий в природі").
    public static string? Label(IReadOnlyDictionary<string, string> categories, string status) {
        if (categories.Count == 0) {
            return null;
        }
        var text = Collapse(status);
        foreach (var (code, label) in categories) {
            if (string.Equals(Collapse(code), text, StringComparison.OrdinalIgnoreCase)) {
                return label;
            }
        }
        return null;
    }

    private static string? Code(string text) =>
        Codes.FirstOrDefault(c => string.Equals(c, text, StringComparison.OrdinalIgnoreCase));

    // Every run of white space (no-break spaces included) as one space, trimmed.
    private static string Collapse(string text) => Spaces().Replace(text, " ").Trim();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();

    [GeneratedRegex(@"^(?<name>[A-Za-z /]+?)\s*\((?<code>[A-Za-z]{2})\)$")]
    private static partial Regex NameWithCode();

    [GeneratedRegex(@"^(?<code>[A-Za-z]{2})(?<mark>\s*\(\s*PEW?\s*\)|-PEW?|[°º*]|[abcd])$")]
    private static partial Regex MarkedCode();

    [GeneratedRegex(@"PEW?|\*")]
    private static partial Regex PossiblyExtinctMark();
}
