using System.Text.RegularExpressions;

// What JNCC's Master List does not say in a column of its own, read from what it does say:
//   ScopeOf   where a designation applies, from its code ("Designation abbreviation"): the whole UK
//             or Great Britain, part of the UK, or an international list (a convention, an EU
//             directive or regulation, IUCN's global or European red list);
//   StatusOf  the category code in a red list designation's code (RedList_GB_post2001-CR(PE): CR(PE))
//             and the bird red list's breeding or non-breeding population;
//   KingdomOf the kingdom, in IUCN's spelling, from JNCC's category and UKSI's taxon group;
//   RankOf    the rank, from the form of the name and the kingdom (JNCC gives no rank).
// A designation code that no rule knows gets no scope, so a new list is reported, not guessed.

namespace BeastieBot3.StatusLists;

internal static partial class JnccClassification {
    /// The whole United Kingdom or Great Britain.
    public const string Uk = "uk";
    /// Part of the UK: England, Scotland, Wales, Northern Ireland, or England and Wales.
    public const string Country = "country";
    /// A convention, an EU directive or regulation, or IUCN's global or European red list.
    public const string International = "international";

    public const string UnitedKingdom = "United Kingdom";
    public const string GreatBritain = "Great Britain";
    public const string England = "England";
    public const string Scotland = "Scotland";
    public const string Wales = "Wales";
    public const string NorthernIreland = "Northern Ireland";
    public const string EnglandAndWales = "England and Wales";

    // The designation codes of the 2026-06-09 file, by prefix, first match wins.
    private static readonly (Regex Code, string Scope, string Area)[] Rules = [
        (Rule(@"^(Bird_)?RedList_GB_"), Uk, GreatBritain),
        (Rule(@"^RedList_ENG_"), Country, England),
        (Rule(@"^WL$"), Country, England),                       // Waiting List of the Vascular Plant Red List for England
        (Rule(@"^RedList_Global_"), International, "World"),
        (Rule(@"^RedList_Europe_"), International, "Europe"),
        (Rule(@"^Bird-(Red|Amber)$"), Uk, UnitedKingdom),         // Birds of Conservation Concern
        (Rule(@"^Spider-Amber$"), Uk, GreatBritain),
        (Rule(@"^(NR|NS)-(includes|excludes)$"), Uk, GreatBritain), // Nationally Rare, Nationally Scarce
        (Rule(@"^Notable(-[AB])?$"), Uk, GreatBritain),
        (Rule(@"^Marine-(NR|NS)$"), Uk, GreatBritain),
        (Rule(@"^BAP-"), Uk, UnitedKingdom),
        (Rule(@"^WACA-"), Uk, GreatBritain),                      // Wildlife and Countryside Act 1981
        (Rule(@"^Protection_of_Badgers_Act"), Uk, GreatBritain),
        (Rule(@"^England_NERC"), Country, England),
        (Rule(@"^Scottish_Biodiversity_List$"), Country, Scotland),
        (Rule(@"^Env \(Wales\) Act"), Country, Wales),
        (Rule(@"^NI_Priority$"), Country, NorthernIreland),
        (Rule(@"^W\(NI\)O-"), Country, NorthernIreland),          // Wildlife (Northern Ireland) Order 1985
        (Rule(@"^ConsRegsNI-"), Country, NorthernIreland),
        (Rule(@"^HabReg-"), Country, EnglandAndWales),            // Conservation of Habitats and Species Regulations 2010 extend to England and Wales only
        (Rule(@"^Bern-"), International, "Europe"),
        (Rule(@"^(BirdsDir|HabDir|ECCITES)-"), International, "European Union"),
        (Rule(@"^CMS_AEWA"), International, "Africa-Eurasia"),
        (Rule(@"^CMS_ASCOBANS"), International, "North-East Atlantic and Baltic"),
        (Rule(@"^CMS_EUROBATS"), International, "Europe"),
        (Rule(@"^CMS_"), International, "World"),
        (Rule(@"^OSPAR$"), International, "North-East Atlantic"),
    ];

    private static Regex Rule(string pattern) => new(pattern, RegexOptions.CultureInvariant);

    /// Where the designation applies; null for a code no rule knows. A Great Britain designation is
    /// narrowed by its source or comment: JNCC codes the Extinct and Extinct in the Wild rows of the
    /// Vascular Plant Red List for England RedList_GB_post2001-EX/-EW, and the Wildlife and
    /// Countryside Act rows that no longer apply in Scotland say so only in their comments.
    public static (string Scope, string Area)? ScopeOf(string designationCode, string? source, string? comments) {
        foreach (var (code, scope, area) in Rules) {
            if (!code.IsMatch(designationCode)) {
                continue;
            }
            if (area == GreatBritain) {
                if (source is not null && source.Contains("Red List for England", StringComparison.OrdinalIgnoreCase)) {
                    return (Country, England);
                }
                if (comments is not null && NotInScotland().IsMatch(comments)) {
                    return (Country, EnglandAndWales);
                }
                if (comments is not null && EnglandOnly().IsMatch(comments)) {
                    return (Country, England);
                }
            }
            return (scope, area);
        }
        return null;
    }

    /// The category in a red list's designation code (VU, CR(PE), LR(cd), Insu, WL; Red or Amber for
    /// Birds of Conservation Concern and the spider list), and "breeding" or "non-breeding" for the
    /// bird red list's populations. Nulls for every other designation.
    public static (string? StatusCode, string? Population) StatusOf(string designationCode) {
        if (RedListCode().Match(designationCode) is { Success: true } red) {
            var population = red.Groups["season"].Value switch {
                "Breeding" => "breeding",
                "NonBreeding" => "non-breeding",
                _ => null,
            };
            return (red.Groups["code"].Value, population);
        }
        if (designationCode == "WL") {
            return ("WL", null);
        }
        if (ConcernCode().Match(designationCode) is { Success: true } concern) {
            return (concern.Groups["code"].Value, null);
        }
        return (null, null);
    }

    /// The kingdom in IUCN's spelling; null where JNCC's group spans kingdoms (algae, slime moulds).
    public static string? KingdomOf(string? category, string? taxonGroup) => category switch {
        "Invertebrate" or "Bird" or "Mammal" or "Fish" or "Reptile" or "Amphibian" => "ANIMALIA",
        "Vascular plant" => "PLANTAE",
        "Fungi" => "FUNGI",
        "Algae" when taxonGroup == "chromist" => "CHROMISTA",
        "Non-vascular plant" => taxonGroup switch {
            "lichen" => "FUNGI",
            "moss" or "liverwort" or "hornwort" or "stonewort" => "PLANTAE",
            _ => null,
        },
        _ => null,
    };

    /// The rank the form of the name shows: species, subspecies, variety, form, aggregate, section,
    /// hybrid, or "above species" for one word. Three words with no rank word are a subspecies only
    /// in an animal (a botanical name has "subsp." or "var."). Null for a name of another form
    /// ("Mycetoporus 'species A'", "Cantharis nigra (=thoracica)"). A subgenus in brackets after the
    /// genus and a trailing "s. lat.", "s.l." or "s. str." do not count.
    public static string? RankOf(string name, string? kingdom) {
        var plain = TrailingSense().Replace(Subgenus().Replace(name.Trim(), "$1 "), "");
        var words = plain.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Contains("x") || words.Contains("×") || plain.Contains(" = ", StringComparison.Ordinal)) {
            return "hybrid";
        }
        if (words.Contains("agg.") || plain.Contains('/', StringComparison.Ordinal)) {
            return "aggregate";
        }
        if (!PlainName().IsMatch(plain)) {
            return null;
        }
        if (words.Contains("sect.")) {
            return "section";
        }
        if (words.Contains("var.")) {
            return "variety";
        }
        if (words.Contains("f.") || words.Contains("forma") || words.Contains("form")) {
            return "form";
        }
        if (words.Contains("subsp.") || words.Contains("ssp.")) {
            return "subspecies";
        }
        return words.Length switch {
            1 => "above species",
            2 when IsEpithet(words[1]) => "species",
            3 when kingdom == "ANIMALIA" && IsEpithet(words[1]) && IsEpithet(words[2]) => "subspecies",
            _ => null,
        };
    }

    private static bool IsEpithet(string word) => word.Length > 0 && char.IsLower(word[0]);

    [GeneratedRegex(@"^(?:Bird_)?RedList_[A-Za-z]+_[A-Za-z0-9]+-(?<code>[^_]+)(?:_(?<season>Breeding|NonBreeding))?$")]
    private static partial Regex RedListCode();

    [GeneratedRegex(@"^(?:Bird|Spider)-(?<code>Red|Amber)$")]
    private static partial Regex ConcernCode();

    [GeneratedRegex(@"^(?:Designation )?does not apply (?:in|to) Scotland", RegexOptions.IgnoreCase)]
    private static partial Regex NotInScotland();

    [GeneratedRegex(@"^England(?:, other than the excluded waters,)? only", RegexOptions.IgnoreCase)]
    private static partial Regex EnglandOnly();

    [GeneratedRegex(@"^(\S+) \([A-Z][a-z]+\) ")]
    private static partial Regex Subgenus();

    [GeneratedRegex(@" (?:s\. ?lat\.|s\. ?str\.|s\.s\.?|s\.l\.)$")]
    private static partial Regex TrailingSense();

    // Letters, hyphens and the rank words' full stops only: no brackets, quotes, question marks or digits.
    [GeneratedRegex(@"^[\p{L}][\p{L}\-. ]*$")]
    private static partial Regex PlainName();
}
