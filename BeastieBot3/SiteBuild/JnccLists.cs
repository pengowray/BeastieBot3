using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

// How the site shows JNCC's designations (`statuses jncc-import`, table jncc_designation): which list
// a designation code belongs to, the list's name and place among the UK lists, and the status text.
// JNCC's codes name the list and the status together ("RedList_GB_post2001-VU", "WACA-Sch5_sect9.4b",
// "Bird-Red"); the site shows one row per list with the status in the Status column. Only designations
// for the UK, Great Britain or one of its countries are shown (scope 'uk' or 'country'): international
// ones (Bern, Bonn, EU directives, EC CITES annexes, OSPAR) are listed for UK taxa only, so they would
// read as the whole picture.

namespace BeastieBot3.SiteBuild;

/// One list of JNCC designations as the site shows it. Key: other_status_list.list_key. Title: the
/// row label's hover title, or null.
internal sealed record JnccList(string Key, string Name, int SortOrder, string? Title = null);

/// A designation as the site shows it: its list, the status, and for a law, the section of the
/// schedule (JNCC's "Section 9.4b" as UK law writes it, "9(4)(b)"), or null.
internal sealed record JnccStatus(JnccList List, string Status, string? Section = null);

internal static partial class JnccLists {
    /// The list a designation belongs to and the status to show, from its code, its name as JNCC
    /// writes it, its status code and the area it applies to. Null for a code the site does not show.
    public static JnccStatus? Classify(string code, string designation, string? statusCode, string? area, string? population) {
        var season = population?.Trim().ToLowerInvariant() switch {
            "breeding" => " (breeding)",
            "non-breeding" or "nonbreeding" or "wintering" => " (non-breeding)",
            _ => "",
        };
        if (code.StartsWith("RedList_GB_post2001-", StringComparison.Ordinal) || code.StartsWith("Bird_RedList_GB_post2001-", StringComparison.Ordinal)
            || code.StartsWith("RedList_ENG_post2001-", StringComparison.Ordinal) || code == "WL") {
            var england = string.Equals(area, "England", StringComparison.OrdinalIgnoreCase);
            var redList = england ? new JnccList("jncc-redlist-england", "England Red List", 11) : new JnccList("jncc-redlist-gb", "Great Britain Red List", 10);
            var category = code == "WL" ? "Waiting List" : RedListCategory(statusCode) ?? Tidy(designation);
            return new JnccStatus(redList, category + season);
        }
        if (code.StartsWith("RedList_GB_post94-", StringComparison.Ordinal)) {
            return new JnccStatus(new JnccList("jncc-redlist-gb-1994", "Great Britain Red List (1994 IUCN criteria)", 60), (RedListCategory(statusCode) ?? Tidy(designation)) + season);
        }
        if (code.StartsWith("RedList_GB_Pre94-", StringComparison.Ordinal)) {
            return new JnccStatus(new JnccList("jncc-redlist-gb-pre1994", "Great Britain Red Data Book (pre-1994 IUCN categories)", 61),
                Tidy(Regex.Replace(designation, @"^IUCN \(pre 1994\) - ", "")) + season);
        }
        var (list, status) = code switch {
            "Bird-Red" or "Bird-Amber" => (new JnccList("jncc-bocc", "Birds of Conservation Concern 5", 20), code == "Bird-Red" ? "Red" : "Amber"),
            "Spider-Amber" => (new JnccList("jncc-spiders", "Scarce and threatened spiders of Great Britain (2017)", 21), "Amber list"),
            "NR-includes" or "NR-excludes" => (Rarity, "Nationally Rare"),
            "NS-includes" or "NS-excludes" => (Rarity, "Nationally Scarce"),
            "Notable" => (Rarity, "Nationally Notable"),
            "Notable-A" => (Rarity, "Nationally Notable A"),
            "Notable-B" => (Rarity, "Nationally Notable B"),
            "Marine-NR" => (Rarity, "Nationally Rare (marine)"),
            "Marine-NS" => (Rarity, "Nationally Scarce (marine)"),
            "Protection_of_Badgers_Act_1992" => (new JnccList("jncc-badgers", "Protection of Badgers Act 1992", 44), "Protected"),
            "England_NERC_S.41" => (new JnccList("jncc-nerc-s41", "NERC Act 2006, section 41 (England)", 50,
                "Natural Environment and Rural Communities Act 2006"), "Species of principal importance"),
            "Env (Wales) Act S7" => (new JnccList("jncc-wales-s7", "Environment (Wales) Act 2016, section 7", 51), "Species of principal importance"),
            "Scottish_Biodiversity_List" => (new JnccList("jncc-sbl", "Scottish Biodiversity List", 52), "Listed"),
            "NI_Priority" => (new JnccList("jncc-ni-priority", "Northern Ireland Priority Species List", 53), "Priority species"),
            "BAP-2007" => (new JnccList("jncc-uk-bap", "UK Biodiversity Action Plan (2007)", 54), "Priority species"),
            _ => ((JnccList?)null, (string?)null),
        };
        if (list is not null) {
            return new JnccStatus(list, status!);
        }
        list = code switch {
            _ when code.StartsWith("WACA-", StringComparison.Ordinal) => new JnccList("jncc-waca", "Wildlife and Countryside Act 1981", 40),
            _ when code.StartsWith("HabReg-", StringComparison.Ordinal) =>
                new JnccList("jncc-habregs", "Conservation of Habitats and Species Regulations 2010", 41),
            _ when code.StartsWith("W(NI)O-", StringComparison.Ordinal) => new JnccList("jncc-wnio", "Wildlife (Northern Ireland) Order 1985", 42),
            _ when code.StartsWith("ConsRegsNI-", StringComparison.Ordinal) =>
                new JnccList("jncc-consregs-ni", "Conservation (Natural Habitats, etc.) Regulations (Northern Ireland) 1995", 43),
            _ => null,
        };
        if (list is null) {
            return null;
        }
        var (schedule, section) = Schedule(designation);
        return new JnccStatus(list, schedule, section);
    }

    private static readonly JnccList Rarity = new("jncc-rarity", "Rare and scarce species, Great Britain", 30);

    // "Schedule 5 Section 9.4b" -> ("Schedule 5", "9(4)(b)"); "Schedule 1 - Part 1" -> ("Schedule 1, Part 1", null);
    // "Schedule 5 Section 9.1 (killing/injuring)" -> ("Schedule 5", "9(1) (killing or injuring)").
    internal static (string Schedule, string? Section) Schedule(string designation) {
        var text = designation.Trim();
        var part = Regex.Match(text, @"^Schedule (\d+)\s*-?\s*Part (\d+)$");
        if (part.Success) {
            return ($"Schedule {part.Groups[1].Value}, Part {part.Groups[2].Value}", null);
        }
        var section = Regex.Match(text, @"^Schedule (\d+)\s+Section (\d+)\.(\d+)\.?([A-Za-z])?(?:\s+\((.+)\))?$");
        if (section.Success) {
            var letter = section.Groups[4].Value;
            var number = section.Groups[2].Value + "(" + section.Groups[3].Value
                + (letter.Length == 1 && char.IsUpper(letter[0]) ? letter + ")" : ")" + (letter.Length == 1 ? $"({letter})" : ""));
            var note = section.Groups[5].Success ? $" ({section.Groups[5].Value.Replace("/", " or ", StringComparison.Ordinal)})" : "";
            return ($"Schedule {section.Groups[1].Value}", number + note);
        }
        var schedule = Regex.Match(text, @"^Schedule (\d+)$");
        return schedule.Success ? ($"Schedule {schedule.Groups[1].Value}", null) : (Tidy(text), null);
    }

    /// The sections of a schedule as a line under it: "section 9(2)", "sections 9(4)(b) and 9(5)(a)",
    /// "sections 9(1) (taking), 9(2) and 9(4)(b)"; null when none.
    public static string? Sections(IReadOnlyList<string> sections) => sections.Count switch {
        0 => null,
        1 => "section " + sections[0],
        _ => "sections " + string.Join(", ", sections.Take(sections.Count - 1)) + " and " + sections[^1],
    };

    // A red list category from its code, as IUCN names it.
    private static string? RedListCategory(string? code) => code?.Trim() switch {
        "EX" => "Extinct",
        "EW" => "Extinct in the Wild",
        "RE" => "Regionally Extinct",
        "CR(PE)" => "Critically Endangered (Possibly Extinct)",
        "CR" => "Critically Endangered",
        "EN" => "Endangered",
        "VU" => "Vulnerable",
        "NT" => "Near Threatened",
        "LC" => "Least Concern",
        "DD" => "Data Deficient",
        "NA" => "Not Applicable",
        "NE" => "Not Evaluated",
        _ => null,
    };

    // JNCC's capitals as written, with the first letter of each word capitalised ("Insufficiently known").
    private static string Tidy(string text) => string.Join(' ', text.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries)
        .Select(w => w.Length > 0 && char.IsLower(w[0]) ? char.ToUpperInvariant(w[0]) + w[1..] : w));

    /// The lists in their order, for other_status_list.
    public static OtherStatusList ToListRow(JnccList list, string? fetched, string? version, string? citation) =>
        new(list.Key, OtherStatusSystems.Jncc, "GB", null, list.Name, list.Title, list.SortOrder, "Joint Nature Conservation Committee (JNCC)",
            StatusLists.JnccDesignations.Licence, "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/", citation,
            StatusLists.JnccDesignations.ResourcePageUrl, version, fetched);
}
