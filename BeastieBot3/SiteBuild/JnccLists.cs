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

/// One list of JNCC designations as the site shows it. Key: other_status_list.list_key.
internal sealed record JnccList(string Key, string Name, int SortOrder);

internal static partial class JnccLists {
    /// The list a designation belongs to and the status to show, from its code, its name as JNCC
    /// writes it, its status code and the area it applies to. Null for a code the site does not show.
    public static (JnccList List, string Status)? Classify(string code, string designation, string? statusCode, string? area, string? population) {
        var season = population?.Trim().ToLowerInvariant() switch {
            "breeding" => " (breeding)",
            "non-breeding" or "nonbreeding" or "wintering" => " (non-breeding)",
            _ => "",
        };
        if (code.StartsWith("RedList_GB_post2001-", StringComparison.Ordinal) || code.StartsWith("Bird_RedList_GB_post2001-", StringComparison.Ordinal)
            || code.StartsWith("RedList_ENG_post2001-", StringComparison.Ordinal) || code == "WL") {
            var england = string.Equals(area, "England", StringComparison.OrdinalIgnoreCase);
            var list = england ? new JnccList("jncc-redlist-england", "England Red List", 11) : new JnccList("jncc-redlist-gb", "Great Britain Red List", 10);
            var status = code == "WL" ? "Waiting List" : RedListCategory(statusCode) ?? Tidy(designation);
            return (list, status + season);
        }
        if (code.StartsWith("RedList_GB_post94-", StringComparison.Ordinal)) {
            return (new JnccList("jncc-redlist-gb-1994", "Great Britain Red List (1994 IUCN criteria)", 60), (RedListCategory(statusCode) ?? Tidy(designation)) + season);
        }
        if (code.StartsWith("RedList_GB_Pre94-", StringComparison.Ordinal)) {
            return (new JnccList("jncc-redlist-gb-pre1994", "Great Britain Red Data Book (pre-1994 IUCN categories)", 61),
                Tidy(Regex.Replace(designation, @"^IUCN \(pre 1994\) - ", "")) + season);
        }
        return code switch {
            "Bird-Red" or "Bird-Amber" => (new JnccList("jncc-bocc", "Birds of Conservation Concern 5", 20), code == "Bird-Red" ? "Red" : "Amber"),
            "Spider-Amber" => (new JnccList("jncc-spiders", "Spider status review, Great Britain", 21), "Amber"),
            "NR-includes" or "NR-excludes" => (Rarity, "Nationally Rare"),
            "NS-includes" or "NS-excludes" => (Rarity, "Nationally Scarce"),
            "Notable" => (Rarity, "Nationally Notable"),
            "Notable-A" => (Rarity, "Nationally Notable A"),
            "Notable-B" => (Rarity, "Nationally Notable B"),
            "Marine-NR" => (Rarity, "Nationally Rare (marine)"),
            "Marine-NS" => (Rarity, "Nationally Scarce (marine)"),
            "Protection_of_Badgers_Act_1992" => (new JnccList("jncc-badgers", "Protection of Badgers Act 1992", 44), "Protected"),
            "England_NERC_S.41" => (new JnccList("jncc-nerc-s41", "NERC Act 2006, section 41", 50), "Species of principal importance"),
            "Env (Wales) Act S7" => (new JnccList("jncc-wales-s7", "Environment (Wales) Act 2016, section 7", 51), "Species of principal importance"),
            "Scottish_Biodiversity_List" => (new JnccList("jncc-sbl", "Scottish Biodiversity List", 52), "Listed"),
            "NI_Priority" => (new JnccList("jncc-ni-priority", "Northern Ireland Priority Species List", 53), "Priority species"),
            "BAP-2007" => (new JnccList("jncc-uk-bap", "UK Biodiversity Action Plan (2007)", 54), "Priority species"),
            _ when code.StartsWith("WACA-", StringComparison.Ordinal) =>
                (new JnccList("jncc-waca", "Wildlife and Countryside Act 1981", 40), Schedule(designation)),
            _ when code.StartsWith("HabReg-", StringComparison.Ordinal) =>
                (new JnccList("jncc-habregs", "Conservation of Habitats and Species Regulations", 41), Schedule(designation)),
            _ when code.StartsWith("W(NI)O-", StringComparison.Ordinal) =>
                (new JnccList("jncc-wnio", "Wildlife (Northern Ireland) Order 1985", 42), Schedule(designation)),
            _ when code.StartsWith("ConsRegsNI-", StringComparison.Ordinal) =>
                (new JnccList("jncc-consregs-ni", "Conservation (Natural Habitats, etc.) Regulations (Northern Ireland) 1995", 43), Schedule(designation)),
            _ => null,
        };
    }

    private static readonly JnccList Rarity = new("jncc-rarity", "Rare and scarce species, Great Britain", 30);

    // "Schedule 5 Section 9.4b" -> "Schedule 5, section 9.4b"; "Schedule 1 - Part 1" -> "Schedule 1, Part 1".
    private static string Schedule(string designation) {
        var text = Regex.Replace(designation.Trim(), @"\s+-\s+", ", ");
        return Regex.Replace(text, @"(Schedule \d+)\s+(Section|Part)\b", m => $"{m.Groups[1].Value}, {(m.Groups[2].Value == "Section" ? "section" : "Part")}");
    }

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
        new(list.Key, OtherStatusSystems.Jncc, "GB", null, list.Name, null, list.SortOrder, "Joint Nature Conservation Committee (JNCC)",
            StatusLists.JnccDesignations.Licence, "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/", citation,
            StatusLists.JnccDesignations.ResourcePageUrl, version, fetched);
}
