using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's counts by category of the updated wikitext, and its choice of another list
// whose statuses are shown beside IUCN's (OtherStatusChoices).

public static partial class UpdateText {
    // Counts by category (IucnStatusesSummary.Counts), under the result's summary.
    public const string CountsLabel = "Statuses in the updated wikitext:";
    public static string CountsTotal(int items) => $"Total {SiteFormat.Number(items)}";

    /// After the CR count: "(including 1 possibly extinct)", or with both kinds "(including 1 possibly
    /// extinct, 2 possibly extinct in the wild)"; null when there are none.
    public static string? CountsPossiblyExtinct(int possiblyExtinct, int inTheWild) {
        var parts = new List<string>();
        if (possiblyExtinct > 0) {
            parts.Add($"{SiteFormat.Number(possiblyExtinct)} possibly extinct");
        }
        if (inTheWild > 0) {
            parts.Add($"{SiteFormat.Number(inTheWild)} possibly extinct in the wild");
        }
        return parts.Count == 0 ? null : $"(including {string.Join(", ", parts)})";
    }

    /// The help line under the counts: what was counted, the codes not counted, and how the LR codes count.
    public static string CountsHelp(IucnStatusesSummary.Counts counts) {
        var item = counts.FromRows ? "row" : "template";
        var text = counts.FromRows
            ? "Counted from the iucn-status of each {{Species table/row}}, once per row, so a species with two rows is counted twice. A row with no iucn-status is counted as NE."
            : "Counted from the code of each {{IUCN status}} template, once per template, so a species with two templates is counted twice.";
        if (counts.Uncounted > 0) {
            var codes = string.Join(" or ", (counts.UncountedCodes ?? []).Select(c => c.Length == 0 ? "blank" : c));
            text += counts.Uncounted == 1
                ? $" 1 {item} is not counted: its {(counts.FromRows ? "status" : "code")} is {codes}, and the {{{{IUCN statuses}}}} box has no parameter for it."
                : $" {SiteFormat.Number(counts.Uncounted)} {item}s are not counted: their {(counts.FromRows ? "statuses are" : "codes are")} {codes}, and the {{{{IUCN statuses}}}} box has no parameter for these.";
        }
        if (counts.LowerRisk) {
            text += " LR/nt and LR/cd are counted as NT, and LR/lc as LC.";
        }
        return text;
    }

    public const string CountsBoxLabel = "{{IUCN statuses}} box for the updated wikitext:";

    // The choice of another list.
    public const string OtherListLabel = "Other conservation status";
    public const string OtherListNoneOption = "None";
    public static string OtherListOption(string name, int taxa) =>
        $"{name} ({SiteFormat.Number(taxa)} {(taxa == 1 ? "taxon" : "taxa")})";
    public const string OtherListHelp =
        "Adds a column with each taxon's status in the chosen list to the report and to the tables of the “Comparison with” section, including the table of taxa missing from the page. The wikitext is not changed. The number after each list is how many IUCN Red List taxa have a status in that list. When you load a page from Wikipedia and its title names a country, that country's main list is chosen if it has a status for any of the page's taxa.";

    /// Above the table of missing taxa with a status in the chosen list: "EPBC Act: 12 of the 40 taxa
    /// missing from the page have a status in this list."
    public static string MissingOtherList(int listed, int missing, string heading) => (listed, missing) switch {
        (0, 1) => $"{heading}: the one taxon missing from the page has no status in this list.",
        (_, 1) => $"{heading}: the one taxon missing from the page has a status in this list.",
        (0, _) => $"{heading}: none of the {SiteFormat.Number(missing)} taxa missing from the page has a status in this list.",
        (1, _) => $"{heading}: 1 of the {SiteFormat.Number(missing)} taxa missing from the page has a status in this list.",
        _ => $"{heading}: {SiteFormat.Number(listed)} of the {SiteFormat.Number(missing)} taxa missing from the page have a status in this list.",
    };
}
