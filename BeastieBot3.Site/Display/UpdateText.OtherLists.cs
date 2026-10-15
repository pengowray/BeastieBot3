using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's counts by category of the updated wikitext, and its choice of another list
// whose statuses are shown beside IUCN's (OtherStatusChoices).

public static partial class UpdateText {
    // The table of the wikitext's taxa by category (StatusCountTable), under the result's summary.
    public const string CountsCaption = "Taxa by the category of their latest IUCN assessment";
    public const string CountsWikitextHeading = "In the wikitext";
    public const string CountsTotalLabel = "Total";

    /// The heading of the compared group's columns: "IUCN Red List: Subclass Theria, native or
    /// reintroduced in the United Kingdom"; "IUCN Red List (Europe): Subclass Theria".
    public static string CountsIucnHeading(ListScopeResult r, AreaNames areas) {
        var group = GroupList.HeadingText(r.Scope);
        return r.Region is { } region ? $"IUCN Red List ({region}): {group}"
            : r.Area is { } code ? $"IUCN Red List: {group}, {WithoutAs(AreaPhrase(r.AreaMode, areas.ByCode(code)?.SentenceName ?? code))}"
            : $"IUCN Red List: {group}";
    }

    // "native or reintroduced in Brazil" for "as native or reintroduced in Brazil".
    private static string WithoutAs(string phrase) => phrase.StartsWith("as ", StringComparison.Ordinal) ? phrase[3..] : phrase;

    /// The help line under the table with a comparison: which taxa the wikitext columns include, where
    /// the others are, and how each is counted.
    public static string CountsTableHelpCompared(ListScopeResult r, AreaNames areas) {
        var group = GroupList.HeadingText(r.Scope);
        var taxa = r.Region is { } region ? $"the taxa in {group} that IUCN has assessed for {region}"
            : r.Area is { } code ? $"the taxa in {group} that IUCN records {AreaPhrase(r.AreaMode, areas.ByCode(code)?.SentenceName ?? code)}"
            : $"the taxa in {group}";
        var latest = r.Region is { } inRegion ? $"its latest assessment for {inRegion}" : "its latest IUCN assessment";
        var text = $"The wikitext columns include only {taxa}. The other taxa in the wikitext are listed under “{ScopeHeading(r.Scope)}”. "
            + $"Each taxon is counted once, by the category of {latest}, and subpopulations are left out.";
        return r.Categories is null ? text
            : text + " The IUCN Red List columns include every category, even when the comparison covers only some categories.";
    }

    public const string CountsTableHelp =
        "Each taxon found in the wikitext is counted once, by the category of its latest IUCN assessment. Subpopulations are left out.";

    /// The help line under the {{IUCN statuses}} box: what was counted, the codes not counted, and how
    /// the LR codes count.
    public static string CountsHelp(IucnStatusesSummary.Counts counts) {
        var item = counts.FromRows ? "row" : "template";
        var text = counts.FromRows
            ? "Counted from the iucn-status of each {{Species table/row}}, once per row: a species with two rows is counted twice, and a row with no iucn-status is counted as NE. Rows of taxa left out of the table above, such as subpopulations, are counted too."
            : "Counted from the code of each {{IUCN status}} template, once per template: a species with two templates is counted twice, and a taxon with no template is not counted. Templates of taxa left out of the table above, such as subpopulations, are counted too.";
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
        "Adds a column with each taxon's status in the chosen list to the report and to the tables of the “Comparison with” section, including the table of taxa missing from the page. The wikitext is not changed. The number after each list is how many IUCN Red List taxa have a status in that list. When you load a page from Wikipedia and its title names a country, a list of that country is chosen if it has a status for any of the page's taxa.";

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
