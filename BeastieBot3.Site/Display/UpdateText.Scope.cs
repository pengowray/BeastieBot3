using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's comparison of the taxa in the wikitext with their group (Update/ListScope.cs).

public static partial class UpdateText {
    public static string ScopeHeading(GroupRow group) => $"Comparison with {GroupList.HeadingText(group)}";

    /// "The wikitext includes 39 of the 39 species that IUCN has in Family Felidae." With categories:
    /// "... 12 of the 15 EN species ...".
    public static string ScopeSummary(ListScopeResult r) =>
        $"The wikitext includes {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species that IUCN has in {GroupList.HeadingText(r.Scope)}.";

    public static string ScopeCategories(ListScopeResult r, IReadOnlySet<string> categories) => r.CategoriesChosen
        ? $"Only {CategoryList(categories, "and")} taxa are compared, as chosen in Categories below."
        : r.CategoriesFromCodes
        ? $"Only {CategoryList(categories, "and")} taxa are compared, because every status code in the wikitext is {CategoryList(categories, "or")}."
        : $"Only {CategoryList(categories, "and")} taxa are compared: the wikitext has no status codes, and {Count(r.Species)} of its {Count(r.SpeciesListed)} species are {CategoryList(categories, "or")} in their latest assessments.";

    public static string ScopeInfra(ListScopeResult r) {
        var group = GroupList.HeadingText(r.Scope);
        return r.InfraChecked
            ? $"Subspecies and varieties are compared too: the wikitext includes {Count(r.Infra)} of the {Count(r.InfraInScope)} {CategoryAdjective(r.Categories)}subspecies and varieties in {group}."
            : $"Subspecies and varieties are not compared or listed as missing: the wikitext includes only {Count(r.Infra)} of the {Count(r.InfraInScope)} in {group}.";
    }

    public const string ScopeChooseLabel = "Compare with";
    public const string ScopeChooseButton = "Compare";
    public const string CategoriesChooseLabel = "Categories";
    /// The category choice that leaves the categories to the codes in the wikitext.
    public const string CategoriesFromCodesOption = "From the status codes in the wikitext";

    public static string ScopePartial(ListScopeResult r) =>
        $"Missing taxa are not listed: the wikitext includes only {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)}, so it may be a regional list or a list of part of the group.";
    public const string ScopeListAnywayButton = "List the missing taxa anyway";

    public static string MissingHeading(int n) => n == 1 ? "1 taxon missing from the wikitext" : $"{Count(n)} taxa missing from the wikitext";
    public const string MissingNone = "Every taxon compared is in the wikitext.";
    public static string MissingCapped(int shown, int total) => $"{Count(shown)} of the {Count(total)} missing taxa are shown.";
    public const string MissingLinesLabel = "List lines for the missing taxa (read only)";
    public const string CopyMissingAccessible = "Copy the list lines for the missing taxa";
    public const string MissingLinesHelp = "One line for each missing taxon, with {{IUCN status}} filled in. Paste each line into the right section of the list.";

    // Putting the missing species into the wikitext (ListPlacement).
    public const string AddMissingOption = "Add the missing species to the updated wikitext";
    public const string AddMissingButton = "Add the missing species";
    public const string AddMissingHelp = "Each species goes next to a species of the same genus, in the same form: a list line, a {{Species table/row}} or a table row. A genus with no {{Species table}} gets a new one next to the other genera of its family. A missing subspecies or variety goes on a line under its species. Not added: taxa of a genus that the wikitext has no taxon of, and taxa missing from tables with rowspan or colspan.";
    public const string AddMissingPartial = "Missing species were not added, because this may be a regional list or a list of part of the group.";

    public static string AddMissingResult(int added, int missing) => (added, missing) switch {
        (1, 1) => "Added the missing taxon to the updated wikitext.",
        (0, 1) => "The missing taxon was not added to the updated wikitext.",
        (0, _) => $"None of the {Count(missing)} missing taxa were added to the updated wikitext.",
        _ when added == missing => $"Added all {Count(missing)} missing taxa to the updated wikitext.",
        _ => $"Added {Count(added)} of the {Count(missing)} missing taxa to the updated wikitext.",
    };

    public const string ColumnPlacedNextTo = "Added";
    public static string PlacedNextTo(bool before) => before ? "Before" : "After";
    public static string UnplacedLinesLabel(int n) =>
        n == 1 ? "List line for the 1 missing taxon that was not added (read only)" : $"List lines for the {Count(n)} missing taxa that were not added (read only)";

    public static string ScopeOption(GroupRow group, int listed) =>
        $"{GroupList.HeadingText(group)} ({Count(listed)} of {Count(group.SpeciesCount)} species in the wikitext)";

    // Species of the Catalogue of Life that are not in the IUCN Red List (ListScopeResult.MissingExtra).
    public const string ExtraSpeciesOption = "Also compare with Catalogue of Life species not in the IUCN Red List";
    public const string ExtraSpeciesHelp = "Most are species that IUCN has not assessed. Some are fossils, hybrids, or names that IUCN treats as synonyms. Species the wikitext already names are not listed.";
    public const string ExtraSpeciesButton = "Add Catalogue of Life species to the comparison";
    public static string ExtraHeading(int n) => n == 1
        ? "1 Catalogue of Life species not in the IUCN Red List is missing from the wikitext"
        : $"{Count(n)} Catalogue of Life species not in the IUCN Red List are missing from the wikitext";
    public static string ExtraNone(GroupRow group) =>
        $"The wikitext includes every Catalogue of Life species in {GroupList.HeadingText(group)} that is not in the IUCN Red List.";
    public static string ExtraNoneExist(GroupRow group) =>
        $"The Catalogue of Life has no species in {GroupList.HeadingText(group)} that are not in the IUCN Red List.";
    public static string ExtraLinesLabel(int n) => $"List lines for the {Count(n)} missing Catalogue of Life species (read only)";
    public const string CopyExtraAccessible = "Copy the list lines for the missing Catalogue of Life species";
    public const string ExtraLinesHelp = "These lines have no {{IUCN status}}. Check each species before you add it: some may be fossils, hybrids, or names that IUCN treats as synonyms.";
    public static string AddExtraResult(int added, int missing) =>
        $"Added {Count(added)} of the {Count(missing)} missing Catalogue of Life species to the updated wikitext.";

    public static string OtherCategoryHeading(int n, IReadOnlySet<string> categories) =>
        $"{Taxa(n)} now in a category other than {CategoryList(categories, "or")}";
    public static string OutsideHeading(int n, GroupRow group) =>
        $"{Taxa(n)} that IUCN places outside {GroupList.HeadingText(group)}";
    public static string DuplicatesHeading(int n) =>
        n == 1 ? "1 taxon that appears under two or more names" : $"{Count(n)} taxa that appear under two or more names";

    public const string ColumnStatusInWikitext = "Status in the wikitext";
    public const string ColumnLatestStatus = "Latest status";
    public const string ColumnLines = "Lines";
    public const string ColumnNames = "Names in the wikitext";

    public static string LineNumbers(IReadOnlyList<int> lines) => string.Join(", ", lines.Select(Count));

    private static string Taxa(int n) => n == 1 ? "1 taxon" : $"{Count(n)} taxa";

    // "EN ", "threatened (CR, EN or VU) ", "extinct (EX or EW) ", or nothing for all categories.
    private static string CategoryAdjective(IReadOnlySet<string>? categories) => categories switch {
        null => "",
        { Count: 1 } => categories.First() + " ",
        _ when categories.SetEquals(["CR", "EN", "VU"]) => "threatened (CR, EN or VU) ",
        _ when categories.SetEquals(["EX", "EW"]) => "extinct (EX or EW) ",
        _ => CategoryList(categories, "or") + " ",
    };

    // "EN"; "CR, EN and VU" or "CR, EN or VU", in the Red List's order.
    private static string CategoryList(IReadOnlySet<string> categories, string conjunction) {
        string[] order = ["EX", "CR(PE)", "CR(PEW)", "EW", "CR", "EN", "VU", "NT", "LC", "DD"];
        var ordered = categories.OrderBy(c => Array.IndexOf(order, c)).ToList();
        return ordered.Count == 1 ? ordered[0] : $"{string.Join(", ", ordered.Take(ordered.Count - 1))} {conjunction} {ordered[^1]}";
    }
}
