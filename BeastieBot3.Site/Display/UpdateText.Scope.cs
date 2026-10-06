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

    public static string ScopeCategories(ListScopeResult r, IReadOnlySet<string> categories) => r.CategoriesFromCodes
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

    public static string ScopePartial(ListScopeResult r) =>
        $"Missing taxa are not listed: the wikitext includes only {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)}, so it may be a regional list or a list of part of the group.";
    public const string ScopeListAnywayButton = "List the missing taxa anyway";

    public static string MissingHeading(int n) => n == 1 ? "1 taxon missing from the wikitext" : $"{Count(n)} taxa missing from the wikitext";
    public const string MissingNone = "Every taxon compared is in the wikitext.";
    public static string MissingCapped(int shown, int total) => $"{Count(shown)} of the {Count(total)} missing taxa are shown.";
    public const string MissingLinesLabel = "List lines for the missing taxa (read only)";
    public const string CopyMissingAccessible = "Copy the list lines for the missing taxa";
    public const string MissingLinesHelp = "One line for each missing taxon, with {{IUCN status}} filled in. Paste each line into the right section of the list.";

    // Putting the missing taxa into the wikitext (ListPlacement). PROVISIONAL wording.
    public const string AddMissingOption = "Add the missing species to the wikitext, each next to a listed species of the same genus";
    public const string AddMissingButton = "Add the missing species";
    public const string AddMissingHelp = "Only list lines are changed, not tables. Missing subspecies and varieties, and species of a genus the list does not have, stay in the box below.";
    public const string AddMissingPartial = "The missing taxa are not added to a list that may be regional.";
    public static string AddMissingResult(int added, int missing) =>
        $"Added {Count(added)} of the {Count(missing)} missing taxa to the wikitext.";
    public const string ColumnPlacedNextTo = "Added";
    public static string PlacedNextTo(bool before) => before ? "before" : "after";
    public const string UnplacedLinesLabel = "List lines for the missing taxa that were not added (read only)";

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
        string[] order = ["EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD"];
        var ordered = categories.OrderBy(c => Array.IndexOf(order, c)).ToList();
        return ordered.Count == 1 ? ordered[0] : $"{string.Join(", ", ordered.Take(ordered.Count - 1))} {conjunction} {ordered[^1]}";
    }
}
