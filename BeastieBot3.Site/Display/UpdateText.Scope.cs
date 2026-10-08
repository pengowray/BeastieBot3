using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Display;

// The status update page's comparison of the taxa in the wikitext with their group (Update/ListScope.cs).

public static partial class UpdateText {
    public static string ScopeHeading(GroupRow group) => $"Comparison with {GroupList.HeadingText(group)}";

    /// "The wikitext includes 39 of the 39 species that IUCN has in Family Felidae." With categories:
    /// "... 12 of the 15 EN species ...".
    /// With an area: "... of the 1,230 species in Class Aves that IUCN records as native to Brazil."
    public static string ScopeSummary(ListScopeResult r, string? areaName = null) => r.Region is { } region
        ? $"The wikitext includes {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)} that IUCN has assessed for {region}."
        : areaName is null
        ? $"The wikitext includes {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species that IUCN has in {GroupList.HeadingText(r.Scope)}."
        : $"The wikitext includes {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)} that IUCN records {AreaPhrase(r.AreaMode, areaName)}.";

    /// fromTitle: the categories were set from the title of the page loaded from Wikipedia.
    public static string ScopeCategories(ListScopeResult r, IReadOnlySet<string> categories, bool fromTitle = false) => r.CategoriesChosen
        ? fromTitle
            ? $"Only {CategoryList(categories, "and")} taxa are compared, because of the page's title. Change this in \"{CategoriesChooseLabel}\"."
            : $"Only {CategoryList(categories, "and")} taxa are compared, as chosen in \"{CategoriesChooseLabel}\"."
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
    public const string CategoriesChooseLabel = "Red List categories";
    /// The category choice that leaves the categories to the codes in the wikitext.
    public const string CategoriesFromCodesOption = "From the status codes in the wikitext";

    public static string ScopePartial(ListScopeResult r, string? areaName = null) => areaName is null
        ? $"Missing taxa are not listed: the wikitext includes only {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)}, so it may be a regional list or a list of part of the group."
        : $"Missing taxa are not listed: the wikitext includes only {Count(r.Species)} of the {Count(r.SpeciesInScope)} {CategoryAdjective(r.Categories)}species in {GroupList.HeadingText(r.Scope)} that IUCN records {AreaPhrase(r.AreaMode, areaName)}, so it may be a list of part of the group.";
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
    /// How the missing taxa go in, for the forms this text lists its taxa in.
    public static string AddMissingHelp(ListForms forms) {
        var parts = new List<string>();
        if (forms.ListLines) {
            parts.Add(AddMissingLines);
            parts.Add(forms.Headings ? AddMissingHeadings : AddMissingNoHeadings);
        }
        if (forms.SpeciesTables) {
            parts.Add(AddMissingSpeciesTables);
        }
        if (forms.TableRows) {
            parts.Add(AddMissingTableRows);
        }
        if (forms.InfraOnLines) {
            parts.Add(AddMissingInfra);
        }
        return string.Join(" ", parts);
    }

    public const string AddMissingLines = "A missing species goes on a new list line next to a species of its genus, in the same form as that line.";
    public const string AddMissingHeadings = "If no species of its genus is listed, it goes among the lines under the heading of its order, family or other group. An order or family with no heading gets a new heading beside the others of its rank.";
    public const string AddMissingNoHeadings = "If no species of its genus is listed, it goes among the list lines in alphabetical order, or after the last line when the list is not in alphabetical order.";
    public const string AddMissingSpeciesTables = "In a {{Species table}}, it gets a new {{Species table/row}} in the table of its genus. A genus with no table gets a new {{Species table}} next to the tables of other genera of its family.";
    public const string AddMissingTableRows = "In a wikitable, it gets a new row next to a species of its genus. Tables with rowspan or colspan get no new rows.";
    public const string AddMissingInfra = "A missing subspecies or variety goes on a line under its species.";

    /// The headings put in for missing taxa.
    public static string NewHeadings(IReadOnlyList<string> headings) =>
        headings.Count == 1 ? $"Added 1 heading: {headings[0]}." : $"Added {Count(headings.Count)} headings: {string.Join(", ", headings)}.";

    public const string ColumnHeading = "Heading";
    public static string NewHeadingValue(string heading) => $"{heading} (new)";

    public const string ColumnWhyNotAdded = "Why not added";
    public static string UnplacedReasonText(UnplacedReason reason) => reason switch {
        UnplacedReason.TableLayout => "The species of its genus are in a table with rowspan or colspan.",
        UnplacedReason.SpeciesNotOnLine => "Its species is not on a list line.",
        UnplacedReason.RowLayout => "The {{Species table/row}} rows of its genus have no name, binomial or iucn-status parameter.",
        UnplacedReason.RemovedLine => "Its place was on a line removed from the wikitext.",
        _ => "The wikitext has no species of its genus and no heading of its group.",
    };

    // Taking out the taxa now in another category (ListPlacement.Removal.cs).
    public const string RemoveButton = "Remove them from the wikitext";
    public const string RemoveHelp = "Removes their list lines, with any lines under them, and their {{Species table/row}} rows. A heading left with no taxa is removed too. Each taxon then has a checkbox to keep it.";
    public static string RemoveResult(int removed, int total) => (removed, total) switch {
        (0, _) => "None of these taxa were removed from the updated wikitext.",
        _ when removed == total => total == 1 ? "Removed this taxon from the updated wikitext." : $"Removed all {Count(total)} taxa from the updated wikitext.",
        _ => $"Removed {Count(removed)} of the {Count(total)} taxa from the updated wikitext.",
    };
    public const string RemoveInstruction = "Untick a taxon to keep it in the wikitext, then select Update statuses.";
    public const string ColumnRemove = "Remove";
    public static string RemoveCheckboxAccessible(string name) => $"Remove {name}";
    public static string KeptReasonText(KeptReason reason) => reason switch {
        KeptReason.SharesLine => "Kept: its line names a taxon that stays, or has lines under it that do.",
        KeptReason.DefinesReference => "Kept: its line defines a reference that other lines use.",
        _ => "Kept: it is named in a table, a taxobox or running text, not on a list line or in a {{Species table/row}}.",
    };
    public static string RemovedHeadings(IReadOnlyList<string> headings) =>
        (headings.Count == 1 ? "Removed 1 heading with no taxa left: " : $"Removed {Count(headings.Count)} headings with no taxa left: ") + string.Join(", ", headings) + ".";
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
