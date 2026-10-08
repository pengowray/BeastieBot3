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
        if (forms.ListLines || forms.SpeciesTables || forms.TableRows) {
            parts.Add(AddMissingOrder);
        }
        return string.Join(" ", parts);
    }

    public const string AddMissingLines = "Each missing species goes on a new list line next to a species of its genus, in the same format as that species' line.";
    public const string AddMissingHeadings = "A missing species with no other species of its genus listed goes on a new line under the heading of its order, family or other group. If the wikitext has no heading for that order or family, one is added beside the other order or family headings, in the same format.";
    public const string AddMissingNoHeadings = "In a list without headings, a missing species with no other species of its genus listed goes among the list lines in alphabetical order. If the list is not in alphabetical order, that species goes after the last line.";
    public const string AddMissingSpeciesTables = "A missing species gets a new {{Species table/row}} in the {{Species table}} of its genus. A genus without a {{Species table}} gets a new one next to the tables of the other genera in its family.";
    public const string AddMissingTableRows = "In ordinary wikitables, a missing species gets a new row next to a species of its genus, except in tables that use rowspan or colspan.";
    public const string AddMissingInfra = "A missing subspecies or variety goes on a new line under its species.";
    public const string AddMissingOrder = "New lines and rows go in alphabetical order among their neighbours, by scientific name or by common name, whichever the list is sorted by.";

    /// The headings put in for missing taxa.
    public static string NewHeadings(IReadOnlyList<string> headings) =>
        headings.Count == 1 ? $"Added 1 heading: {headings[0]}." : $"Added {Count(headings.Count)} headings: {string.Join(", ", headings)}.";

    public const string ColumnHeading = "Under heading";
    public static string NewHeadingValue(string heading) => $"{heading} (new heading)";

    public const string ColumnWhyNotAdded = "Why not added";
    public static string UnplacedReasonText(UnplacedReason reason) => reason switch {
        UnplacedReason.TableLayout => "The species of its genus are in a wikitable that uses rowspan or colspan.",
        UnplacedReason.SpeciesNotOnLine => "Its species is not on a list line.",
        UnplacedReason.RowLayout => "The {{Species table/row}} rows of its genus are missing a name, binomial or iucn-status parameter.",
        UnplacedReason.RemovedLine => "It would have gone next to a line that was removed from the updated wikitext.",
        _ => "No species of its genus is listed, and no heading names its order, family or other group.",
    };

    // Rebuilding the list (ListRebuild). Provisional wording.
    public const string RebuildHeading = "Rebuild the list";
    public const string RebuildButton = "Rebuild the list";
    public const string RebuildOption = "Rebuild the list in the updated wikitext";
    public const string RebuildHelp = "Puts every taxon of the comparison under the heading of its group, adds the missing taxa and removes the taxa now in another category. Headings, text, images and the wording of existing lines are kept.";
    public static string RebuildRefused(RebuildRefusal refusal, int lines) => refusal switch {
        RebuildRefusal.NotLineList => "The list was not rebuilt: this wikitext lists its taxa in tables, not on list lines.",
        RebuildRefusal.Partial => "The list was not rebuilt: the wikitext may be a list of part of the group. Compare with the whole group first.",
        RebuildRefusal.TooLong => $"The list was not rebuilt: it would have {Count(lines)} lines, more than the {Count(GroupList.MaxLines)} one Wikipedia page can hold.",
        _ => string.Empty,
    };
    public static string RebuildSummary(RebuildResult r) {
        var added = r.Taxa.Count(t => t.Change == RebuildChange.Added);
        var moved = r.Taxa.Count(t => t.Change == RebuildChange.Moved);
        return $"The rebuilt list has {Count(r.Taxa.Count)} taxa: {Count(added)} added, {Count(moved)} moved to another heading, {Count(r.Removed.Count)} removed."
            + (r.DuplicateLines.Count > 0 ? $" {Count(r.DuplicateLines.Count)} lines of taxa listed twice were removed." : string.Empty)
            + (r.OtherLines > 0 ? $" {Count(r.OtherLines)} lines that name no taxon of the list were kept where they were." : string.Empty);
    }
    public static string RebuildDropped(IReadOnlyList<string> headings) =>
        (headings.Count == 1 ? "Removed 1 heading with no taxa left under it: " : $"Removed {Count(headings.Count)} headings with no taxa left under them: ") + string.Join(", ", headings) + ".";
    public static string RebuildDroppedText(string heading) => $"Text that was under {heading}:";
    public const string ColumnRebuildChange = "Change";
    public const string RebuildOptionsLegend = "How to rebuild";
    public const string RebuildHeadingsLabel = "Headings:";
    public const string RebuildHeadingsAsText = "As in the wikitext";
    public const string RebuildHeadingsByRank = "One for each group of these ranks:";
    public const string RebuildOrderLabel = "Order of headings";
    public const string RebuildOrderText = "As in the wikitext";
    public const string RebuildOrderIucn = "IUCN order (alphabetical)";
    public const string RebuildWordingLabel = "Lines already in the list";
    public const string RebuildWordingKeep = "Keep their wording";
    public const string RebuildWordingNew = "Write them anew";
    public const string RebuildStyleLabel = "Style of new lines";
    public const string RebuildStyleText = "As the lines in the wikitext";
    public const string RebuildStyleSci = "Scientific name first";
    public const string RebuildStyleCommon = "Common name first";
    public const string RebuildStyleCommonOnly = "Common name only";
    public const string RebuildSortLabel = "Order of lines";
    public const string RebuildSortText = "As in the wikitext";
    public const string RebuildSortSci = "By scientific name";
    public const string RebuildSortCommon = "By common name";
    public const string RebuildInfraLabel = "Subspecies and varieties";
    public const string RebuildInfraText = "As in the comparison";
    public const string RebuildInfraNone = "Leave out";
    public const string RebuildInfraSeparate = "After the species";
    public const string RebuildInfraUnder = "Under their species";
    public const string RebuildApply = "Rebuild";
    public static string RebuildChangeText(RebuiltTaxon t) => t.Change switch {
        RebuildChange.Added => "Added",
        RebuildChange.Moved => t.OldHeading is { } old ? $"Moved from {old}" : "Moved",
        _ => "",
    };

    // Taking out the taxa now in another category (ListPlacement.Removal.cs).
    public const string RemoveButton = "Remove these taxa";
    public const string RemoveHelp = "Removes the list lines and {{Species table/row}} rows of these taxa, and any heading left with no taxa under it. After removal, untick a taxon in the table to keep it.";
    public static string RemoveResult(int removed, int total) => (removed, total) switch {
        (0, 1) => "Removed none of the taxa from the updated wikitext.",
        (0, _) => $"Removed none of the {Count(total)} taxa from the updated wikitext.",
        (1, 1) => "Removed the taxon from the updated wikitext.",
        _ when removed == total => $"Removed all {Count(total)} taxa from the updated wikitext.",
        _ => $"Removed {Count(removed)} of the {Count(total)} taxa from the updated wikitext.",
    };
    public const string RemoveInstruction = "To keep a taxon, untick it, then click Update statuses.";
    public const string ColumnRemove = "Remove";
    public static string RemoveCheckboxAccessible(string name) => $"Remove {name}";
    public static string KeptReasonText(KeptReason reason) => reason switch {
        KeptReason.SharesLine => "Kept: its list line names another taxon that stays, or has lines indented under it that stay.",
        KeptReason.DefinesReference => "Kept: its line defines a named reference (<ref name=\"...\">) that other lines use.",
        _ => "Kept: named only in a wikitable, a taxobox or prose.",
    };
    public static string RemovedHeadings(IReadOnlyList<string> headings) => headings.Count == 1
        ? $"Removed 1 heading with no taxa left under it: {headings[0]}."
        : $"Removed {Count(headings.Count)} headings with no taxa left under them: {string.Join(", ", headings)}.";

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
